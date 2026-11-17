from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit
import errno
import fcntl
import hashlib
import json
import math
import os
import re
import tempfile
import threading
import time

from .errors import FetchError, ValidationError


@dataclass(frozen=True)
class RequestPolicy:
    requests_per_second: float = 10
    burst: int = 4
    concurrency: int = 2
    permit_timeout: float = 30
    maximum_retry_after: float = 30

    def __post_init__(self):
        for key in ("requests_per_second", "permit_timeout", "maximum_retry_after"):
            value = getattr(self, key)
            if type(value) not in {int, float} or not math.isfinite(value) or value <= 0:
                raise ValidationError(f"{key} must be finite and positive")
        for key in ("burst", "concurrency"):
            value = getattr(self, key)
            if type(value) is not int or not 1 <= value <= 64:
                raise ValidationError(f"{key} must be between 1 and 64")

    def retry_after(self, value):
        if not value:
            return 0
        try:
            seconds = float(value)
        except ValueError:
            try:
                target = parsedate_to_datetime(value)
                if target.tzinfo is None:
                    target = target.replace(tzinfo=timezone.utc)
                seconds = (target - datetime.now(timezone.utc)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                return 0
        if not math.isfinite(seconds):
            return 0
        return max(0, min(seconds, self.maximum_retry_after))

    def document(self):
        # Only limits that every executor must agree on are part of the shared
        # identity. permit_timeout governs local waiting, so it does not make
        # two processes conflict.
        return {
            "requests_per_second": self.requests_per_second,
            "burst": self.burst,
            "concurrency": self.concurrency,
            "maximum_retry_after": self.maximum_retry_after,
        }


class OriginGate:
    """In-process admission gate.

    Kept for callers that only need thread-level admission and for backwards
    compatibility. Processes that share a state directory should use
    ``gate_for(..., state_dir=...)`` instead.
    """

    def __init__(self, policy):
        self.policy = policy
        self.lock = threading.Lock()
        self.slots = threading.BoundedSemaphore(policy.concurrency)
        self.tokens = float(policy.burst)
        self.updated = time.monotonic()
        self.blocked_until = 0.0
        self.admitted = 0
        self.throttled = 0

    def delay(self, seconds):
        with self.lock:
            self.blocked_until = max(self.blocked_until, time.monotonic() + seconds)

    def take_token(self, deadline):
        waited = False
        while True:
            current = time.monotonic()
            with self.lock:
                self.tokens = min(
                    self.policy.burst,
                    self.tokens + max(0, current - self.updated) * self.policy.requests_per_second,
                )
                self.updated = current
                pause = max(0, self.blocked_until - current)
                if pause == 0 and self.tokens >= 1:
                    self.tokens -= 1
                    self.admitted += 1
                    if waited:
                        self.throttled += 1
                    return
                if pause == 0:
                    pause = (1 - self.tokens) / self.policy.requests_per_second
            remaining = deadline - current
            if remaining <= 0 or pause > remaining:
                raise FetchError("source request admission timed out")
            waited = True
            time.sleep(min(pause, 0.1))

    @contextmanager
    def permit(self):
        deadline = time.monotonic() + self.policy.permit_timeout
        if not self.slots.acquire(timeout=self.policy.permit_timeout):
            raise FetchError("source concurrency admission timed out")
        try:
            self.take_token(deadline)
            yield
        finally:
            self.slots.release()

    def metrics(self):
        with self.lock:
            return {
                "admitted": self.admitted,
                "throttled": self.throttled,
                "retry_pause_remaining": max(0, self.blocked_until - time.monotonic()),
                "rate": self.policy.requests_per_second,
                "concurrency": self.policy.concurrency,
            }


def _origin_key(origin):
    scheme, host, port = origin
    readable = re.sub(r"[^A-Za-z0-9_.-]", "_", f"{scheme}-{host}-{port}")[:80]
    digest = hashlib.sha256(f"{scheme}://{host}:{port}".encode("utf-8")).hexdigest()[:12]
    return f"{readable}-{digest}"


def _lock_blocking(descriptor, operation):
    fcntl.flock(descriptor, operation)


class SharedOriginGate:
    """Admission gate shared by every executor using one state directory.

    The token bucket, Retry-After deadline and policy fingerprint live in a
    small JSON file guarded by an advisory file lock. Concurrency slots are
    identical files that an executor holds with ``flock`` for the lifetime of
    a request: the kernel releases them when the process exits or crashes, and
    while a request is in flight the lock is still held, so nobody can mistake
    it for idle capacity.
    """

    def __init__(self, origin, policy, state_dir):
        self.origin = origin
        self.policy = policy
        self.root = Path(state_dir).resolve() / "request_admission" / _origin_key(origin)
        self.root.mkdir(parents=True, exist_ok=True)
        self.state_path = self.root / "state.json"
        self.lock_path = self.root / "gate.lock"
        self.slots = self.root / "slots"
        self.slots.mkdir(parents=True, exist_ok=True)
        self._ensure_state()

    @contextmanager
    def _locked(self):
        descriptor = os.open(
            self.lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        try:
            _lock_blocking(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            try:
                _lock_blocking(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    def _read_state(self):
        try:
            with self.state_path.open("r", encoding="utf-8") as stream:
                state = json.load(stream)
        except (OSError, json.JSONDecodeError) as error:
            raise ValidationError(f"request admission state is unreadable: {error}") from error
        if not isinstance(state, dict):
            raise ValidationError("request admission state is invalid")
        return state

    def _write_state(self, state, durable=False):
        descriptor, temporary = tempfile.mkstemp(prefix=".state-", dir=self.root)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(state, stream, sort_keys=True, separators=(",", ":"))
                stream.flush()
                # Token accounting is advisory and only needs atomic
                # replacement; a power loss may refund a few tokens. A
                # Retry-After deadline is an external commitment, so it is
                # flushed to disk.
                if durable:
                    os.fsync(stream.fileno())
            os.replace(temporary, self.state_path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def _ensure_state(self):
        document = self.policy.document()
        with self._locked():
            if self.state_path.exists():
                state = self._read_state()
                if state.get("policy") != document:
                    raise ValidationError(
                        "same origin cannot use conflicting request policies"
                    )
                return
            self._write_state({
                "origin": f"{self.origin[0]}://{self.origin[1]}:{self.origin[2]}",
                "policy": document,
                "tokens": float(self.policy.burst),
                "updated": time.time(),
                "blocked_until": 0.0,
                "admitted": 0,
                "throttled": 0,
            }, durable=True)

    def _slot_paths(self):
        return [self.slots / f"slot-{index}" for index in range(self.policy.concurrency)]

    def _try_acquire_slot(self):
        for path in self._slot_paths():
            descriptor = os.open(
                path,
                os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0),
                0o600,
            )
            try:
                _lock_blocking(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as error:
                os.close(descriptor)
                if error.errno in {errno.EACCES, errno.EAGAIN}:
                    continue
                raise
            return descriptor
        return None

    def _acquire_slot(self, deadline):
        while True:
            descriptor = self._try_acquire_slot()
            if descriptor is not None:
                return descriptor
            if time.monotonic() >= deadline:
                raise FetchError("source concurrency admission timed out")
            time.sleep(0.01)

    def _release_slot(self, descriptor):
        try:
            _lock_blocking(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def delay(self, seconds):
        if seconds <= 0:
            return
        with self._locked():
            state = self._verified_state()
            state["blocked_until"] = max(
                float(state.get("blocked_until", 0.0)), time.time() + seconds
            )
            self._write_state(state, durable=True)

    def _verified_state(self):
        state = self._read_state()
        if state.get("policy") != self.policy.document():
            raise ValidationError("same origin cannot use conflicting request policies")
        return state

    def take_token(self, deadline):
        waited = False
        while True:
            now = time.time()
            granted = False
            with self._locked():
                state = self._verified_state()
                updated = float(state.get("updated", now))
                tokens = min(
                    float(self.policy.burst),
                    float(state.get("tokens", self.policy.burst))
                    + max(0.0, now - updated) * self.policy.requests_per_second,
                )
                blocked_until = float(state.get("blocked_until", 0.0))
                if blocked_until <= now and tokens >= 1:
                    state["tokens"] = tokens - 1
                    state["updated"] = now
                    state["admitted"] = int(state.get("admitted", 0)) + 1
                    if waited:
                        state["throttled"] = int(state.get("throttled", 0)) + 1
                    self._write_state(state)
                    granted = True
                else:
                    pause = max(0.0, blocked_until - now, (1.0 - tokens) / self.policy.requests_per_second)
            if granted:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0 or pause > remaining:
                raise FetchError("source request admission timed out")
            waited = True
            time.sleep(min(max(pause, 0.001), 0.1))

    @contextmanager
    def permit(self):
        deadline = time.monotonic() + self.policy.permit_timeout
        descriptor = self._acquire_slot(deadline)
        try:
            self.take_token(deadline)
            yield
        finally:
            self._release_slot(descriptor)

    def metrics(self):
        with self._locked():
            state = self._verified_state()
            blocked_until = float(state.get("blocked_until", 0.0))
            admitted = int(state.get("admitted", 0))
            throttled = int(state.get("throttled", 0))
        return {
            "admitted": admitted,
            "throttled": throttled,
            "retry_pause_remaining": max(0.0, blocked_until - time.time()),
            "rate": self.policy.requests_per_second,
            "concurrency": self.policy.concurrency,
            "origin": f"{self.origin[0]}://{self.origin[1]}:{self.origin[2]}",
            "shared": True,
        }


_origins = {}
_shared_origins = {}
_origins_lock = threading.Lock()


def admission_status(state_dir):
    """Report the shared admission state for every recorded origin."""
    root = Path(state_dir).resolve() / "request_admission"
    report = []
    if not root.is_dir():
        return report
    try:
        directories = sorted(root.iterdir())
    except OSError:
        return [{"origin": str(root), "readable": False}]
    for directory in directories:
        state_path = directory / "state.json"
        if not state_path.is_file():
            continue
        try:
            with state_path.open("r", encoding="utf-8") as stream:
                state = json.load(stream)
        except (OSError, json.JSONDecodeError):
            report.append({"origin": directory.name, "key": directory.name, "readable": False})
            continue
        now = time.time()
        try:
            document = state.get("policy", {})
            updated = float(state.get("updated", now))
            stored = float(state.get("tokens", 0.0))
            burst = float(document.get("burst", stored))
            rate = float(document.get("requests_per_second", 0.0))
            tokens = min(burst, stored + max(0.0, now - updated) * rate)
            entry = {
                "origin": state.get("origin", directory.name),
                "key": directory.name,
                "readable": True,
                "policy": document,
                "tokens": round(tokens, 6),
                "retry_pause_remaining": max(0.0, float(state.get("blocked_until", 0.0)) - now),
                "admitted": int(state.get("admitted", 0)),
                "throttled": int(state.get("throttled", 0)),
            }
        except (TypeError, ValueError):
            entry = {"origin": directory.name, "key": directory.name, "readable": False}
        report.append(entry)
    return report


def _origin_of(url):
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValidationError("request policy requires an HTTP origin")
    return (
        parsed.scheme,
        parsed.hostname.lower(),
        parsed.port or (443 if parsed.scheme == "https" else 80),
    )


def gate_for(url, policy=None, state_dir=None):
    origin = _origin_of(url)
    policy = policy or RequestPolicy()
    if state_dir is None:
        with _origins_lock:
            if origin not in _origins:
                _origins[origin] = OriginGate(policy)
            elif _origins[origin].policy != policy:
                raise ValidationError("same origin cannot use conflicting request policies")
            return _origins[origin]
    root = str(Path(state_dir).resolve())
    key = (root, origin)
    with _origins_lock:
        existing = _shared_origins.get(key)
        if existing is not None:
            # permit_timeout is a local waiting preference, so only the
            # shared limits need to match.
            if existing.policy.document() != policy.document():
                raise ValidationError("same origin cannot use conflicting request policies")
            return existing
        # Constructing performs the on-disk policy comparison before any
        # request can be sent.
        gate = SharedOriginGate(origin, policy, root)
        _shared_origins[key] = gate
        return gate
