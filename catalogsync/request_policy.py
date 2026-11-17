from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit
import math
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


class OriginGate:
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


_origins = {}
_origins_lock = threading.Lock()


def gate_for(url, policy=None):
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValidationError("request policy requires an HTTP origin")
    policy = policy or RequestPolicy()
    origin = (parsed.scheme, parsed.hostname.lower(), parsed.port or (443 if parsed.scheme == "https" else 80))
    with _origins_lock:
        if origin not in _origins:
            _origins[origin] = OriginGate(policy)
        elif _origins[origin].policy != policy:
            raise ValidationError("same origin cannot use conflicting request policies")
        return _origins[origin]
