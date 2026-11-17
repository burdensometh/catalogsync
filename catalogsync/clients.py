from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
import json
import time

from .errors import FetchError, ValidationError
from .urlnorm import canonicalize, origin


MAX_PAGE_BYTES = 8 * 1024 * 1024


def decode_page(raw):
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValidationError(f"invalid UTF-8 JSON page: {error}") from error
    if not isinstance(value, dict):
        raise ValidationError("page root must be an object")
    return value


class FileClient:
    def __init__(self, directory):
        self.root = Path(directory).resolve()
        manifest_path = self.root / "manifest.json"
        try:
            self.manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise FetchError(f"cannot read source manifest: {error}") from error
        self.pages = self.manifest.get("pages")
        if not isinstance(self.pages, dict) or "" not in self.pages:
            raise ValidationError("manifest pages must include the initial empty cursor")

    def fetch(self, cursor):
        if cursor not in self.pages:
            raise FetchError(f"cursor does not exist: {cursor!r}")
        relative = self.pages[cursor]
        if not isinstance(relative, str):
            raise ValidationError("manifest page path must be a string")
        path = (self.root / relative).resolve()
        if not path.is_relative_to(self.root):
            raise ValidationError("page path escapes source directory")
        try:
            with path.open("rb") as stream:
                raw = stream.read(MAX_PAGE_BYTES + 1)
        except OSError as error:
            raise FetchError(str(error)) from error
        if len(raw) > MAX_PAGE_BYTES:
            raise FetchError("page exceeds size limit")
        return raw


class SameOriginRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        old = urlsplit(request.full_url)
        new = urlsplit(newurl)
        if (old.scheme, old.netloc) != (new.scheme, new.netloc):
            raise FetchError("cross-origin redirect refused")
        return super().redirect_request(request, fp, code, msg, headers, newurl)


def http_get(opener, gate, url, accept, timeout, attempts):
    last_error = None
    for attempt in range(attempts):
        request = Request(
            url,
            headers={"Accept": accept, "User-Agent": "CatalogSync/0.1"},
        )
        try:
            with gate.permit(), opener.open(request, timeout=timeout) as response:
                raw = response.read(MAX_PAGE_BYTES + 1)
                if len(raw) > MAX_PAGE_BYTES:
                    raise FetchError("HTTP page exceeds size limit")
                return raw
        except HTTPError as error:
            last_error = f"HTTP {error.code}"
            gate.delay(gate.policy.retry_after(error.headers.get("Retry-After")))
            error.close()
            if error.code not in {408, 429, 500, 502, 503, 504}:
                raise FetchError(last_error) from error
        except (URLError, TimeoutError, OSError) as error:
            last_error = str(error)
        if attempt + 1 < attempts:
            time.sleep(0.05 * 2**attempt)
    raise FetchError(f"source fetch exhausted {attempts} attempts: {last_error}")


class HttpClient:
    def __init__(self, base_url, timeout=10, attempts=3):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.attempts = attempts
        self.opener = build_opener(SameOriginRedirect())
        from .request_policy import gate_for
        self.gate = gate_for(base_url)

    def fetch(self, cursor):
        url = self.base_url + "/catalog?" + urlencode({"cursor": cursor})
        return http_get(self.opener, self.gate, url, "application/json", self.timeout, self.attempts)


class CrawlClient:
    """Fetches absolute crawl URLs confined to the seed's origin."""

    def __init__(self, seed_url, timeout=10, attempts=3):
        seed = canonicalize(seed_url)
        if seed is None:
            raise ValidationError("crawl seed must be an HTTP or HTTPS URL")
        self.seed = seed
        self.origin = origin(seed)
        self.timeout = timeout
        self.attempts = attempts
        self.opener = build_opener(SameOriginRedirect())
        from .request_policy import gate_for
        self.gate = gate_for(seed)

    def fetch(self, url):
        if origin(url) != self.origin:
            raise FetchError("crawl URL escapes the seed origin")
        return http_get(
            self.opener, self.gate, url,
            "text/html, application/json", self.timeout, self.attempts,
        )


def client_for(source):
    if source["kind"] == "file":
        return FileClient(source["location"])
    return HttpClient(source["location"])
