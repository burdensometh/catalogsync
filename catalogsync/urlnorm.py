from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit


MAX_URL_LENGTH = 2000
TRACKING_PREFIX = "utm_"


def origin(url):
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError:
        return None
    scheme = parts.scheme.lower()
    if scheme not in {"http", "https"} or not parts.hostname:
        return None
    if port is None:
        port = 443 if scheme == "https" else 80
    return (scheme, parts.hostname.lower(), port)


def canonicalize(url, base=None):
    """Canonical absolute URL for crawl dedup, or None when not fetchable.

    Fragments and utm_* tracking parameters are dropped so that links
    differing only in those parts collapse onto one crawl item. Business
    query parameters such as sku are preserved verbatim (sorted only for
    canonical ordering), so distinct parameter values stay distinct items.
    """
    if not isinstance(url, str):
        return None
    url = url.strip()
    if not url:
        return None
    if base is not None:
        url = urljoin(base, url)
    try:
        parts = urlsplit(url)
        port = parts.port
        pairs = parse_qsl(parts.query, keep_blank_values=True, max_num_fields=100)
    except ValueError:
        return None
    scheme = parts.scheme.lower()
    if scheme not in {"http", "https"}:
        return None
    host = parts.hostname
    if not host or parts.username or parts.password:
        return None
    host = host.lower()
    default_port = 443 if scheme == "https" else 80
    netloc = f"[{host}]" if ":" in host else host
    if port is not None and port != default_port:
        netloc = f"{netloc}:{port}"
    path = parts.path or "/"
    kept = sorted(
        (name, value) for name, value in pairs
        if not name.lower().startswith(TRACKING_PREFIX)
    )
    candidate = urlunsplit((scheme, netloc, path, urlencode(kept), ""))
    if len(candidate) > MAX_URL_LENGTH:
        return None
    return candidate
