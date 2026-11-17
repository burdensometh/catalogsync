from html.parser import HTMLParser
import json

from .clients import decode_page
from .errors import ValidationError


class ListingParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.metadata = {}
        self.records = []
        self.current = None
        self.current_depth = 0
        self.depth = 0
        self.field = None
        self.field_depth = None
        self.field_text = []
        self.next_cursor = None
        self.links = []
        self.scripts = []
        self.script = None
        self.errors = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        void = tag in {"meta", "link", "img", "br", "hr", "input", "source", "wbr", "area", "base", "embed", "param", "track", "col"}
        if not void:
            self.depth += 1
        if tag == "meta":
            key = attributes.get("name")
            if key and key.startswith("catalog:"):
                if key in self.metadata:
                    self.errors.append(f"duplicate metadata: {key}")
                self.metadata[key] = attributes.get("content", "")
        if tag == "link" and attributes.get("rel") == "next":
            cursor = attributes.get("data-cursor")
            if cursor:
                if self.next_cursor is not None:
                    self.errors.append("duplicate next cursor")
                self.next_cursor = cursor
        if tag == "a":
            href = attributes.get("href")
            if href is not None:
                self.links.append(href)
                if len(self.links) == 10001:
                    self.errors.append("page declares too many links")
        if tag == "article" and "data-sku" in attributes:
            if self.current is not None:
                self.errors.append("nested product articles are unsupported")
            self.current = {
                "sku": attributes["data-sku"],
                "revision": attributes.get("data-revision"),
            }
            self.current_depth = self.depth
            for key in ("category", "currency", "stock", "active"):
                if "data-" + key in attributes:
                    self.current[key] = attributes["data-" + key]
        if self.current is not None and "data-field" in attributes:
            if self.field is not None:
                self.errors.append("nested data-field elements are unsupported")
            self.field = attributes["data-field"]
            self.field_depth = self.depth
            self.field_text = []
        if tag == "script" and attributes.get("type") == "application/ld+json":
            self.script = []

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in {"meta", "link", "img", "br", "hr", "input", "source", "wbr", "area", "base", "embed", "param", "track", "col"}:
            self.handle_endtag(tag)

    def handle_data(self, data):
        if self.script is not None:
            self.script.append(data)
        if self.field is not None:
            self.field_text.append(data)

    def handle_endtag(self, tag):
        if self.field is not None and self.depth == self.field_depth:
            self.current[self.field] = "".join(self.field_text).strip()
            self.field = None
            self.field_depth = None
            self.field_text = []
        if tag == "article" and self.current is not None and self.depth == self.current_depth:
            self.records.append(self.current)
            self.current = None
        if tag == "script" and self.script is not None:
            self.scripts.append("".join(self.script))
            self.script = None
        self.depth = max(0, self.depth - 1)

    def envelope(self, cursor):
        if self.errors:
            raise ValidationError("; ".join(self.errors))
        if self.current is not None or self.script is not None:
            raise ValidationError("incomplete HTML listing")
        try:
            boundary = int(self.metadata["catalog:boundary"])
            snapshot_id = self.metadata["catalog:snapshot"]
        except (KeyError, ValueError):
            raise ValidationError("HTML listing requires snapshot and integer boundary metadata") from None
        response_cursor = self.metadata.get("catalog:cursor", "")
        if response_cursor != cursor:
            raise ValidationError("HTML response cursor does not match request")
        return {
            "snapshot_id": snapshot_id,
            "boundary": boundary,
            "cursor": response_cursor,
            "next_cursor": self.next_cursor,
            "records": [],
        }


def html_scalar(record):
    result = dict(record)
    for key in ("revision", "stock"):
        if key in result:
            value = result[key]
            try:
                result[key] = int(value)
            except (TypeError, ValueError):
                pass
    if "active" in result and isinstance(result["active"], str):
        normalized = result["active"].strip().lower()
        if normalized in {"true", "false"}:
            result["active"] = normalized == "true"
    return result


def jsonld_products(scripts):
    records = []
    queue = []
    for script in scripts:
        try:
            queue.append(json.loads(script))
        except json.JSONDecodeError:
            raise ValidationError("invalid JSON-LD script") from None
    while queue:
        node = queue.pop()
        if isinstance(node, list):
            queue.extend(reversed(node))
            continue
        if not isinstance(node, dict):
            continue
        if "@graph" in node:
            queue.append(node["@graph"])
        if node.get("@type") == "ItemList":
            for item in reversed(node.get("itemListElement", [])):
                queue.append(item.get("item", item) if isinstance(item, dict) else item)
        kinds = node.get("@type", [])
        kinds = [kinds] if isinstance(kinds, str) else kinds
        if "Product" not in kinds:
            continue
        offers = node.get("offers", {})
        if not isinstance(offers, dict):
            raise ValidationError("baseline supports one JSON-LD offer per product")
        availability = offers.get("availability", "")
        active = availability not in {"https://schema.org/Discontinued", "http://schema.org/Discontinued"}
        record = {
            "sku": node.get("sku"),
            "revision": node.get("catalogRevision"),
            "title": node.get("name"),
            "category": node.get("category", "uncategorized"),
            "price": offers.get("price"),
            "currency": offers.get("priceCurrency", "CNY"),
            "stock": offers.get("inventoryLevel", {}).get("value", 0) if isinstance(offers.get("inventoryLevel", {}), dict) else offers.get("inventoryLevel"),
            "active": active,
        }
        records.append(html_scalar(record))
    return records


def extract_page(raw, parser, cursor):
    if parser == "json":
        return decode_page(raw)
    try:
        html = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise ValidationError("HTML source must use UTF-8") from None
    listing = ListingParser()
    listing.feed(html)
    listing.close()
    envelope = listing.envelope(cursor)
    if parser == "html":
        envelope["records"] = [html_scalar(record) for record in listing.records]
    elif parser == "jsonld":
        envelope["records"] = jsonld_products(listing.scripts)
    else:
        raise ValidationError("unknown extraction parser")
    return envelope


def extract_crawl_page(raw, parser):
    """Extract discovered links and product records from one crawled page.

    Unlike extract_page there is no catalog envelope: the crawl round itself
    provides page identity, so ordinary site pages without catalog:*
    metadata can be parsed. Returns {"links": [...], "records": [...]}.
    """
    if parser == "json":
        value = decode_page(raw)
        records = value.get("records", [])
        if not isinstance(records, list):
            raise ValidationError("records must be an array")
        if len(records) > 10000:
            raise ValidationError("page exceeds 10000 records")
        links = value.get("links", [])
        if not isinstance(links, list) or len(links) > 10000:
            raise ValidationError("links must be an array of up to 10000 entries")
        if any(not isinstance(link, str) for link in links):
            raise ValidationError("links must be strings")
        return {"links": list(links), "records": records}
    try:
        html = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise ValidationError("HTML source must use UTF-8") from None
    listing = ListingParser()
    listing.feed(html)
    listing.close()
    if listing.errors:
        raise ValidationError("; ".join(listing.errors))
    if listing.current is not None or listing.script is not None:
        raise ValidationError("incomplete HTML listing")
    if parser == "html":
        records = [html_scalar(record) for record in listing.records]
    elif parser == "jsonld":
        records = jsonld_products(listing.scripts)
    else:
        raise ValidationError("unknown extraction parser")
    if len(records) > 10000:
        raise ValidationError("page exceeds 10000 records")
    return {"links": list(listing.links), "records": records}
