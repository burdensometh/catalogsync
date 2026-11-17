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
