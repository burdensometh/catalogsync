from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re

from .errors import ValidationError


def now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def canonical(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def identifier(value, label="identifier"):
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be a string")
    value = value.strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,79}", value):
        raise ValidationError(f"{label} contains unsupported characters")
    return value


def positive_int(value, label, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{label} must be an integer")
    if value < minimum:
        raise ValidationError(f"{label} must be >= {minimum}")
    return value


def text(value, label, maximum=500):
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} is required")
    value = value.strip()
    if len(value) > maximum:
        raise ValidationError(f"{label} exceeds {maximum} characters")
    return value


def money(value):
    if isinstance(value, bool):
        raise ValidationError("price must be a decimal amount")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValidationError("price must be a decimal amount") from None
    if not result.is_finite() or result < 0:
        raise ValidationError("price must be finite and nonnegative")
    cents = result * 100
    if cents != cents.to_integral_value():
        raise ValidationError("price cannot have fractions of a cent")
    if cents > 10**12:
        raise ValidationError("price exceeds supported range")
    return int(cents)


@dataclass(frozen=True)
class Product:
    sku: str
    revision: int
    title: str
    category: str
    currency: str
    price_cents: int
    stock: int
    active: bool
    attributes: dict

    @classmethod
    def parse(cls, record):
        if not isinstance(record, dict):
            raise ValidationError("product must be an object")
        currency = record.get("currency", "CNY")
        if currency not in {"CNY", "USD", "EUR", "GBP", "JPY"}:
            raise ValidationError("unsupported currency")
        active = record.get("active", True)
        if not isinstance(active, bool):
            raise ValidationError("active must be a boolean")
        attributes = record.get("attributes", {})
        if not isinstance(attributes, dict):
            raise ValidationError("attributes must be an object")
        if len(canonical(attributes).encode("utf-8")) > 16384:
            raise ValidationError("attributes exceed 16 KiB")
        return cls(
            sku=identifier(record.get("sku"), "sku"),
            revision=positive_int(record.get("revision"), "revision", 1),
            title=text(record.get("title"), "title"),
            category=text(record.get("category", "uncategorized"), "category", 100),
            currency=currency,
            price_cents=money(record.get("price")),
            stock=positive_int(record.get("stock", 0), "stock"),
            active=active,
            attributes=attributes,
        )

    def payload(self):
        return asdict(self)

    def fingerprint(self):
        return digest(self.payload())

    def public(self):
        value = self.payload()
        value["price"] = f"{self.price_cents // 100}.{self.price_cents % 100:02d}"
        value["sellable"] = self.active and self.stock > 0
        return value


@dataclass(frozen=True)
class Page:
    snapshot_id: str
    boundary: int
    cursor: str
    next_cursor: str | None
    records: list

    @classmethod
    def parse(cls, payload, requested_cursor):
        if not isinstance(payload, dict):
            raise ValidationError("page must be an object")
        records = payload.get("records")
        if not isinstance(records, list):
            raise ValidationError("records must be an array")
        if len(records) > 10000:
            raise ValidationError("page exceeds 10000 records")
        cursor = payload.get("cursor", "")
        if cursor != requested_cursor:
            raise ValidationError("response cursor does not match request")
        next_cursor = payload.get("next_cursor")
        if next_cursor is not None:
            next_cursor = text(next_cursor, "next_cursor", 500)
            if next_cursor == cursor:
                raise ValidationError("page cursor cannot point to itself")
        return cls(
            snapshot_id=identifier(payload.get("snapshot_id"), "snapshot_id"),
            boundary=positive_int(payload.get("boundary"), "boundary", 1),
            cursor=cursor,
            next_cursor=next_cursor,
            records=records,
        )
