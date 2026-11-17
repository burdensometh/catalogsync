from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re

from .errors import Conflict, ValidationError


# Money is stored as an integer count of the currency's minor unit.
# CNY/USD/EUR/GBP keep two decimals, JPY is counted in whole yen.
CURRENCY_DIGITS = {"CNY": 2, "USD": 2, "EUR": 2, "GBP": 2, "JPY": 0}


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


def money(value, currency="CNY"):
    """Parse a major-unit decimal amount into currency minor units.

    Validation always follows the current convention: the amount must be
    exactly expressible in the currency's minor unit (two decimals for
    CNY/USD/EUR/GBP, whole units for JPY). Never rounds.
    """
    if isinstance(value, bool):
        raise ValidationError("price must be a decimal amount")
    if currency not in CURRENCY_DIGITS:
        raise ValidationError("unsupported currency")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValidationError("price must be a decimal amount") from None
    if not result.is_finite() or result < 0:
        raise ValidationError("price must be finite and nonnegative")
    minor = result * (10 ** CURRENCY_DIGITS[currency])
    if minor != minor.to_integral_value():
        raise ValidationError(f"price is not expressible in {currency} minor units")
    if minor > 10**12:
        raise ValidationError("price exceeds supported range")
    return int(minor)


def to_storage_scale(minor, currency, scale):
    """Convert minor units into the database's storage scale.

    minor -> centi is always exact (multiply); the reverse is not.
    """
    if scale == "minor":
        return minor
    if scale == "centi":
        return minor * 10 ** (2 - CURRENCY_DIGITS[currency])
    raise ValidationError(f"unknown money storage scale: {scale}")


def to_minor_units(stored, currency, scale):
    """Convert a stored integer back to minor units, or None if the
    conversion would not be lossless (the caller must never round)."""
    if currency not in CURRENCY_DIGITS:
        return None
    if scale == "minor":
        return stored
    if scale == "centi":
        divisor = 10 ** (2 - CURRENCY_DIGITS[currency])
        quotient, remainder = divmod(stored, divisor)
        return quotient if remainder == 0 else None
    raise ValidationError(f"unknown money storage scale: {scale}")


def format_price(stored, currency, scale):
    """Render a stored integer as a major-unit decimal string using the
    decimals of the given storage scale (legacy centi data keeps its
    historical two-decimal rendering)."""
    digits = 2 if scale == "centi" else CURRENCY_DIGITS[currency]
    if digits == 0:
        return str(stored)
    whole, fraction = divmod(stored, 10 ** digits)
    return f"{whole}.{fraction:0{digits}d}"


def normalize_corrections(entries):
    """Validate operator-supplied migration corrections.

    Each entry names a record by (source_id, sku[, revision]) and gives the
    exact replacement price as a major-unit decimal; the price itself must
    be expressible in the record currency's minor units.
    """
    if not isinstance(entries, list):
        raise ValidationError("corrections must be an array")
    normalized = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValidationError("correction must be an object")
        if set(entry) - {"source_id", "sku", "revision", "price"}:
            raise ValidationError("unknown correction properties")
        if "price" not in entry:
            raise ValidationError("correction requires a price")
        revision = entry.get("revision")
        normalized.append({
            "source_id": identifier(entry.get("source_id"), "source_id"),
            "sku": identifier(entry.get("sku"), "sku"),
            "revision": positive_int(revision, "revision", 1) if revision is not None else None,
            "price": entry["price"],
        })
    return normalized


def correction_for(corrections, source_id, sku, revision):
    """Find the correction covering a stored record, if any."""
    found = [
        (index, entry) for index, entry in enumerate(corrections)
        if entry["source_id"] == source_id and entry["sku"] == sku
        and (entry["revision"] is None or entry["revision"] == revision)
    ]
    if len(found) > 1:
        raise Conflict(f"multiple corrections match {source_id}/{sku} revision {revision}")
    return found[0] if found else None


def plan_scale_changes(rows, corrections, matched=None, table="products"):
    """Plan the centi -> minor conversion for stored money rows.

    rows are dicts with source_id, sku, revision, currency, price_cents.
    Returns (changes, blockers, applied): changes are (row, minor) pairs that
    need an UPDATE; blockers identify records that cannot be converted
    losslessly and have no correction; applied counts corrected rows.
    """
    changes = []
    blockers = []
    applied = 0
    for row in rows:
        currency = row["currency"]
        stored = row["price_cents"]
        key = {"source_id": row["source_id"], "sku": row["sku"], "revision": row["revision"]}
        natural = to_minor_units(stored, currency, "centi")
        found = correction_for(corrections, key["source_id"], key["sku"], key["revision"])
        if found is not None:
            index, entry = found
            if natural is not None:
                raise ValidationError(
                    f"correction for {key['source_id']}/{key['sku']} revision {key['revision']} "
                    "targets a record that converts losslessly; remove the correction"
                )
            if matched is not None:
                matched.add(index)
            minor = money(entry["price"], currency)
            applied += 1
            changes.append((row, minor))
            continue
        if natural is None:
            blocker = {"table": table, **key, "price_cents": stored}
            if currency in CURRENCY_DIGITS:
                blocker["currency"] = currency
                blocker["amount"] = format_price(stored, currency, "centi")
                blocker["reason"] = f"not expressible in {currency} minor units"
            else:
                blocker["currency"] = currency
                blocker["reason"] = "unsupported currency"
            blockers.append(blocker)
            continue
        if natural != stored:
            changes.append((row, natural))
    return changes, blockers, applied


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
    def parse(cls, record, scale="minor"):
        if not isinstance(record, dict):
            raise ValidationError("product must be an object")
        currency = record.get("currency", "CNY")
        if currency not in CURRENCY_DIGITS:
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
            price_cents=to_storage_scale(money(record.get("price"), currency), currency, scale),
            stock=positive_int(record.get("stock", 0), "stock"),
            active=active,
            attributes=attributes,
        )

    def payload(self):
        return asdict(self)

    def fingerprint(self):
        return digest(self.payload())

    def public(self, scale="minor"):
        value = self.payload()
        value["price"] = format_price(self.price_cents, self.currency, scale)
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
