import json

from .errors import NotFound, ValidationError
from .models import format_price


def product_row(row, scale="minor"):
    value = dict(row)
    value["attributes"] = json.loads(value["attributes"])
    value["active"] = bool(value["active"])
    value["sellable"] = value["active"] and value["stock"] > 0
    value["price"] = format_price(value["price_cents"], value["currency"], scale)
    return value


class CatalogQuery:
    def __init__(self, database):
        self.db = database

    def filters(self, source_id=None, category=None, sellable=False, search=None):
        clauses = []
        params = []
        if source_id:
            clauses.append("source_id = ?")
            params.append(source_id)
        if category:
            clauses.append("category = ?")
            params.append(category)
        if sellable:
            clauses.append("active = 1 AND stock > 0")
        if search:
            clauses.append("instr(lower(title), lower(?)) > 0")
            params.append(search)
        return " AND ".join(clauses) or "1 = 1", params

    def products(self, source_id=None, category=None, sellable=False, search=None, limit=100, offset=0):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValidationError("limit must be between 1 and 1000")
        if type(offset) is not int or offset < 0:
            raise ValidationError("offset must be nonnegative")
        where, params = self.filters(source_id, category, sellable, search)
        with self.db.read() as connection:
            scale = self.db.money_scale(connection)
            total = connection.execute(
                f"SELECT count(*) FROM products WHERE {where}", params
            ).fetchone()[0]
            rows = connection.execute(
                f"SELECT * FROM products WHERE {where} ORDER BY source_id, sku LIMIT ? OFFSET ?",
                [*params, limit, offset],
            )
            return {"total": total, "items": [product_row(row, scale) for row in rows]}

    def product(self, source_id, sku):
        with self.db.read() as connection:
            scale = self.db.money_scale(connection)
            row = connection.execute(
                "SELECT * FROM products WHERE source_id = ? AND sku = ?", (source_id, sku)
            ).fetchone()
            if row is None:
                raise NotFound("product does not exist")
            return product_row(row, scale)

    def history(self, source_id, sku):
        with self.db.read() as connection:
            rows = connection.execute(
                """SELECT * FROM product_versions WHERE source_id = ? AND sku = ?
                ORDER BY revision DESC""", (source_id, sku)
            )
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result

    def summary(self):
        with self.db.read() as connection:
            source_counts = connection.execute(
                """SELECT source_id, count(*) AS products,
                sum(CASE WHEN active = 1 AND stock > 0 THEN 1 ELSE 0 END) AS sellable,
                max(revision) AS maximum_revision
                FROM products GROUP BY source_id ORDER BY source_id"""
            )
            categories = connection.execute(
                """SELECT category, currency, count(*) AS products,
                sum(price_cents * stock) AS stock_value_cents
                FROM products GROUP BY category, currency ORDER BY category, currency"""
            ).fetchall()
            return {
                "sources": [dict(row) for row in source_counts],
                "categories": [dict(row) for row in categories],
                "runs": dict(connection.execute(
                    "SELECT status, count(*) FROM runs GROUP BY status"
                ).fetchall()),
            }

    def events(self, after=0, limit=100):
        if after < 0 or not 1 <= limit <= 1000:
            raise ValidationError("invalid event range")
        with self.db.read() as connection:
            rows = connection.execute(
                "SELECT * FROM audit_events WHERE sequence > ? ORDER BY sequence LIMIT ?",
                (after, limit),
            )
            events = []
            for row in rows:
                item = dict(row)
                item["detail"] = json.loads(item["detail"])
                events.append(item)
            return events
