from .errors import ValidationError
from .models import canonical, now


class ProductMerger:
    def merge(self, connection, source_id, run_id, product):
        fingerprint = product.fingerprint()
        version = connection.execute(
            """SELECT fingerprint FROM product_versions
            WHERE source_id = ? AND sku = ? AND revision = ?""",
            (source_id, product.sku, product.revision),
        ).fetchone()
        if version and version["fingerprint"] != fingerprint:
            raise ValidationError("same product revision has conflicting content")
        current = connection.execute(
            "SELECT revision, fingerprint FROM products WHERE source_id = ? AND sku = ?",
            (source_id, product.sku),
        ).fetchone()
        connection.execute(
            """INSERT OR IGNORE INTO product_versions
            (source_id, sku, revision, fingerprint, payload, first_run_id, observed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                source_id,
                product.sku,
                product.revision,
                fingerprint,
                canonical(product.payload()),
                run_id,
                now(),
            ),
        )
        if current:
            if current["revision"] > product.revision:
                return "stale"
            if current["revision"] == product.revision:
                return "unchanged"
        outcome = "updated" if current else "inserted"
        connection.execute(
            """INSERT INTO products
            (source_id, sku, revision, title, category, currency, price_cents,
             stock, active, attributes, fingerprint, last_run_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id, sku) DO UPDATE SET
                revision = excluded.revision,
                title = excluded.title,
                category = excluded.category,
                currency = excluded.currency,
                price_cents = excluded.price_cents,
                stock = excluded.stock,
                active = excluded.active,
                attributes = excluded.attributes,
                fingerprint = excluded.fingerprint,
                last_run_id = excluded.last_run_id,
                updated_at = excluded.updated_at""",
            (
                source_id,
                product.sku,
                product.revision,
                product.title,
                product.category,
                product.currency,
                product.price_cents,
                product.stock,
                int(product.active),
                canonical(product.attributes),
                fingerprint,
                run_id,
                now(),
            ),
        )
        return outcome
