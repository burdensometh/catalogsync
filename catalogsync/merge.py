from .errors import ValidationError
from .models import canonical, digest, now


class ProductMerger:
    """Merge one normalized product into the live catalogue.

    Version identity is (source, sku, source revision, cleaning rule version).
    A source revision may legitimately be re-interpreted under a new rule
    version; that is not a conflict.  A genuine source-side conflict - the same
    source revision carrying different raw source content - is still rejected
    even across rule versions, because that comparison falls back to the
    parser-level ``source_fingerprint`` derived from the raw record.
    """

    def source_fingerprint(self, parser, raw_record):
        return digest({"parser": parser, "record": raw_record})

    def version_conflict(self, connection, source_id, sku, revision, rule_version,
                         fingerprint, source_fingerprint, parser):
        """Return a conflict message, or None when the version agrees.

        Two independent invariants are enforced:
        * the same cleaning rule set must always produce the same payload for a
          given source revision;
        * the same source revision must always carry the same raw source
          content, no matter which rule set parsed it.
        """
        same_rule = connection.execute(
            """SELECT fingerprint FROM product_versions
            WHERE source_id = ? AND sku = ? AND revision = ? AND rule_version = ?""",
            (source_id, sku, revision, rule_version),
        ).fetchone()
        if same_rule and same_rule["fingerprint"] != fingerprint:
            return "same product revision has conflicting content"
        other = connection.execute(
            """SELECT source_fingerprint, parser FROM product_versions
            WHERE source_id = ? AND sku = ? AND revision = ? AND rule_version != ?""",
            (source_id, sku, revision, rule_version),
        ).fetchall()
        for row in other:
            if (
                row["source_fingerprint"]
                and source_fingerprint
                and row["parser"] == parser
                and row["source_fingerprint"] != source_fingerprint
            ):
                return "same source revision has conflicting source content"
        return None

    def merge(self, connection, source_id, run_id, product, rule_version,
              source_fingerprint=None, parser=None, raw_record=None, job_id=None):
        if job_id is not None:
            origin = {"first_run_id": None, "first_job_id": job_id,
                      "last_run_id": None, "last_job_id": job_id}
        else:
            origin = {"first_run_id": run_id, "first_job_id": None,
                      "last_run_id": run_id, "last_job_id": None}
        fingerprint = product.fingerprint()
        conflict = self.version_conflict(
            connection, source_id, product.sku, product.revision, rule_version,
            fingerprint, source_fingerprint, parser,
        )
        if conflict:
            raise ValidationError(conflict)
        current = connection.execute(
            "SELECT revision, fingerprint, rule_version FROM products WHERE source_id = ? AND sku = ?",
            (source_id, product.sku),
        ).fetchone()
        self.record_version(
            connection, source_id, product, rule_version, fingerprint,
            source_fingerprint, parser, raw_record, origin,
        )
        if current:
            if current["revision"] > product.revision:
                return "stale"
            if current["revision"] == product.revision:
                # Equal source revision: never replace during ordinary
                # ingestion, whatever the rule version.  A rule change is
                # applied only by an explicit, all-or-nothing reprocess switch.
                return "unchanged"
        outcome = "updated" if current else "inserted"
        self.write_product(connection, source_id, product, rule_version, fingerprint, origin)
        return outcome

    def apply_reprocess(self, connection, source_id, job_id, product, rule_version,
                        source_fingerprint, parser, raw_record):
        """Apply one staged replay result during an all-or-nothing switch.

        Unlike ordinary ingestion this may reinterpret a source revision that
        is already live, because the operator explicitly asked for a different
        cleaning rule set.  A newer source revision that arrived in the
        meantime always wins and is never overwritten by the older archive.
        """
        fingerprint = product.fingerprint()
        conflict = self.version_conflict(
            connection, source_id, product.sku, product.revision, rule_version,
            fingerprint, source_fingerprint, parser,
        )
        if conflict:
            raise ValidationError(conflict)
        origin = {"first_run_id": None, "first_job_id": job_id,
                  "last_run_id": None, "last_job_id": job_id}
        current = connection.execute(
            "SELECT revision, fingerprint, rule_version FROM products WHERE source_id = ? AND sku = ?",
            (source_id, product.sku),
        ).fetchone()
        self.record_version(
            connection, source_id, product, rule_version, fingerprint,
            source_fingerprint, parser, raw_record, origin,
        )
        if current is not None and current["revision"] > product.revision:
            return "stale"
        if current is not None and current["revision"] == product.revision:
            if current["fingerprint"] == fingerprint:
                if current["rule_version"] != rule_version:
                    self.mark_effective_rule(connection, source_id, product.sku, rule_version, origin)
                return "unchanged"
            self.write_product(connection, source_id, product, rule_version, fingerprint, origin)
            return "updated"
        self.write_product(connection, source_id, product, rule_version, fingerprint, origin)
        return "updated" if current is not None else "inserted"

    def record_version(self, connection, source_id, product, rule_version, fingerprint,
                       source_fingerprint, parser, raw_record, origin):
        connection.execute(
            """INSERT OR IGNORE INTO product_versions
            (source_id, sku, revision, rule_version, fingerprint, source_fingerprint,
             parser, payload, raw_record, first_run_id, first_job_id, observed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                source_id,
                product.sku,
                product.revision,
                rule_version,
                fingerprint,
                source_fingerprint,
                parser,
                canonical(product.payload()),
                canonical(raw_record) if raw_record is not None else None,
                origin["first_run_id"],
                origin["first_job_id"],
                now(),
            ),
        )

    def mark_effective_rule(self, connection, source_id, sku, rule_version, origin):
        connection.execute(
            """UPDATE products SET rule_version = ?, last_run_id = ?, last_job_id = ?,
            updated_at = ? WHERE source_id = ? AND sku = ?""",
            (rule_version, origin["last_run_id"], origin["last_job_id"], now(), source_id, sku),
        )

    def write_product(self, connection, source_id, product, rule_version, fingerprint, origin):
        connection.execute(
            """INSERT INTO products
            (source_id, sku, revision, rule_version, title, category, currency,
             price_cents, stock, active, attributes, fingerprint,
             last_run_id, last_job_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id, sku) DO UPDATE SET
                revision = excluded.revision,
                rule_version = excluded.rule_version,
                title = excluded.title,
                category = excluded.category,
                currency = excluded.currency,
                price_cents = excluded.price_cents,
                stock = excluded.stock,
                active = excluded.active,
                attributes = excluded.attributes,
                fingerprint = excluded.fingerprint,
                last_run_id = excluded.last_run_id,
                last_job_id = excluded.last_job_id,
                updated_at = excluded.updated_at""",
            (
                source_id,
                product.sku,
                product.revision,
                rule_version,
                product.title,
                product.category,
                product.currency,
                product.price_cents,
                product.stock,
                int(product.active),
                canonical(product.attributes),
                fingerprint,
                origin["last_run_id"],
                origin["last_job_id"],
                now(),
            ),
        )
