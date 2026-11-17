import json
from pathlib import Path

from .consumer import DatasetConsumer
from .errors import ValidationError
from .models import (
    canonical,
    digest,
    normalize_corrections,
    now,
    plan_scale_changes,
)


class Migration:
    """Resumable centi -> minor money scale migration.

    The producer database and each consumer database are converted in their
    own single write transaction, so an interrupted migration rolls back and
    can simply be rerun, and concurrent readers/writers never observe a
    half-converted state. Records whose stored amount cannot be expressed in
    the currency's minor unit block the switch and are reported by identity;
    the operator either corrects them through the normal ingestion path or
    supplies exact replacement values via a corrections file. Nothing is
    ever rounded, and a database already in the new scale is left untouched
    (no amount is scaled twice).
    """

    def __init__(self, application):
        self.app = application
        self.db = application.database

    def migrate(self, consumer_path=None, corrections=None):
        corrections = normalize_corrections(corrections or [])
        if consumer_path is None:
            candidate = self.app.home.parent / "consumer.sqlite"
            consumer_path = candidate if candidate.is_file() else None
        elif not Path(consumer_path).is_file():
            raise ValidationError("consumer database does not exist")
        matched = set()

        # Phase 1: read-only plans for every database involved, so invalid
        # corrections and blocking records are reported before anything is
        # committed anywhere.
        producer_plan = self.plan_producer(corrections, matched)
        consumer = None
        consumer_plan = None
        if consumer_path is not None:
            consumer = DatasetConsumer(consumer_path)
            consumer_plan = consumer.plan_scale_migration(corrections, matched)

        scanned = producer_plan["scale"] == "centi" or (
            consumer_plan is not None and consumer_plan["scale"] == "centi"
        )
        if scanned:
            unmatched = [
                entry for index, entry in enumerate(corrections) if index not in matched
            ]
            if unmatched:
                raise ValidationError(
                    "corrections match no stored records: "
                    + ", ".join(
                        f"{entry['source_id']}/{entry['sku']}" for entry in unmatched
                    )
                )

        blockers = list(producer_plan["blockers"])
        if consumer_plan is not None:
            blockers += consumer_plan["blockers"]
        report = {
            "ok": not blockers,
            "corrections": len(corrections),
            "producer": None,
            "consumer": None,
            "consumer_path": str(consumer_path) if consumer_path is not None else None,
        }
        if blockers:
            with self.db.write() as connection:
                self.db.event(connection, "money_scale_migration_blocked", {
                    "blockers": blockers[:50], "total": len(blockers),
                })
            report["producer"] = self._blocked_result(producer_plan)
            if consumer_plan is not None:
                report["consumer"] = self._blocked_result(consumer_plan)
            return report

        # Phase 2: apply each database in its own atomic transaction. Each
        # apply rescans inside the write transaction, so a concurrent write
        # that landed between plan and apply is converted in the same commit.
        report["producer"] = self.apply_producer(corrections)
        if report["producer"]["status"] == "blocked":
            report["ok"] = False
            return report
        if consumer is not None:
            report["consumer"] = consumer.apply_scale_migration(corrections)
            if report["consumer"]["status"] == "blocked":
                report["ok"] = False
                return report
        return report

    def _blocked_result(self, plan):
        if plan["scale"] == "minor":
            return {
                "status": "already_migrated",
                "scale": "minor",
                "converted": {},
                "corrections_applied": 0,
                "blockers": [],
            }
        return {
            "status": "blocked",
            "scale": plan["scale"],
            "converted": {},
            "corrections_applied": 0,
            "blockers": plan["blockers"],
        }

    def scan_producer(self, connection, corrections, matched=None):
        products = [
            dict(row) for row in connection.execute(
                "SELECT * FROM products ORDER BY source_id, sku"
            )
        ]
        product_changes, product_blockers, applied = plan_scale_changes(
            products, corrections, matched, "products"
        )
        versions = []
        for row in connection.execute(
            "SELECT * FROM product_versions ORDER BY source_id, sku, revision"
        ):
            payload = json.loads(row["payload"])
            versions.append({
                "source_id": row["source_id"],
                "sku": row["sku"],
                "revision": row["revision"],
                "currency": payload["currency"],
                "price_cents": payload["price_cents"],
                "payload": payload,
            })
        version_changes, version_blockers, version_applied = plan_scale_changes(
            versions, corrections, matched, "product_versions"
        )
        return (
            product_changes,
            version_changes,
            product_blockers + version_blockers,
            applied + version_applied,
        )

    def plan_producer(self, corrections, matched=None):
        with self.db.read() as connection:
            scale = self.db.money_scale(connection)
            if scale == "minor":
                return {"scale": scale, "blockers": [], "corrections_applied": 0}
            _, _, blockers, applied = self.scan_producer(connection, corrections, matched)
            return {"scale": scale, "blockers": blockers, "corrections_applied": applied}

    def apply_producer(self, corrections):
        with self.db.write() as connection:
            if self.db.money_scale(connection) == "minor":
                return {
                    "status": "already_migrated",
                    "scale": "minor",
                    "converted": {"products": 0, "product_versions": 0},
                    "corrections_applied": 0,
                    "blockers": [],
                }
            product_changes, version_changes, blockers, applied = self.scan_producer(
                connection, corrections
            )
            if blockers:
                return {
                    "status": "blocked",
                    "scale": "centi",
                    "converted": {"products": 0, "product_versions": 0},
                    "corrections_applied": 0,
                    "blockers": blockers,
                }
            for row, minor in product_changes:
                payload = {
                    "sku": row["sku"],
                    "revision": row["revision"],
                    "title": row["title"],
                    "category": row["category"],
                    "currency": row["currency"],
                    "price_cents": minor,
                    "stock": row["stock"],
                    "active": bool(row["active"]),
                    "attributes": json.loads(row["attributes"]),
                }
                connection.execute(
                    "UPDATE products SET price_cents=?, fingerprint=? WHERE source_id=? AND sku=?",
                    (minor, digest(payload), row["source_id"], row["sku"]),
                )
            for row, minor in version_changes:
                payload = row["payload"]
                payload["price_cents"] = minor
                connection.execute(
                    """UPDATE product_versions SET payload=?, fingerprint=?
                    WHERE source_id=? AND sku=? AND revision=?""",
                    (
                        canonical(payload),
                        digest(payload),
                        row["source_id"],
                        row["sku"],
                        row["revision"],
                    ),
                )
            connection.execute("INSERT INTO schema_version VALUES (?, ?)", (2, now()))
            self.db.event(connection, "money_scale_migrated", {
                "scale": "minor",
                "converted": {
                    "products": len(product_changes),
                    "product_versions": len(version_changes),
                },
                "corrections": applied,
            })
            return {
                "status": "migrated",
                "scale": "minor",
                "converted": {
                    "products": len(product_changes),
                    "product_versions": len(version_changes),
                },
                "corrections_applied": applied,
                "blockers": [],
            }
