from pathlib import Path
import hashlib
import json
import shutil
import sqlite3
import tempfile
import threading
import unittest

from catalogsync.app import Application
from catalogsync.consumer import DatasetConsumer
from catalogsync.errors import Conflict, ValidationError
from catalogsync.models import Product, format_price, money, to_minor_units


FIXTURE = Path(__file__).resolve().parent.parent / "fixtures" / "legacy-v1"
LEGACY_JSONL = "d680ea8f757d418f96d3c2e42ff8e435"
LEGACY_CSV = "ccd7bbb14a03468eb466b19706d32a5e"


def copy_fixture(test, name="legacy"):
    target = Path(test.temporary.name) / name
    shutil.copytree(FIXTURE, target)
    return target


class MoneyConventionTest(unittest.TestCase):
    def test_minor_unit_parsing_per_currency(self):
        self.assertEqual(money("1.25", "CNY"), 125)
        self.assertEqual(money("1.25", "USD"), 125)
        self.assertEqual(money("1.25", "EUR"), 125)
        self.assertEqual(money("1.25", "GBP"), 125)
        self.assertEqual(money("100", "JPY"), 100)
        self.assertEqual(money("100.00", "JPY"), 100)
        for bad in ["0.50", "0.01", "100.5"]:
            with self.subTest(bad=bad), self.assertRaises(ValidationError):
                money(bad, "JPY")
        with self.assertRaises(ValidationError):
            money("1.001", "CNY")

    def test_storage_scale_round_trip_and_lossless_guard(self):
        self.assertEqual(to_minor_units(10000, "JPY", "centi"), 100)
        self.assertIsNone(to_minor_units(50, "JPY", "centi"))
        self.assertEqual(to_minor_units(125, "CNY", "centi"), 125)
        self.assertEqual(to_minor_units(100, "JPY", "minor"), 100)

    def test_format_price_per_scale(self):
        self.assertEqual(format_price(10000, "JPY", "centi"), "100.00")
        self.assertEqual(format_price(50, "JPY", "centi"), "0.50")
        self.assertEqual(format_price(100, "JPY", "minor"), "100")
        self.assertEqual(format_price(125, "CNY", "minor"), "1.25")

    def test_fresh_database_is_minor_scale(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        app = Application(Path(temporary.name) / "state")
        self.assertEqual(app.database.money_scale(), "minor")
        self.assertEqual(app.database.schema_version(), 2)


class LegacyMigrationTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.site = copy_fixture(self)
        self.state = self.site / "state"
        self.consumer = self.site / "consumer.sqlite"

    def app(self):
        return Application(self.state)

    def product(self, app, sku):
        return app.query.product("s", sku)

    def test_legacy_database_stays_readable_in_old_scale(self):
        app = self.app()
        self.assertEqual(app.database.money_scale(), "centi")
        jpy1 = self.product(app, "JPY-1")
        self.assertEqual((jpy1["price_cents"], jpy1["price"]), (10000, "100.00"))
        jpy2 = self.product(app, "JPY-2")
        self.assertEqual((jpy2["price_cents"], jpy2["price"]), (50, "0.50"))
        history = app.query.history("s", "JPY-1")
        self.assertEqual(history[0]["payload"]["price_cents"], 10000)
        self.assertTrue(app.diagnostics.check(True)["ok"])

    def test_migration_blocks_on_unconvertible_records_and_rolls_back(self):
        app = self.app()
        before = {
            row["sku"]: row["price_cents"]
            for row in app.query.products(limit=100)["items"]
        }
        report = app.migration.migrate()
        self.assertFalse(report["ok"])
        self.assertEqual(report["producer"]["status"], "blocked")
        self.assertEqual(report["consumer"]["status"], "blocked")
        blockers = report["producer"]["blockers"] + report["consumer"]["blockers"]
        named = {(b["table"], b["sku"], b["revision"], b["price_cents"]) for b in blockers}
        self.assertIn(("products", "JPY-2", 3, 50), named)
        self.assertIn(("product_versions", "JPY-2", 3, 50), named)
        self.assertIn(("catalog_records", "JPY-2", 3, 50), named)
        for blocker in blockers:
            self.assertEqual(blocker["amount"], "0.50")
            self.assertEqual(blocker["currency"], "JPY")
        # Nothing was converted: the switch is all-or-nothing.
        self.assertEqual(app.database.money_scale(), "centi")
        after = {
            row["sku"]: row["price_cents"]
            for row in app.query.products(limit=100)["items"]
        }
        self.assertEqual(before, after)
        consumer = DatasetConsumer(self.consumer)
        self.assertEqual(consumer.query()[1]["price_cents"], 10000)
        self.assertTrue(app.diagnostics.check(True)["ok"])

    def test_correction_allows_migration_and_preserves_everything_else(self):
        app = self.app()
        dataset_dir = self.state / "datasets"
        digests_before = {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in dataset_dir.iterdir()
        }
        corrections = [{"source_id": "s", "sku": "JPY-2", "price": "1"}]
        report = app.migration.migrate(corrections=corrections)
        self.assertTrue(report["ok"])
        self.assertEqual(report["producer"]["status"], "migrated")
        self.assertEqual(report["consumer"]["status"], "migrated")
        self.assertEqual(app.database.money_scale(), "minor")
        self.assertEqual(self.product(app, "CNY-1")["price_cents"], 125)
        self.assertEqual(self.product(app, "CNY-1")["price"], "1.25")
        self.assertEqual(self.product(app, "JPY-1")["price_cents"], 100)
        self.assertEqual(self.product(app, "JPY-1")["price"], "100")
        self.assertEqual(self.product(app, "JPY-2")["price_cents"], 1)
        self.assertEqual(self.product(app, "JPY-2")["price"], "1")
        history = {v["revision"]: v["payload"]["price_cents"] for v in app.query.history("s", "JPY-1")}
        self.assertEqual(history, {2: 100})
        # Old publication files keep their bytes, checksums and centi marking.
        digests_after = {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in dataset_dir.iterdir()
        }
        self.assertEqual(digests_before, digests_after)
        for publication in app.publisher.list():
            self.assertEqual(publication["money_format"], "centi")
        self.assertEqual(str(app.publisher.artifact(LEGACY_JSONL).name), LEGACY_JSONL + ".jsonl")
        # Consumer database converted once, in the same run.
        records = {row["sku"]: row["price_cents"] for row in DatasetConsumer(self.consumer).query()}
        self.assertEqual(records, {"CNY-1": 125, "JPY-1": 100, "JPY-2": 1})
        self.assertTrue(app.diagnostics.check(True)["ok"])

    def test_remigration_and_replay_never_rescale(self):
        app = self.app()
        corrections = [{"source_id": "s", "sku": "JPY-2", "price": "1"}]
        app.migration.migrate(corrections=corrections)
        again = app.migration.migrate(corrections=corrections)
        self.assertTrue(again["ok"])
        self.assertEqual(again["producer"]["status"], "already_migrated")
        self.assertEqual(again["consumer"]["status"], "already_migrated")
        self.assertEqual(self.product(app, "JPY-1")["price_cents"], 100)
        self.assertEqual(self.product(app, "JPY-2")["price_cents"], 1)
        sink = DatasetConsumer(self.consumer)
        replay = sink.apply(app.publisher, LEGACY_JSONL)
        self.assertTrue(replay["replayed"])
        records = {row["sku"]: row["price_cents"] for row in sink.query()}
        self.assertEqual(records, {"CNY-1": 125, "JPY-1": 100, "JPY-2": 1})

    def test_new_publication_uses_minor_scale_and_consumer_applies_it(self):
        app = self.app()
        app.migration.migrate(corrections=[{"source_id": "s", "sku": "JPY-2", "price": "1"}])
        publication = app.publisher.publish("s")
        self.assertEqual(publication["money_format"], "minor")
        with app.publisher.artifact(publication["id"]).open(encoding="utf-8") as stream:
            rows = {json.loads(line)["sku"]: json.loads(line) for line in stream if line.strip()}
        self.assertEqual(rows["JPY-1"]["price"], "100")
        self.assertEqual(rows["JPY-2"]["price"], "1")
        self.assertEqual(rows["CNY-1"]["price"], "1.25")
        sink = DatasetConsumer(self.consumer)
        result = sink.apply(app.publisher, publication["id"])
        self.assertFalse(result["replayed"])
        records = {row["sku"]: row["price_cents"] for row in sink.query()}
        self.assertEqual(records, {"CNY-1": 125, "JPY-1": 100, "JPY-2": 1})

    def test_legacy_publication_with_fractional_yen_is_not_materialized(self):
        app = self.app()
        app.migration.migrate(corrections=[{"source_id": "s", "sku": "JPY-2", "price": "1"}])
        fresh = DatasetConsumer(Path(self.temporary.name) / "fresh.sqlite")
        with self.assertRaises(ValidationError):
            fresh.apply(app.publisher, LEGACY_JSONL)
        self.assertEqual(fresh.query(), [])
        with self.assertRaises(ValidationError):
            fresh.apply(app.publisher, LEGACY_CSV)
        self.assertEqual(fresh.query(), [])

    def test_reingestion_after_migration_matches_migrated_fingerprints(self):
        app = self.app()
        app.migration.migrate(corrections=[{"source_id": "s", "sku": "JPY-2", "price": "1"}])
        archived = json.loads(
            next((self.state / "archive").rglob("*.json")).read_text(encoding="utf-8")
        )
        result = app.ingest.apply_delta("s", archived["records"])
        # CNY-1 and JPY-1 re-read as unchanged; the fractional yen record is
        # now quarantined at ingress instead of being stored.
        self.assertEqual((result["accepted"], result["rejected"]), (2, 1))
        self.assertEqual(result["unchanged"], 2)
        self.assertEqual(self.product(app, "JPY-1")["price_cents"], 100)
        self.assertTrue(app.diagnostics.check(True)["ok"])

    def test_ingestion_on_legacy_database_uses_new_rules_and_migrates(self):
        app = self.app()
        # New writes on the not-yet-migrated database validate in the new
        # convention and store in the database's current (centi) scale.
        result = app.ingest.apply_delta("s", [
            {"sku": "JPY-3", "revision": 1, "title": "JPY-3", "currency": "JPY",
             "price": "0.75", "stock": 1},
            {"sku": "JPY-3", "revision": 2, "title": "JPY-3", "currency": "JPY",
             "price": "100", "stock": 1},
        ])
        self.assertEqual((result["accepted"], result["rejected"]), (1, 1))
        self.assertEqual(self.product(app, "JPY-3")["price_cents"], 10000)
        report = app.migration.migrate(corrections=[{"source_id": "s", "sku": "JPY-2", "price": "1"}])
        self.assertTrue(report["ok"])
        self.assertEqual(self.product(app, "JPY-3")["price_cents"], 100)
        self.assertEqual(self.product(app, "JPY-3")["price"], "100")

    def test_fix_current_record_via_ingestion_then_correct_history(self):
        app = self.app()
        app.ingest.apply_delta("s", [
            {"sku": "JPY-2", "revision": 4, "title": "JPY-2", "currency": "JPY",
             "price": "1", "stock": 3},
        ])
        blocked = app.migration.migrate()
        self.assertFalse(blocked["ok"])
        named = {(b["table"], b["revision"]) for b in blocked["producer"]["blockers"]}
        self.assertEqual(named, {("product_versions", 3)})
        corrections = [{"source_id": "s", "sku": "JPY-2", "revision": 3, "price": "1"}]
        report = app.migration.migrate(corrections=corrections)
        self.assertTrue(report["ok"])
        self.assertEqual(self.product(app, "JPY-2")["revision"], 4)
        self.assertEqual(self.product(app, "JPY-2")["price_cents"], 1)
        self.assertTrue(app.diagnostics.check(True)["ok"])

    def test_correction_validation(self):
        app = self.app()
        with self.assertRaises(ValidationError):
            app.migration.migrate(corrections=[{"source_id": "s", "sku": "GHOST", "price": "1"}])
        with self.assertRaises(ValidationError):
            app.migration.migrate(corrections=[
                {"source_id": "s", "sku": "JPY-2", "price": "1"},
                {"source_id": "s", "sku": "CNY-1", "price": "9.99"},
            ])
        with self.assertRaises(Conflict):
            app.migration.migrate(corrections=[
                {"source_id": "s", "sku": "JPY-2", "price": "1"},
                {"source_id": "s", "sku": "JPY-2", "revision": 3, "price": "2"},
            ])
        with self.assertRaises(ValidationError):
            app.migration.migrate(corrections=[{"source_id": "s", "sku": "JPY-2", "price": "0.5"}])
        self.assertEqual(app.database.money_scale(), "centi")

    def test_migration_racing_writes_stays_consistent(self):
        app = self.app()
        corrections = [{"source_id": "s", "sku": "JPY-2", "price": "1"}]
        errors = []

        def writer(index):
            try:
                for n in range(10):
                    app.ingest.apply_delta("s", [{
                        "sku": f"JPY-{index}-{n}", "revision": 1, "title": "w",
                        "currency": "JPY", "price": str(100 + n), "stock": 1,
                    }])
            except Exception as error:  # pragma: no cover - failure detail
                errors.append(error)

        def migrator():
            try:
                for _ in range(100):
                    if app.migration.migrate(corrections=corrections)["ok"]:
                        return
                errors.append(AssertionError("migration never completed"))
            except Exception as error:  # pragma: no cover - failure detail
                errors.append(error)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(3)]
        threads.append(threading.Thread(target=migrator))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(app.database.money_scale(), "minor")
        self.assertTrue(app.diagnostics.check(True)["ok"])
        # Every racing write landed entirely in one scale and was converted
        # exactly once: all JPY amounts are whole minor units.
        for item in app.query.products(limit=1000)["items"]:
            if item["currency"] == "JPY":
                self.assertEqual(item["price"], str(item["price_cents"]))


if __name__ == "__main__":
    unittest.main()
