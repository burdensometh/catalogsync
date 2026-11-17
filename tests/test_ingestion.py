from pathlib import Path
import json
import tempfile
import threading
import unittest

from catalogsync.app import Application
from catalogsync.errors import Conflict, FetchError, IntegrityError, ValidationError
from catalogsync.models import Product
from catalogsync.supplier import SupplierProcess


def record(sku="x", revision=1, **changes):
    value = {"sku": sku, "revision": revision, "title": sku, "price": "2.50", "stock": 3}
    value.update(changes)
    return value


def page(cursor, records, next_cursor=None, snapshot="snap", boundary=10):
    return json.dumps({
        "snapshot_id": snapshot, "boundary": boundary, "cursor": cursor,
        "next_cursor": next_cursor, "records": records,
    }).encode()


class Client:
    def __init__(self, pages, failures=None):
        self.pages = pages
        self.failures = failures or set()
        self.requests = []

    def fetch(self, cursor):
        self.requests.append(cursor)
        if cursor in self.failures:
            raise FetchError("source temporarily unavailable")
        return self.pages[cursor]


class IngestionTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.app = Application(self.root / "state")
        self.app.registry.register("s", "Supplier", "file", str(self.source))

    def test_resume_keeps_committed_page_and_exact_counters(self):
        client = Client({"": page("", [record()], "two"), "two": page("two", [record("y", 2)])}, {"two"})
        with self.assertRaises(FetchError):
            self.app.ingest.start("s", client)
        run = self.app.runs.list()[0]
        self.assertEqual((run["status"], run["page_count"], run["accepted"]), ("failed", 1, 1))
        client.failures.clear()
        result = self.app.ingest.execute(run["id"], client)
        self.assertEqual(client.requests, ["", "two", "two"])
        self.assertEqual((result["accepted"], result["inserted"], result["page_count"]), (2, 2, 2))
        self.assertTrue(self.app.diagnostics.check(True)["ok"])

    def test_revision_order_conflict_and_rejects(self):
        records = [record(revision=5), record(revision=3), record(revision=5), record(revision=5, price="8.00"), record("bad", price="NaN")]
        result = self.app.ingest.apply_delta("s", records)
        self.assertEqual((result["accepted"], result["rejected"]), (3, 2))
        self.assertEqual((result["inserted"], result["stale"], result["unchanged"]), (1, 1, 1))
        self.assertEqual(self.app.query.product("s", "x")["price_cents"], 250)
        self.assertEqual(len(self.app.query.history("s", "x")), 2)
        self.assertTrue(self.app.diagnostics.check()["ok"])

    def test_snapshot_identity_change_does_not_advance_second_page(self):
        client = Client({"": page("", [record()], "two"), "two": page("two", [record("y")], snapshot="changed")})
        with self.assertRaises(ValidationError):
            self.app.ingest.start("s", client)
        run = self.app.runs.list()[0]
        self.assertEqual(run["page_count"], 1)
        self.assertEqual(self.app.query.products()["total"], 1)

    def test_pagination_cycle_fails_without_replaying_first_page(self):
        client = Client({"": page("", [record()], "two"), "two": page("two", [], "two")})
        with self.assertRaises(ValidationError):
            self.app.ingest.start("s", client)
        self.assertEqual(self.app.runs.list()[0]["page_count"], 1)

    def test_completed_run_is_idempotent(self):
        client = Client({"": page("", [record()])})
        result = self.app.ingest.start("s", client)
        self.app.ingest.execute(result["id"], client)
        self.assertEqual(client.requests, [""])

    def test_two_snapshot_creators_only_one_succeeds(self):
        barrier = threading.Barrier(2)
        outcomes = []
        def create():
            barrier.wait()
            try:
                self.app.runs.create("s")
                outcomes.append("created")
            except Conflict:
                outcomes.append("conflict")
        threads = [threading.Thread(target=create) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertCountEqual(outcomes, ["created", "conflict"])

    def test_publication_is_immutable_and_checksum_verified(self):
        self.app.ingest.apply_delta("s", [record(), record("zero", stock=0)])
        first = self.app.publisher.publish("s")
        artifact = self.app.publisher.artifact(first["id"])
        original = artifact.read_bytes()
        self.assertEqual(first["row_count"], 1)
        self.app.ingest.apply_delta("s", [record(revision=2, price="3.00")])
        second = self.app.publisher.publish("s")
        self.assertEqual(artifact.read_bytes(), original)
        self.assertEqual(len(self.app.publisher.diff(first["id"], second["id"])["changed"]), 1)
        artifact.write_bytes(b"corrupt")
        with self.assertRaises(IntegrityError):
            self.app.publisher.artifact(first["id"])

    def test_complete_snapshot_delists_only_absent_identities(self):
        self.app.ingest.apply_delta("s", [record(), record("y")])
        self.app.ingest.start("s", Client({"": page("", [record(revision=2)])}))
        shown = {item["sku"] for item in self.app.query.products()["items"]}
        self.assertEqual(shown, {"x"})
        self.assertTrue(self.app.query.product("s", "y")["delisted"])
        self.assertFalse(self.app.query.product("s", "x")["delisted"])

    def test_real_http_supplier_and_archive(self):
        (self.source / "manifest.json").write_text(json.dumps({"pages": {"": "p.json"}}))
        (self.source / "p.json").write_bytes(page("", [record()]))
        with SupplierProcess(self.source) as url:
            self.app.registry.register("web", "HTTP", "http", url)
            run = self.app.ingest.start("web")
        self.assertEqual(run["inserted"], 1)
        self.assertTrue(self.app.diagnostics.check(True)["ok"])

    def test_numeric_validation(self):
        for price in ["NaN", "Infinity", "-1", "1.001", True]:
            with self.subTest(price=price), self.assertRaises(ValidationError):
                Product.parse(record(price=price))


if __name__ == "__main__":
    unittest.main()
