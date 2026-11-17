from pathlib import Path
import json
import tempfile
import unittest

from catalogsync.app import Application
from catalogsync.consumer import DatasetConsumer
from catalogsync.errors import FetchError
from catalogsync.worker import Collector

from test_ingestion import Client, page, record


class DelistingTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.app = Application(self.root / "state")
        self.app.registry.register("s", "Supplier", "file", str(self.source))

    def visible(self):
        return {item["sku"] for item in self.app.query.products()["items"]}

    def test_only_complete_snapshot_delists_and_pages_must_all_commit(self):
        self.app.ingest.apply_delta("s", [record("a"), record("b"), record("c")])
        client = Client(
            {"": page("", [record("a", 2)], "two"), "two": page("two", [record("b", 2)])},
            {"two"},
        )
        with self.assertRaises(FetchError):
            self.app.ingest.start("s", client)
        # Page one is committed but the snapshot is not complete yet.
        self.assertEqual(self.visible(), {"a", "b", "c"})
        client.failures.clear()
        self.app.ingest.execute(self.app.runs.list()[0]["id"], client)
        self.assertEqual(self.visible(), {"a", "b"})

    def test_snapshot_with_rejected_row_does_not_delist(self):
        self.app.ingest.apply_delta("s", [record("a"), record("gone")])
        client = Client({"": page("", [record("a", 2), record("bad", price="NaN")])})
        run = self.app.ingest.start("s", client)
        self.assertEqual(run["rejected"], 1)
        self.assertEqual(self.visible(), {"a", "gone"})
        with self.app.database.read() as connection:
            self.assertIsNone(connection.execute("SELECT * FROM snapshot_barriers").fetchone())

    def test_newer_increment_survives_snapshot_and_late_old_record(self):
        self.app.ingest.apply_delta("s", [record("a", 5), record("b", 5)])
        # A version newer than the snapshot boundary arrives before the snapshot
        # that no longer lists it.
        self.app.ingest.apply_delta("s", [record("b", 12, price="9.00")])
        self.app.ingest.start("s", Client({"": page("", [record("a", 6)])}))
        self.assertEqual(self.visible(), {"a", "b"})
        self.assertEqual(self.app.query.product("s", "b")["revision"], 12)
        # A late record inside the snapshot's boundary must not revive an absent
        # identity, and must not resurrect it either.
        self.app.ingest.apply_delta("s", [record("c", 3)])
        self.assertEqual(self.visible(), {"a", "b"})
        self.assertTrue(self.app.query.product("s", "c")["delisted"])

    def test_never_stored_identity_late_record_stays_delisted(self):
        self.app.ingest.apply_delta("s", [record("a", 5)])
        self.app.ingest.start("s", Client({"": page("", [record("a", 6)])}))
        # "ghost" never existed before the snapshot, and its late revision is
        # below the snapshot boundary.
        self.app.ingest.apply_delta("s", [record("ghost", 4)])
        self.assertEqual(self.visible(), {"a"})
        product = self.app.query.product("s", "ghost")
        self.assertTrue(product["delisted"])
        self.assertEqual(product["revision"], 4)

    def test_reprocessing_snapshot_is_stable(self):
        self.app.ingest.apply_delta("s", [record("a", 2), record("b", 2)])
        client = Client({"": page("", [record("a", 3)])})
        run = self.app.ingest.start("s", client)
        self.app.ingest.execute(run["id"], client)
        self.assertEqual(client.requests, [""])
        self.assertEqual(self.visible(), {"a"})

    def test_late_increment_outside_barrier_removes_delisting(self):
        self.app.ingest.apply_delta("s", [record("a", 2), record("b", 2)])
        self.app.ingest.start("s", Client({"": page("", [record("a", 3)])}))
        self.assertTrue(self.app.query.product("s", "b")["delisted"])
        # A new version beyond the snapshot cut proves the identity is back.
        self.app.ingest.apply_delta("s", [record("b", 11, price="4.00")])
        self.assertEqual(self.visible(), {"a", "b"})
        self.assertFalse(self.app.query.product("s", "b")["delisted"])

    def test_delist_evidence_is_traceable(self):
        self.app.ingest.apply_delta("s", [record("a", 2), record("b", 4)])
        run = self.app.ingest.start("s", Client({"": page("", [record("a", 3)])}))
        trace = self.app.provenance.explain("s", "b")
        self.assertTrue(trace["delisted"])
        self.assertEqual(trace["snapshot_barrier"]["run_id"], run["id"])
        self.assertFalse(trace["snapshot_barrier"]["in_snapshot"])
        evidence = self.app.query.delistings("s", "b")
        self.assertEqual(evidence["total"], 1)
        row = evidence["items"][0]
        self.assertEqual((row["run_id"], row["boundary"], row["prior_revision"]), (run["id"], 10, 4))
        self.assertEqual(trace["delist_evidence"][0]["run_id"], run["id"])

    def test_delist_evidence_records_unknown_prior_for_new_identity(self):
        self.app.ingest.apply_delta("s", [record("a", 5)])
        run = self.app.ingest.start("s", Client({"": page("", [record("a", 6)])}))
        self.app.ingest.apply_delta("s", [record("ghost", 3)])
        evidence = self.app.query.delistings("s", "ghost")["items"][0]
        self.assertIsNone(evidence["prior_revision"])
        self.assertIsNone(evidence["prior_fingerprint"])

    def test_publication_and_consumer_reflect_effective_state(self):
        self.app.ingest.apply_delta("s", [record("a"), record("gone")])
        first = self.app.publisher.publish("s", sellable_only=False)
        sink = DatasetConsumer(self.root / "consumer.sqlite")
        sink.apply(self.app.publisher, first["id"])
        self.assertEqual({row["sku"] for row in sink.query()}, {"a", "gone"})
        self.app.ingest.start("s", Client({"": page("", [record("a", 2)])}))
        # The earlier publication is immutable and still carries the old result.
        self.assertEqual(
            len([line for line in self.app.publisher.artifact(first["id"]).read_text().splitlines() if line]),
            2,
        )
        second = self.app.publisher.publish("s", sellable_only=False)
        self.assertEqual(second["row_count"], 1)
        sink.apply(self.app.publisher, second["id"])
        self.assertEqual({row["sku"] for row in sink.query()}, {"a"})

    def test_collector_cycle_reaches_delisting(self):
        (self.source / "manifest.json").write_text(json.dumps({"pages": {"": "p.json"}}))
        (self.source / "p.json").write_bytes(page("", [record("a")]))
        Collector(self.app, 1, False).cycle()
        self.app.ingest.apply_delta("s", [record("late")])
        (self.source / "p.json").write_bytes(page("", [record("a", 2)], snapshot="snap2"))
        result = Collector(self.app, 1, False).cycle()
        self.assertEqual(result["completed"], 1)
        self.assertEqual(self.visible(), {"a"})

    def test_resume_and_retry_converge_on_same_delisting(self):
        self.app.ingest.apply_delta("s", [record("a", 2), record("b", 2), record("c", 2)])
        client = Client(
            {"": page("", [record("a", 3)], "two"), "two": page("two", [record("b", 3)])},
            {"two"},
        )
        with self.assertRaises(FetchError):
            self.app.ingest.start("s", client)
        client.failures.clear()
        self.app.ingest.execute(self.app.runs.list()[0]["id"], client)
        first = sorted(self.app.query.delistings("s")["items"])
        # Retrying the completed run does not run again and does not change state.
        completed = self.app.runs.list()[0]["id"]
        self.app.ingest.execute(completed, client)
        again = sorted(self.app.query.delistings("s")["items"])
        self.assertEqual([(r["sku"], r["run_id"], r["boundary"]) for r in first],
                         [(r["sku"], r["run_id"], r["boundary"]) for r in again])
        self.assertEqual(self.visible(), {"a", "b"})

    def test_inbox_reconcile_retry_does_not_resurrect_delisted(self):
        self.app.ingest.apply_delta("s", [record("kept", 5), record("gone", 5)])
        self.app.ingest.start("s", Client({"": page("", [record("kept", 6)])}))
        self.assertTrue(self.app.query.product("s", "gone")["delisted"])
        # A retried inbox event inside the boundary must stay delisted.
        self.app.inbox.submit("s", [{"id": "e1", "record": record("gone", 4)}])
        self.app.inbox.consume("s")
        self.assertFalse(self.app.query.product("s", "kept")["delisted"])
        self.assertTrue(self.app.query.product("s", "gone")["delisted"])
        self.assertEqual(self.visible(), {"kept"})
        self.assertTrue(self.app.diagnostics.check()["ok"])

    def test_interrupted_completion_retry_is_idempotent(self):
        self.app.ingest.apply_delta("s", [record("a", 2), record("b", 2)])
        client = Client({"": page("", [record("a", 3)])})
        run = self.app.ingest.start("s", client)
        self.assertTrue(self.app.query.product("s", "b")["delisted"])
        before = self.app.query.delistings("s")
        again = self.app.ingest.execute(run["id"], client)
        self.assertEqual(again["status"], "completed")
        self.assertEqual(self.visible(), {"a"})
        self.assertEqual(self.app.query.delistings("s")["total"], before["total"])
        self.assertEqual(
            [r["delisted_at"] for r in before["current"]],
            [r["delisted_at"] for r in self.app.query.delistings("s")["current"]],
        )
        self.assertTrue(self.app.diagnostics.check()["ok"])

    def test_increment_arriving_between_pages_is_preserved_by_boundary(self):
        self.app.ingest.apply_delta("s", [record("a", 2), record("b", 2)])
        # A two-page snapshot; the second page will be resumed later.
        client = Client(
            {"": page("", [record("a", 3)], "two"), "two": page("two", [record("d", 3)])},
            {"two"},
        )
        with self.assertRaises(FetchError):
            self.app.ingest.start("s", client)
        snapshot = next(r for r in self.app.runs.list() if r["mode"] == "snapshot")
        # While the snapshot is incomplete, two increments arrive: one above the
        # snapshot boundary and one inside it.
        self.app.ingest.apply_delta("s", [record("b", 12, price="5.00")])
        self.app.ingest.apply_delta("s", [record("c", 7)])
        client.failures.clear()
        self.app.ingest.execute(snapshot["id"], client)
        # b is newer than the cut and survives; c is inside the cut and absent
        # from the snapshot, so it is delisted.
        self.assertEqual(self.visible(), {"a", "b", "d"})
        self.assertEqual(self.app.query.product("s", "b")["revision"], 12)
        self.assertTrue(self.app.query.product("s", "c")["delisted"])

    def test_empty_complete_snapshot_delists_everything(self):
        self.app.ingest.apply_delta("s", [record("a", 2), record("b", 2)])
        self.app.ingest.start("s", Client({"": page("", [])}))
        self.assertEqual(self.visible(), set())
        self.assertEqual(self.app.query.delistings("s")["total"], 2)

    def test_same_boundary_newer_snapshot_can_relist_its_member(self):
        self.app.ingest.apply_delta("s", [record("a", 2), record("b", 2)])
        self.app.ingest.start("s", Client({"": page("", [record("a", 3)])}))
        self.assertTrue(self.app.query.product("s", "b")["delisted"])
        # A later snapshot at the same cut that includes b proves it is back.
        self.app.ingest.start("s", Client({"": page("", [record("a", 3), record("b", 4)])}))
        self.assertEqual(self.visible(), {"a", "b"})
        self.assertFalse(self.app.query.product("s", "b")["delisted"])


if __name__ == "__main__":
    unittest.main()
