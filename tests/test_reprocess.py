from pathlib import Path
import json
import tempfile
import unittest

from catalogsync.app import Application
from catalogsync.consumer import DatasetConsumer
from catalogsync.errors import Conflict, IntegrityError, NotFound, ValidationError

from test_ingestion import Client, page, record


IDENTITY_FIELDS = {
    "sku": "sku", "revision": "revision", "price": "price", "stock": "stock",
}


def upper_title():
    return {
        "fields": {
            **IDENTITY_FIELDS,
            "title": {"path": "title", "steps": ["upper"]},
        },
    }


class ReprocessTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.app = Application(self.root / "state")
        self.app.registry.register("s", "Supplier", "file", str(self.source))

    def policy(self, title="old"):
        return {
            "fields": {**IDENTITY_FIELDS, "title": "title"},
            "defaults": {"category": title},
        }

    def ingest_pages(self, pages, failures=None):
        client = Client(pages, failures)
        return self.app.ingest.start("s", client), client

    def two_page_source(self, title="old"):
        rows = [record("a", revision=4), record("b", revision=5)]
        return {
            "": page("", [record("a", revision=4)], "two"),
            "two": page("two", [record("b", revision=5)]),
        }

    # ------------------------------------------------- versions vs rule identity

    def test_reprocess_archived_pages_under_new_rule_switches_effective_result(self):
        self.app.profiles.set("s", self.policy())
        run = self.app.ingest.apply_delta("s", [record("x", revision=3, title="tea")])
        self.assertEqual(self.app.query.product("s", "x")["title"], "tea")
        self.assertEqual(self.app.query.product("s", "x")["rule_version"], 1)

        self.app.profiles.set("s", upper_title())
        job = self.app.reprocess.start("s")
        self.assertEqual(job["status"], "completed")
        self.assertGreater(job["switched_at"] or "", "")
        product = self.app.query.product("s", "x")
        self.assertEqual(product["title"], "TEA")
        self.assertEqual(product["rule_version"], 2)
        # The source revision is unchanged; only the cleaning rule differs.
        self.assertEqual(product["revision"], 3)
        # Both rule interpretations are retained as observations of revision 3.
        revisions = {(item["revision"], item["rule_version"]) for item in self.app.query.history("s", "x")}
        self.assertEqual(revisions, {(3, 1), (3, 2)})

    def test_reprocess_of_archived_run_does_not_reread_source(self):
        self.app.profiles.set("s", self.policy())
        pages = {"": page("", [record("x", revision=4)], "two"), "two": page("two", [record("y", revision=4)])}
        run = self.app.ingest.start("s", Client(pages))["id"]
        # Remove the source entirely: reprocessing must rely only on the archive.
        for path in self.source.iterdir():
            path.unlink()
        self.app.profiles.set("s", upper_title())
        job = self.app.reprocess.start("s", run_id=run)
        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["page_total"], 2)
        self.assertEqual(job["accepted"], 2)
        self.assertEqual(self.app.query.product("s", "x")["title"], "X")

    def test_reprocess_failure_keeps_previous_effective_results_and_published_files(self):
        self.app.profiles.set("s", self.policy())
        self.app.ingest.apply_delta("s", [record("x", revision=3, title="tea")])
        publication = self.app.publisher.publish("s")
        artifact = self.app.publisher.artifact(publication["id"])
        original = artifact.read_bytes()

        self.app.profiles.set("s", upper_title())
        job = self.app.reprocess.create("s")
        # Simulate a crash mid-replay: stage only page zero of a two-page run.
        with self.app.database.write() as connection:
            connection.execute(
                "UPDATE reprocess_jobs SET status = 'running', page_total = 2, next_ordinal = 0 WHERE id = ?",
                (job["id"],),
            )
        self.app.reprocess.stage_page(job["id"], 0, self.app.reprocess.profile(job["id"]), "json")
        with self.app.database.write() as connection:
            connection.execute(
                "UPDATE reprocess_jobs SET status = 'failed', error = 'source interrupted' WHERE id = ?",
                (job["id"],),
            )
        # Nothing was switched: the old rule result and published bytes remain.
        self.assertEqual(self.app.query.product("s", "x")["title"], "tea")
        self.assertEqual(self.app.query.product("s", "x")["rule_version"], 1)
        self.assertEqual(artifact.read_bytes(), original)
        self.assertEqual(self.app.publisher.publish("s")["row_count"], 1)

    def test_failed_reprocess_can_resume_from_durable_page_progress(self):
        self.app.profiles.set("s", self.policy())
        pages = {
            "": page("", [record("x", revision=4, title="tea")], "two"),
            "two": page("two", [record("y", revision=4, title="cup")]),
        }
        run = self.app.ingest.start("s", Client(pages))["id"]
        self.app.profiles.set("s", upper_title())
        job = self.app.reprocess.create("s", run_id=run)
        self.app.reprocess.claim(job["id"])
        profile = self.app.reprocess.profile(job["id"])
        self.app.reprocess.stage_page(job["id"], 0, profile, "json")
        self.app.reprocess.fail(job["id"], "interrupted")

        resumed = self.app.reprocess.execute(job["id"])
        self.assertEqual(resumed["status"], "completed")
        self.assertEqual(resumed["page_count"], 2)
        self.assertEqual(self.app.query.product("s", "x")["title"], "TEA")
        self.assertEqual(self.app.query.product("s", "y")["title"], "CUP")
        self.assertTrue(self.app.reprocess.reconcile(job["id"])["balanced"])

    # ------------------------------------------------ concurrent newer source version

    def test_newer_source_version_arriving_during_reprocess_wins_after_switch(self):
        self.app.profiles.set("s", self.policy())
        pages = {"": page("", [record("x", revision=4, title="tea")], "two"),
                 "two": page("two", [record("y", revision=4, title="cup")])}
        run = self.app.ingest.start("s", Client(pages))["id"]

        self.app.profiles.set("s", upper_title())
        job = self.app.reprocess.create("s", run_id=run)
        self.app.reprocess.claim(job["id"])
        profile = self.app.reprocess.profile(job["id"])
        self.app.reprocess.stage_page(job["id"], 0, profile, "json")

        # While the job is in flight a higher source revision arrives through the
        # normal path; it must be parsed with the incoming rule set.
        delta = self.app.ingest.apply_delta("s", [record("x", revision=9, title="fresh")])
        self.assertEqual(delta["accepted"], 1)
        self.assertEqual(self.app.query.product("s", "x")["revision"], 9)
        self.assertEqual(self.app.query.product("s", "x")["title"], "FRESH")

        self.app.reprocess.stage(job["id"])
        job = self.app.reprocess.switch(job["id"])
        self.assertEqual(job["status"], "completed")
        # The old archive (revision 4) must never roll the live revision back.
        product = self.app.query.product("s", "x")
        self.assertEqual(product["revision"], 9)
        self.assertEqual(product["title"], "FRESH")
        self.assertEqual(product["rule_version"], 2)
        # The replay records its own observation of revision 4 without overwriting.
        observations = {(item["revision"], item["rule_version"])
                        for item in self.app.query.history("s", "x")}
        self.assertIn((9, 2), observations)

    def test_runs_created_during_reprocess_use_target_rule_version(self):
        self.app.profiles.set("s", self.policy())
        self.app.ingest.apply_delta("s", [record("x", revision=3)])
        self.app.profiles.set("s", upper_title())
        job = self.app.reprocess.create("s")
        self.assertEqual(job["target_profile_version"], 2)
        # A delta committed while the job is active is frozen against the target.
        delta = self.app.ingest.apply_delta("s", [record("y", revision=3, title="late")])
        profile = self.app.provenance.run_profile(delta["id"])
        self.assertEqual(profile["profile_version"], 2)
        self.assertEqual(self.app.query.product("s", "y")["title"], "LATE")

    # --------------------------------------------------------- source conflicts

    def test_same_source_version_conflict_survives_rule_change(self):
        self.app.profiles.set("s", self.policy())
        tea = self.app.ingest.apply_delta("s", [record("x", revision=4, title="tea")])
        # A genuine source conflict: same revision, different raw content.
        result = self.app.ingest.apply_delta("s", [record("x", revision=4, title="coffee")])
        self.assertEqual(result["rejected"], 1)
        # Changing rules must not launder that conflict.  Replaying the archived
        # page that carried the conflicting observation rejects it again under
        # the new rule instead of admitting a second interpretation of the same
        # source revision.
        self.app.profiles.set("s", upper_title())
        job = self.app.reprocess.start("s", run_id=result["id"])
        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["conflicts"], 1)
        self.assertIn("conflicting", self.app.reprocess.rejects(job["id"])[0]["reason"])
        # The live product still reflects the first, non-conflicting observation.
        self.assertEqual(self.app.query.product("s", "x")["title"], "tea")
        self.assertEqual(self.app.query.product("s", "x")["rule_version"], 1)

    def test_rule_change_does_not_hide_a_source_conflict_at_switch_time(self):
        self.app.profiles.set("s", self.policy())
        tea = self.app.ingest.apply_delta("s", [record("x", revision=4, title="tea")])
        # Manually record a different raw observation of revision 4 under an
        # unrelated rule version, as a genuine source conflict would look.
        with self.app.database.write() as connection:
            connection.execute(
                """INSERT INTO product_versions
                (source_id, sku, revision, rule_version, fingerprint, source_fingerprint,
                 parser, payload, raw_record, first_run_id, first_job_id, observed_at)
                VALUES ('s','x',4,5,'other','conflicting-source', 'json','{}','{}',
                        ?, NULL, 'now')""",
                (tea["id"],),
            )
        self.app.profiles.set("s", upper_title())
        job = self.app.reprocess.start("s", run_id=tea["id"])
        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["conflicts"], 1)
        rejected = self.app.reprocess.rejects(job["id"])
        self.assertEqual(len(rejected), 1)
        self.assertIn("conflicting", rejected[0]["reason"])
        # The live product still reflects the original source content.
        self.assertEqual(self.app.query.product("s", "x")["title"], "tea")
        self.assertEqual(self.app.query.product("s", "x")["rule_version"], 1)

    def test_same_rule_conflict_is_still_rejected(self):
        self.app.profiles.set("s", self.policy())
        self.app.ingest.apply_delta("s", [record("x", revision=4, title="tea")])
        result = self.app.ingest.apply_delta("s", [record("x", revision=4, title="tea", price="9.00")])
        self.assertEqual(result["rejected"], 1)

    # ----------------------------------------------------------- atomic switching

    def test_switch_is_atomic_when_a_staged_record_conflicts(self):
        self.app.profiles.set("s", self.policy())
        pages = {"": page("", [record("x", revision=4, title="tea")], "two"),
                 "two": page("two", [record("y", revision=4, title="cup")])}
        run = self.app.ingest.start("s", Client(pages))["id"]
        self.app.profiles.set("s", upper_title())
        job = self.app.reprocess.execute(self.app.reprocess.create("s", run_id=run)["id"])
        before = {item["sku"]: item["title"] for item in self.app.query.products()["items"]}
        self.assertEqual(before, {"x": "TEA", "y": "CUP"})

        # A previously-recorded observation of y/revision 4 with different raw
        # content makes the replayed record a genuine source conflict.
        with self.app.database.write() as connection:
            connection.execute(
                """INSERT INTO product_versions
                (source_id, sku, revision, rule_version, fingerprint, source_fingerprint,
                 parser, payload, raw_record, first_run_id, first_job_id, observed_at)
                VALUES ('s','y',4,9,'deadbeef','different', 'json','{}','{}',
                        ?, NULL, 'now')""",
                (run,),
            )
        conflicting = self.app.reprocess.execute(self.app.reprocess.create("s", run_id=run)["id"])
        # x replayed and switched; y's conflicting record was quarantined and
        # the live product keeps its original source content rather than being
        # dropped or overwritten.
        after = {item["sku"]: item["title"] for item in self.app.query.products()["items"]}
        self.assertEqual(after, {"x": "TEA", "y": "CUP"})
        self.assertEqual(conflicting["conflicts"], 1)
        self.assertEqual(conflicting["status"], "completed")

    def test_switch_refuses_an_incomplete_job(self):
        self.app.profiles.set("s", self.policy())
        pages = {"": page("", [record("x", revision=4)], "two"),
                 "two": page("two", [record("y", revision=4)])}
        run = self.app.ingest.start("s", Client(pages))["id"]
        self.app.profiles.set("s", upper_title())
        job = self.app.reprocess.create("s", run_id=run)
        with self.assertRaises(Conflict):
            self.app.reprocess.switch(job["id"])
        self.assertEqual(self.app.reprocess.get(job["id"])["status"], "pending")

    # -------------------------------------------------- publication and downstream

    def test_publication_and_consumer_use_switched_effective_result(self):
        self.app.profiles.set("s", self.policy())
        self.app.ingest.apply_delta("s", [record("x", revision=3, title="tea")])
        first = self.app.publisher.publish("s")
        sink = DatasetConsumer(self.root / "consumer.sqlite")
        sink.apply(self.app.publisher, first["id"])
        self.assertEqual(sink.query()[0]["title"], "tea")

        self.app.profiles.set("s", upper_title())
        self.app.reprocess.start("s")
        second = self.app.publisher.publish("s")
        self.assertEqual(second["row_count"], 1)
        result = sink.apply(self.app.publisher, second["id"])
        self.assertFalse(result["replayed"])
        self.assertEqual(sink.query()[0]["title"], "TEA")
        # The first artifact stays immutable.
        self.assertEqual(
            json.loads(self.app.publisher.artifact(first["id"]).read_text())["title"], "tea"
        )

    def test_reprocess_recovers_rows_rejected_under_the_old_rule(self):
        # The old rules map price from a field that is missing, so every row is
        # quarantined; the archive still holds the raw records.
        self.app.profiles.set("s", {"fields": {
            "sku": "sku", "revision": "revision", "price": "missing", "stock": "stock",
            "title": "title",
        }})
        run = self.app.ingest.apply_delta("s", [record("x", revision=3, title="tea")])
        self.assertEqual((run["accepted"], run["rejected"]), (0, 1))
        self.assertEqual(self.app.query.products()["total"], 0)
        # Corrected rules now read price from the right field; replaying the
        # same source revision admits the row without a source-side conflict.
        self.app.profiles.set("s", self.policy())
        job = self.app.reprocess.create("s", run_id=run["id"])
        # The original run rejected the row; the replayed page under the fixed
        # rule no longer carries a rejection.
        self.assertEqual(len(self.app.runs.rejects(run["id"])), 1)
        self.assertIsNone(self.app.reprocess.page(job["id"], 0)["records"][0]["rejection"])
        job = self.app.reprocess.execute(job["id"])
        self.assertEqual(job["status"], "completed")
        self.assertEqual(job["inserted"], 1)
        product = self.app.query.product("s", "x")
        self.assertEqual(product["price_cents"], 250)
        self.assertEqual(product["rule_version"], 2)

    def test_unexpected_switch_failure_does_not_wedge_the_job(self):
        self.app.profiles.set("s", self.policy())
        self.app.ingest.apply_delta("s", [record("x", revision=3, title="tea")])
        self.app.profiles.set("s", upper_title())
        job = self.app.reprocess.create("s")
        self.app.reprocess.claim(job["id"])
        self.app.reprocess.stage(job["id"])
        original = self.app.reprocess.merger.apply_reprocess
        self.app.reprocess.merger.apply_reprocess = lambda *a, **k: (_ for _ in ()).throw(
            RuntimeError("unexpected disk failure")
        )
        self.addCleanup(setattr, self.app.reprocess.merger, "apply_reprocess", original)
        from catalogsync.errors import CatalogError
        with self.assertRaises(CatalogError):
            self.app.reprocess.switch(job["id"])
        self.app.reprocess.merger.apply_reprocess = original
        failed = self.app.reprocess.get(job["id"])
        self.assertEqual(failed["status"], "failed")
        self.assertTrue(failed["error"])
        self.assertEqual(self.app.query.product("s", "x")["title"], "tea")
        resumed = self.app.reprocess.execute(job["id"])
        self.assertEqual(resumed["status"], "completed")
        self.assertEqual(self.app.query.product("s", "x")["title"], "TEA")

    def test_reprocess_requires_a_completed_base_run(self):
        pages = {"": page("", [record("x", revision=4)], "two"), "two": page("two", [record("y", revision=4)])}
        client = Client(pages, {"two"})
        from catalogsync.errors import FetchError
        with self.assertRaises(FetchError):
            self.app.ingest.start("s", client)
        failed = self.app.runs.list()[0]
        with self.assertRaises(Conflict):
            self.app.reprocess.start("s", run_id=failed["id"])

    def test_reprocess_unknown_profile_version_is_rejected(self):
        self.app.profiles.set("s", self.policy())
        self.app.ingest.apply_delta("s", [record("x", revision=3)])
        with self.assertRaises(NotFound):
            self.app.reprocess.start("s", profile_version=99)

    def test_reprocess_archives_are_verified_against_the_base_run(self):
        self.app.profiles.set("s", self.policy())
        pages = {"": page("", [record("x", revision=4)], "two"), "two": page("two", [record("y", revision=4)])}
        run = self.app.ingest.start("s", Client(pages))["id"]
        archived = self.app.runs.pages(run)
        path = self.app.ingest.archive.root / archived[0]["archive_path"]
        path.write_bytes(b'{"tampered": true}')
        self.app.profiles.set("s", upper_title())
        with self.assertRaises(IntegrityError):
            self.app.reprocess.start("s", run_id=run)

    def test_reprocess_page_trace_reports_new_interpretation(self):
        self.app.profiles.set("s", self.policy())
        run = self.app.ingest.apply_delta("s", [record("x", revision=3, title="tea")])
        self.app.profiles.set("s", upper_title())
        job = self.app.reprocess.start("s", run_id=run["id"])
        trace = self.app.reprocess.page(job["id"], 0)
        self.assertEqual(trace["profile"]["version"], 2)
        self.assertEqual(trace["records"][0]["raw"]["title"], "tea")

    def test_cancelled_reprocess_never_switches(self):
        self.app.profiles.set("s", self.policy())
        self.app.ingest.apply_delta("s", [record("x", revision=3, title="tea")])
        self.app.profiles.set("s", upper_title())
        job = self.app.reprocess.create("s")
        self.app.reprocess.cancel(job["id"])
        with self.assertRaises(Conflict):
            self.app.reprocess.execute(job["id"])
        self.assertEqual(self.app.query.product("s", "x")["title"], "tea")
        self.assertEqual(self.app.reprocess.get(job["id"])["status"], "cancelled")

    def test_only_one_active_reprocess_per_source(self):
        self.app.profiles.set("s", self.policy())
        self.app.ingest.apply_delta("s", [record("x", revision=3)])
        self.app.profiles.set("s", upper_title())
        first = self.app.reprocess.create("s")
        with self.assertRaises(Conflict):
            self.app.reprocess.create("s")
        self.app.reprocess.cancel(first["id"])
        self.app.reprocess.create("s")



if __name__ == "__main__":
    unittest.main()
