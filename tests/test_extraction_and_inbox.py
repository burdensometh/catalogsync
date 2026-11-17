from pathlib import Path
import json
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from catalogsync.api import serve
from catalogsync.app import Application
from catalogsync.consumer import DatasetConsumer
from catalogsync.errors import Conflict, ValidationError
from catalogsync.extraction import extract_page
from catalogsync.normalization import Profile
from catalogsync.worker import Collector

from test_ingestion import Client, page, record


class ExtendedTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.app = Application(self.root / "state")
        self.app.registry.register("s", "Supplier", "file", str(self.source))

    def test_nested_mapping_and_quality_quarantine(self):
        specification = {
            "fields": {
                "sku": {"path": "item.code", "steps": ["trim", "lower"]},
                "title": "item.name",
                "revision": {"path": "seq", "steps": ["integer"]},
                "price": {"path": "cents", "steps": ["minor_units"]},
                "stock": {"path": "qty", "steps": ["integer"]},
            },
            "defaults": {"currency": "CNY"},
            "quality": {"ranges": {"price": {"min": 1}}},
        }
        self.app.profiles.set("s", specification)
        rows = [
            {"item": {"code": " ABC ", "name": "Tea"}, "seq": "4", "cents": 250, "qty": "3"},
            {"item": {"code": " BAD ", "name": "Tea"}, "seq": "5", "cents": 10, "qty": "3"},
        ]
        result = self.app.ingest.apply_delta("s", rows)
        self.assertEqual((result["accepted"], result["rejected"]), (1, 1))
        self.assertEqual(self.app.query.product("s", "abc")["price_cents"], 250)

    def test_failed_run_uses_frozen_profile_after_source_changes(self):
        self.app.profiles.set("s", {"defaults": {"category": "old"}})
        client = Client({"": page("", [record()], "two"), "two": page("two", [record("y")])}, {"two"})
        from catalogsync.errors import FetchError
        with self.assertRaises(FetchError):
            self.app.ingest.start("s", client)
        run = self.app.runs.list()[0]
        self.app.profiles.set("s", {"defaults": {"category": "new"}})
        client.failures.clear()
        self.app.ingest.execute(run["id"], client)
        self.assertEqual(self.app.query.product("s", "y")["category"], "old")
        self.assertEqual(self.app.provenance.run_profile(run["id"])["profile_version"], 1)

    def test_html_listing_extraction_and_ingestion(self):
        html = b'''<html><head>
        <meta name="catalog:snapshot" content="html-1">
        <meta name="catalog:boundary" content="9">
        </head><body><article data-sku="web" data-revision="5" data-stock="3">
        <h2 data-field="title">Tea &amp; <b>Cup</b></h2>
        <span data-field="price">2.50</span></article></body></html>'''
        self.app.profiles.set("s", {"parser": "html"})
        result = self.app.ingest.start("s", Client({"": html}))
        self.assertEqual(result["accepted"], 1)
        self.assertEqual(self.app.query.product("s", "web")["title"], "Tea & Cup")
        trace = self.app.provenance.page(result["id"])
        self.assertEqual(trace["records"][0]["raw"]["sku"], "web")

    def test_jsonld_item_list(self):
        document = {"@type": "ItemList", "itemListElement": [{"item": {
            "@type": "Product", "sku": "ld", "catalogRevision": "3", "name": "Cup",
            "offers": {"price": "7.90", "priceCurrency": "CNY", "inventoryLevel": {"value": 8}},
        }}]}
        html = ('<meta name="catalog:snapshot" content="ld-1">'
                '<meta name="catalog:boundary" content="9">'
                '<script type="application/ld+json">' + json.dumps(document) + '</script>').encode()
        parsed = extract_page(html, "jsonld", "")
        self.assertEqual(parsed["records"][0]["sku"], "ld")
        self.assertEqual(parsed["records"][0]["revision"], 3)

    def test_inbox_duplicate_event_and_immutable_identity(self):
        event = {"id": "e-1", "record": record()}
        self.assertEqual(self.app.inbox.submit("s", [event])["accepted"], 1)
        self.assertEqual(self.app.inbox.submit("s", [event])["duplicates"], 1)
        with self.assertRaises(Conflict):
            self.app.inbox.submit("s", [{"id": "e-1", "record": record(revision=2)}])
        consumed = self.app.inbox.consume("s")
        self.assertEqual(consumed["run"]["inserted"], 1)
        self.assertEqual(self.app.inbox.status("s")["events"], {"committed": 1})
        self.assertTrue(self.app.inbox.consume("s")["empty"])

    def test_inbox_ack_recovery_does_not_reingest(self):
        self.app.inbox.submit("s", [{"id": "a", "record": record()}, {"id": "b", "record": record("bad", price="NaN")}])
        original = self.app.inbox.acknowledge
        self.app.inbox.acknowledge = lambda batch_id: (_ for _ in ()).throw(OSError("ack interrupted"))
        with self.assertRaises(OSError):
            self.app.inbox.consume("s")
        self.assertEqual(len(self.app.runs.list()), 1)
        self.app.inbox.acknowledge = original
        result = self.app.inbox.consume("s")
        self.assertEqual(len(self.app.runs.list()), 1)
        self.assertEqual(result["run"]["attempt"], 1)
        self.assertEqual(self.app.inbox.status("s")["events"], {"committed": 1, "rejected": 1})

    def test_materialized_sink_idempotence_snapshot_and_old_rejection(self):
        self.app.ingest.apply_delta("s", [record()])
        first = self.app.publisher.publish("s")
        sink = DatasetConsumer(self.root / "consumer.sqlite")
        self.assertFalse(sink.apply(self.app.publisher, first["id"])["replayed"])
        self.assertTrue(sink.apply(self.app.publisher, first["id"])["replayed"])
        self.app.ingest.apply_delta("s", [record(revision=2, active=False)])
        second = self.app.publisher.publish("s", "csv")
        sink.apply(self.app.publisher, second["id"])
        self.assertEqual(sink.query(), [])
        self.assertEqual(first["revision_vector"], {"s": 1})

    def test_consumer_rejects_older_unseen_publication(self):
        self.app.ingest.apply_delta("s", [record()])
        old = self.app.publisher.publish("s")
        self.app.ingest.apply_delta("s", [record(revision=2)])
        new = self.app.publisher.publish("s")
        sink = DatasetConsumer(self.root / "consumer.sqlite")
        sink.apply(self.app.publisher, new["id"])
        with self.assertRaises(Conflict):
            sink.apply(self.app.publisher, old["id"])
        self.assertEqual(sink.query()[0]["revision"], 2)

    def test_collector_uses_real_source_files(self):
        (self.source / "manifest.json").write_text(json.dumps({"pages": {"": "p.json"}}))
        (self.source / "p.json").write_bytes(page("", [record()]))
        result = Collector(self.app, 2, True).cycle()
        self.assertEqual(result["completed"], 1)
        self.assertEqual(result["results"][0]["publication"]["row_count"], 1)

    def test_profile_rejects_unknown_operation(self):
        with self.assertRaises(ValidationError):
            Profile({"fields": {"sku": {"steps": ["execute_code"]}}})

    def test_rejected_row_can_be_corrected_and_requeued_without_duplication(self):
        run = self.app.ingest.apply_delta("s", [record(price="NaN")])
        rejection = self.app.runs.rejects(run["id"])[0]
        first = self.app.rejections.requeue(rejection["id"], record(price="2.50"))
        second = self.app.rejections.requeue(rejection["id"], record(price="2.50"))
        self.assertEqual(first["submission"]["accepted"], 1)
        self.assertEqual(second["submission"]["duplicates"], 1)
        self.app.inbox.consume("s")
        self.assertEqual(self.app.query.product("s", "x")["price_cents"], 250)
        self.assertEqual(self.app.rejections.search()["total"], 1)

    def test_file_feed_waits_for_complete_line_and_keeps_restart_offset(self):
        path = self.root / "events.ndjson"
        event = json.dumps({"id": "one", "record": record()}).encode()
        path.write_bytes(event)
        self.app.file_feeds.register("tail", "s", path)
        partial = self.app.file_feeds.poll("tail")
        self.assertTrue(partial["waiting_for_newline"])
        self.assertEqual(partial["feed"]["byte_offset"], 0)
        with path.open("ab") as stream:
            stream.write(b"\n")
        complete = self.app.file_feeds.poll("tail")
        self.assertEqual(complete["accepted_events"], 1)
        restarted = Application(self.app.home)
        self.assertEqual(restarted.file_feeds.poll("tail")["lines"], 0)
        restarted.inbox.consume("s")
        self.assertEqual(restarted.query.products()["total"], 1)
        path.write_bytes(b"")
        with self.assertRaises(Conflict):
            restarted.file_feeds.poll("tail")

    def test_request_policy_rejects_invalid_limits_and_clamps_retry_after(self):
        from catalogsync.request_policy import OriginGate, RequestPolicy
        with self.assertRaises(ValidationError):
            RequestPolicy(requests_per_second=float("nan"))
        policy = RequestPolicy(maximum_retry_after=2)
        self.assertEqual(policy.retry_after("1000"), 2)
        self.assertEqual(policy.retry_after("nonsense"), 0)
        self.assertEqual(policy.retry_after("-2"), 0)
        gate = OriginGate(policy)
        with self.assertRaises(RuntimeError):
            with gate.permit():
                raise RuntimeError("request failed")
        with gate.permit():
            pass
        self.assertEqual(gate.metrics()["admitted"], 2)

    def test_http_api_uses_persisted_core_and_validation(self):
        server = serve(self.app.home, port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = "http://127.0.0.1:" + str(server.server_address[1])
            raw = json.dumps({"source_id": "s", "records": [record()]}).encode()
            request = Request(base + "/deltas", raw, {"Content-Type": "application/json"})
            with urlopen(request, timeout=3) as response:
                self.assertEqual(response.status, 201)
            with urlopen(base + "/products?sellable=true", timeout=3) as response:
                self.assertEqual(json.load(response)["total"], 1)
            with self.assertRaises(HTTPError) as error:
                urlopen(base + "/products?limit=-2", timeout=3)
            self.assertEqual(error.exception.code, 400)
            with urlopen(base + "/runs", timeout=3) as response:
                run_id = json.load(response)[0]["id"]
            with urlopen(base + "/runs/" + run_id + "/balance", timeout=3) as response:
                self.assertTrue(json.load(response)["balanced"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)


if __name__ == "__main__":
    unittest.main()
