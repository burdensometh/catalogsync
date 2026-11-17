"""Integration fault regressions for row-level bad data, page-level protocol
errors and interrupted commits.

These tests deliberately do NOT treat the production implementation's output as
the expected result.  ``reference`` is an independent, re-implemented model of
the documented contract (README "基线行为" / "来源协议").  Every expectation
below is computed by that model from the test's own inputs, then compared against
the durable state of the real application: version results, rejected rows, page
progress, fetch sequences and the archived raw evidence.

Tests that expose current defects are written against the contract and left
failing; they are marked ``DEFECT`` with the minimal reproducing input and the
observed current behavior in the docstring.  Business code must not be changed
just to turn them green.
"""

from decimal import Decimal, InvalidOperation
from pathlib import Path
import hashlib
import json
import tempfile
import unittest

from catalogsync.app import Application
from catalogsync.errors import CatalogError, FetchError, ValidationError
from catalogsync.models import canonical

# Storage bound used by the contract checks below: SQLite columns are declared
# INTEGER, i.e. signed 64-bit.  A row carrying a value beyond this range must be
# quarantined as a bad row rather than take down the page it travels in.
SQLITE_INTEGER_MAX = 2**63 - 1


# --------------------------------------------------------------------------
# Deterministic test inputs and a controllable, fault-injecting source client
# --------------------------------------------------------------------------

def record(sku="x", revision=1, **changes):
    value = {"sku": sku, "revision": revision, "title": sku,
             "price": "2.50", "stock": 3}
    value.update(changes)
    return value


def page_bytes(cursor, records, next_cursor=None, snapshot="snap", boundary=10):
    return json.dumps({
        "snapshot_id": snapshot, "boundary": boundary, "cursor": cursor,
        "next_cursor": next_cursor, "records": records,
    }).encode()


class ScriptedClient:
    """Deterministic source: fixed raw bytes per cursor plus injectable faults.

    ``fail_cursors`` make fetch raise a transient FetchError (recoverable on
    resume); ``protocol_bad`` replaces the bytes with malformed protocol data.
    Every requested cursor is recorded so the fetch sequence itself is asserted.
    """

    def __init__(self, pages, fail_cursors=(), protocol_bad=None):
        self.pages = dict(pages)
        self.fail_cursors = set(fail_cursors)
        self.protocol_bad = dict(protocol_bad or {})
        self.requests = []

    def fetch(self, cursor):
        self.requests.append(cursor)
        if cursor in self.fail_cursors:
            raise FetchError("source temporarily unavailable")
        if cursor in self.protocol_bad:
            return self.protocol_bad[cursor]
        return self.pages[cursor]


# --------------------------------------------------------------------------
# Independent reference oracle
#
# Re-implemented from the documented contract only.  It imports nothing from the
# ingestion/merge implementation and never queries the application database.
# --------------------------------------------------------------------------

class ReferenceError(Exception):
    """A row-level rejection in the reference model."""


def reference_parse_product(raw, boundary):
    """Validate one source record independently; raise ReferenceError if bad."""
    if not isinstance(raw, dict):
        raise ReferenceError("product must be an object")
    currency = raw.get("currency", "CNY")
    if currency not in {"CNY", "USD", "EUR", "GBP", "JPY"}:
        raise ReferenceError("unsupported currency")
    active = raw.get("active", True)
    if not isinstance(active, bool):
        raise ReferenceError("active must be a boolean")
    attributes = raw.get("attributes", {})
    if not isinstance(attributes, dict):
        raise ReferenceError("attributes must be an object")
    sku = raw.get("sku")
    if not isinstance(sku, str) or not sku.strip():
        raise ReferenceError("sku required")
    revision = raw.get("revision")
    if isinstance(revision, bool) or not isinstance(revision, int):
        raise ReferenceError("revision must be an integer")
    if revision < 1:
        raise ReferenceError("revision must be >= 1")
    try:
        amount = Decimal(str(raw.get("price")))
    except (InvalidOperation, ValueError):
        raise ReferenceError("price must be a decimal amount")
    if not amount.is_finite() or amount < 0:
        raise ReferenceError("price must be finite and nonnegative")
    cents = amount * 100
    if cents != cents.to_integral_value():
        raise ReferenceError("price cannot have fractions of a cent")
    if cents > 10**12:
        raise ReferenceError("price exceeds supported range")
    stock = raw.get("stock", 0)
    if isinstance(stock, bool) or not isinstance(stock, int) or stock < 0:
        raise ReferenceError("stock must be a nonnegative integer")
    # Contract: every value that lands in a storage INTEGER must fit.
    for name, value in (("revision", revision), ("stock", stock),
                        ("price_cents", int(cents))):
        if not -(2**63) <= value <= SQLITE_INTEGER_MAX:
            raise ReferenceError(f"{name} exceeds supported storage range")
    if not isinstance(raw.get("title", sku), str) or not str(raw.get("title", sku)).strip():
        raise ReferenceError("title is required")
    if revision > boundary:
        raise ReferenceError("record revision exceeds snapshot boundary")
    return {
        "sku": sku, "revision": revision,
        "title": str(raw["title"]) if "title" in raw else sku,
        "category": str(raw.get("category", "uncategorized")),
        "currency": currency, "price_cents": int(cents), "stock": stock,
        "active": active, "attributes": attributes,
    }


def reference_fingerprint(product):
    return hashlib.sha256(canonical(product).encode("utf-8")).hexdigest()


class ReferenceCatalog:
    """Minimal independent model of version merge + run/page/reject state."""

    def __init__(self):
        # (source, sku) -> current product dict
        self.products = {}
        # (source, sku, revision) -> fingerprint, for same-version conflict checks
        self.versions = {}
        self.history = {}  # (source, sku) -> set of observed revisions
        self.rejects = []  # (cursor, ordinal, raw)
        self.run = {
            "status": "pending", "page_count": 0, "accepted": 0, "rejected": 0,
            "inserted": 0, "updated": 0, "unchanged": 0, "stale": 0,
            "next_cursor": "", "snapshot_id": None, "boundary": None,
        }
        self.pages = []  # (cursor, next_cursor, record_count, accepted, rejected)
        self.snapshot_identity = None

    def merge_row(self, source, product):
        """Apply documented merge semantics; return the outcome label."""
        key = (source, product["sku"])
        fp = reference_fingerprint(product)
        version_key = (source, product["sku"], product["revision"])
        if version_key in self.versions and self.versions[version_key] != fp:
            raise ReferenceError("same product revision has conflicting content")
        self.versions.setdefault(version_key, fp)
        current = self.products.get(key)
        self.history.setdefault(key, set()).add(product["revision"])
        if current is not None:
            if current["revision"] > product["revision"]:
                return "stale"
            if current["revision"] == product["revision"]:
                return "unchanged"
        outcome = "updated" if current is not None else "inserted"
        self.products[key] = product
        return outcome

    def apply_page(self, source, envelope, cursor):
        # Page-level protocol rules are validated by the caller; here we model
        # the all-or-nothing boundary: any non-row error aborts the whole page.
        if self.snapshot_identity is None:
            self.snapshot_identity = (envelope["snapshot_id"], envelope["boundary"])
        elif self.snapshot_identity != (envelope["snapshot_id"], envelope["boundary"]):
            raise ReferenceError("snapshot identity changed during pagination")
        counts = {"accepted": 0, "rejected": 0, "inserted": 0,
                  "updated": 0, "unchanged": 0, "stale": 0}
        page_rejects = []
        for ordinal, raw in enumerate(envelope["records"]):
            try:
                product = reference_parse_product(raw, envelope["boundary"])
                outcome = self.merge_row(source, product)
            except ReferenceError as error:
                counts["rejected"] += 1
                page_rejects.append((cursor, ordinal, raw, str(error)))
            else:
                counts["accepted"] += 1
                counts[outcome] += 1
        # A page commits atomically: rows, rejects, page row and run progress
        # become visible together.
        self.rejects.extend(page_rejects)
        self.pages.append((cursor, envelope["next_cursor"],
                           len(envelope["records"]),
                           counts["accepted"], counts["rejected"]))
        for name, value in counts.items():
            self.run[name] += value
        self.run["page_count"] += 1
        self.run["next_cursor"] = envelope["next_cursor"]
        self.run["snapshot_id"], self.run["boundary"] = self.snapshot_identity

    def complete(self):
        assert self.run["next_cursor"] is None
        self.run["status"] = "completed"

    def fail(self):
        self.run["status"] = "failed"

    def expected_product_map(self, source):
        return {sku: product for (src, sku), product in self.products.items()
                if src == source}

    def expected_history(self, source, sku):
        return sorted(self.history.get((source, sku), set()))


def simulate_paged(source, envelopes, fail_before_cursors=()):
    """Drive the reference model through a paged run with transient failures.

    ``envelopes`` maps cursor -> decoded envelope dict.  A cursor listed in
    ``fail_before_cursors`` fails before its page is applied, then succeeds on
    the retry.  Returns (catalog, expected_request_sequence).
    """
    catalog = ReferenceCatalog()
    requests = []
    cursor = ""
    failed_once = set()
    while cursor is not None:
        requests.append(cursor)
        if cursor in fail_before_cursors and cursor not in failed_once:
            failed_once.add(cursor)
            catalog.fail()
            # On resume the same cursor is re-requested; nothing from the
            # failed attempt was committed.
            continue
        envelope = envelopes[cursor]
        catalog.apply_page(source, envelope, cursor)
        cursor = envelope["next_cursor"]
    catalog.complete()
    return catalog, requests


def simulate_delta(source, records, boundary):
    """Reference model for a single-page direct delta entry."""
    catalog = ReferenceCatalog()
    envelope = {"snapshot_id": "delta", "boundary": boundary, "cursor": "",
                "next_cursor": None, "records": records}
    catalog.apply_page(source, envelope, "")
    catalog.complete()
    return catalog


def delta_boundary(records):
    """Independently compute the documented delta boundary (max valid rev)."""
    revisions = []
    for raw in records:
        try:
            revisions.append(reference_parse_product(raw, SQLITE_INTEGER_MAX)["revision"])
        except ReferenceError:
            continue
    return max(revisions, default=1)


# --------------------------------------------------------------------------
# Durable-state readers and contract assertions (no expected values borrowed
# from the implementation: every comparison target comes from the oracle)
# --------------------------------------------------------------------------

class FaultRegressionBase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "source").mkdir()
        self.app = Application(self.root / "state")
        self.app.registry.register("s", "Supplier", "file",
                                   str(self.root / "source"))

    def actual_products(self):
        with self.app.database.read() as connection:
            rows = connection.execute(
                "SELECT sku,revision,title,category,currency,price_cents,stock,"
                "active,attributes,fingerprint FROM products ORDER BY sku"
            ).fetchall()
        result = {}
        for row in rows:
            value = dict(row)
            value["active"] = bool(value["active"])
            value["attributes"] = json.loads(value["attributes"])
            fp = value.pop("fingerprint")
            result[value["sku"]] = (value, fp)
        return result

    def actual_pages(self):
        with self.app.database.read() as connection:
            return [tuple(row) for row in connection.execute(
                "SELECT cursor,next_cursor,record_count,accepted,rejected "
                "FROM pages ORDER BY committed_at, cursor")]

    def actual_rejects(self):
        with self.app.database.read() as connection:
            return [tuple(row) for row in connection.execute(
                "SELECT cursor,ordinal,raw_record,reason FROM rejects ORDER BY id")]

    def actual_versions(self, sku):
        with self.app.database.read() as connection:
            return sorted(row[0] for row in connection.execute(
                "SELECT revision FROM product_versions WHERE source_id='s' AND sku=? "
                "ORDER BY revision", (sku,)))

    def actual_run(self):
        run = self.app.runs.list()[0]
        return {name: run[name] for name in (
            "status", "page_count", "accepted", "rejected", "inserted",
            "updated", "unchanged", "stale", "next_cursor",
            "snapshot_id", "boundary")}

    def assert_matches_reference(self, reference, *, request_sequence=None,
                                 actual_requests=None):
        """Compare every durable outcome against the independent oracle."""
        # --- run-level counters and progress ---
        actual = self.actual_run()
        for name in ("page_count", "accepted", "rejected", "inserted",
                     "updated", "unchanged", "stale"):
            self.assertEqual(
                actual[name], reference.run[name],
                f"run counter {name!r}: actual={actual[name]} "
                f"reference={reference.run[name]}")
        self.assertEqual(actual["status"], reference.run["status"])
        self.assertEqual(
            actual["next_cursor"],
            None if reference.run["next_cursor"] is None else reference.run["next_cursor"])

        # --- page progress rows ---
        self.assertEqual(self.actual_pages(), reference.pages)

        # --- current version of every product, independently fingerprinted ---
        actual_products = self.actual_products()
        expected = reference.expected_product_map("s")
        self.assertEqual(
            sorted(actual_products), sorted(expected),
            "committed product SKU set differs from reference")
        for sku, product in expected.items():
            stored, stored_fp = actual_products[sku]
            expected_fp = reference_fingerprint(product)
            self.assertEqual(stored_fp, expected_fp,
                             f"stored fingerprint for {sku!r} is not the "
                             "canonical payload fingerprint")
            self.assertEqual(stored, product,
                             f"stored payload for {sku!r} differs from reference")
            self.assertEqual(self.actual_versions(sku),
                             reference.expected_history("s", sku),
                             f"version history for {sku!r} differs from reference")

        # --- rejected rows: cursor/ordinal/raw evidence, reason must be text ---
        actual_rejects = self.actual_rejects()
        self.assertEqual(
            [(cursor, ordinal) for cursor, ordinal, _, _ in actual_rejects],
            [(cursor, ordinal) for cursor, ordinal, _, _ in reference.rejects],
            "rejected (cursor, ordinal) coordinates differ from reference")
        for (cursor, ordinal, raw_record, reason), ref in zip(
                actual_rejects, reference.rejects):
            self.assertEqual(json.loads(raw_record), ref[2],
                             "archived reject raw record differs from source row")
            self.assertIsInstance(reason, str)
            self.assertTrue(reason, "rejection reason must be nonempty")

        # --- fetch sequence (pagination order + resume behavior) ---
        if request_sequence is not None:
            self.assertEqual(actual_requests, request_sequence)

    def assert_archive_is_raw_evidence(self, run_id, envelopes_by_cursor):
        """Every committed page's archive must hash back to the exact bytes the
        source returned, and re-decode to the original envelope."""
        for page_row in self.app.runs.pages(run_id):
            raw = self.app.ingest.archive.read(
                page_row["archive_path"], page_row["raw_sha256"])
            self.assertEqual(
                hashlib.sha256(raw).hexdigest(), page_row["raw_sha256"])
            envelope = json.loads(raw.decode("utf-8"))
            self.assertEqual(envelope, envelopes_by_cursor[page_row["cursor"]])


# --------------------------------------------------------------------------
# Scenario A: out-of-order revisions and duplicates across paged collection
# and the direct delta entry, checked entirely against the reference oracle.
# --------------------------------------------------------------------------

class OutOfOrderAndDuplicateTest(FaultRegressionBase):
    def build_paged_envelopes(self):
        # Page 1: rev 2 first, then older rev 1 for another sku (out of order).
        # Page 2: same sku at an equal revision with identical content (repeat),
        # then an equal revision with conflicting content (must be rejected),
        # plus a structurally bad row that must not affect its neighbours.
        # Page 3: stale revision observed after a newer one (stale, still kept).
        p1 = page_bytes("", [record("a", 2), record("b", 1)], "two")
        p2 = page_bytes("two", [
            record("a", 2),                       # unchanged repeat
            record("a", 2, price="9.99"),         # same rev, different content
            record("bad", price="NaN"),           # row-level bad data
        ], "three")
        p3 = page_bytes("three", [
            record("b", 1),                       # stale repeat of b
            record("c", 3),                       # fresh insert
        ])
        return {"": json.loads(p1), "two": json.loads(p2),
                "three": json.loads(p3)}, {"": p1, "two": p2, "three": p3}

    def test_paged_collection_matches_independent_reference(self):
        envelopes, raw = self.build_paged_envelopes()
        reference, requests = simulate_paged("s", envelopes)
        client = ScriptedClient(raw)
        run = self.app.ingest.start("s", client)
        self.assertEqual(run["status"], "completed")
        self.assert_matches_reference(
            reference, request_sequence=requests, actual_requests=client.requests)
        self.assert_archive_is_raw_evidence(run["id"], envelopes)
        self.assertTrue(self.app.diagnostics.check(True)["ok"])

    def test_direct_delta_matches_independent_reference(self):
        records = [
            record("a", 3),
            record("b", 1),
            record("a", 2),                        # stale relative to rev 3
            record("a", 3),                        # unchanged repeat
            record("a", 3, price="9.99"),          # conflicting same revision
            record("bad", price="NaN"),            # row-level bad data
        ]
        boundary = delta_boundary(records)
        reference = simulate_delta("s", records, boundary)
        run = self.app.ingest.apply_delta("s", records)
        self.assertEqual(run["status"], "completed")
        self.assertEqual(run["boundary"], boundary)
        self.assert_matches_reference(reference)
        envelope = {"snapshot_id": run["snapshot_id"], "boundary": boundary,
                    "cursor": "", "next_cursor": None, "records": records}
        self.assert_archive_is_raw_evidence(run["id"], {"": envelope})
        self.assertTrue(self.app.diagnostics.check(True)["ok"])

    def test_paged_and_delta_converge_on_same_version_outcome(self):
        # Feed the identical record stream through both entry points against
        # separate homes and prove the oracle says they end at the same state.
        stream = [record("a", 2), record("b", 1), record("a", 2),
                  record("b", 3), record("bad", price="NaN")]
        boundary = delta_boundary(stream)
        delta_ref = simulate_delta("s", stream, boundary)

        paged_envelope = {"snapshot_id": "snap", "boundary": boundary,
                          "cursor": "", "next_cursor": None, "records": stream}
        paged_ref, _ = simulate_paged("s", {"": paged_envelope})

        # The two independent simulations must agree on product versions.
        self.assertEqual(paged_ref.expected_product_map("s"),
                         delta_ref.expected_product_map("s"))

        run = self.app.ingest.apply_delta("s", stream)
        self.assert_matches_reference(delta_ref)
        self.assertEqual(run["accepted"] + run["rejected"], len(stream))


# --------------------------------------------------------------------------
# Scenario B: a bad row whose integer exceeds the storage range must be
# isolated while the good rows sharing its page still commit.
# --------------------------------------------------------------------------

class OutOfRangeIntegerIsolationTest(FaultRegressionBase):
    def test_paged_bad_integer_row_is_quarantined_not_page_fatal(self):
        """DEFECT (current behavior): OverflowError escapes the per-row
        ValidationError handler in Ingestor.commit_page, so the whole page
        transaction rolls back, the run fails and the good row is never stored.
        Contract: bad row -> rejects; good row -> product; run completes.
        """
        rows = [record("good", 1), record("huge-stock", 1, stock=SQLITE_INTEGER_MAX + 1)]
        envelope = {"snapshot_id": "snap", "boundary": 10, "cursor": "",
                    "next_cursor": None, "records": rows}
        reference, _ = simulate_paged("s", {"": envelope})
        client = ScriptedClient({"": page_bytes("", rows)})
        try:
            run = self.app.ingest.start("s", client)
        except (CatalogError, OverflowError, ValueError):
            run = self.app.runs.list()[0]
            self.fail(
                "out-of-range integer row must be isolated, but ingestion "
                f"aborted the whole page; run status={run['status']!r} "
                f"error={run['error']!r}")
        self.assert_matches_reference(reference, actual_requests=client.requests)
        self.assertEqual(self.actual_products()["good"][0]["stock"], 3)
        self.assertTrue(self.app.diagnostics.check(True)["ok"])

    def test_delta_out_of_range_revision_row_is_quarantined(self):
        """DEFECT (current behavior): apply_delta raises OverflowError and the
        delta run fails; the valid sibling record is lost.  Contract: the
        oversized revision is a bad row and is rejected on its own."""
        rows = [record("good", 1), record("huge-rev", SQLITE_INTEGER_MAX + 1)]
        boundary = delta_boundary(rows)
        reference = simulate_delta("s", rows, boundary)
        try:
            run = self.app.ingest.apply_delta("s", rows)
        except (CatalogError, OverflowError, ValueError) as error:
            self.fail(
                "out-of-range revision row must be isolated, but apply_delta "
                f"aborted: {type(error).__name__}: {error}")
        self.assert_matches_reference(reference)
        self.assertTrue(self.app.diagnostics.check(True)["ok"])

    def test_paged_boundary_out_of_range_fails_at_page_level_without_partial_commit(self):
        """A page envelope whose boundary itself exceeds storage range is a
        page-level problem: nothing from that page may partially commit, while
        previously committed pages stay intact and the run remains resumable."""
        first = page_bytes("", [record("a", 1)], "two")
        bad_envelope = {"snapshot_id": "snap", "boundary": SQLITE_INTEGER_MAX + 1,
                        "cursor": "two", "next_cursor": None,
                        "records": [record("b", 1)]}
        client = ScriptedClient({
            "": first,
            "two": json.dumps(bad_envelope).encode("utf-8"),
        })
        with self.assertRaises((ValidationError, CatalogError, OverflowError)):
            self.app.ingest.start("s", client)
        run = self.app.runs.list()[0]
        self.assertEqual(run["status"], "failed")
        # First page durably committed; the bad second page left no trace.
        self.assertEqual(run["page_count"], 1)
        self.assertEqual(run["next_cursor"], "two")
        self.assertEqual(self.actual_pages(), [("", "two", 1, 1, 0)])
        self.assertEqual(sorted(self.actual_products()), ["a"])
        self.assertEqual(self.actual_rejects(), [])

    def test_inbox_out_of_range_row_is_quarantined_without_stalling_batch(self):
        """DEFECT (current behavior): inbox.consume drives Ingestor through the
        same commit path, so the oversized integer aborts the whole run.  The
        inbox batch is left 'reserved' and its events never reach a terminal
        state.  Contract: the bad event is recorded as rejected, its sibling is
        committed, and the batch reaches 'committed' in one consumption."""
        events = [
            {"id": "e-good", "record": record("good", 1)},
            {"id": "e-bad", "record": record("huge", SQLITE_INTEGER_MAX + 1)},
        ]
        submission = self.app.inbox.submit("s", events)
        self.assertEqual(submission["accepted"], 2)
        try:
            result = self.app.inbox.consume("s")
        except (CatalogError, OverflowError, ValueError) as error:
            self.fail(
                "out-of-range inbox event must be isolated, but consume aborted: "
                f"{type(error).__name__}: {error}")
        run = result["run"]
        self.assertEqual(run["status"], "completed")
        self.assertEqual((run["accepted"], run["rejected"]), (1, 1))
        status = self.app.inbox.status("s")
        self.assertEqual(status["events"], {"committed": 1, "rejected": 1})
        self.assertEqual(status["batches"][0]["status"], "committed")
        self.assertEqual(sorted(self.actual_products()), ["good"])
        self.assertEqual(len(self.actual_rejects()), 1)

    def test_nonfinite_float_row_is_quarantined_not_page_fatal(self):
        """DEFECT (current behavior): a float NaN price passes Decimal parsing
        but later blows up canonical()/SQLite binding with a raw ValueError,
        again escaping row isolation and taking the page down."""
        rows = [record("good", 1), record("nan", 1, price=float("nan"))]
        boundary = 10
        envelope = {"snapshot_id": "snap", "boundary": boundary, "cursor": "",
                    "next_cursor": None, "records": rows}
        reference = simulate_delta("s", rows, boundary)
        try:
            self.app.ingest.apply_delta("s", rows)
        except (CatalogError, OverflowError, ValueError) as error:
            self.fail(
                "nonfinite float row must be isolated, but apply_delta aborted: "
                f"{type(error).__name__}: {error}")
        self.assert_matches_reference(reference)


# --------------------------------------------------------------------------
# Scenario C: page-level protocol errors must never partially commit.
# --------------------------------------------------------------------------

class ProtocolPageAtomicityTest(FaultRegressionBase):
    def test_malformed_json_page_aborts_without_partial_commit(self):
        first = page_bytes("", [record("a", 1), record("b", 1)], "two")
        client = ScriptedClient(
            {"": first, "two": b"{this is not valid json"},
            protocol_bad={"two": b"{this is not valid json"})
        with self.assertRaises(ValidationError):
            self.app.ingest.start("s", client)
        run = self.app.runs.list()[0]
        self.assertEqual(run["status"], "failed")
        self.assertEqual(run["next_cursor"], "two")       # resumable cursor
        self.assertEqual(run["page_count"], 1)            # bad page not counted
        self.assertEqual(self.actual_pages(), [("", "two", 2, 2, 0)])
        self.assertEqual(sorted(self.actual_products()), ["a", "b"])
        self.assertEqual(self.actual_rejects(), [])
        # The failed page must not have created a page/reject row.
        with self.app.database.read() as connection:
            self.assertEqual(
                connection.execute("SELECT count(*) FROM pages WHERE cursor='two'")
                .fetchone()[0], 0)

    def test_snapshot_identity_change_aborts_without_partial_commit(self):
        first = page_bytes("", [record("a", 1)], "two")
        changed = page_bytes("two", [record("b", 1)], snapshot="different")
        client = ScriptedClient({"": first, "two": changed})
        with self.assertRaises(ValidationError):
            self.app.ingest.start("s", client)
        run = self.app.runs.list()[0]
        self.assertEqual(run["status"], "failed")
        self.assertEqual(run["page_count"], 1)
        self.assertEqual(run["next_cursor"], "two")
        self.assertEqual(sorted(self.actual_products()), ["a"])
        with self.app.database.read() as connection:
            self.assertEqual(
                connection.execute("SELECT count(*) FROM pages WHERE cursor='two'")
                .fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT count(*) FROM rejects").fetchone()[0], 0)

    def test_pagination_cycle_aborts_without_replaying_committed_page(self):
        first = page_bytes("", [record("a", 1)], "two")
        cycling = page_bytes("two", [], "two")          # self loop
        client = ScriptedClient({"": first, "two": cycling})
        with self.assertRaises(ValidationError):
            self.app.ingest.start("s", client)
        run = self.app.runs.list()[0]
        self.assertEqual(run["page_count"], 1)
        self.assertEqual(self.actual_pages(), [("", "two", 1, 1, 0)])
        # First page fetched once; the cycling page fetched once then rejected.
        self.assertEqual(client.requests, ["", "two"])


# --------------------------------------------------------------------------
# Scenario D: failures mid-run and mid-commit; recovery must resume exactly at
# the durable cursor and a commit-phase failure must roll the whole page back.
# --------------------------------------------------------------------------

class FailureRecoveryTest(FaultRegressionBase):
    def test_transient_fetch_failure_resumes_against_reference(self):
        p1 = page_bytes("", [record("a", 2), record("b", 1)], "two")
        p2 = page_bytes("two", [record("a", 3), record("c", 4)], "three")
        p3 = page_bytes("three", [record("b", 5)])
        envelopes = {"": json.loads(p1), "two": json.loads(p2),
                     "three": json.loads(p3)}
        reference, requests = simulate_paged(
            "s", envelopes, fail_before_cursors={"two"})
        client = ScriptedClient(
            {"": p1, "two": p2, "three": p3}, fail_cursors={"two"})
        with self.assertRaises(FetchError):
            self.app.ingest.start("s", client)
        failed_run = self.app.runs.list()[0]
        self.assertEqual(failed_run["status"], "failed")
        self.assertEqual(failed_run["next_cursor"], "two")
        self.assertEqual(failed_run["page_count"], 1)
        client.fail_cursors.clear()
        resumed = self.app.ingest.execute(failed_run["id"], client)
        self.assertEqual(resumed["status"], "completed")
        self.assert_matches_reference(
            reference, request_sequence=requests, actual_requests=client.requests)
        self.assertEqual(client.requests, ["", "two", "two", "three"])
        self.assert_archive_is_raw_evidence(resumed["id"], envelopes)
        self.assertTrue(self.app.diagnostics.check(True)["ok"])

    def test_commit_phase_failure_rolls_back_entire_page(self):
        # Inject a non-validation storage failure while processing the middle
        # row of a page.  The page must commit nothing: no products, no
        # rejects, no page row; the run is failed and still owns its cursor.
        original_merge = self.app.ingest.merger.merge
        def failing_merge(connection, source_id, run_id, product):
            if product.sku == "second":
                raise RuntimeError("simulated storage outage")
            return original_merge(connection, source_id, run_id, product)
        self.app.ingest.merger.merge = failing_merge
        client = ScriptedClient({"": page_bytes(
            "", [record("first"), record("second"), record("third")])})
        with self.assertRaises(CatalogError) as captured:
            self.app.ingest.start("s", client)
        self.assertIn("simulated storage outage", str(captured.exception))
        run = self.app.runs.list()[0]
        self.assertEqual(run["status"], "failed")
        self.assertEqual(run["next_cursor"], "")
        self.assertEqual(run["page_count"], 0)
        self.assertEqual(self.actual_products(), {})
        self.assertEqual(self.actual_pages(), [])
        self.assertEqual(self.actual_rejects(), [])
        # SQLite itself must still be internally consistent after the rollback.
        self.assertTrue(self.app.database.integrity()["ok"])

    def test_resume_after_commit_failure_reprocesses_full_page(self):
        # After the rollback above, retrying with the fault removed must apply
        # the complete page exactly once and match the oracle.
        client = ScriptedClient({"": page_bytes(
            "", [record("first"), record("second"), record("third")])})
        envelope = json.loads(client.pages[""])
        reference, requests = simulate_paged("s", {"": envelope})
        run = self.app.ingest.start("s", client)
        self.assertEqual(run["status"], "completed")
        self.assert_matches_reference(
            reference, request_sequence=requests, actual_requests=client.requests)
        self.assertEqual(client.requests, [""])


if __name__ == "__main__":
    unittest.main(verbosity=2)
