from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from catalogsync.app import Application
from catalogsync.clients import HttpClient
from catalogsync.errors import FetchError, ValidationError
from catalogsync.request_policy import (
    OriginGate,
    RequestPolicy,
    SharedOriginGate,
    admission_status,
    gate_for,
)

from test_ingestion import page, record


ROOT = Path(__file__).resolve().parent.parent


def run_python(code, *arguments, timeout=30):
    return subprocess.run(
        [sys.executable, "-c", code, str(ROOT), *arguments],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


HOLD_SLOT = r"""
import sys, time
sys.path.insert(0, sys.argv[1])
from catalogsync.request_policy import RequestPolicy, gate_for
url, state = sys.argv[2], sys.argv[3]
policy = RequestPolicy(requests_per_second=50, burst=50, concurrency=1,
                       permit_timeout=5, maximum_retry_after=5)
gate = gate_for(url, policy=policy, state_dir=state)
with gate.permit():
    print("ready", flush=True)
    time.sleep(30)
"""

CONSUME_BURST = r"""
import sys
sys.path.insert(0, sys.argv[1])
from catalogsync.request_policy import RequestPolicy, gate_for
url, state, count = sys.argv[2], sys.argv[3], int(sys.argv[4])
policy = RequestPolicy(requests_per_second=5, burst=2, concurrency=4,
                       permit_timeout=5, maximum_retry_after=5)
gate = gate_for(url, policy=policy, state_dir=state)
for _ in range(count):
    with gate.permit():
        pass
print("done", flush=True)
"""

CONFLICTING_POLICY = r"""
import sys
sys.path.insert(0, sys.argv[1])
from catalogsync.request_policy import RequestPolicy, gate_for
gate_for(sys.argv[2], policy=RequestPolicy(requests_per_second=7), state_dir=sys.argv[3])
print("unexpected", flush=True)
"""

PULL_ONE = r"""
import sys
sys.path.insert(0, sys.argv[1])
from catalogsync.app import Application
from catalogsync.request_policy import RequestPolicy
policy = RequestPolicy(requests_per_second=1, burst=1, concurrency=2,
                       permit_timeout=20, maximum_retry_after=5)
app = Application(sys.argv[2], request_policy=policy)
app.registry.register(sys.argv[3], "Supplier", "http", sys.argv[4])
run = app.ingest.start(sys.argv[3])
print(run["status"], flush=True)
"""


class HttpFixture:
    """A tiny scripted supplier whose responses a test can change at runtime."""

    def __init__(self, responder):
        self.responder = responder
        self.calls = []
        self.lock = threading.Lock()
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
                cursor = query.get("cursor", [""])[0]
                with fixture.lock:
                    index = len(fixture.calls)
                    fixture.calls.append((time.monotonic(), cursor))
                status, headers, body = fixture.responder(cursor, index)
                self.send_response(status)
                for name, value in headers.items():
                    self.send_header(name, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.url = "http://127.0.0.1:%d" % self.server.server_address[1]

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)


class SharedAdmissionTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.state = self.root / "state"
        self.state.mkdir()

    def test_burst_and_rate_are_shared_across_executors(self):
        first = run_python(CONSUME_BURST, "http://shared.test", str(self.state), "2")
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(first.stdout.strip(), "done")
        gate = gate_for(
            "http://shared.test",
            policy=RequestPolicy(
                requests_per_second=5, burst=2, concurrency=4, maximum_retry_after=5
            ),
            state_dir=self.state,
        )
        started = time.monotonic()
        with gate.permit():
            pass
        elapsed = time.monotonic() - started
        # The other executor already drained the burst; a fifth-of-a-second
        # refill is required, proving the budget is shared rather than reset.
        self.assertGreaterEqual(elapsed, 0.12)
        self.assertLess(elapsed, 2.0)

    def test_retry_counts_against_budget_and_honours_retry_after(self):
        def responder(cursor, index):
            if index == 0:
                return 429, {"Retry-After": "1", "Content-Type": "application/json"}, b"{}"
            return 200, {"Content-Type": "application/json"}, page("", [record()])

        policy = RequestPolicy(
            requests_per_second=50, burst=50, concurrency=2,
            permit_timeout=5, maximum_retry_after=5,
        )
        with HttpFixture(responder) as fixture:
            client = HttpClient(fixture.url, attempts=2, policy=policy, state_dir=self.state)
            started = time.monotonic()
            raw = client.fetch("")
            elapsed = time.monotonic() - started
        self.assertEqual(raw, page("", [record()]))
        self.assertEqual(len(fixture.calls), 2)
        # The retry is a second admitted request and waits out the shared
        # Retry-After cooldown recorded by the first response.
        self.assertGreaterEqual(elapsed, 0.9)
        self.assertEqual(client.gate.metrics()["admitted"], 2)
        self.assertEqual(admission_status(self.state)[0]["admitted"], 2)

    def test_in_flight_slot_is_held_isolated_and_reclaimed_after_crash(self):
        holder = subprocess.Popen(
            [sys.executable, "-c", HOLD_SLOT, str(ROOT), "http://held.test", str(self.state)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            self.assertEqual(holder.stdout.readline().strip(), "ready")
            held = gate_for(
                "http://held.test",
                policy=RequestPolicy(
                    requests_per_second=50, burst=50, concurrency=1,
                    permit_timeout=0.4, maximum_retry_after=5,
                ),
                state_dir=self.state,
            )
            started = time.monotonic()
            with self.assertRaises(FetchError):
                with held.permit():
                    pass
            self.assertGreaterEqual(time.monotonic() - started, 0.35)

            other = gate_for(
                "http://unrelated.test",
                policy=RequestPolicy(
                    requests_per_second=50, burst=50, concurrency=1,
                    permit_timeout=0.4, maximum_retry_after=5,
                ),
                state_dir=self.state,
            )
            started = time.monotonic()
            with other.permit():
                pass
            self.assertLess(time.monotonic() - started, 0.3)

            holder.kill()
            holder.wait(timeout=5)
            started = time.monotonic()
            with held.permit():
                pass
            self.assertLess(time.monotonic() - started, 1.5)
        finally:
            if holder.poll() is None:
                holder.kill()
                holder.wait(timeout=5)
            holder.stdout.close()
            holder.stderr.close()

    def test_conflicting_policy_is_refused_before_any_request(self):
        calls = []

        def responder(cursor, index):
            calls.append(cursor)
            return 200, {"Content-Type": "application/json"}, page("", [record()])

        with HttpFixture(responder) as fixture:
            seeded = gate_for(
                fixture.url,
                policy=RequestPolicy(requests_per_second=1),
                state_dir=self.state,
            )
            self.assertIsInstance(seeded, SharedOriginGate)
            app = Application(self.state, request_policy=RequestPolicy(requests_per_second=9))
            app.registry.register("s", "Supplier", "http", fixture.url)
            with self.assertRaises(ValidationError):
                app.ingest.start("s")
            self.assertEqual(calls, [])
            # The refusal happens before any run exists, so no progress,
            # attempt or page can be attributed to the rejected request.
            self.assertEqual(app.runs.list(), [])

            conflict = run_python(CONFLICTING_POLICY, fixture.url, str(self.state))
            self.assertNotEqual(conflict.returncode, 0)
            self.assertIn("conflicting request policies", conflict.stderr)
            self.assertEqual(calls, [])

    def test_admission_timeout_does_not_advance_progress_and_resume_is_complete(self):
        pages = {
            "": page("", [record()], "two"),
            "two": page("two", [record("y", revision=2)]),
        }

        def responder(cursor, index):
            return 200, {"Content-Type": "application/json"}, pages[cursor]

        with HttpFixture(responder) as fixture:
            holder = subprocess.Popen(
                [sys.executable, "-c", HOLD_SLOT, str(ROOT), fixture.url, str(self.state)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            try:
                self.assertEqual(holder.stdout.readline().strip(), "ready")
                app = Application(
                    self.state,
                    request_policy=RequestPolicy(
                        requests_per_second=50, burst=50, concurrency=1,
                        permit_timeout=0.4, maximum_retry_after=5,
                    ),
                )
                app.registry.register("s", "Supplier", "http", fixture.url)
                with self.assertRaises(FetchError):
                    app.ingest.start("s")
                failed = app.runs.list()[0]
                self.assertEqual((failed["status"], failed["page_count"], failed["next_cursor"]), ("failed", 0, ""))
                self.assertEqual(fixture.calls, [])
            finally:
                holder.kill()
                holder.wait(timeout=5)
                holder.stdout.close()
                holder.stderr.close()

            resumed = app.ingest.execute(failed["id"])
            self.assertEqual(
                (resumed["status"], resumed["page_count"], resumed["accepted"]),
                ("completed", 2, 2),
            )
            publication = app.publisher.publish("s")
            self.assertEqual(publication["row_count"], 2)
            check = app.diagnostics.check(True)
            self.assertTrue(check["ok"], check["issues"])
            self.assertTrue(check["request_admission"])

    def test_two_collectors_on_one_home_respect_one_budget(self):
        # Two independent executors take turns pulling the same source. The
        # second starts with the first burst token already spent, so its first
        # request must wait for the shared one-per-second refill.
        pages = {"": page("", [record()], None)}

        def responder(cursor, index):
            return 200, {"Content-Type": "application/json"}, pages[cursor]

        with HttpFixture(responder) as fixture:
            first = subprocess.Popen(
                [sys.executable, "-c", PULL_ONE, str(ROOT), str(self.state), "s", fixture.url],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            first_out, first_err = first.communicate(timeout=30)
            self.assertEqual(first.returncode, 0, first_err)
            self.assertEqual(first_out.strip(), "completed")
            started = time.monotonic()
            second = subprocess.Popen(
                [sys.executable, "-c", PULL_ONE, str(ROOT), str(self.state), "s", fixture.url],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            second_out, second_err = second.communicate(timeout=30)
            elapsed = time.monotonic() - started
            self.assertEqual(second.returncode, 0, second_err)
            # Re-claiming the source happens only after the shared token
            # refills; a per-process budget would return at once.
            self.assertGreaterEqual(elapsed, 0.5)
            self.assertEqual(len(fixture.calls), 2)
            status = admission_status(self.state)[0]
            self.assertEqual(status["admitted"], 2)
            self.assertEqual(status["policy"]["requests_per_second"], 1)

    def test_transient_failure_keeps_committed_pages_for_full_resume(self):
        healthy = {"": page("", [record()], "two"), "two": page("two", [record("y")])}
        broken = {"two": True}

        def responder(cursor, index):
            if cursor in broken:
                return 500, {"Content-Type": "application/json"}, b"{}"
            return 200, {"Content-Type": "application/json"}, healthy[cursor]

        with HttpFixture(responder) as fixture:
            app = Application(
                self.state,
                request_policy=RequestPolicy(
                    requests_per_second=50, burst=50, concurrency=2,
                    permit_timeout=5, maximum_retry_after=5,
                ),
            )
            app.registry.register("s", "Supplier", "http", fixture.url)
            with self.assertRaises(FetchError):
                app.ingest.start("s")
            failed = app.runs.list()[0]
            self.assertEqual((failed["status"], failed["page_count"], failed["accepted"]), ("failed", 1, 1))
            broken.clear()
            resumed = app.ingest.execute(failed["id"])
            self.assertEqual((resumed["status"], resumed["page_count"], resumed["accepted"]), ("completed", 2, 2))
            self.assertEqual(app.publisher.publish("s")["row_count"], 2)
            self.assertTrue(app.diagnostics.check(True)["ok"])

    def test_legacy_process_gate_still_releases_after_failure(self):
        policy = RequestPolicy()
        gate = OriginGate(policy)
        with self.assertRaises(RuntimeError):
            with gate.permit():
                raise RuntimeError("boom")
        with gate.permit():
            pass
        self.assertEqual(gate.metrics()["admitted"], 2)


if __name__ == "__main__":
    unittest.main()
