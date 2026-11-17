from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import json
import math
import signal
import threading
import time

from .errors import CatalogError, Conflict, ValidationError


class Collector:
    def __init__(self, app, workers=2, publish=False):
        if type(workers) is not int or not 1 <= workers <= 16:
            raise ValidationError("workers must be between 1 and 16")
        self.app = app
        self.workers = workers
        self.publish = publish
        self.stopped = threading.Event()
        self.failures = {}
        self.next_attempt = {}

    def one_source(self, source):
        source_id = source["id"]
        started = time.monotonic()
        try:
            previous = self.app.runs.list(source_id, 100)
            running = [run for run in previous if run["mode"] == "snapshot" and run["status"] == "running"]
            if running:
                return {"source_id": source_id, "status": "busy", "run_id": running[0]["id"]}
            pending = [run for run in previous if run["mode"] == "snapshot" and run["status"] == "pending"]
            failed = [run for run in previous if run["mode"] == "snapshot" and run["status"] == "failed"]
            if pending:
                run = self.resume_run(pending[0])
            elif failed and previous and failed[0]["id"] == previous[0]["id"]:
                run = self.resume_run(failed[0])
            else:
                run = self.app.ingest.start(source_id)
            inbox = self.app.inbox.consume(source_id)
            result = {"source_id": source_id, "status": "completed", "run": run, "inbox": inbox}
            if self.publish:
                result["publication"] = self.app.publisher.publish(source_id)
            result["elapsed_seconds"] = round(time.monotonic() - started, 4)
            return result
        except Conflict as error:
            return {"source_id": source_id, "status": "conflict", "error": str(error)}
        except (CatalogError, OSError, ValueError) as error:
            return {"source_id": source_id, "status": "failed", "error": str(error)}

    def resume_run(self, run):
        crawl = self.app.crawls.for_run(run["id"])
        if crawl is not None:
            return self.app.crawler.execute(crawl["id"])
        return self.app.ingest.execute(run["id"])

    def cycle(self):
        sources = [source for source in self.app.registry.list() if source["enabled"]]
        results = []
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {executor.submit(self.one_source, source): source["id"] for source in sources}
            for future in as_completed(futures):
                results.append(future.result())
        results.sort(key=lambda item: item["source_id"])
        return {
            "sources": len(sources),
            "completed": sum(result["status"] == "completed" for result in results),
            "failed": sum(result["status"] == "failed" for result in results),
            "results": results,
        }

    def stop(self, *args):
        self.stopped.set()

    def watch(self, interval=60):
        if type(interval) not in {int, float} or not math.isfinite(interval) or interval < 1:
            raise ValidationError("interval must be >=1 second")
        prior_handlers = {}
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGINT, signal.SIGTERM):
                prior_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, self.stop)
        cycles = 0
        try:
            while not self.stopped.is_set():
                due = [
                    source for source in self.app.registry.list()
                    if source["enabled"] and time.monotonic() >= self.next_attempt.get(source["id"], 0)
                ]
                with ThreadPoolExecutor(max_workers=self.workers) as executor:
                    futures = {executor.submit(self.one_source, source): source["id"] for source in due}
                    for future in as_completed(futures):
                        result = future.result()
                        source_id = result["source_id"]
                        if result["status"] == "failed":
                            failures = self.failures.get(source_id, 0) + 1
                            self.failures[source_id] = failures
                            delay = min(interval * 2 ** min(failures, 8), 3600)
                        else:
                            self.failures[source_id] = 0
                            delay = interval
                        self.next_attempt[source_id] = time.monotonic() + delay
                        result["observed_at"] = datetime.now(timezone.utc).isoformat()
                        print(json.dumps(result, ensure_ascii=False), flush=True)
                cycles += 1
                self.stopped.wait(min(interval, 1))
        finally:
            for signum, handler in prior_handlers.items():
                signal.signal(signum, handler)
        return {"stopped": True, "cycles": cycles}
