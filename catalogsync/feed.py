import json
import re
import uuid

from .errors import Conflict, NotFound, ValidationError
from .models import Page, canonical, digest, now


class DeltaInbox:
    def __init__(self, database, ingestor):
        self.db = database
        self.ingestor = ingestor

    def submit(self, source_id, events):
        if not isinstance(events, list) or not events or len(events) > 10000:
            raise ValidationError("events must contain between 1 and 10000 entries")
        accepted = 0
        duplicates = 0
        with self.db.write() as connection:
            source = connection.execute("SELECT * FROM sources WHERE id=?", (source_id,)).fetchone()
            if source is None:
                raise NotFound("source does not exist")
            if not source["enabled"]:
                raise Conflict("source is disabled")
            for event in events:
                if not isinstance(event, dict):
                    raise ValidationError("event must be an object")
                event_id = event.get("id")
                if not isinstance(event_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,199}", event_id):
                    raise ValidationError("event id contains unsupported characters")
                payload = event.get("record")
                if not isinstance(payload, dict):
                    raise ValidationError("event record must be an object")
                fingerprint = digest(payload)
                old = connection.execute(
                    "SELECT fingerprint FROM inbox_events WHERE source_id=? AND event_id=?",
                    (source_id, event_id),
                ).fetchone()
                if old:
                    if old["fingerprint"] != fingerprint:
                        raise Conflict("event id reused with different content")
                    duplicates += 1
                    continue
                connection.execute(
                    """INSERT INTO inbox_events
                    (source_id,event_id,payload,fingerprint,status,received_at)
                    VALUES (?,?,?,?,'pending',?)""",
                    (source_id, event_id, canonical(payload), fingerprint, now()),
                )
                accepted += 1
            self.db.event(connection, "events_received", {"accepted": accepted, "duplicates": duplicates}, source_id)
        return {"accepted": accepted, "duplicates": duplicates}

    def reserve(self, source_id, limit=100):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValidationError("batch size must be between 1 and 1000")
        with self.db.write() as connection:
            if not connection.execute("SELECT 1 FROM sources WHERE id=?", (source_id,)).fetchone():
                raise NotFound("source does not exist")
            existing = connection.execute(
                "SELECT * FROM inbox_batches WHERE source_id=? AND status='reserved' ORDER BY created_at LIMIT 1",
                (source_id,),
            ).fetchone()
            if existing:
                return dict(existing)
            rows = connection.execute(
                "SELECT sequence FROM inbox_events WHERE source_id=? AND status='pending' ORDER BY sequence LIMIT ?",
                (source_id, limit),
            ).fetchall()
            if not rows:
                return None
            batch_id = uuid.uuid4().hex
            timestamp = now()
            connection.execute(
                """INSERT INTO inbox_batches
                (id,source_id,status,event_count,created_at)
                VALUES (?,?,'reserved',?,?)""",
                (batch_id, source_id, len(rows), timestamp),
            )
            for row in rows:
                connection.execute(
                    "UPDATE inbox_events SET status='reserved', batch_id=? WHERE sequence=?",
                    (batch_id, row["sequence"]),
                )
            self.db.event(connection, "inbox_reserved", {"batch_id": batch_id, "events": len(rows)}, source_id)
        return self.batch(batch_id)

    def batch(self, batch_id):
        with self.db.read() as connection:
            row = connection.execute("SELECT * FROM inbox_batches WHERE id=?", (batch_id,)).fetchone()
            if row is None:
                raise NotFound("inbox batch does not exist")
            return dict(row)

    def records(self, batch_id):
        self.batch(batch_id)
        with self.db.read() as connection:
            return [
                {**dict(row), "record": json.loads(row["payload"])}
                for row in connection.execute(
                    "SELECT * FROM inbox_events WHERE batch_id=? ORDER BY sequence", (batch_id,)
                )
            ]

    def attach_run(self, batch_id):
        with self.db.write() as connection:
            batch = connection.execute("SELECT * FROM inbox_batches WHERE id=?", (batch_id,)).fetchone()
            if batch is None:
                raise NotFound("inbox batch does not exist")
            if batch["run_id"]:
                return batch["run_id"]
            run_id = uuid.uuid4().hex
            connection.execute(
                """INSERT INTO runs
                (id,source_id,mode,status,next_cursor,created_at)
                VALUES (?,?,'delta','pending','',?)""",
                (run_id, batch["source_id"], now()),
            )
            from .normalization import Profile
            profile = connection.execute(
                "SELECT * FROM source_profiles WHERE source_id=? ORDER BY version DESC LIMIT 1",
                (batch["source_id"],),
            ).fetchone()
            if profile:
                version, document, fingerprint = profile["version"], profile["document"], profile["fingerprint"]
            else:
                value = Profile().document()
                version, document, fingerprint = 0, canonical(value), digest(value)
            connection.execute("INSERT INTO run_profiles VALUES (?,?,?,?)", (run_id, version, document, fingerprint))
            connection.execute("UPDATE inbox_batches SET run_id=? WHERE id=?", (run_id, batch_id))
            return run_id

    def acknowledge(self, batch_id):
        with self.db.write() as connection:
            batch = connection.execute("SELECT * FROM inbox_batches WHERE id=?", (batch_id,)).fetchone()
            if batch is None:
                raise NotFound("inbox batch does not exist")
            if batch["status"] == "committed":
                return
            run = connection.execute("SELECT * FROM runs WHERE id=?", (batch["run_id"],)).fetchone()
            if not run or run["status"] != "completed":
                raise Conflict("inbox batch cannot acknowledge an unfinished run")
            events = connection.execute(
                "SELECT sequence FROM inbox_events WHERE batch_id=? ORDER BY sequence", (batch_id,)
            ).fetchall()
            if len(events) != run["accepted"] + run["rejected"]:
                raise Conflict("inbox event count differs from durable ingestion result")
            rejected = {
                row[0] for row in connection.execute("SELECT ordinal FROM rejects WHERE run_id=?", (run["id"],))
            }
            for ordinal, event in enumerate(events):
                status = "rejected" if ordinal in rejected else "committed"
                connection.execute(
                    "UPDATE inbox_events SET status=?, processed_at=? WHERE sequence=?",
                    (status, now(), event["sequence"]),
                )
            connection.execute(
                "UPDATE inbox_batches SET status='committed', completed_at=? WHERE id=?", (now(), batch_id)
            )
            self.db.event(connection, "inbox_committed", {"batch_id": batch_id, "run_id": run["id"]}, batch["source_id"])

    def consume(self, source_id, limit=100):
        batch = self.reserve(source_id, limit)
        if batch is None:
            return {"empty": True, "source_id": source_id}
        run_id = self.attach_run(batch["id"])
        run = self.ingestor.runs.get(run_id)
        if run["status"] != "completed":
            profile = self.ingestor.profiles.for_run(run_id)
            records = [event["record"] for event in self.records(batch["id"])]
            boundary = 1
            for record in records:
                try:
                    revision = profile.normalize(record).get("revision", 0)
                    if type(revision) is int:
                        boundary = max(boundary, revision)
                except ValidationError:
                    continue
            payload = {
                "snapshot_id": batch["id"], "boundary": boundary, "cursor": "",
                "next_cursor": None, "records": records,
            }
            class InboxClient:
                def fetch(self, cursor):
                    return canonical(payload).encode("utf-8")
            if profile.parser != "json":
                self.execute_records(run_id, payload)
            else:
                self.ingestor.execute(run_id, InboxClient())
        self.acknowledge(batch["id"])
        return {"batch": self.batch(batch["id"]), "run": self.ingestor.runs.get(run_id)}

    def execute_records(self, run_id, payload):
        self.ingestor.runs.claim(run_id)
        run = self.ingestor.runs.get(run_id)
        try:
            if run["next_cursor"] is not None:
                raw = canonical(payload).encode("utf-8")
                relative, checksum = self.ingestor.archive.put(run["source_id"], run_id, raw)
                self.ingestor.commit_page(run_id, Page.parse(payload, ""), relative, checksum)
            self.ingestor.runs.complete(run_id)
        except Exception as error:
            self.ingestor.runs.fail(run_id, error)
            raise

    def status(self, source_id):
        with self.db.read() as connection:
            counts = {
                row[0]: row[1] for row in connection.execute(
                    "SELECT status,count(*) FROM inbox_events WHERE source_id=? GROUP BY status", (source_id,)
                )
            }
            batches = [dict(row) for row in connection.execute(
                "SELECT * FROM inbox_batches WHERE source_id=? ORDER BY created_at DESC LIMIT 20", (source_id,)
            )]
            return {"source_id": source_id, "events": counts, "batches": batches}

    def events(self, source_id, after=0, limit=100):
        if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 1000:
            raise ValidationError("invalid inbox event range")
        with self.db.read() as connection:
            rows = connection.execute(
                "SELECT * FROM inbox_events WHERE source_id=? AND sequence>? ORDER BY sequence LIMIT ?",
                (source_id, after, limit),
            )
            return [dict(row) for row in rows]
