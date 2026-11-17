import sqlite3
import uuid

from .errors import Conflict, NotFound
from .models import canonical, digest, now
from .normalization import Profile


COUNTERS = ("accepted", "rejected", "inserted", "updated", "unchanged", "stale")


class Runs:
    def __init__(self, database):
        self.db = database

    def create(self, source_id, mode="snapshot"):
        run_id = uuid.uuid4().hex
        try:
            with self.db.write() as connection:
                source = connection.execute(
                    "SELECT * FROM sources WHERE id = ?", (source_id,)
                ).fetchone()
                if source is None:
                    raise NotFound(f"unknown source: {source_id}")
                if not source["enabled"]:
                    raise Conflict("source is disabled")
                connection.execute(
                    """INSERT INTO runs
                    (id, source_id, mode, status, next_cursor, created_at)
                    VALUES (?, ?, ?, 'pending', '', ?)""",
                    (run_id, source_id, mode, now()),
                )
                configured = connection.execute(
                    "SELECT * FROM source_profiles WHERE source_id=? ORDER BY version DESC LIMIT 1", (source_id,)
                ).fetchone()
                document = canonical(Profile().document())
                version = 0
                fingerprint = digest(Profile().document())
                if configured:
                    document = configured["document"]
                    version = configured["version"]
                    fingerprint = configured["fingerprint"]
                connection.execute(
                    "INSERT INTO run_profiles VALUES (?,?,?,?)", (run_id, version, document, fingerprint)
                )
                self.db.event(connection, "run_created", {"mode": mode}, source_id, run_id)
        except sqlite3.IntegrityError as error:
            raise Conflict("another snapshot is pending or running for this source") from error
        return self.get(run_id)

    def get(self, run_id):
        with self.db.read() as connection:
            row = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
            if row is None:
                raise NotFound(f"unknown run: {run_id}")
            return dict(row)

    def claim(self, run_id):
        try:
            with self.db.write() as connection:
                row = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
                if row is None:
                    raise NotFound("run does not exist")
                if row["status"] not in {"pending", "failed"}:
                    raise Conflict(f"cannot start a {row['status']} run")
                source = connection.execute(
                    "SELECT enabled FROM sources WHERE id = ?", (row["source_id"],)
                ).fetchone()
                if not source[0]:
                    raise Conflict("source is disabled")
                connection.execute(
                    """UPDATE runs SET status = 'running', attempt = attempt + 1,
                    error = NULL, finished_at = NULL, started_at = ? WHERE id = ?""",
                    (now(), run_id),
                )
                self.db.event(connection, "run_started", {}, row["source_id"], run_id)
        except sqlite3.IntegrityError as error:
            raise Conflict("another snapshot owns this source") from error
        return self.get(run_id)

    def fail(self, run_id, error):
        with self.db.write() as connection:
            row = connection.execute("SELECT source_id, status FROM runs WHERE id = ?", (run_id,)).fetchone()
            if row is None:
                raise NotFound("run does not exist")
            if row["status"] != "running":
                raise Conflict("only running runs can fail")
            connection.execute(
                "UPDATE runs SET status = 'failed', error = ?, finished_at = ? WHERE id = ?",
                (str(error)[:4000], now(), run_id),
            )
            self.db.event(connection, "run_failed", {"error": str(error)}, row["source_id"], run_id)

    def complete(self, run_id):
        with self.db.write() as connection:
            row = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
            if row is None:
                raise NotFound("run does not exist")
            if row["status"] != "running" or row["next_cursor"] is not None:
                raise Conflict("run has not consumed its terminal page")
            connection.execute(
                "UPDATE runs SET status = 'completed', finished_at = ? WHERE id = ?",
                (now(), run_id),
            )
            from .delisting import Delisting
            Delisting().finalize(connection, dict(row))
            self.db.event(connection, "run_completed", {}, row["source_id"], run_id)
        return self.get(run_id)

    def list(self, source_id=None, limit=100):
        with self.db.read() as connection:
            if source_id:
                rows = connection.execute(
                    "SELECT * FROM runs WHERE source_id = ? ORDER BY created_at DESC LIMIT ?",
                    (source_id, limit),
                )
            else:
                rows = connection.execute(
                    "SELECT * FROM runs ORDER BY created_at DESC LIMIT ?", (limit,)
                )
            return [dict(row) for row in rows]

    def pages(self, run_id):
        self.get(run_id)
        with self.db.read() as connection:
            return [dict(row) for row in connection.execute(
                "SELECT * FROM pages WHERE run_id = ? ORDER BY committed_at, cursor", (run_id,)
            )]

    def rejects(self, run_id, limit=100):
        self.get(run_id)
        with self.db.read() as connection:
            return [dict(row) for row in connection.execute(
                "SELECT * FROM rejects WHERE run_id = ? ORDER BY id LIMIT ?", (run_id, limit)
            )]
