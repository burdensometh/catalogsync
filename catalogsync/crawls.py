import sqlite3
import uuid

from .errors import Conflict, NotFound, ValidationError
from .models import now
from .runs import freeze_profile
from .urlnorm import canonicalize


ITEM_SELECT = """SELECT i.id, i.crawl_id, i.url, i.state, i.depth, i.parent_id,
i.attempts, i.records, i.accepted, i.rejected, i.error, i.created_at, i.finished_at,
p.archive_path, p.raw_sha256
FROM crawl_items i
JOIN crawls c ON c.id = i.crawl_id
LEFT JOIN pages p ON p.run_id = c.run_id AND p.cursor = i.url"""


class Crawls:
    """Durable crawl rounds and their per-round URL frontier."""

    def __init__(self, database):
        self.db = database

    def create(self, source_id, seed_url, max_items=1000):
        if type(max_items) is not int or not 1 <= max_items <= 10000:
            raise ValidationError("max_items must be between 1 and 10000")
        seed_url = canonicalize(seed_url)
        if seed_url is None:
            raise ValidationError("crawl seed must be an HTTP or HTTPS URL")
        crawl_id = uuid.uuid4().hex
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
                if source["kind"] != "http":
                    raise ValidationError("crawl requires an http source")
                timestamp = now()
                connection.execute(
                    """INSERT INTO runs
                    (id, source_id, mode, status, next_cursor, created_at)
                    VALUES (?, ?, 'snapshot', 'pending', '', ?)""",
                    (run_id, source_id, timestamp),
                )
                freeze_profile(connection, source_id, run_id)
                connection.execute(
                    """INSERT INTO crawls
                    (id, source_id, run_id, status, seed_url, max_items, created_at)
                    VALUES (?, ?, ?, 'pending', ?, ?, ?)""",
                    (crawl_id, source_id, run_id, seed_url, max_items, timestamp),
                )
                connection.execute(
                    """INSERT INTO crawl_items
                    (crawl_id, url, state, depth, parent_id, created_at)
                    VALUES (?, ?, 'pending', 0, NULL, ?)""",
                    (crawl_id, seed_url, timestamp),
                )
                self.db.event(
                    connection,
                    "crawl_created",
                    {"seed_url": seed_url, "max_items": max_items},
                    source_id,
                    run_id,
                )
        except sqlite3.IntegrityError as error:
            raise Conflict("another crawl or snapshot is active for this source") from error
        return self.get(crawl_id)

    def get(self, crawl_id):
        with self.db.read() as connection:
            row = connection.execute(
                "SELECT * FROM crawls WHERE id = ?", (crawl_id,)
            ).fetchone()
            if row is None:
                raise NotFound(f"unknown crawl: {crawl_id}")
            result = dict(row)
            result["items"] = self.tallies(connection, crawl_id)
            return result

    def tallies(self, connection, crawl_id):
        row = connection.execute(
            """SELECT count(*) AS discovered,
            coalesce(sum(state = 'completed'), 0) AS completed,
            coalesce(sum(state = 'failed'), 0) AS failed,
            coalesce(sum(state = 'pending'), 0) AS pending
            FROM crawl_items WHERE crawl_id = ?""",
            (crawl_id,),
        ).fetchone()
        return {key: row[key] for key in ("discovered", "completed", "failed", "pending")}

    def list(self, source_id=None, limit=100):
        with self.db.read() as connection:
            if source_id:
                rows = connection.execute(
                    "SELECT * FROM crawls WHERE source_id = ? ORDER BY created_at DESC LIMIT ?",
                    (source_id, limit),
                )
            else:
                rows = connection.execute(
                    "SELECT * FROM crawls ORDER BY created_at DESC LIMIT ?", (limit,)
                )
            result = []
            for row in rows:
                item = dict(row)
                item["items"] = self.tallies(connection, item["id"])
                result.append(item)
            return result

    def claim(self, crawl_id):
        try:
            with self.db.write() as connection:
                row = connection.execute(
                    "SELECT * FROM crawls WHERE id = ?", (crawl_id,)
                ).fetchone()
                if row is None:
                    raise NotFound("crawl does not exist")
                if row["status"] not in {"pending", "failed"}:
                    raise Conflict(f"cannot start a {row['status']} crawl")
                source = connection.execute(
                    "SELECT enabled FROM sources WHERE id = ?", (row["source_id"],)
                ).fetchone()
                if not source[0]:
                    raise Conflict("source is disabled")
                timestamp = now()
                connection.execute(
                    """UPDATE crawls SET status = 'running', attempt = attempt + 1,
                    error = NULL, finished_at = NULL, started_at = ? WHERE id = ?""",
                    (timestamp, crawl_id),
                )
                connection.execute(
                    """UPDATE runs SET status = 'running', attempt = attempt + 1,
                    error = NULL, finished_at = NULL, started_at = ? WHERE id = ?""",
                    (timestamp, row["run_id"]),
                )
                self.db.event(
                    connection, "crawl_started", {"run_id": row["run_id"]},
                    row["source_id"], row["run_id"],
                )
        except sqlite3.IntegrityError as error:
            raise Conflict("another crawl or snapshot owns this source") from error
        return self.get(crawl_id)

    def fail(self, crawl_id, error):
        with self.db.write() as connection:
            row = connection.execute(
                "SELECT * FROM crawls WHERE id = ?", (crawl_id,)
            ).fetchone()
            if row is None:
                raise NotFound("crawl does not exist")
            if row["status"] != "running":
                raise Conflict("only running crawls can fail")
            timestamp = now()
            connection.execute(
                "UPDATE crawls SET status = 'failed', error = ?, finished_at = ? WHERE id = ?",
                (str(error)[:4000], timestamp, crawl_id),
            )
            connection.execute(
                "UPDATE runs SET status = 'failed', error = ?, finished_at = ? WHERE id = ? AND status = 'running'",
                (str(error)[:4000], timestamp, row["run_id"]),
            )
            self.db.event(
                connection, "crawl_failed", {"error": str(error)},
                row["source_id"], row["run_id"],
            )

    def complete(self, crawl_id):
        with self.db.write() as connection:
            row = connection.execute(
                "SELECT * FROM crawls WHERE id = ?", (crawl_id,)
            ).fetchone()
            if row is None:
                raise NotFound("crawl does not exist")
            if row["status"] != "running":
                raise Conflict("crawl is not running")
            unfinished = connection.execute(
                "SELECT count(*) FROM crawl_items WHERE crawl_id = ? AND state != 'completed'",
                (crawl_id,),
            ).fetchone()[0]
            if unfinished:
                raise Conflict("crawl has unfinished items")
            run = connection.execute(
                "SELECT status FROM runs WHERE id = ?", (row["run_id"],)
            ).fetchone()
            if run is None or run["status"] != "running":
                raise Conflict("crawl run is not running")
            timestamp = now()
            connection.execute(
                "UPDATE crawls SET status = 'completed', finished_at = ? WHERE id = ?",
                (timestamp, crawl_id),
            )
            connection.execute(
                "UPDATE runs SET status = 'completed', next_cursor = NULL, finished_at = ? WHERE id = ?",
                (timestamp, row["run_id"]),
            )
            self.db.event(
                connection, "crawl_completed", {}, row["source_id"], row["run_id"]
            )
        return self.get(crawl_id)

    def next_unfinished(self, crawl_id):
        with self.db.read() as connection:
            row = connection.execute(
                """SELECT * FROM crawl_items
                WHERE crawl_id = ? AND state != 'completed'
                ORDER BY id LIMIT 1""",
                (crawl_id,),
            ).fetchone()
            return dict(row) if row else None

    def fail_item(self, crawl_id, item_id, error):
        with self.db.write() as connection:
            crawl = connection.execute(
                "SELECT source_id, run_id FROM crawls WHERE id = ?", (crawl_id,)
            ).fetchone()
            if crawl is None:
                raise NotFound("crawl does not exist")
            connection.execute(
                """UPDATE crawl_items SET state = 'failed', attempts = attempts + 1,
                error = ?, finished_at = ?
                WHERE id = ? AND crawl_id = ? AND state != 'completed'""",
                (str(error)[:4000], now(), item_id, crawl_id),
            )
            self.db.event(
                connection,
                "crawl_item_failed",
                {"item_id": item_id, "error": str(error)},
                crawl["source_id"],
                crawl["run_id"],
            )

    def items(self, crawl_id):
        self.get(crawl_id)
        with self.db.read() as connection:
            return [
                dict(row) for row in connection.execute(
                    ITEM_SELECT + " WHERE i.crawl_id = ? ORDER BY i.id", (crawl_id,)
                )
            ]

    def item(self, crawl_id, item_id):
        self.get(crawl_id)
        with self.db.read() as connection:
            row = connection.execute(
                ITEM_SELECT + " WHERE i.crawl_id = ? AND i.id = ?",
                (crawl_id, item_id),
            ).fetchone()
            if row is None:
                raise NotFound("crawl item does not exist")
            return dict(row)

    def item_by_url(self, crawl_id, url):
        with self.db.read() as connection:
            row = connection.execute(
                ITEM_SELECT + " WHERE i.crawl_id = ? AND i.url = ?",
                (crawl_id, url),
            ).fetchone()
            return dict(row) if row else None

    def for_run(self, run_id):
        with self.db.read() as connection:
            row = connection.execute(
                "SELECT * FROM crawls WHERE run_id = ?", (run_id,)
            ).fetchone()
            return dict(row) if row else None
