from contextlib import contextmanager
from pathlib import Path
import sqlite3

from .errors import Conflict
from .models import canonical, now


class Database:
    def __init__(self, path):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def connect(self):
        connection = sqlite3.connect(
            self.path,
            timeout=15,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 15000")
        return connection

    def initialize(self):
        schema = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
        connection = self.connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(schema)
            connection.execute(
                "INSERT OR IGNORE INTO schema_version VALUES (?, ?)",
                (1, now()),
            )
            versions = connection.execute(
                "SELECT version FROM schema_version ORDER BY version"
            ).fetchall()
            if [row[0] for row in versions] != [1]:
                raise Conflict("unsupported database schema")
        finally:
            connection.close()

    @contextmanager
    def read(self):
        connection = self.connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def write(self):
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def event(self, connection, action, detail, source_id=None, run_id=None):
        connection.execute(
            """INSERT INTO audit_events
            (source_id, run_id, action, detail, created_at)
            VALUES (?, ?, ?, ?, ?)""",
            (source_id, run_id, action, canonical(detail), now()),
        )

    def integrity(self):
        with self.read() as connection:
            physical = connection.execute("PRAGMA integrity_check").fetchall()
            foreign = connection.execute("PRAGMA foreign_key_check").fetchall()
            return {
                "physical": [row[0] for row in physical],
                "foreign_keys": [tuple(row) for row in foreign],
                "ok": [row[0] for row in physical] == ["ok"] and not foreign,
            }
