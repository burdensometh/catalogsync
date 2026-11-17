from contextlib import contextmanager
from pathlib import Path
import sqlite3

from .errors import Conflict
from .models import canonical, now


SCHEMA_VERSION = 2


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
            installed = self.installed_versions(connection)
            if installed - {1, SCHEMA_VERSION}:
                raise Conflict("unsupported database schema")
            if not installed:
                connection.executescript(schema)
                connection.execute(
                    "INSERT OR IGNORE INTO schema_version VALUES (?, ?)",
                    (SCHEMA_VERSION, now()),
                )
            elif installed == {1}:
                self.migrate_v1(connection, schema)
            else:
                connection.executescript(schema)
            versions = [
                row[0] for row in connection.execute(
                    "SELECT version FROM schema_version"
                )
            ]
            if versions != [SCHEMA_VERSION]:
                raise Conflict("unsupported database schema")
        finally:
            connection.close()

    @staticmethod
    def installed_versions(connection):
        if connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_version'"
        ).fetchone() is None:
            return set()
        return {row[0] for row in connection.execute("SELECT version FROM schema_version")}

    def migrate_v1(self, connection, schema):
        """Upgrade an existing baseline database in place, atomically.

        v2 adds a cleaning-rule version to the identity of every stored product
        version, so the same source revision may be recorded once per rule set,
        and adds the reprocess staging tables.  Existing rows become rule
        version 0, which is the baseline profile version.
        """
        connection.executescript(
            """
            PRAGMA foreign_keys = OFF;
            BEGIN IMMEDIATE;
            DROP TABLE IF EXISTS products_v2;
            CREATE TABLE products_v2 (
                source_id TEXT NOT NULL REFERENCES sources(id),
                sku TEXT NOT NULL,
                revision INTEGER NOT NULL,
                rule_version INTEGER NOT NULL DEFAULT 0,
                title TEXT NOT NULL,
                category TEXT NOT NULL,
                currency TEXT NOT NULL,
                price_cents INTEGER NOT NULL,
                stock INTEGER NOT NULL,
                active INTEGER NOT NULL CHECK(active IN (0, 1)),
                attributes TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                last_run_id TEXT REFERENCES runs(id),
                last_job_id TEXT,
                updated_at TEXT NOT NULL,
                CHECK ((last_run_id IS NULL) != (last_job_id IS NULL)),
                PRIMARY KEY(source_id, sku)
            );
            INSERT INTO products_v2
                (source_id, sku, revision, rule_version, title, category, currency,
                 price_cents, stock, active, attributes, fingerprint,
                 last_run_id, last_job_id, updated_at)
            SELECT source_id, sku, revision, 0, title, category, currency,
                   price_cents, stock, active, attributes, fingerprint,
                   last_run_id, NULL, updated_at
            FROM products;
            DROP TABLE products;
            ALTER TABLE products_v2 RENAME TO products;
            CREATE INDEX IF NOT EXISTS product_category ON products(category, source_id);
            CREATE INDEX IF NOT EXISTS product_revision ON products(source_id, revision);

            DROP TABLE IF EXISTS product_versions_v2;
            CREATE TABLE product_versions_v2 (
                source_id TEXT NOT NULL REFERENCES sources(id),
                sku TEXT NOT NULL,
                revision INTEGER NOT NULL,
                rule_version INTEGER NOT NULL DEFAULT 0,
                fingerprint TEXT NOT NULL,
                source_fingerprint TEXT,
                parser TEXT,
                payload TEXT NOT NULL,
                raw_record TEXT,
                first_run_id TEXT REFERENCES runs(id),
                first_job_id TEXT,
                observed_at TEXT NOT NULL,
                CHECK ((first_run_id IS NULL) != (first_job_id IS NULL)),
                PRIMARY KEY(source_id, sku, revision, rule_version)
            );
            INSERT INTO product_versions_v2
                (source_id, sku, revision, rule_version, fingerprint,
                 source_fingerprint, parser, payload, raw_record, first_run_id,
                 first_job_id, observed_at)
            SELECT source_id, sku, revision, 0, fingerprint, NULL, NULL, payload,
                   NULL, first_run_id, NULL, observed_at
            FROM product_versions;
            DROP TABLE product_versions;
            ALTER TABLE product_versions_v2 RENAME TO product_versions;
            CREATE INDEX IF NOT EXISTS product_version_source
            ON product_versions(source_id, rule_version);

            DELETE FROM schema_version;
            COMMIT;
            PRAGMA foreign_keys = ON;
            """
        )
        # Add the remaining new tables (reprocess staging) from the current schema.
        connection.executescript(schema)
        connection.execute(
            "INSERT INTO schema_version VALUES (?, ?)", (SCHEMA_VERSION, now())
        )

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
