from pathlib import Path
import csv
import json
import sqlite3

from .errors import Conflict, IntegrityError, ValidationError
from .models import Product, canonical, now, plan_scale_changes


CONSUMER_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    installed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS datasets (
    publication_id TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL,
    published_at TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    rows INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS source_heads (
    source_id TEXT PRIMARY KEY,
    publication_id TEXT NOT NULL REFERENCES datasets(publication_id),
    published_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS catalog_records (
    source_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    revision INTEGER NOT NULL,
    title TEXT NOT NULL,
    category TEXT NOT NULL,
    currency TEXT NOT NULL,
    price_cents INTEGER NOT NULL,
    stock INTEGER NOT NULL,
    active INTEGER NOT NULL,
    attributes TEXT NOT NULL,
    publication_id TEXT NOT NULL REFERENCES datasets(publication_id),
    PRIMARY KEY(source_id, sku)
);
"""


class DatasetConsumer:
    def __init__(self, path):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = self.connect()
        try:
            # A database that already has materialized data but no version
            # marker is a legacy (centi-scale) consumer; fresh consumer
            # databases are created in the current minor-unit scale.
            legacy = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='datasets'"
            ).fetchone() is not None
            connection.executescript(CONSUMER_SCHEMA)
            connection.execute(
                "INSERT OR IGNORE INTO schema_version VALUES (?, ?)",
                (1 if legacy else 2, now()),
            )
            versions = {
                row[0] for row in connection.execute("SELECT version FROM schema_version")
            }
            if versions - {1, 2}:
                raise Conflict("unsupported consumer database schema")
        finally:
            connection.close()

    def connect(self):
        connection = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def money_scale(self, connection):
        version = connection.execute("SELECT max(version) FROM schema_version").fetchone()[0]
        return "minor" if version is not None and version >= 2 else "centi"

    def records(self, publication, path, scale="minor"):
        with Path(path).open(encoding="utf-8", newline="") as stream:
            if publication["format"] == "jsonl":
                iterator = (json.loads(line) for line in stream if line.strip())
            elif publication["format"] == "csv":
                def from_csv():
                    for row in csv.DictReader(stream):
                        row["revision"] = int(row["revision"])
                        row["stock"] = int(row["stock"])
                        if row["active"] not in {"True", "False"}:
                            raise ValidationError("invalid exported active flag")
                        row["active"] = row["active"] == "True"
                        row["attributes"] = json.loads(row["attributes"])
                        yield row
                iterator = from_csv()
            else:
                raise ValidationError("unsupported dataset format")
            for raw in iterator:
                source_id = raw.get("source_id")
                if source_id not in publication["revision_vector"]:
                    raise IntegrityError("dataset row is outside publication source scope")
                product = Product.parse(raw, scale)
                if product.revision > publication["revision_vector"][source_id]:
                    raise IntegrityError("dataset revision exceeds publication vector")
                if publication["sellable_only"] and not product.public(scale)["sellable"]:
                    raise IntegrityError("unavailable row in sellable-only dataset")
                yield source_id, product

    def apply(self, publisher, publication_id):
        publication = publisher.get(publication_id)
        path = publisher.artifact(publication_id)
        source_ids = list(publication["revision_vector"])
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            scale = self.money_scale(connection)
            old = connection.execute("SELECT * FROM datasets WHERE publication_id=?", (publication_id,)).fetchone()
            if old:
                if old["sha256"] != publication["sha256"]:
                    raise Conflict("publication id has changed content")
                connection.rollback()
                return {"publication_id": publication_id, "replayed": True, "rows": old["rows"]}
            for source_id in source_ids:
                head = connection.execute("SELECT * FROM source_heads WHERE source_id=?", (source_id,)).fetchone()
                if head and head["published_at"] > publication["created_at"]:
                    raise Conflict("cannot replace a newer materialized source dataset")
            connection.execute(
                "INSERT INTO datasets VALUES (?,?,?,?,?)",
                (publication_id, publication["sha256"], publication["created_at"], now(), publication["row_count"]),
            )
            for source_id in source_ids:
                connection.execute("DELETE FROM catalog_records WHERE source_id=?", (source_id,))
            count = 0
            for source_id, product in self.records(publication, path, scale):
                try:
                    connection.execute(
                        "INSERT INTO catalog_records VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            source_id, product.sku, product.revision, product.title, product.category,
                            product.currency, product.price_cents, product.stock, int(product.active),
                            canonical(product.attributes), publication_id,
                        ),
                    )
                except sqlite3.IntegrityError as error:
                    raise IntegrityError("duplicate identity in published dataset") from error
                count += 1
            if count != publication["row_count"]:
                raise IntegrityError("publication row count mismatch")
            for source_id in source_ids:
                connection.execute(
                    """INSERT INTO source_heads VALUES (?,?,?) ON CONFLICT(source_id)
                    DO UPDATE SET publication_id=excluded.publication_id,published_at=excluded.published_at""",
                    (source_id, publication_id, publication["created_at"]),
                )
            connection.commit()
            return {"publication_id": publication_id, "replayed": False, "rows": count}
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def scan_scale_migration(self, connection, corrections, matched=None):
        rows = [
            dict(row) for row in connection.execute(
                "SELECT * FROM catalog_records ORDER BY source_id, sku"
            )
        ]
        return plan_scale_changes(rows, corrections, matched, "catalog_records")

    def plan_scale_migration(self, corrections, matched=None):
        connection = self.connect()
        try:
            connection.execute("BEGIN")
            scale = self.money_scale(connection)
            if scale == "minor":
                connection.commit()
                return {"scale": scale, "changes": [], "blockers": [], "corrections_applied": 0}
            changes, blockers, applied = self.scan_scale_migration(connection, corrections, matched)
            connection.commit()
            return {
                "scale": scale,
                "changes": changes,
                "blockers": blockers,
                "corrections_applied": applied,
            }
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def apply_scale_migration(self, corrections):
        """Convert stored centi-scale amounts to currency minor units in one
        transaction. Interrupted runs roll back; reruns are no-ops. Records
        that cannot be converted losslessly abort the switch unchanged."""
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            if self.money_scale(connection) == "minor":
                connection.rollback()
                return {"status": "already_migrated", "converted": {"catalog_records": 0},
                        "corrections_applied": 0, "blockers": []}
            changes, blockers, applied = self.scan_scale_migration(connection, corrections)
            if blockers:
                connection.rollback()
                return {"status": "blocked", "converted": {"catalog_records": 0},
                        "corrections_applied": 0, "blockers": blockers}
            for row, minor in changes:
                connection.execute(
                    "UPDATE catalog_records SET price_cents=? WHERE source_id=? AND sku=?",
                    (minor, row["source_id"], row["sku"]),
                )
            connection.execute("INSERT INTO schema_version VALUES (?, ?)", (2, now()))
            connection.commit()
            return {"status": "migrated", "converted": {"catalog_records": len(changes)},
                    "corrections_applied": applied, "blockers": []}
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def query(self, source_id=None):
        connection = self.connect()
        try:
            if source_id:
                rows = connection.execute("SELECT * FROM catalog_records WHERE source_id=? ORDER BY sku", (source_id,))
            else:
                rows = connection.execute("SELECT * FROM catalog_records ORDER BY source_id,sku")
            return [dict(row) for row in rows]
        finally:
            connection.close()

    def heads(self):
        connection = self.connect()
        try:
            return [dict(row) for row in connection.execute("SELECT * FROM source_heads ORDER BY source_id")]
        finally:
            connection.close()
