from pathlib import Path
import csv
import hashlib
import json
import os
import tempfile
import uuid

from .errors import Conflict, IntegrityError, NotFound, ValidationError
from .models import canonical, now
from .query import product_row


EXPORT_FIELDS = [
    "source_id", "sku", "revision", "title", "category", "currency",
    "price", "stock", "active", "attributes",
]


class Publisher:
    def __init__(self, database, output_root):
        self.db = database
        self.root = Path(output_root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def publish(self, source_id=None, format="jsonl", sellable_only=True):
        if format not in {"jsonl", "csv"}:
            raise ValidationError("format must be jsonl or csv")
        if not isinstance(sellable_only, bool):
            raise ValidationError("sellable_only must be boolean")
        publication_id = uuid.uuid4().hex
        with self.db.write() as connection:
            if source_id and not connection.execute(
                "SELECT 1 FROM sources WHERE id = ?", (source_id,)
            ).fetchone():
                raise NotFound("source does not exist")
            connection.execute(
                """INSERT INTO publications
                (id, source_id, status, format, sellable_only, created_at)
                VALUES (?, ?, 'building', ?, ?, ?)""",
                (publication_id, source_id, format, int(sellable_only), now()),
            )
        path = self.root / f"{publication_id}.{format}"
        descriptor, temporary = tempfile.mkstemp(prefix=".publish-", dir=self.root)
        try:
            with self.db.read() as connection:
                clauses = ["1 = 1"]
                params = []
                if source_id:
                    clauses.append("source_id = ?")
                    params.append(source_id)
                if sellable_only:
                    clauses.append("active = 1 AND stock > 0")
                rows = connection.execute(
                    "SELECT * FROM products WHERE " + " AND ".join(clauses)
                    + " ORDER BY source_id, sku",
                    params,
                )
                vector_query = """SELECT s.id, coalesce(max(p.revision),0)
                FROM sources s LEFT JOIN products p ON s.id=p.source_id"""
                vector_params = []
                if source_id:
                    vector_query += " WHERE s.id=?"
                    vector_params.append(source_id)
                vector_query += " GROUP BY s.id"
                vector = {row[0]: row[1] for row in connection.execute(vector_query, vector_params)}
                with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
                    count = self.write_rows(stream, rows, format)
                    stream.flush()
                    os.fsync(stream.fileno())
            sha = self.file_hash(temporary)
            os.replace(temporary, path)
            with self.db.write() as connection:
                connection.execute(
                    """UPDATE publications SET status = 'ready', output_path = ?,
                    sha256 = ?, row_count = ?, revision_vector = ?, completed_at = ?
                    WHERE id = ?""",
                    (path.name, sha, count, canonical(vector), now(), publication_id),
                )
                self.db.event(connection, "dataset_published", {
                    "publication_id": publication_id, "sha256": sha, "rows": count,
                }, source_id)
            return self.get(publication_id)
        except Exception as error:
            with self.db.write() as connection:
                connection.execute(
                    """UPDATE publications SET status = 'failed', error = ?, completed_at = ?
                    WHERE id = ?""", (str(error), now(), publication_id)
                )
            raise
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def write_rows(self, stream, rows, format):
        writer = None
        if format == "csv":
            writer = csv.DictWriter(stream, fieldnames=EXPORT_FIELDS)
            writer.writeheader()
        count = 0
        for row in rows:
            item = product_row(row)
            item = {field: item[field] for field in EXPORT_FIELDS}
            if writer:
                item["attributes"] = canonical(item["attributes"])
                writer.writerow(item)
            else:
                stream.write(canonical(item) + "\n")
            count += 1
        return count

    def file_hash(self, path):
        hasher = hashlib.sha256()
        with open(path, "rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                hasher.update(block)
        return hasher.hexdigest()

    def get(self, publication_id):
        with self.db.read() as connection:
            row = connection.execute(
                "SELECT * FROM publications WHERE id = ?", (publication_id,)
            ).fetchone()
            if row is None:
                raise NotFound("publication does not exist")
            result = dict(row)
            result["revision_vector"] = json.loads(result["revision_vector"] or "{}")
            return result

    def list(self, limit=100):
        with self.db.read() as connection:
            return [dict(row) for row in connection.execute(
                "SELECT * FROM publications ORDER BY created_at DESC LIMIT ?", (limit,)
            )]

    def artifact(self, publication_id):
        record = self.get(publication_id)
        if record["status"] != "ready":
            raise Conflict("publication is not ready")
        path = (self.root / record["output_path"]).resolve()
        if not path.is_relative_to(self.root) or not path.is_file():
            raise IntegrityError("publication file is missing")
        if self.file_hash(path) != record["sha256"]:
            raise IntegrityError("publication file checksum does not match")
        return path

    def diff(self, first_id, second_id):
        first = self.get(first_id)
        second = self.get(second_id)
        if first["format"] != "jsonl" or second["format"] != "jsonl":
            raise ValidationError("dataset diff requires JSONL publications")
        def load(publication_id):
            with self.artifact(publication_id).open(encoding="utf-8") as stream:
                values = [json.loads(line) for line in stream if line.strip()]
            return {(item["source_id"], item["sku"]): item for item in values}
        old = load(first_id)
        new = load(second_id)
        return {
            "added": [new[key] for key in sorted(new.keys() - old.keys())],
            "removed": [old[key] for key in sorted(old.keys() - new.keys())],
            "changed": [
                {"before": old[key], "after": new[key]}
                for key in sorted(old.keys() & new.keys()) if old[key] != new[key]
            ],
        }
