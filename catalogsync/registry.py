from pathlib import Path
from urllib.parse import urlsplit

from .errors import Conflict, NotFound, ValidationError
from .models import identifier, now, text


class Registry:
    def __init__(self, database):
        self.db = database

    def register(self, source_id, name, kind, location):
        source_id = identifier(source_id, "source_id")
        name = text(name, "name", 200)
        if kind == "file":
            location = str(Path(location).resolve())
            if not Path(location).is_dir():
                raise ValidationError("file source must be an existing directory")
        elif kind == "http":
            parsed = urlsplit(location)
            if parsed.scheme not in {"http", "https"} or not parsed.hostname:
                raise ValidationError("source URL must use HTTP or HTTPS")
            if parsed.username or parsed.password or parsed.fragment:
                raise ValidationError("source URL cannot contain credentials or fragment")
            if parsed.query:
                raise ValidationError("source base URL cannot contain query parameters")
            location = location.rstrip("/")
        else:
            raise ValidationError("source kind must be file or http")
        with self.db.write() as connection:
            existing = connection.execute(
                "SELECT * FROM sources WHERE id = ?", (source_id,)
            ).fetchone()
            if existing:
                if (existing["kind"], existing["location"]) != (kind, location):
                    raise Conflict("a registered source cannot change identity")
                connection.execute(
                    "UPDATE sources SET name = ?, updated_at = ? WHERE id = ?",
                    (name, now(), source_id),
                )
            else:
                timestamp = now()
                connection.execute(
                    "INSERT INTO sources VALUES (?, ?, ?, ?, 1, ?, ?)",
                    (source_id, name, kind, location, timestamp, timestamp),
                )
            self.db.event(connection, "source_registered", {"kind": kind}, source_id)
        return self.get(source_id)

    def get(self, source_id):
        with self.db.read() as connection:
            row = connection.execute(
                "SELECT * FROM sources WHERE id = ?", (source_id,)
            ).fetchone()
            if row is None:
                raise NotFound(f"unknown source: {source_id}")
            return dict(row)

    def list(self):
        with self.db.read() as connection:
            return [
                dict(row)
                for row in connection.execute("SELECT * FROM sources ORDER BY id")
            ]

    def enable(self, source_id, enabled):
        if not isinstance(enabled, bool):
            raise ValidationError("enabled must be boolean")
        self.get(source_id)
        with self.db.write() as connection:
            if not enabled:
                active = connection.execute(
                    "SELECT 1 FROM runs WHERE source_id = ? AND status = 'running'",
                    (source_id,),
                ).fetchone()
                if active:
                    raise Conflict("cannot disable a running source")
            connection.execute(
                "UPDATE sources SET enabled = ?, updated_at = ? WHERE id = ?",
                (int(enabled), now(), source_id),
            )
            self.db.event(connection, "source_enabled", {"enabled": enabled}, source_id)
        return self.get(source_id)
