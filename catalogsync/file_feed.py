from pathlib import Path
import json

from .errors import Conflict, NotFound, ValidationError
from .models import identifier, now


class FileFeed:
    def __init__(self, application):
        self.app = application
        self.db = application.database

    def register(self, feed_id, source_id, path):
        identifier(feed_id, "feed_id")
        self.app.registry.get(source_id)
        path = Path(path).resolve()
        if not path.is_file():
            raise ValidationError("feed must be an existing append-only NDJSON file")
        stat = path.stat()
        identity = f"{stat.st_dev}:{stat.st_ino}"
        with self.db.write() as connection:
            old = connection.execute("SELECT * FROM file_feeds WHERE id=?", (feed_id,)).fetchone()
            if old:
                if (old["source_id"], old["path"], old["file_identity"]) != (source_id, str(path), identity):
                    raise Conflict("feed identity cannot be rebound")
            else:
                connection.execute(
                    "INSERT INTO file_feeds VALUES (?,?,?,?,0,0,?)",
                    (feed_id, source_id, str(path), identity, now()),
                )
                self.db.event(connection, "file_feed_registered", {"feed_id": feed_id}, source_id)
        return self.get(feed_id)

    def get(self, feed_id):
        with self.db.read() as connection:
            row = connection.execute("SELECT * FROM file_feeds WHERE id=?", (feed_id,)).fetchone()
            if row is None:
                raise NotFound("file feed does not exist")
            return dict(row)

    def check_file(self, feed):
        path = Path(feed["path"])
        try:
            stat = path.stat()
        except OSError as error:
            raise ValidationError(f"feed is unavailable: {error}") from error
        identity = f"{stat.st_dev}:{stat.st_ino}"
        if identity != feed["file_identity"]:
            raise Conflict("feed file was replaced; register a new feed identity")
        if stat.st_size < feed["byte_offset"]:
            raise Conflict("feed file was truncated below the saved offset")
        return path

    def poll(self, feed_id, batch_size=100, max_lines=1000):
        if type(batch_size) is not int or not 1 <= batch_size <= 1000:
            raise ValidationError("batch_size must be between 1 and 1000")
        if type(max_lines) is not int or not 1 <= max_lines <= 100000:
            raise ValidationError("max_lines must be between 1 and 100000")
        feed = self.get(feed_id)
        path = self.check_file(feed)
        committed = 0
        accepted = 0
        duplicates = 0
        incomplete = False
        with path.open("rb") as stream:
            stream.seek(feed["byte_offset"])
            expected_offset = feed["byte_offset"]
            lines = 0
            events = []
            while committed + lines < max_lines:
                start = stream.tell()
                raw = stream.readline(1024 * 1024 + 1)
                if not raw:
                    break
                if len(raw) > 1024 * 1024:
                    raise ValidationError("feed line exceeds 1 MiB")
                if not raw.endswith(b"\n"):
                    incomplete = True
                    stream.seek(start)
                    break
                lines += 1
                if raw.strip():
                    try:
                        event = json.loads(raw.decode("utf-8-sig" if start == 0 else "utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError) as error:
                        raise ValidationError(f"invalid event at byte offset {start}: {error}") from error
                    if not isinstance(event, dict) or "id" not in event or "record" not in event:
                        raise ValidationError(f"event at offset {start} requires id and record")
                    events.append(event)
                if lines >= batch_size:
                    result = self.commit(feed, expected_offset, stream.tell(), lines, events)
                    accepted += result["accepted"]
                    duplicates += result["duplicates"]
                    committed += lines
                    expected_offset = stream.tell()
                    lines = 0
                    events = []
            if lines:
                result = self.commit(feed, expected_offset, stream.tell(), lines, events)
                accepted += result["accepted"]
                duplicates += result["duplicates"]
                committed += lines
        return {
            "feed": self.get(feed_id),
            "lines": committed,
            "accepted_events": accepted,
            "duplicates": duplicates,
            "waiting_for_newline": incomplete,
        }

    def commit(self, feed, expected_offset, new_offset, lines, events):
        submission = self.app.inbox.submit(feed["source_id"], events) if events else {"accepted": 0, "duplicates": 0}
        with self.db.write() as connection:
            updated = connection.execute(
                """UPDATE file_feeds SET byte_offset=?,line_count=line_count+?,checked_at=?
                WHERE id=? AND byte_offset=?""",
                (new_offset, lines, now(), feed["id"], expected_offset),
            )
            if updated.rowcount != 1:
                raise Conflict("another poll advanced the feed; reload its checkpoint")
            self.db.event(connection, "feed_offset_committed", {
                "feed_id": feed["id"], "offset": new_offset, "lines": lines,
            }, feed["source_id"])
        return submission

    def list(self):
        with self.db.read() as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM file_feeds ORDER BY id")]
