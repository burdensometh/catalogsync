from pathlib import Path
import hashlib
import os
import tempfile

from .errors import IntegrityError, NotFound
from .models import identifier


class Archive:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def put(self, source_id, run_id, payload):
        identifier(source_id)
        identifier(run_id)
        sha = hashlib.sha256(payload).hexdigest()
        directory = self.root / source_id / run_id
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / f"{sha}.json"
        if target.exists():
            self.read(str(target.relative_to(self.root)), sha)
            return str(target.relative_to(self.root)), sha
        descriptor, temporary = tempfile.mkstemp(prefix=".page-", dir=directory)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return str(target.relative_to(self.root)), sha

    def read(self, relative, expected_sha=None):
        path = (self.root / relative).resolve()
        if not path.is_relative_to(self.root):
            raise IntegrityError("archive path escapes its root")
        if not path.is_file():
            raise NotFound("archived page does not exist")
        data = path.read_bytes()
        if expected_sha and hashlib.sha256(data).hexdigest() != expected_sha:
            raise IntegrityError("archived page checksum does not match")
        return data

    def verify(self, pages):
        results = []
        for page in pages:
            try:
                data = self.read(page["archive_path"], page["raw_sha256"])
                results.append({"path": page["archive_path"], "ok": True, "bytes": len(data)})
            except (IntegrityError, NotFound) as error:
                results.append({"path": page["archive_path"], "ok": False, "error": str(error)})
        return results
