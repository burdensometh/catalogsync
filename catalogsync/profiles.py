import json

from .errors import NotFound
from .models import canonical, digest, now
from .normalization import Profile


class Profiles:
    def __init__(self, database):
        self.db = database

    def set(self, source_id, specification):
        profile = Profile(specification)
        document = profile.document()
        fingerprint = digest(document)
        with self.db.write() as connection:
            source = connection.execute("SELECT 1 FROM sources WHERE id=?", (source_id,)).fetchone()
            if source is None:
                raise NotFound("source does not exist")
            current = connection.execute(
                "SELECT version, fingerprint FROM source_profiles WHERE source_id=? ORDER BY version DESC LIMIT 1",
                (source_id,),
            ).fetchone()
            if current and current["fingerprint"] == fingerprint:
                version = current["version"]
            else:
                version = current["version"] + 1 if current else 1
                connection.execute(
                    "INSERT INTO source_profiles VALUES (?,?,?,?,?)",
                    (source_id, version, canonical(document), fingerprint, now()),
                )
                self.db.event(connection, "profile_updated", {
                    "version": version, "fingerprint": fingerprint,
                }, source_id)
        return {"source_id": source_id, "version": version, "fingerprint": fingerprint, "document": document}

    def get(self, source_id, version=None):
        with self.db.read() as connection:
            if not connection.execute("SELECT 1 FROM sources WHERE id=?", (source_id,)).fetchone():
                raise NotFound("source does not exist")
            return self.load(connection, source_id, version)

    def load(self, connection, source_id, version=None):
        if version is None:
            row = connection.execute(
                "SELECT * FROM source_profiles WHERE source_id=? ORDER BY version DESC LIMIT 1", (source_id,)
            ).fetchone()
        else:
            row = connection.execute(
                "SELECT * FROM source_profiles WHERE source_id=? AND version=?", (source_id, version)
            ).fetchone()
        if row is None:
            if version not in {None, 0}:
                raise NotFound("profile version does not exist")
            document = Profile().document()
            return {"source_id": source_id, "version": 0, "fingerprint": digest(document), "document": document}
        result = dict(row)
        result["document"] = json.loads(result["document"])
        return result

    def resolve(self, connection, source_id, version=None):
        """Return (version, document, fingerprint) for a profile inside a transaction."""
        profile = self.load(connection, source_id, version)
        return profile["version"], profile["document"], profile["fingerprint"]

    def active_reprocess_version(self, connection, source_id):
        row = connection.execute(
            """SELECT target_profile_version FROM reprocess_jobs
            WHERE source_id=? AND status IN ('pending', 'running', 'failed')
            ORDER BY created_at DESC LIMIT 1""",
            (source_id,),
        ).fetchone()
        return None if row is None else row["target_profile_version"]

    def freeze(self, connection, source_id, run_id):
        """Freeze the cleaning rules for a new run.

        While a reprocess job is active the target rule version is authoritative:
        source versions arriving during the replay must be parsed under the new
        rule set so they become part of the switched result.  Otherwise the
        latest configured profile applies, matching the baseline contract.
        """
        version = self.active_reprocess_version(connection, source_id)
        if version is None:
            version, document, fingerprint = self.resolve(connection, source_id, None)
        else:
            version, document, fingerprint = self.resolve(connection, source_id, version)
        connection.execute(
            "INSERT INTO run_profiles VALUES (?,?,?,?)",
            (run_id, version, canonical(document), fingerprint),
        )
        return version

    def for_run(self, run_id):
        with self.db.read() as connection:
            row = connection.execute("SELECT document FROM run_profiles WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                raise NotFound("run profile does not exist")
            return Profile(json.loads(row["document"]))

    def version_for_run(self, run_id):
        with self.db.read() as connection:
            row = connection.execute(
                "SELECT profile_version FROM run_profiles WHERE run_id=?", (run_id,)
            ).fetchone()
            if row is None:
                raise NotFound("run profile does not exist")
            return row["profile_version"]

    def preview(self, source_id, records):
        profile = Profile(self.get(source_id)["document"])
        from .models import Product
        from .errors import ValidationError
        result = []
        for index, raw in enumerate(records):
            try:
                value = Product.parse(profile.normalize(raw)).public()
                result.append({"index": index, "ok": True, "product": value})
            except ValidationError as error:
                result.append({"index": index, "ok": False, "error": str(error)})
        return result
