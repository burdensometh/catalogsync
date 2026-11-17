import json

from .errors import IntegrityError, NotFound
from .models import digest


class Provenance:
    def __init__(self, application):
        self.app = application

    def explain(self, source_id, sku):
        product = self.app.query.product(source_id, sku)
        versions = self.app.query.history(source_id, sku)
        with self.app.database.read() as connection:
            observations = [dict(row) for row in connection.execute(
                """SELECT v.rule_version,
                coalesce(r.id, j.id) AS origin_id,
                CASE WHEN v.first_run_id IS NOT NULL THEN 'run' ELSE 'reprocess' END AS origin,
                r.mode, r.status, r.snapshot_id, r.boundary,
                v.revision, v.observed_at
                FROM product_versions v
                LEFT JOIN runs r ON r.id = v.first_run_id
                LEFT JOIN reprocess_jobs j ON j.id = v.first_job_id
                WHERE v.source_id = ? AND v.sku = ?
                ORDER BY v.revision, v.rule_version""", (source_id, sku)
            )]
        return {
            "current": product,
            "effective_rule_version": product.get("rule_version"),
            "versions": versions,
            "first_observations": observations,
        }

    def run_profile(self, run_id):
        self.app.runs.get(run_id)
        with self.app.database.read() as connection:
            row = connection.execute("SELECT * FROM run_profiles WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                raise NotFound("run has no frozen profile")
            value = dict(row)
            value["document"] = json.loads(value["document"])
            if digest(value["document"]) != value["fingerprint"]:
                raise IntegrityError("run profile checksum does not match")
            return value

    def page(self, run_id, cursor=""):
        pages = self.app.runs.pages(run_id)
        page = next((item for item in pages if item["cursor"] == cursor), None)
        if page is None:
            raise NotFound("page does not exist")
        raw = self.app.ingest.archive.read(page["archive_path"], page["raw_sha256"])
        profile = self.run_profile(run_id)
        from .extraction import extract_page
        run = self.app.runs.get(run_id)
        parser = "json" if run["mode"] == "delta" else profile["document"]["parser"]
        payload = extract_page(raw, parser, cursor)
        rejected = {row["ordinal"]: row["reason"] for row in self.app.runs.rejects(run_id, 100000) if row["cursor"] == cursor}
        return {
            "page": page,
            "profile": profile,
            "records": [
                {"ordinal": index, "raw": record, "rejection": rejected.get(index)}
                for index, record in enumerate(payload["records"])
            ],
        }

    def reconcile_counts(self, run_id):
        run = self.app.runs.get(run_id)
        pages = self.app.runs.pages(run_id)
        with self.app.database.read() as connection:
            versions = connection.execute(
                "SELECT count(*) FROM product_versions WHERE first_run_id=?", (run_id,)
            ).fetchone()[0]
        return {
            "run_id": run_id,
            "profile_version": self.app.profiles.version_for_run(run_id),
            "pages": len(pages),
            "input_records": sum(page["record_count"] for page in pages),
            "accepted": run["accepted"],
            "rejected": run["rejected"],
            "new_version_observations": versions,
            "balanced": sum(page["record_count"] for page in pages) == run["accepted"] + run["rejected"],
        }
