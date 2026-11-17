import json

from .archive import Archive
from .models import digest


class Diagnostics:
    def __init__(self, database, archive_root):
        self.db = database
        self.archive = Archive(archive_root)

    def check(self, verify_archives=False):
        issues = []
        integrity = self.db.integrity()
        if not integrity["ok"]:
            issues.append({"kind": "database", "detail": integrity})
        with self.db.read() as connection:
            for row in connection.execute("SELECT * FROM runs"):
                run = dict(row)
                aggregate = connection.execute(
                    """SELECT count(*) AS pages, coalesce(sum(accepted), 0) AS accepted,
                    coalesce(sum(rejected), 0) AS rejected FROM pages WHERE run_id = ?""",
                    (run["id"],),
                ).fetchone()
                if (run["page_count"], run["accepted"], run["rejected"]) != tuple(aggregate):
                    issues.append({"kind": "run_page_totals", "run_id": run["id"]})
                outcomes = sum(run[name] for name in ("inserted", "updated", "unchanged", "stale"))
                if outcomes != run["accepted"]:
                    issues.append({"kind": "run_outcome_totals", "run_id": run["id"]})
                rejected = connection.execute(
                    "SELECT count(*) FROM rejects WHERE run_id = ?", (run["id"],)
                ).fetchone()[0]
                if rejected != run["rejected"]:
                    issues.append({"kind": "reject_totals", "run_id": run["id"]})
                if run["status"] == "completed" and run["next_cursor"] is not None:
                    issues.append({"kind": "completed_cursor", "run_id": run["id"]})
            for row in connection.execute("SELECT * FROM products"):
                version = connection.execute(
                    """SELECT fingerprint, payload FROM product_versions
                    WHERE source_id = ? AND sku = ? AND revision = ? AND rule_version = ?""",
                    (row["source_id"], row["sku"], row["revision"], row["rule_version"]),
                ).fetchone()
                if version is None or version["fingerprint"] != row["fingerprint"]:
                    issues.append({"kind": "product_provenance", "sku": row["sku"]})
                elif digest(json.loads(version["payload"])) != row["fingerprint"]:
                    issues.append({"kind": "version_checksum", "sku": row["sku"]})
                if (row["last_run_id"] is None) == (row["last_job_id"] is None):
                    issues.append({"kind": "product_origin", "sku": row["sku"]})
                elif row["last_job_id"] is not None and connection.execute(
                    "SELECT 1 FROM reprocess_jobs WHERE id = ?", (row["last_job_id"],)
                ).fetchone() is None:
                    issues.append({"kind": "product_reprocess_origin", "sku": row["sku"]})
            for row in connection.execute("SELECT * FROM reprocess_jobs"):
                job = dict(row)
                aggregate = connection.execute(
                    """SELECT count(*) AS pages, coalesce(sum(accepted), 0) AS accepted,
                    coalesce(sum(rejected), 0) AS rejected
                    FROM reprocess_pages WHERE job_id = ?""",
                    (job["id"],),
                ).fetchone()
                if job["page_count"] != aggregate["pages"]:
                    issues.append({"kind": "reprocess_page_totals", "job_id": job["id"]})
                if job["accepted"] + job["rejected"] != aggregate["accepted"] + aggregate["rejected"]:
                    issues.append({"kind": "reprocess_record_totals", "job_id": job["id"]})
                if job["status"] == "completed":
                    outcomes = sum(job[name] for name in ("inserted", "updated", "unchanged", "stale"))
                    if outcomes != job["accepted"]:
                        issues.append({"kind": "reprocess_outcome_totals", "job_id": job["id"]})
                    if job["next_ordinal"] != job["page_total"] or job["switched_at"] is None:
                        issues.append({"kind": "reprocess_completed_state", "job_id": job["id"]})
            pages = [dict(row) for row in connection.execute("SELECT * FROM pages")]
        archives = self.archive.verify(pages) if verify_archives else []
        for result in archives:
            if not result["ok"]:
                issues.append({"kind": "archive", **result})
        return {"ok": not issues, "issues": issues, "archives_checked": len(archives)}
