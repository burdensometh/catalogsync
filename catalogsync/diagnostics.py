import json

from .archive import Archive
from .models import digest
from .request_policy import admission_status


class Diagnostics:
    def __init__(self, database, archive_root, state_dir=None):
        self.db = database
        self.archive = Archive(archive_root)
        self.state_dir = state_dir or archive_root

    def check(self, verify_archives=False):
        issues = []
        integrity = self.db.integrity()
        if not integrity["ok"]:
            issues.append({"kind": "database", "detail": integrity})
        admission = admission_status(self.state_dir)
        for entry in admission:
            if not entry.get("readable"):
                issues.append({"kind": "request_admission", "origin": entry["origin"]})
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
                    WHERE source_id = ? AND sku = ? AND revision = ?""",
                    (row["source_id"], row["sku"], row["revision"]),
                ).fetchone()
                if version is None or version["fingerprint"] != row["fingerprint"]:
                    issues.append({"kind": "product_provenance", "sku": row["sku"]})
                elif digest(json.loads(version["payload"])) != row["fingerprint"]:
                    issues.append({"kind": "version_checksum", "sku": row["sku"]})
            pages = [dict(row) for row in connection.execute("SELECT * FROM pages")]
        archives = self.archive.verify(pages) if verify_archives else []
        for result in archives:
            if not result["ok"]:
                issues.append({"kind": "archive", **result})
        return {
            "ok": not issues,
            "issues": issues,
            "archives_checked": len(archives),
            "request_admission": admission,
        }
