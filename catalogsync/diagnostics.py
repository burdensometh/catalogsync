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
            for row in connection.execute("SELECT * FROM crawls"):
                crawl = dict(row)
                run = connection.execute(
                    "SELECT status FROM runs WHERE id = ?", (crawl["run_id"],)
                ).fetchone()
                if run is None:
                    issues.append({"kind": "crawl_run_missing", "crawl_id": crawl["id"]})
                elif crawl["status"] != run["status"]:
                    issues.append({"kind": "crawl_run_status", "crawl_id": crawl["id"]})
                states = {
                    item[0]: item[1] for item in connection.execute(
                        "SELECT state, count(*) FROM crawl_items WHERE crawl_id = ? GROUP BY state",
                        (crawl["id"],),
                    )
                }
                if crawl["status"] == "completed" and (
                    states.get("pending") or states.get("failed")
                ):
                    issues.append({"kind": "crawl_unfinished_items", "crawl_id": crawl["id"]})
                page_rows = connection.execute(
                    "SELECT count(*) FROM pages WHERE run_id = ?", (crawl["run_id"],)
                ).fetchone()[0]
                if page_rows != states.get("completed", 0):
                    issues.append({"kind": "crawl_page_items", "crawl_id": crawl["id"]})
                orphans = connection.execute(
                    """SELECT count(*) FROM crawl_items i
                    LEFT JOIN crawl_items p ON p.id = i.parent_id
                    WHERE i.crawl_id = ? AND i.parent_id IS NOT NULL AND p.id IS NULL""",
                    (crawl["id"],),
                ).fetchone()[0]
                if orphans:
                    issues.append({"kind": "crawl_parent_missing", "crawl_id": crawl["id"]})
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
        return {"ok": not issues, "issues": issues, "archives_checked": len(archives)}
