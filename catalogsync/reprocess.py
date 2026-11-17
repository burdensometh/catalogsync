import json
import uuid

from .errors import CatalogError, Conflict, NotFound, ValidationError
from .extraction import extract_page
from .merge import ProductMerger
from .models import Page, Product, canonical, now
from .normalization import Profile


COUNTERS = ("inserted", "updated", "unchanged", "stale")
ACTIVE_REPROCESS = ("pending", "running", "failed")


class Reprocessor:
    """Replay archived raw pages for one source under a specified rule version.

    Reprocessing separates two orthogonal notions of version:

    * the *source version* a record carries (its ``revision`` and the page
      boundary), which the archive fixed when the page was first committed;
    * the *cleaning-rule version* (a source profile version) that turns a raw
      record into a normalized product.

    Replaying the same archived page under different rules therefore produces
    a new interpretation of the *same* source version rather than a
    same-version conflict.  Staged results live in dedicated tables and reach
    the live catalogue only through an all-or-nothing switch.  A failed job
    leaves the previous effective result, every prior run and every published
    artifact untouched, and can be resumed from its durable page progress.
    """

    def __init__(self, application):
        self.app = application
        self.db = application.database
        self.archive = application.ingest.archive
        self.merger = ProductMerger()

    # --------------------------------------------------------------- lifecycle

    def create(self, source_id, profile_version=None, run_id=None):
        if profile_version is not None and (
            type(profile_version) is not int or profile_version < 0
        ):
            raise ValidationError("profile_version must be a nonnegative integer")
        with self.db.write() as connection:
            if connection.execute(
                "SELECT 1 FROM sources WHERE id = ?", (source_id,)
            ).fetchone() is None:
                raise NotFound(f"unknown source: {source_id}")
            active = connection.execute(
                """SELECT id FROM reprocess_jobs
                WHERE source_id = ? AND status IN ('pending', 'running', 'failed')""",
                (source_id,),
            ).fetchone()
            if active is not None:
                raise Conflict("source already has an active reprocess job")
            version, document, fingerprint = self.app.profiles.resolve(
                connection, source_id, profile_version
            )
            base = self.select_base_run(connection, source_id, run_id)
            pages = connection.execute(
                "SELECT * FROM pages WHERE run_id = ? ORDER BY committed_at, cursor",
                (base["id"],),
            ).fetchall()
            if not pages:
                raise Conflict("base run has no archived pages to reprocess")
            if base["boundary"] is None:
                raise Conflict("base run has no snapshot boundary to reprocess")
            job_id = uuid.uuid4().hex
            timestamp = now()
            connection.execute(
                """INSERT INTO reprocess_jobs
                (id, source_id, base_run_id, target_profile_version,
                 profile_document, profile_fingerprint, snapshot_id, boundary,
                 status, page_total, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)""",
                (
                    job_id,
                    source_id,
                    base["id"],
                    version,
                    canonical(document),
                    fingerprint,
                    base["snapshot_id"],
                    base["boundary"],
                    len(pages),
                    timestamp,
                ),
            )
            for ordinal, page in enumerate(pages):
                connection.execute(
                    """INSERT INTO reprocess_pages
                    (job_id, ordinal, source_run_id, cursor, next_cursor,
                     boundary, raw_sha256, archive_path, record_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        job_id,
                        ordinal,
                        base["id"],
                        page["cursor"],
                        page["next_cursor"],
                        base["boundary"],
                        page["raw_sha256"],
                        page["archive_path"],
                        page["record_count"],
                    ),
                )
            self.db.event(
                connection,
                "reprocess_created",
                {"job_id": job_id, "base_run_id": base["id"],
                 "profile_version": version, "pages": len(pages)},
                source_id,
            )
        return self.get(job_id)

    @staticmethod
    def select_base_run(connection, source_id, run_id):
        if run_id is not None:
            row = connection.execute(
                "SELECT * FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise NotFound(f"unknown run: {run_id}")
            if row["source_id"] != source_id:
                raise ValidationError("base run belongs to another source")
            if row["status"] != "completed":
                raise Conflict("base run is not completed")
            return row
        row = connection.execute(
            """SELECT * FROM runs WHERE source_id = ? AND status = 'completed'
            ORDER BY finished_at DESC, created_at DESC LIMIT 1""",
            (source_id,),
        ).fetchone()
        if row is None:
            raise Conflict("source has no completed run to reprocess")
        return row

    def start(self, source_id, profile_version=None, run_id=None):
        return self.execute(self.create(source_id, profile_version, run_id)["id"])

    def execute(self, job_id):
        job = self.get(job_id)
        if job["status"] == "completed":
            return job
        self.claim(job_id)
        try:
            self.stage(job_id)
        except Exception as error:
            self.fail(job_id, error)
            if isinstance(error, CatalogError):
                raise
            raise CatalogError(f"reprocessing failed: {error}") from error
        return self.switch(job_id)

    def claim(self, job_id):
        with self.db.write() as connection:
            row = connection.execute(
                "SELECT * FROM reprocess_jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if row is None:
                raise NotFound("reprocess job does not exist")
            if row["status"] not in {"pending", "failed"}:
                raise Conflict(f"cannot start a {row['status']} reprocess job")
            source = connection.execute(
                "SELECT enabled FROM sources WHERE id = ?", (row["source_id"],)
            ).fetchone()
            if not source[0]:
                raise Conflict("source is disabled")
            connection.execute(
                """UPDATE reprocess_jobs SET status = 'running', attempt = attempt + 1,
                error = NULL, finished_at = NULL, started_at = ? WHERE id = ?""",
                (now(), job_id),
            )
            self.db.event(connection, "reprocess_started", {"job_id": job_id}, row["source_id"])
        return self.get(job_id)

    def cancel(self, job_id):
        with self.db.write() as connection:
            row = connection.execute(
                "SELECT * FROM reprocess_jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if row is None:
                raise NotFound("reprocess job does not exist")
            if row["status"] == "completed":
                raise Conflict("completed reprocess jobs cannot be cancelled")
            connection.execute(
                """UPDATE reprocess_jobs SET status = 'cancelled', error = ?,
                finished_at = ? WHERE id = ?""",
                ("cancelled by operator", now(), job_id),
            )
            self.db.event(connection, "reprocess_cancelled", {"job_id": job_id}, row["source_id"])
        return self.get(job_id)

    def fail(self, job_id, error, conflict=False):
        with self.db.write() as connection:
            row = connection.execute(
                "SELECT source_id, status FROM reprocess_jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if row is None or row["status"] in {"completed", "cancelled"}:
                return
            connection.execute(
                """UPDATE reprocess_jobs SET status = 'failed', error = ?,
                conflicts = conflicts + ?, finished_at = ? WHERE id = ?""",
                (str(error)[:4000], int(conflict), now(), job_id),
            )
            self.db.event(
                connection, "reprocess_failed",
                {"job_id": job_id, "error": str(error)},
                row["source_id"],
            )

    # ----------------------------------------------------------------- staging

    def stage(self, job_id):
        job = self.get_raw(job_id)
        if job["status"] != "running":
            raise Conflict("reprocess staging requires a running job")
        profile = Profile(json.loads(job["profile_document"]))
        base = self.app.runs.get(job["base_run_id"])
        parser = "json" if base["mode"] == "delta" else profile.parser
        while True:
            job = self.get_raw(job_id)
            if job["next_ordinal"] >= job["page_total"]:
                return job
            self.stage_page(job_id, job["next_ordinal"], profile, parser)

    def stage_page(self, job_id, ordinal, profile, parser):
        job = self.get_raw(job_id)
        page_row = self.page_record(job_id, ordinal)
        raw = self.archive.read(page_row["archive_path"], page_row["raw_sha256"])
        payload = extract_page(raw, parser, page_row["cursor"])
        page = Page.parse(payload, page_row["cursor"])
        if (page.snapshot_id, page.boundary) != (job["snapshot_id"], job["boundary"]):
            raise ValidationError("archived page identity differs from its run")
        accepted = []
        rejected = []
        for record_ordinal, record in enumerate(page.records):
            try:
                product = Product.parse(profile.normalize(record))
                if product.revision > page.boundary:
                    raise ValidationError("record revision exceeds snapshot boundary")
                accepted.append((record_ordinal, product, record))
            except ValidationError as error:
                rejected.append((record_ordinal, str(error), record))
        with self.db.write() as connection:
            current = connection.execute(
                "SELECT * FROM reprocess_jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if current is None or current["status"] != "running":
                raise Conflict("reprocess page requires an owned running job")
            if current["next_ordinal"] != ordinal:
                raise Conflict("reprocess page does not match durable progress")
            connection.execute(
                "DELETE FROM reprocess_records WHERE job_id = ? AND page_ordinal = ?",
                (job_id, ordinal),
            )
            connection.execute(
                "DELETE FROM reprocess_rejects WHERE job_id = ? AND page_ordinal = ?",
                (job_id, ordinal),
            )
            for record_ordinal, product, record in accepted:
                connection.execute(
                    """INSERT INTO reprocess_records
                    (job_id, page_ordinal, record_ordinal, sku, revision,
                     rule_version, fingerprint, source_fingerprint, parser,
                     payload, raw_record, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        job_id,
                        ordinal,
                        record_ordinal,
                        product.sku,
                        product.revision,
                        current["target_profile_version"],
                        product.fingerprint(),
                        self.merger.source_fingerprint(parser, record),
                        parser,
                        canonical(product.payload()),
                        canonical(record),
                        now(),
                    ),
                )
            for record_ordinal, reason, record in rejected:
                connection.execute(
                    """INSERT INTO reprocess_rejects
                    (job_id, page_ordinal, record_ordinal, cursor, reason,
                     raw_record, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (job_id, ordinal, record_ordinal, page.cursor, reason,
                     canonical(record), now()),
                )
            connection.execute(
                """UPDATE reprocess_pages SET record_count = ?, accepted = ?,
                rejected = ?, committed_at = ? WHERE job_id = ? AND ordinal = ?""",
                (len(page.records), len(accepted), len(rejected), now(), job_id, ordinal),
            )
            connection.execute(
                """UPDATE reprocess_jobs SET next_ordinal = ?, page_count = page_count + 1,
                accepted = accepted + ?, rejected = rejected + ? WHERE id = ?""",
                (ordinal + 1, len(accepted), len(rejected), job_id),
            )
            self.db.event(
                connection,
                "reprocess_page_staged",
                {"job_id": job_id, "ordinal": ordinal, "cursor": page.cursor,
                 "accepted": len(accepted), "rejected": len(rejected)},
                current["source_id"],
            )

    # ------------------------------------------------------------------ switch

    def switch(self, job_id):
        """Apply every staged result in one transaction; never expose a partial switch.

        A staged record that turns out to be a genuine source-version conflict
        is quarantined as a reject, exactly as ordinary ingestion does, so the
        live product keeps its original source content.  The batch itself only
        switches once every page has been staged, and the switch is a single
        transaction: on any other error nothing is applied and the job is left
        failed, resumable, with the previous effective result intact.
        """
        job = self.get_raw(job_id)
        if job["status"] == "completed":
            return self.get(job_id)
        if job["status"] not in ACTIVE_REPROCESS:
            raise Conflict(f"cannot switch a {job['status']} reprocess job")
        if job["page_total"] == 0 or job["next_ordinal"] != job["page_total"]:
            raise Conflict("all archived pages must be staged before switching")
        counts = dict.fromkeys(COUNTERS, 0)
        conflicts = []
        try:
            with self.db.write() as connection:
                current = connection.execute(
                    "SELECT * FROM reprocess_jobs WHERE id = ?", (job_id,)
                ).fetchone()
                if current["status"] not in ACTIVE_REPROCESS:
                    raise Conflict("reprocess job is no longer switchable")
                if current["next_ordinal"] != current["page_total"]:
                    raise Conflict("all archived pages must be staged before switching")
                staged = list(connection.execute(
                    """SELECT * FROM reprocess_records WHERE job_id = ?
                    ORDER BY page_ordinal, record_ordinal""",
                    (job_id,),
                ))
                for row in staged:
                    product = Product(**json.loads(row["payload"]))
                    try:
                        outcome = self.merger.apply_reprocess(
                            connection,
                            current["source_id"],
                            job_id,
                            product,
                            current["target_profile_version"],
                            row["source_fingerprint"],
                            row["parser"],
                            json.loads(row["raw_record"]),
                        )
                    except ValidationError as error:
                        conflicts.append((row, str(error)))
                        continue
                    counts[outcome] += 1
                totals = connection.execute(
                    """SELECT coalesce(sum(accepted), 0) AS accepted,
                    coalesce(sum(rejected), 0) AS rejected
                    FROM reprocess_pages WHERE job_id = ?""", (job_id,)
                ).fetchone()
                for row, reason in conflicts:
                    connection.execute(
                        """INSERT OR REPLACE INTO reprocess_rejects
                        (job_id, page_ordinal, record_ordinal, cursor, reason,
                         raw_record, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (job_id, row["page_ordinal"], row["record_ordinal"], "",
                         reason, row["raw_record"], now()),
                    )
                timestamp = now()
                connection.execute(
                    """UPDATE reprocess_jobs SET status = 'completed',
                    accepted = ?, rejected = ?, inserted = ?, updated = ?,
                    unchanged = ?, stale = ?, conflicts = ?, error = NULL,
                    finished_at = ?, switched_at = ? WHERE id = ?""",
                    (
                        totals["accepted"] - len(conflicts),
                        totals["rejected"] + len(conflicts),
                        counts["inserted"],
                        counts["updated"],
                        counts["unchanged"],
                        counts["stale"],
                        len(conflicts),
                        timestamp,
                        timestamp,
                        job_id,
                    ),
                )
                self.db.event(
                    connection,
                    "reprocess_switched",
                    {"job_id": job_id,
                     "profile_version": current["target_profile_version"],
                     "conflicts": len(conflicts), **counts},
                    current["source_id"],
                )
        except (ValidationError, Conflict) as error:
            self.fail(job_id, error, conflict=isinstance(error, ValidationError))
            raise
        except Exception as error:
            # Never leave a job wedged in 'running': that would block both
            # resume and any future reprocess for the source.
            self.fail(job_id, error)
            raise CatalogError(f"reprocess switch failed: {error}") from error
        return self.get(job_id)

    # ------------------------------------------------------------------- reads

    def get_raw(self, job_id):
        with self.db.read() as connection:
            row = connection.execute(
                "SELECT * FROM reprocess_jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if row is None:
                raise NotFound(f"unknown reprocess job: {job_id}")
            return dict(row)

    def get(self, job_id):
        job = self.get_raw(job_id)
        job["profile_document"] = json.loads(job["profile_document"])
        return job

    def profile(self, job_id):
        return Profile(json.loads(self.get_raw(job_id)["profile_document"]))

    def list(self, source_id=None, limit=100):
        with self.db.read() as connection:
            if source_id:
                rows = connection.execute(
                    """SELECT * FROM reprocess_jobs WHERE source_id = ?
                    ORDER BY created_at DESC LIMIT ?""",
                    (source_id, limit),
                )
            else:
                rows = connection.execute(
                    "SELECT * FROM reprocess_jobs ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                )
            result = []
            for row in rows:
                item = dict(row)
                item["profile_document"] = json.loads(item["profile_document"])
                result.append(item)
            return result

    def page_record(self, job_id, ordinal):
        with self.db.read() as connection:
            row = connection.execute(
                "SELECT * FROM reprocess_pages WHERE job_id = ? AND ordinal = ?",
                (job_id, ordinal),
            ).fetchone()
            if row is None:
                raise NotFound("reprocess page does not exist")
            return dict(row)

    def pages(self, job_id):
        self.get(job_id)
        with self.db.read() as connection:
            return [dict(row) for row in connection.execute(
                "SELECT * FROM reprocess_pages WHERE job_id = ? ORDER BY ordinal", (job_id,)
            )]

    def rejects(self, job_id, limit=100000):
        self.get(job_id)
        with self.db.read() as connection:
            return [dict(row) for row in connection.execute(
                """SELECT * FROM reprocess_rejects WHERE job_id = ?
                ORDER BY page_ordinal, record_ordinal LIMIT ?""",
                (job_id, limit),
            )]

    def reconcile(self, job_id):
        job = self.get(job_id)
        with self.db.read() as connection:
            pages = connection.execute(
                """SELECT count(*) AS pages, coalesce(sum(record_count), 0) AS records,
                coalesce(sum(accepted), 0) AS accepted, coalesce(sum(rejected), 0) AS rejected
                FROM reprocess_pages WHERE job_id = ?""", (job_id,)
            ).fetchone()
            rejects = connection.execute(
                "SELECT count(*) FROM reprocess_rejects WHERE job_id = ?", (job_id,)
            ).fetchone()[0]
        outcomes = sum(job[name] for name in COUNTERS)
        completed = job["status"] == "completed"
        return {
            "job_id": job_id,
            "status": job["status"],
            "staged_pages": pages["pages"],
            "pages_expected": job["page_total"],
            "input_records": pages["records"],
            "staged_accepted": pages["accepted"],
            "staged_rejected": pages["rejected"],
            "accepted": job["accepted"] if completed else pages["accepted"],
            "rejected": job["rejected"] if completed else pages["rejected"],
            "outcomes": {name: job[name] for name in COUNTERS},
            "conflicts": job["conflicts"],
            "balanced": pages["records"] == pages["accepted"] + pages["rejected"],
            "outcome_balanced": (
                not completed
                or outcomes + job["conflicts"] == pages["accepted"]
            ),
        }

    def page(self, job_id, ordinal):
        job = self.get(job_id)
        record = self.page_record(job_id, ordinal)
        raw = self.archive.read(record["archive_path"], record["raw_sha256"])
        profile = Profile(job["profile_document"])
        base = self.app.runs.get(job["base_run_id"])
        parser = "json" if base["mode"] == "delta" else profile.parser
        payload = extract_page(raw, parser, record["cursor"])
        with self.db.read() as connection:
            rejected = {
                row["record_ordinal"]: row["reason"]
                for row in connection.execute(
                    """SELECT record_ordinal, reason FROM reprocess_rejects
                    WHERE job_id = ? AND page_ordinal = ?""",
                    (job_id, ordinal),
                )
            }
        return {
            "job": job,
            "page": record,
            "profile": {
                "version": job["target_profile_version"],
                "fingerprint": job["profile_fingerprint"],
                "document": job["profile_document"],
            },
            "records": [
                {"ordinal": index, "raw": item, "rejection": rejected.get(index)}
                for index, item in enumerate(payload["records"])
            ],
        }
