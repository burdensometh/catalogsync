from .archive import Archive
from .clients import client_for
from .errors import CatalogError, Conflict, ValidationError
from .merge import ProductMerger
from .models import Page, Product, canonical, now
from .registry import Registry
from .profiles import Profiles
from .extraction import extract_page
from .runs import COUNTERS, Runs


class Ingestor:
    def __init__(self, database, archive_root):
        self.db = database
        self.archive = Archive(archive_root)
        self.registry = Registry(database)
        self.runs = Runs(database)
        self.merger = ProductMerger()
        self.profiles = Profiles(database)

    def start(self, source_id, client=None):
        run = self.runs.create(source_id)
        return self.execute(run["id"], client)

    def execute(self, run_id, client=None):
        run = self.runs.get(run_id)
        if run["status"] == "completed":
            return run
        run = self.runs.claim(run_id)
        try:
            source = self.registry.get(run["source_id"])
            client = client or client_for(source)
            profile = self.profiles.for_run(run_id)
            cursor = run["next_cursor"]
            visited = {page["cursor"] for page in self.runs.pages(run_id)}
            while cursor is not None:
                if cursor in visited:
                    raise ValidationError("source pagination contains a cycle")
                raw = client.fetch(cursor)
                page = Page.parse(extract_page(raw, profile.parser, cursor), cursor)
                if page.next_cursor is not None and page.next_cursor in visited:
                    raise ValidationError("source pagination returns an earlier cursor")
                relative, sha = self.archive.put(source["id"], run_id, raw)
                self.commit_page(run_id, page, relative, sha)
                visited.add(cursor)
                cursor = page.next_cursor
            return self.runs.complete(run_id)
        except Exception as error:
            self.runs.fail(run_id, error)
            if isinstance(error, CatalogError):
                raise
            raise CatalogError(f"ingestion failed: {error}") from error

    def apply_delta(self, source_id, records):
        if not isinstance(records, list) or not records:
            raise ValidationError("delta records must be a nonempty array")
        run = self.runs.create(source_id, "delta")
        self.runs.claim(run["id"])
        try:
            profile = self.profiles.for_run(run["id"])
            revisions = []
            for record in records:
                try:
                    revisions.append(profile.normalize(record).get("revision", 0))
                except ValidationError:
                    continue
            boundary = max((value for value in revisions if type(value) is int), default=1)
            page = Page(run["id"], max(1, boundary), "", None, records)
            raw = canonical({
                "snapshot_id": page.snapshot_id,
                "boundary": page.boundary,
                "cursor": "",
                "next_cursor": None,
                "records": records,
            }).encode("utf-8")
            relative, sha = self.archive.put(source_id, run["id"], raw)
            self.commit_page(run["id"], page, relative, sha)
            return self.runs.complete(run["id"])
        except Exception as error:
            self.runs.fail(run["id"], error)
            raise

    def commit_page(self, run_id, page, relative, sha):
        counts = dict.fromkeys(COUNTERS, 0)
        profile = self.profiles.for_run(run_id)
        rule_version = self.profiles.version_for_run(run_id)
        with self.db.write() as connection:
            run = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
            if not run or run["status"] != "running":
                raise Conflict("page requires an owned running run")
            if run["next_cursor"] != page.cursor:
                raise Conflict("page does not match durable progress")
            if run["snapshot_id"] is not None:
                if (run["snapshot_id"], run["boundary"]) != (page.snapshot_id, page.boundary):
                    raise ValidationError("snapshot identity changed during pagination")
            for ordinal, record in enumerate(page.records):
                try:
                    product = Product.parse(profile.normalize(record))
                    if product.revision > page.boundary:
                        raise ValidationError("record revision exceeds snapshot boundary")
                    outcome = self.merger.merge(
                        connection,
                        run["source_id"],
                        run_id,
                        product,
                        rule_version,
                        source_fingerprint=self.merger.source_fingerprint(profile.parser, record),
                        parser=profile.parser,
                        raw_record=record,
                    )
                except ValidationError as error:
                    counts["rejected"] += 1
                    connection.execute(
                        """INSERT INTO rejects
                        (run_id, cursor, ordinal, reason, raw_record, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)""",
                        (run_id, page.cursor, ordinal, str(error), canonical(record), now()),
                    )
                else:
                    counts["accepted"] += 1
                    counts[outcome] += 1
            connection.execute(
                """INSERT INTO pages
                (run_id, cursor, next_cursor, raw_sha256, archive_path,
                 record_count, accepted, rejected, committed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    run_id,
                    page.cursor,
                    page.next_cursor,
                    sha,
                    relative,
                    len(page.records),
                    counts["accepted"],
                    counts["rejected"],
                    now(),
                ),
            )
            connection.execute(
                """UPDATE runs SET snapshot_id = ?, boundary = ?, next_cursor = ?,
                page_count = page_count + 1, accepted = accepted + ?,
                rejected = rejected + ?, inserted = inserted + ?,
                updated = updated + ?, unchanged = unchanged + ?, stale = stale + ?
                WHERE id = ?""",
                (
                    page.snapshot_id,
                    page.boundary,
                    page.next_cursor,
                    *(counts[name] for name in COUNTERS),
                    run_id,
                ),
            )
            self.db.event(
                connection,
                "page_committed",
                {"cursor": page.cursor, "sha256": sha, **counts},
                run["source_id"],
                run_id,
            )
