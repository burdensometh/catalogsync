from .archive import Archive
from .clients import CrawlClient
from .crawls import Crawls
from .errors import CatalogError, Conflict, ValidationError
from .extraction import extract_crawl_page
from .merge import ProductMerger
from .models import Page, Product, canonical, now
from .profiles import Profiles
from .registry import Registry
from .runs import COUNTERS, Runs
from .urlnorm import canonicalize, origin


class Crawler:
    """Discovers links from a seed page and crawls same-origin pages.

    Each round persists its frontier, so interrupted rounds resume from
    unfinished items. Records are merged through the same profile
    normalization, version merge, archive and rejection isolation used by
    catalog ingestion; every page commit and the discovery of its outbound
    links happen in one transaction.
    """

    def __init__(self, database, archive_root):
        self.db = database
        self.archive = Archive(archive_root)
        self.registry = Registry(database)
        self.runs = Runs(database)
        self.crawls = Crawls(database)
        self.profiles = Profiles(database)
        self.merger = ProductMerger()

    def start(self, source_id, max_items=1000, client=None):
        source = self.registry.get(source_id)
        if source["kind"] != "http":
            raise ValidationError("crawl requires an http source")
        crawl = self.crawls.create(source_id, source["location"], max_items)
        return self.execute(crawl["id"], client)

    def execute(self, crawl_id, client=None):
        crawl = self.crawls.get(crawl_id)
        if crawl["status"] == "completed":
            return crawl
        crawl = self.crawls.claim(crawl_id)
        try:
            client = client or CrawlClient(crawl["seed_url"])
            profile = self.profiles.for_run(crawl["run_id"])
            allowed = origin(crawl["seed_url"])
            while True:
                item = self.crawls.next_unfinished(crawl_id)
                if item is None:
                    break
                self.process_item(crawl, item, client, profile, allowed)
            return self.crawls.complete(crawl_id)
        except Exception as error:
            self.crawls.fail(crawl_id, error)
            if isinstance(error, CatalogError):
                raise
            raise CatalogError(f"crawl failed: {error}") from error

    def process_item(self, crawl, item, client, profile, allowed):
        try:
            raw = client.fetch(item["url"])
            extracted = extract_crawl_page(raw, profile.parser)
            links = []
            dropped = 0
            seen = set()
            for href in extracted["links"]:
                url = canonicalize(href, item["url"])
                if url is None or origin(url) != allowed or url in seen:
                    dropped += 1
                    continue
                seen.add(url)
                links.append(url)
            records = extracted["records"]
            boundary = 1
            for record in records:
                try:
                    revision = profile.normalize(record).get("revision", 0)
                except ValidationError:
                    continue
                if type(revision) is int:
                    boundary = max(boundary, revision)
            page = Page(crawl["id"], boundary, item["url"], None, records)
            relative, sha = self.archive.put(crawl["source_id"], crawl["run_id"], raw)
            self.commit_item(crawl, item, page, links, dropped, relative, sha)
        except Exception as error:
            self.crawls.fail_item(crawl["id"], item["id"], error)
            raise

    def commit_item(self, crawl, item, page, links, dropped, relative, sha):
        counts = dict.fromkeys(COUNTERS, 0)
        profile = self.profiles.for_run(crawl["run_id"])
        with self.db.write() as connection:
            owner = connection.execute(
                "SELECT status FROM crawls WHERE id = ?", (crawl["id"],)
            ).fetchone()
            if not owner or owner["status"] != "running":
                raise Conflict("page requires a running crawl")
            current = connection.execute(
                "SELECT state FROM crawl_items WHERE id = ? AND crawl_id = ?",
                (item["id"], crawl["id"]),
            ).fetchone()
            if current is None:
                raise Conflict("crawl item does not exist")
            if current["state"] == "completed":
                return
            for ordinal, record in enumerate(page.records):
                try:
                    product = Product.parse(profile.normalize(record))
                    if product.revision > page.boundary:
                        raise ValidationError("record revision exceeds snapshot boundary")
                    outcome = self.merger.merge(
                        connection, crawl["source_id"], crawl["run_id"], product
                    )
                except ValidationError as error:
                    counts["rejected"] += 1
                    connection.execute(
                        """INSERT INTO rejects
                        (run_id, cursor, ordinal, reason, raw_record, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)""",
                        (
                            crawl["run_id"],
                            page.cursor,
                            ordinal,
                            str(error),
                            canonical(record),
                            now(),
                        ),
                    )
                else:
                    counts["accepted"] += 1
                    counts[outcome] += 1
            connection.execute(
                """INSERT INTO pages
                (run_id, cursor, next_cursor, raw_sha256, archive_path,
                 record_count, accepted, rejected, committed_at)
                VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?)""",
                (
                    crawl["run_id"],
                    page.cursor,
                    sha,
                    relative,
                    len(page.records),
                    counts["accepted"],
                    counts["rejected"],
                    now(),
                ),
            )
            connection.execute(
                """UPDATE runs SET snapshot_id = ?,
                page_count = page_count + 1, accepted = accepted + ?,
                rejected = rejected + ?, inserted = inserted + ?,
                updated = updated + ?, unchanged = unchanged + ?, stale = stale + ?
                WHERE id = ?""",
                (
                    page.snapshot_id,
                    *(counts[name] for name in COUNTERS),
                    crawl["run_id"],
                ),
            )
            enqueued = 0
            for url in links:
                result = connection.execute(
                    """INSERT OR IGNORE INTO crawl_items
                    (crawl_id, url, state, depth, parent_id, created_at)
                    VALUES (?, ?, 'pending', ?, ?, ?)""",
                    (crawl["id"], url, item["depth"] + 1, item["id"], now()),
                )
                enqueued += result.rowcount
            total = connection.execute(
                "SELECT count(*) FROM crawl_items WHERE crawl_id = ?", (crawl["id"],)
            ).fetchone()[0]
            if total > crawl["max_items"]:
                raise ValidationError("crawl exceeds item limit")
            connection.execute(
                """UPDATE crawl_items SET state = 'completed',
                attempts = attempts + 1, records = ?, accepted = ?, rejected = ?,
                error = NULL, finished_at = ? WHERE id = ?""",
                (
                    len(page.records),
                    counts["accepted"],
                    counts["rejected"],
                    now(),
                    item["id"],
                ),
            )
            self.db.event(
                connection,
                "crawl_page_committed",
                {
                    "url": page.cursor,
                    "sha256": sha,
                    "enqueued": enqueued,
                    "dropped": dropped,
                    **counts,
                },
                crawl["source_id"],
                crawl["run_id"],
            )
