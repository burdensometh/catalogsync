from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import json
import tempfile
import threading
import unittest
from urllib.request import Request, urlopen

from catalogsync.api import serve
from catalogsync.app import Application
from catalogsync.errors import Conflict, FetchError, ValidationError
from catalogsync.urlnorm import canonicalize, origin
from catalogsync.worker import Collector


SEED = "http://supplier.test/"
CATALOG = "http://supplier.test/catalog"
ITEM_A = "http://supplier.test/item?sku=a"
ITEM_B = "http://supplier.test/item?sku=b"


def product_jsonld(sku, revision, name="Tea", price="2.50", stock=3):
    document = {
        "@type": "Product",
        "sku": sku,
        "catalogRevision": revision,
        "name": name,
        "offers": {
            "price": price,
            "priceCurrency": "CNY",
            "inventoryLevel": {"value": stock},
        },
    }
    return (
        '<html><head><script type="application/ld+json">'
        + json.dumps(document)
        + "</script></head><body></body></html>"
    ).encode()


def links_page(*hrefs):
    anchors = "".join(f'<a href="{href}">go</a>' for href in hrefs)
    return f"<html><head></head><body>{anchors}</body></html>".encode()


def site_pages():
    return {
        SEED: links_page(
            "/catalog",
            "catalog#section",
            "/catalog?utm_source=email",
            "http://other.test/elsewhere",
            "javascript:void(0)",
        ),
        CATALOG: links_page(
            "/item?sku=a&utm_source=ads",
            "/item?sku=a",
            "/item?sku=b",
            "/catalog",
            "/",
        ),
        ITEM_A: product_jsonld("a", 1),
        ITEM_B: product_jsonld("b", 1),
    }


class CrawlClientStub:
    def __init__(self, pages, failures=None):
        self.pages = pages
        self.failures = failures or set()
        self.requests = []

    def fetch(self, url):
        self.requests.append(url)
        if url in self.failures:
            raise FetchError("source temporarily unavailable")
        if url not in self.pages:
            raise FetchError(f"no such page: {url}")
        return self.pages[url]


class CrawlSite:
    def __init__(self, pages):
        self.pages = pages
        site = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                body = site.pages.get(self.path)
                if body is None:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def __exit__(self, *args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)


class UrlNormalizationTest(unittest.TestCase):
    def test_fragment_and_tracking_params_collapse(self):
        self.assertEqual(
            canonicalize("HTTP://Example.COM:80/p?utm_source=x&sku=1#frag"),
            "http://example.com/p?sku=1",
        )
        self.assertEqual(
            canonicalize("http://example.com/p?utm_medium=m&sku=1&utm_campaign=c"),
            "http://example.com/p?sku=1",
        )
        self.assertEqual(canonicalize("http://example.com"), "http://example.com/")

    def test_business_params_are_preserved_and_sorted(self):
        self.assertEqual(
            canonicalize("/p?b=2&sku=9&a=1", "http://example.com/x"),
            "http://example.com/p?a=1&b=2&sku=9",
        )
        self.assertNotEqual(
            canonicalize("http://example.com/p?sku=a"),
            canonicalize("http://example.com/p?sku=b"),
        )

    def test_unfetchable_links_are_rejected(self):
        for href in ["javascript:alert(1)", "mailto:a@b.c", "ftp://h/x", "", "http://user:pw@h/"]:
            self.assertIsNone(canonicalize(href, "http://example.com/"), href)

    def test_origin_includes_scheme_host_and_port(self):
        self.assertEqual(origin("http://example.com"), ("http", "example.com", 80))
        self.assertEqual(origin("https://example.com"), ("https", "example.com", 443))
        self.assertNotEqual(
            origin("http://example.com:8080/x"), origin("http://example.com/x")
        )


class CrawlTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.app = Application(self.root / "state")
        self.app.registry.register("web", "Web supplier", "http", "http://supplier.test")

    def crawl(self, client, **kwargs):
        return self.app.crawler.start("web", client=client, **kwargs)

    def test_seed_discovery_dedup_and_same_origin(self):
        self.app.profiles.set("web", {"parser": "jsonld"})
        client = CrawlClientStub(site_pages())
        crawl = self.crawl(client)
        self.assertEqual(crawl["status"], "completed")
        self.assertEqual(
            crawl["items"], {"discovered": 4, "completed": 4, "failed": 0, "pending": 0}
        )
        self.assertEqual(len(client.requests), 4)
        self.assertNotIn("http://other.test/elsewhere", client.requests)
        self.assertEqual(self.app.query.products("web")["total"], 2)
        run = self.app.runs.get(crawl["run_id"])
        self.assertEqual(
            (run["status"], run["page_count"], run["inserted"]), ("completed", 4, 2)
        )
        publication = self.app.publisher.publish("web")
        self.assertEqual(publication["row_count"], 2)
        self.assertTrue(self.app.diagnostics.check(True)["ok"])

    def test_discovery_chain_traces_archive_to_seed(self):
        self.app.profiles.set("web", {"parser": "jsonld"})
        crawl = self.crawl(CrawlClientStub(site_pages()))
        items = {item["url"]: item for item in self.app.crawls.items(crawl["id"])}
        detail, listing, seed = items[ITEM_A], items[CATALOG], items[SEED]
        self.assertEqual(detail["parent_id"], listing["id"])
        self.assertEqual(listing["parent_id"], seed["id"])
        self.assertIsNone(seed["parent_id"])
        self.assertTrue(detail["archive_path"])
        chain = self.app.provenance.crawl_chain(crawl["id"], detail["id"])["chain"]
        self.assertEqual(
            [node["url"] for node in chain], [SEED, CATALOG, ITEM_A]
        )
        trace = self.app.provenance.page(crawl["run_id"], ITEM_A)
        self.assertEqual(trace["records"][0]["raw"]["sku"], "a")
        self.assertEqual(
            [node["url"] for node in trace["crawl"]["chain"]], [SEED, CATALOG, ITEM_A]
        )

    def test_failed_fetch_is_not_completed_and_resume_continues(self):
        self.app.profiles.set("web", {"parser": "jsonld"})
        client = CrawlClientStub(site_pages(), failures={CATALOG})
        with self.assertRaises(FetchError):
            self.crawl(client)
        crawl = self.app.crawls.list("web")[0]
        self.assertEqual(crawl["status"], "failed")
        items = {item["url"]: item for item in self.app.crawls.items(crawl["id"])}
        self.assertEqual(items[SEED]["state"], "completed")
        self.assertEqual(items[CATALOG]["state"], "failed")
        self.assertEqual(self.app.query.products()["total"], 0)
        restarted = Application(self.app.home)
        client.failures.clear()
        delivered = len(client.requests)
        result = restarted.crawler.execute(crawl["id"], client=client)
        self.assertEqual(result["status"], "completed")
        self.assertNotIn(SEED, client.requests[delivered:])
        self.assertEqual(client.requests.count(CATALOG), 2)
        self.assertEqual(self.app.query.products()["total"], 2)
        self.assertTrue(self.app.diagnostics.check(True)["ok"])

    def test_parse_failure_marks_item_failed_until_fixed(self):
        self.app.profiles.set("web", {"parser": "jsonld"})
        pages = {
            SEED: links_page("/detail"),
            "http://supplier.test/detail": b'<script type="application/ld+json">{broken</script>',
        }
        client = CrawlClientStub(pages)
        with self.assertRaises(ValidationError):
            self.crawl(client)
        crawl = self.app.crawls.list("web")[0]
        item = self.app.crawls.items(crawl["id"])[1]
        self.assertEqual((item["state"], item["url"]), ("failed", "http://supplier.test/detail"))
        self.assertEqual(self.app.runs.get(crawl["run_id"])["status"], "failed")
        client.pages["http://supplier.test/detail"] = product_jsonld("fixed", 2)
        result = self.app.crawler.execute(crawl["id"], client=client)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(self.app.query.product("web", "fixed")["revision"], 2)

    def test_completed_crawl_replay_is_a_noop(self):
        self.app.profiles.set("web", {"parser": "jsonld"})
        client = CrawlClientStub(site_pages())
        crawl = self.crawl(client)
        requests = list(client.requests)
        again = self.app.crawler.execute(crawl["id"], client=client)
        self.assertEqual(again["status"], "completed")
        self.assertEqual(client.requests, requests)
        with self.app.database.read() as connection:
            versions = connection.execute("SELECT count(*) FROM product_versions").fetchone()[0]
            pages = connection.execute("SELECT count(*) FROM pages").fetchone()[0]
        self.assertEqual((versions, pages), (2, 4))

    def test_new_round_revisits_pages_and_merges_updates(self):
        self.app.profiles.set("web", {"parser": "jsonld"})
        pages = site_pages()
        client = CrawlClientStub(pages)
        first = self.crawl(client)
        pages[ITEM_A] = product_jsonld("a", 2, price="3.00")
        second = self.crawl(client)
        self.assertNotEqual(first["id"], second["id"])
        self.assertEqual(second["status"], "completed")
        product = self.app.query.product("web", "a")
        self.assertEqual((product["revision"], product["price_cents"]), (2, 300))
        run = self.app.runs.get(second["run_id"])
        self.assertEqual((run["updated"], run["unchanged"]), (1, 1))
        self.assertEqual(len(self.app.query.history("web", "a")), 2)
        self.assertTrue(self.app.diagnostics.check(True)["ok"])

    def test_item_limit_rolls_back_the_overflowing_page(self):
        self.app.profiles.set("web", {"parser": "jsonld"})
        client = CrawlClientStub(site_pages())
        with self.assertRaises(ValidationError):
            self.crawl(client, max_items=2)
        crawl = self.app.crawls.list("web")[0]
        self.assertEqual(crawl["status"], "failed")
        self.assertIn("item limit", crawl["error"])
        items = self.app.crawls.items(crawl["id"])
        self.assertEqual(len(items), 2)
        self.assertEqual(self.app.query.products()["total"], 0)

    def test_row_level_rejects_are_isolated_per_page(self):
        self.app.profiles.set("web", {"parser": "jsonld"})
        document = {
            "@type": "ItemList",
            "itemListElement": [
                {"item": json.loads(product_jsonld("good", 1).split(b"<script type=\"application/ld+json\">")[1].split(b"</script>")[0])},
                {"item": {
                    "@type": "Product", "sku": "bad", "catalogRevision": 1,
                    "name": "Bad", "offers": {"price": "NaN"},
                }},
            ],
        }
        pages = {
            SEED: (
                '<script type="application/ld+json">'
                + json.dumps(document)
                + "</script>"
            ).encode()
        }
        crawl = self.crawl(CrawlClientStub(pages))
        self.assertEqual(crawl["status"], "completed")
        run = self.app.runs.get(crawl["run_id"])
        self.assertEqual((run["accepted"], run["rejected"]), (1, 1))
        rejection = self.app.runs.rejects(crawl["run_id"])[0]
        self.assertEqual(rejection["cursor"], SEED)
        self.assertEqual(self.app.query.products("web")["total"], 1)

    def test_json_and_html_parsers_drive_crawls(self):
        record = {"sku": "j", "revision": 1, "title": "Jar", "price": "1.00", "stock": 1}
        pages = {
            SEED: json.dumps({"records": [], "links": ["/detail"]}).encode(),
            "http://supplier.test/detail": json.dumps({"records": [record]}).encode(),
        }
        crawl = self.crawl(CrawlClientStub(pages))
        self.assertEqual(crawl["status"], "completed")
        self.assertEqual(self.app.query.product("web", "j")["title"], "Jar")

        self.app.profiles.set("web", {"parser": "html"})
        listing = (
            '<article data-sku="h1" data-revision="2" data-stock="4">'
            '<span data-field="title">Herb</span><span data-field="price">2.50</span>'
            '</article><a href="/more">more</a>'
        )
        detail = (
            '<article data-sku="h2" data-revision="3">'
            '<span data-field="title">Mint</span><span data-field="price">4.00</span>'
            "</article>"
        )
        html_pages = {
            SEED: links_page("/list"),
            "http://supplier.test/list": listing.encode(),
            "http://supplier.test/more": detail.encode(),
        }
        second = self.crawl(CrawlClientStub(html_pages))
        self.assertEqual(second["status"], "completed")
        self.assertEqual(self.app.query.product("web", "h2")["price_cents"], 400)

    def test_failed_crawl_keeps_frozen_profile_on_resume(self):
        self.app.profiles.set("web", {"parser": "jsonld"})
        pages = site_pages()
        client = CrawlClientStub(pages, failures={ITEM_A})
        with self.assertRaises(FetchError):
            self.crawl(client)
        crawl = self.app.crawls.list("web")[0]
        self.app.profiles.set("web", {"parser": "html"})
        client.failures.clear()
        result = self.app.crawler.execute(crawl["id"], client=client)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(self.app.query.products("web")["total"], 2)
        self.assertEqual(
            self.app.provenance.run_profile(crawl["run_id"])["document"]["parser"], "jsonld"
        )

    def test_crawl_requires_http_source_and_conflicts_with_active_round(self):
        directory = self.root / "files"
        directory.mkdir()
        self.app.registry.register("files", "Files", "file", str(directory))
        with self.assertRaises(ValidationError):
            self.app.crawler.start("files")
        pending = self.app.crawls.create("web", "http://supplier.test")
        self.assertEqual(pending["status"], "pending")
        with self.assertRaises(Conflict):
            self.app.crawler.start("web")
        with self.assertRaises(Conflict):
            self.app.ingest.execute(pending["run_id"])

    def test_real_http_crawl_end_to_end(self):
        pages = {
            "/": links_page("/catalog"),
            "/catalog": links_page("/item?sku=a", "/item?sku=b"),
            "/item?sku=a": product_jsonld("a", 1),
            "/item?sku=b": product_jsonld("b", 1),
        }
        with CrawlSite(pages) as url:
            self.app.registry.register("live", "Live", "http", url)
            self.app.profiles.set("live", {"parser": "jsonld"})
            crawl = self.app.crawler.start("live")
            self.assertEqual(crawl["status"], "completed")
            self.assertEqual(crawl["items"]["completed"], 4)
            self.assertEqual(self.app.query.products("live")["total"], 2)
            self.assertTrue(self.app.diagnostics.check(True)["ok"])

    def test_collector_resumes_interrupted_crawl(self):
        pages = {
            "/": links_page("/catalog"),
            "/catalog": links_page("/item?sku=a"),
            "/item?sku=a": product_jsonld("a", 1),
        }
        with CrawlSite(pages) as url:
            self.app.registry.register("live", "Live", "http", url)
            self.app.profiles.set("live", {"parser": "jsonld"})
            seed = url + "/"
            stub = CrawlClientStub({}, failures={seed})
            with self.assertRaises(FetchError):
                self.app.crawler.start("live", client=stub)
            result = Collector(self.app, 1, False).cycle()
            self.assertEqual(result["completed"], 1)
            self.assertEqual(result["results"][0]["run"]["status"], "completed")
            self.assertEqual(self.app.query.products("live")["total"], 1)

    def test_http_api_crawl_round_and_chain(self):
        pages = {
            "/": links_page("/item?sku=a"),
            "/item?sku=a": product_jsonld("a", 1),
        }
        with CrawlSite(pages) as url:
            server = serve(self.app.home, port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base = f"http://127.0.0.1:{server.server_address[1]}"
                body = json.dumps({"id": "api", "name": "API", "kind": "http", "location": url}).encode()
                request = Request(base + "/sources", body, {"Content-Type": "application/json"})
                with urlopen(request, timeout=3) as response:
                    self.assertEqual(response.status, 201)
                body = json.dumps({"parser": "jsonld"}).encode()
                request = Request(base + "/sources/api/profile", body, {"Content-Type": "application/json"})
                with urlopen(request, timeout=3) as response:
                    self.assertEqual(response.status, 200)
                body = json.dumps({"source_id": "api"}).encode()
                request = Request(base + "/crawls", body, {"Content-Type": "application/json"})
                with urlopen(request, timeout=3) as response:
                    self.assertEqual(response.status, 201)
                    crawl = json.load(response)
                self.assertEqual(crawl["status"], "completed")
                with urlopen(base + "/crawls/" + crawl["id"] + "/items", timeout=3) as response:
                    items = json.load(response)
                self.assertEqual(len(items), 2)
                detail = next(item for item in items if item["parent_id"] is not None)
                with urlopen(
                    base + f"/crawls/{crawl['id']}/items/{detail['id']}", timeout=3
                ) as response:
                    chain = json.load(response)["chain"]
                self.assertEqual(len(chain), 2)
                self.assertEqual(chain[0]["url"], url + "/")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)


if __name__ == "__main__":
    unittest.main()
