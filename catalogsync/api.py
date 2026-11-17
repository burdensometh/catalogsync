from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit
import json
import mimetypes
import traceback

from .app import Application
from .errors import CatalogError, Conflict, NotFound, ValidationError
from .models import canonical


def integer(query, key, default, minimum=0, maximum=1000):
    try:
        value = int(query.get(key, [str(default)])[0])
    except (TypeError, ValueError):
        raise ValidationError(f"{key} must be an integer") from None
    if not minimum <= value <= maximum:
        raise ValidationError(f"{key} must be between {minimum} and {maximum}")
    return value


def boolean(query, key, default=False):
    value = query.get(key, ["true" if default else "false"])[0]
    if value not in {"true", "false"}:
        raise ValidationError(f"{key} must be true or false")
    return value == "true"


def serve(home, host="127.0.0.1", port=8761):
    application = Application(home)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def respond(self, status, payload):
            raw = canonical(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(raw)

        def body(self):
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                raise ValidationError("invalid Content-Length") from None
            if not 0 < length <= 8 * 1024 * 1024:
                self.close_connection = True
                raise ValidationError("body must be between 1 byte and 8 MiB")
            if self.headers.get_content_type() != "application/json":
                self.close_connection = True
                raise ValidationError("Content-Type must be application/json")
            try:
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                raise ValidationError("invalid JSON body") from None
            if not isinstance(payload, dict):
                raise ValidationError("body must be an object")
            return payload

        def route(self, method):
            parsed = urlsplit(self.path)
            parts = [unquote(part) for part in parsed.path.strip("/").split("/") if part]
            query = parse_qs(parsed.query, keep_blank_values=True)
            app = application
            if method == "GET":
                if parts == ["health"]:
                    return 200, {"service": "catalogsync", "schema": 2}
                if parts == ["sources"]:
                    return 200, app.registry.list()
                if parts == ["rejections"]:
                    return 200, app.rejections.search(
                        query.get("source", [None])[0], query.get("reason", [None])[0],
                        integer(query, "limit", 100, 1), integer(query, "offset", 0, maximum=10**9),
                    )
                if len(parts) == 2 and parts[0] == "sources":
                    return 200, app.registry.get(parts[1])
                if len(parts) == 3 and parts[0] == "sources" and parts[2] == "profile":
                    return 200, app.profiles.get(parts[1])
                if len(parts) == 3 and parts[0] == "sources" and parts[2] == "inbox":
                    return 200, app.inbox.status(parts[1])
                if parts == ["products"]:
                    return 200, app.query.products(
                        source_id=query.get("source", [None])[0],
                        category=query.get("category", [None])[0],
                        sellable=boolean(query, "sellable"),
                        search=query.get("q", [None])[0],
                        limit=integer(query, "limit", 100, 1),
                        offset=integer(query, "offset", 0, maximum=10**9),
                    )
                if len(parts) == 3 and parts[0] == "products":
                    return 200, app.query.product(parts[1], parts[2])
                if len(parts) == 4 and parts[0] == "products" and parts[3] == "history":
                    return 200, app.query.history(parts[1], parts[2])
                if len(parts) == 4 and parts[0] == "products" and parts[3] == "provenance":
                    return 200, app.provenance.explain(parts[1], parts[2])
                if parts == ["runs"]:
                    return 200, app.runs.list(query.get("source", [None])[0])
                if parts == ["crawls"]:
                    return 200, app.crawls.list(query.get("source", [None])[0])
                if len(parts) == 2 and parts[0] == "crawls":
                    return 200, app.crawls.get(parts[1])
                if len(parts) == 3 and parts[0] == "crawls" and parts[2] == "items":
                    return 200, app.crawls.items(parts[1])
                if len(parts) == 4 and parts[0] == "crawls" and parts[2] == "items":
                    return 200, app.provenance.crawl_chain(parts[1], int(parts[3]))
                if len(parts) == 2 and parts[0] == "runs":
                    return 200, app.runs.get(parts[1])
                if len(parts) == 3 and parts[0] == "runs" and parts[2] == "pages":
                    return 200, app.runs.pages(parts[1])
                if len(parts) == 3 and parts[0] == "runs" and parts[2] == "rejects":
                    return 200, app.runs.rejects(parts[1])
                if len(parts) == 3 and parts[0] == "runs" and parts[2] == "profile":
                    return 200, app.provenance.run_profile(parts[1])
                if len(parts) == 3 and parts[0] == "runs" and parts[2] == "balance":
                    return 200, app.provenance.reconcile_counts(parts[1])
                if len(parts) == 3 and parts[0] == "runs" and parts[2] == "raw-page":
                    return 200, app.provenance.page(parts[1], query.get("cursor", [""])[0])
                if parts == ["summary"]:
                    return 200, app.query.summary()
                if parts == ["events"]:
                    return 200, app.query.events(
                        integer(query, "after", 0, maximum=10**12),
                        integer(query, "limit", 100, 1),
                    )
                if parts == ["publications"]:
                    return 200, app.publisher.list()
                if len(parts) == 2 and parts[0] == "publications":
                    return 200, app.publisher.get(parts[1])
                if len(parts) == 3 and parts[0] == "publications" and parts[2] == "artifact":
                    path = app.publisher.artifact(parts[1])
                    self.send_response(200)
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header("Content-Length", str(path.stat().st_size))
                    self.send_header("Content-Disposition", f'attachment; filename="{path.name}"')
                    self.end_headers()
                    with path.open("rb") as stream:
                        for block in iter(lambda: stream.read(65536), b""):
                            self.wfile.write(block)
                    return None
                if parts == ["diagnostics"]:
                    return 200, app.diagnostics.check(boolean(query, "archives"))
            if method == "POST":
                data = self.body()
                if parts == ["sources"]:
                    return 201, app.registry.register(
                        data.get("id"), data.get("name"), data.get("kind"), data.get("location")
                    )
                if len(parts) == 3 and parts[0] == "rejections" and parts[2] == "requeue":
                    return 201, app.rejections.requeue(int(parts[1]), data.get("record"))
                if len(parts) == 3 and parts[0] == "sources" and parts[2] == "enabled":
                    return 200, app.registry.enable(parts[1], data.get("enabled"))
                if len(parts) == 3 and parts[0] == "sources" and parts[2] == "profile":
                    return 200, app.profiles.set(parts[1], data)
                if len(parts) == 3 and parts[0] == "sources" and parts[2] == "preview":
                    return 200, app.profiles.preview(parts[1], data.get("records", []))
                if len(parts) == 3 and parts[0] == "sources" and parts[2] == "events":
                    return 201, app.inbox.submit(parts[1], data.get("events"))
                if len(parts) == 3 and parts[0] == "sources" and parts[2] == "consume":
                    return 200, app.inbox.consume(parts[1], data.get("limit", 100))
                if parts == ["runs"]:
                    return 201, app.ingest.start(data.get("source_id"))
                if len(parts) == 3 and parts[0] == "runs" and parts[2] == "resume":
                    return 200, app.ingest.execute(parts[1])
                if parts == ["crawls"]:
                    return 201, app.crawler.start(
                        data.get("source_id"), data.get("max_items", 1000)
                    )
                if len(parts) == 3 and parts[0] == "crawls" and parts[2] == "resume":
                    return 200, app.crawler.execute(parts[1])
                if parts == ["deltas"]:
                    return 201, app.ingest.apply_delta(data.get("source_id"), data.get("records"))
                if parts == ["publications"]:
                    return 201, app.publisher.publish(
                        data.get("source_id"), data.get("format", "jsonl"), data.get("sellable_only", True)
                    )
                if parts == ["publication-diff"]:
                    return 200, app.publisher.diff(data.get("first"), data.get("second"))
            raise NotFound("endpoint does not exist")

        def dispatch(self, method):
            try:
                result = self.route(method)
                if result is not None:
                    self.respond(*result)
            except NotFound as error:
                self.respond(404, {"code": error.code, "error": str(error)})
            except Conflict as error:
                self.respond(409, {"code": error.code, "error": str(error)})
            except (ValidationError, TypeError, ValueError) as error:
                self.respond(400, {"code": "invalid_input", "error": str(error)})
            except CatalogError as error:
                self.respond(422, {"code": error.code, "error": str(error)})
            except (BrokenPipeError, ConnectionResetError):
                self.close_connection = True
            except Exception:
                traceback.print_exc()
                self.respond(500, {"code": "internal_error", "error": "operation failed"})

        def do_GET(self):
            self.dispatch("GET")

        def do_POST(self):
            self.dispatch("POST")

    return ThreadingHTTPServer((host, port), Handler)
