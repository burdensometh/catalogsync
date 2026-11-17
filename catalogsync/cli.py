from pathlib import Path
import argparse
import json
import sys

from .app import Application
from .errors import CatalogError


def parser():
    root = argparse.ArgumentParser(prog="catalogsync", description="Supplier catalogue ingestion")
    root.add_argument("--home", default="var", help="local state directory")
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("init")
    add = commands.add_parser("source-add")
    add.add_argument("id")
    add.add_argument("location")
    add.add_argument("--name")
    add.add_argument("--kind", choices=["file", "http"], default="file")
    commands.add_parser("sources")
    feed_add = commands.add_parser("feed-add")
    feed_add.add_argument("id")
    feed_add.add_argument("source")
    feed_add.add_argument("file")
    feed_poll = commands.add_parser("feed-poll")
    feed_poll.add_argument("id")
    feed_poll.add_argument("--batch-size", type=int, default=100)
    commands.add_parser("feeds")
    requeue = commands.add_parser("requeue")
    requeue.add_argument("reject", type=int)
    requeue.add_argument("--corrected")
    rejects_all = commands.add_parser("rejections")
    rejects_all.add_argument("--source")
    rejects_all.add_argument("--reason")
    profile_set = commands.add_parser("profile-set")
    profile_set.add_argument("source")
    profile_set.add_argument("file")
    profile_get = commands.add_parser("profile")
    profile_get.add_argument("source")
    preview = commands.add_parser("preview")
    preview.add_argument("source")
    preview.add_argument("file")
    inbox_submit = commands.add_parser("events-submit")
    inbox_submit.add_argument("source")
    inbox_submit.add_argument("file")
    consume = commands.add_parser("consume")
    consume.add_argument("source")
    consume.add_argument("--limit", type=int, default=100)
    inbox = commands.add_parser("inbox")
    inbox.add_argument("source")
    explain = commands.add_parser("explain")
    explain.add_argument("source")
    explain.add_argument("sku")
    raw_page = commands.add_parser("raw-page")
    raw_page.add_argument("run")
    raw_page.add_argument("--cursor", default="")
    materialize = commands.add_parser("materialize")
    materialize.add_argument("publication")
    materialize.add_argument("database")
    collect = commands.add_parser("collect")
    collect.add_argument("--workers", type=int, default=2)
    collect.add_argument("--publish", action="store_true")
    collect.add_argument("--watch", action="store_true")
    collect.add_argument("--interval", type=float, default=60)
    enable = commands.add_parser("source-enable")
    enable.add_argument("id")
    enable.add_argument("enabled", choices=["true", "false"])
    pull = commands.add_parser("pull")
    pull.add_argument("source")
    resume = commands.add_parser("resume")
    resume.add_argument("run")
    delta = commands.add_parser("delta")
    delta.add_argument("source")
    delta.add_argument("file")
    runs = commands.add_parser("runs")
    runs.add_argument("--source")
    run = commands.add_parser("run")
    run.add_argument("id")
    for command in ["pages", "rejects"]:
        detail = commands.add_parser(command)
        detail.add_argument("run")
    products = commands.add_parser("products")
    products.add_argument("--source")
    products.add_argument("--category")
    products.add_argument("--search")
    products.add_argument("--sellable", action="store_true")
    products.add_argument("--limit", type=int, default=100)
    products.add_argument("--offset", type=int, default=0)
    history = commands.add_parser("history")
    history.add_argument("source")
    history.add_argument("sku")
    commands.add_parser("summary")
    events = commands.add_parser("events")
    events.add_argument("--after", type=int, default=0)
    publish = commands.add_parser("publish")
    publish.add_argument("--source")
    publish.add_argument("--format", choices=["jsonl", "csv"], default="jsonl")
    publish.add_argument("--include-unavailable", action="store_true")
    commands.add_parser("publications")
    artifact = commands.add_parser("artifact")
    artifact.add_argument("id")
    diff = commands.add_parser("diff")
    diff.add_argument("first")
    diff.add_argument("second")
    check = commands.add_parser("check")
    check.add_argument("--archives", action="store_true")
    migrate = commands.add_parser("migrate")
    migrate.add_argument("--consumer", help="consumer database path (default: sibling consumer.sqlite)")
    migrate.add_argument("--corrections", help="JSON file with exact price corrections for blocked records")
    server = commands.add_parser("serve")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8761)
    supplier = commands.add_parser("supplier")
    supplier.add_argument("directory")
    supplier.add_argument("--port", type=int, default=8760)
    demo = commands.add_parser("demo")
    demo.add_argument("--http", action="store_true")
    return root


def execute(args):
    app = Application(args.home)
    command = args.command
    if command == "init":
        return {"home": str(app.home), "schema": app.database.schema_version()}
    if command == "source-add":
        return app.registry.register(args.id, args.name or args.id, args.kind, args.location)
    if command == "sources":
        return app.registry.list()
    if command == "feed-add":
        return app.file_feeds.register(args.id, args.source, args.file)
    if command == "feed-poll":
        return app.file_feeds.poll(args.id, args.batch_size)
    if command == "feeds":
        return app.file_feeds.list()
    if command == "requeue":
        corrected = json.loads(Path(args.corrected).read_text(encoding="utf-8")) if args.corrected else None
        return app.rejections.requeue(args.reject, corrected)
    if command == "rejections":
        return app.rejections.search(args.source, args.reason)
    if command == "profile-set":
        return app.profiles.set(args.source, json.loads(Path(args.file).read_text(encoding="utf-8")))
    if command == "profile":
        return app.profiles.get(args.source)
    if command == "preview":
        return app.profiles.preview(args.source, json.loads(Path(args.file).read_text(encoding="utf-8")))
    if command == "events-submit":
        return app.inbox.submit(args.source, json.loads(Path(args.file).read_text(encoding="utf-8")))
    if command == "consume":
        return app.inbox.consume(args.source, args.limit)
    if command == "inbox":
        return app.inbox.status(args.source)
    if command == "explain":
        return app.provenance.explain(args.source, args.sku)
    if command == "raw-page":
        return app.provenance.page(args.run, args.cursor)
    if command == "materialize":
        from .consumer import DatasetConsumer
        return DatasetConsumer(args.database).apply(app.publisher, args.publication)
    if command == "collect":
        from .worker import Collector
        collector = Collector(app, args.workers, args.publish)
        if args.watch:
            return collector.watch(args.interval)
        return collector.cycle()
    if command == "source-enable":
        return app.registry.enable(args.id, args.enabled == "true")
    if command == "pull":
        return app.ingest.start(args.source)
    if command == "resume":
        return app.ingest.execute(args.run)
    if command == "delta":
        records = json.loads(Path(args.file).read_text(encoding="utf-8"))
        return app.ingest.apply_delta(args.source, records)
    if command == "runs":
        return app.runs.list(args.source)
    if command == "run":
        return app.runs.get(args.id)
    if command == "pages":
        return app.runs.pages(args.run)
    if command == "rejects":
        return app.runs.rejects(args.run)
    if command == "products":
        return app.query.products(
            args.source, args.category, args.sellable, args.search, args.limit, args.offset
        )
    if command == "history":
        return app.query.history(args.source, args.sku)
    if command == "summary":
        return app.query.summary()
    if command == "events":
        return app.query.events(args.after)
    if command == "publish":
        return app.publisher.publish(args.source, args.format, not args.include_unavailable)
    if command == "publications":
        return app.publisher.list()
    if command == "artifact":
        return {"path": str(app.publisher.artifact(args.id))}
    if command == "diff":
        return app.publisher.diff(args.first, args.second)
    if command == "check":
        return app.diagnostics.check(args.archives)
    if command == "migrate":
        corrections = json.loads(Path(args.corrections).read_text(encoding="utf-8")) if args.corrections else None
        return app.migration.migrate(args.consumer, corrections)
    if command == "serve":
        from .api import serve
        server = serve(args.home, args.host, args.port)
        print(json.dumps({"listening": server.server_address}), flush=True)
        try:
            server.serve_forever()
        finally:
            server.server_close()
        return {"stopped": True}
    if command == "supplier":
        from .supplier import supplier_server
        server = supplier_server(args.directory, port=args.port)
        try:
            server.serve_forever()
        finally:
            server.server_close()
        return {"stopped": True}
    if command == "demo":
        from .demo import run_demo
        return run_demo(app, args.http)
    raise ValueError(f"unknown command {command}")


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        result = execute(args)
    except (CatalogError, OSError, ValueError) as error:
        print(json.dumps({"error": str(error)}, ensure_ascii=False), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if args.command == "check" and not result["ok"]:
        return 2
    if args.command == "migrate" and not result["ok"]:
        return 1
    return 0
