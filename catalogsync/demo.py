from pathlib import Path

from .supplier import SupplierProcess


def run_demo(app, http=False):
    directory = Path(__file__).resolve().parent.parent / "examples" / "supplier"
    if http:
        with SupplierProcess(directory) as url:
            source_id = "demo-http-" + url.rsplit(":", 1)[1]
            app.registry.register(source_id, "Local HTTP supplier", "http", url)
            run = app.ingest.start(source_id)
    else:
        app.registry.register("demo", "Example supplier", "file", str(directory))
        run = app.ingest.start("demo")
    publication = app.publisher.publish(run["source_id"])
    return {
        "run": run,
        "publication": publication,
        "artifact": str(app.publisher.artifact(publication["id"])),
        "integrity": app.diagnostics.check(True),
    }
