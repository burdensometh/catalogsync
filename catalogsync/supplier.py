from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit
import json
import threading

from .clients import FileClient
from .errors import CatalogError


def supplier_server(directory, host="127.0.0.1", port=0):
    client = FileClient(directory)
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlsplit(self.path)
            if parsed.path != "/catalog":
                self.send_error(404)
                return
            query = parse_qs(parsed.query, keep_blank_values=True)
            cursor = query.get("cursor", [""])[0]
            try:
                raw = client.fetch(cursor)
            except CatalogError as error:
                raw = json.dumps({"error": str(error)}).encode()
                self.send_response(502)
            else:
                self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, format, *args):
            return
    return ThreadingHTTPServer((host, port), Handler)


class SupplierProcess:
    def __init__(self, directory):
        self.server = supplier_server(directory)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        host, port = self.server.server_address
        return f"http://{host}:{port}"

    def __exit__(self, *args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)
