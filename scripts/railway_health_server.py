"""Background /health responder for Railway services without gunicorn."""
from __future__ import annotations

import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_log = logging.getLogger("railway-health")


class _HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:  # noqa: D401
        return

    def do_GET(self) -> None:
        if self.path in ("/health", "/health/"):
            body = b'{"ok":true}\n'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()


def start_railway_health_server() -> None:
    if os.environ.get("RAILWAY_DISABLE_HEALTH_SERVER") == "1":
        return
    port = int(os.environ.get("PORT", "8080"))

    def _run() -> None:
        server = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
        _log.info("Railway health server listening on :%s/health", port)
        server.serve_forever()

    threading.Thread(target=_run, name="railway-health", daemon=True).start()
