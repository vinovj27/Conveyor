from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from urllib.parse import parse_qs, urlparse

from .broker import STATUSES, Broker


def make_handler(broker: Broker):
    page = resources.files("conveyor").joinpath("static/dashboard.html").read_bytes()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args) -> None:
            pass

        def _send(self, status: int, body: bytes, content_type: str = "application/json") -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _json(self, payload, status: int = 200) -> None:
            self._send(status, json.dumps(payload).encode())

        def do_GET(self) -> None:
            url = urlparse(self.path)
            if url.path in ("/", "/index.html"):
                self._send(200, page, "text/html; charset=utf-8")
            elif url.path == "/api/stats":
                self._json(broker.stats())
            elif url.path == "/api/jobs":
                status = parse_qs(url.query).get("status", [None])[0]
                if status and status not in STATUSES:
                    return self._json({"error": f"status must be one of {', '.join(STATUSES)}"}, 400)
                self._json([j.to_dict() for j in broker.jobs(status, 40)])
            else:
                self._json({"error": "not found"}, 404)

        def do_POST(self) -> None:
            try:
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}")
                path = urlparse(self.path).path
                if path == "/api/enqueue":
                    options = {k: body[k] for k in ("queue", "priority", "delay", "max_attempts") if k in body}
                    self._json({"id": broker.enqueue(body["task"], body.get("args"), **options)}, 201)
                elif path == "/api/claim":
                    job = broker.claim(body["worker"], body.get("queues"), float(body.get("lease", 30)))
                    self._json({"job": job.to_dict() if job else None})
                elif path == "/api/heartbeat":
                    self._json({"ok": broker.heartbeat(int(body["id"]), body["worker"], float(body.get("lease", 30)))})
                elif path == "/api/complete":
                    self._json({"ok": broker.complete(int(body["id"]), body["worker"], body.get("result"))})
                elif path == "/api/fail":
                    self._json({"status": broker.fail(int(body["id"]), body["worker"], str(body.get("error", "")))})
                elif path == "/api/retry":
                    self._json({"ok": broker.retry(int(body["id"]))})
                else:
                    self._json({"error": "not found"}, 404)
            except (KeyError, ValueError, TypeError, json.JSONDecodeError) as e:
                self._json({"error": f"bad request: {e}"}, 400)

    return Handler


def make_server(broker: Broker, port: int = 8700, host: str = "127.0.0.1") -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), make_handler(broker))
    server.daemon_threads = True
    return server
