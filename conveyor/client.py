from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from .broker import Job


class RemoteBroker:
    """Same interface as Broker, over the broker's HTTP API, so workers can run on other processes or machines."""

    def __init__(self, url: str = "http://127.0.0.1:8700", timeout: float = 10.0) -> None:
        self.url = url.rstrip("/")
        self.timeout = timeout

    def _call(self, path: str, payload: dict | None = None) -> Any:
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(self.url + path, data=data, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            raise RuntimeError(json.loads(e.read()).get("error", str(e))) from None

    def enqueue(self, task: str, args: dict | None = None, **options) -> int:
        return self._call("/api/enqueue", {"task": task, "args": args or {}, **options})["id"]

    def claim(self, worker: str, queues: list[str] | None = None, lease: float = 30.0) -> Job | None:
        job = self._call("/api/claim", {"worker": worker, "queues": queues, "lease": lease})["job"]
        return Job(**job) if job else None

    def heartbeat(self, job_id: int, worker: str, lease: float = 30.0) -> bool:
        return self._call("/api/heartbeat", {"id": job_id, "worker": worker, "lease": lease})["ok"]

    def complete(self, job_id: int, worker: str, result: Any = None) -> bool:
        return self._call("/api/complete", {"id": job_id, "worker": worker, "result": result})["ok"]

    def fail(self, job_id: int, worker: str, error: str) -> str | None:
        return self._call("/api/fail", {"id": job_id, "worker": worker, "error": error})["status"]

    def stats(self) -> dict:
        return self._call("/api/stats")
