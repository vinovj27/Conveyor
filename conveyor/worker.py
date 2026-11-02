from __future__ import annotations

import os
import socket
import threading
import time
import traceback

from .tasks import REGISTRY


class Worker:
    """Pulls jobs, runs them, renews the lease while running, and reports the outcome."""

    def __init__(self, broker, name: str | None = None, queues: list[str] | None = None,
                 concurrency: int = 2, lease: float = 15.0, poll: float = 0.2) -> None:
        self.broker = broker
        self.name = name or f"{socket.gethostname()}-{os.getpid()}"
        self.queues = queues or ["default"]
        self.concurrency = concurrency
        self.lease = lease
        self.poll = poll
        self.processed = 0
        self.failed = 0
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._count_lock = threading.Lock()

    def start(self) -> "Worker":
        for i in range(self.concurrency):
            t = threading.Thread(target=self._loop, args=(f"{self.name}/{i}",), daemon=True)
            t.start()
            self._threads.append(t)
        return self

    def stop(self, timeout: float = 10.0) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout)

    def run_forever(self) -> None:
        self.start()
        try:
            while not self._stop.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            print(f"\n{self.name}: finishing current jobs…")
            self.stop()

    def _loop(self, slot: str) -> None:
        while not self._stop.is_set():
            try:
                job = self.broker.claim(slot, self.queues, self.lease)
            except Exception:
                time.sleep(1.0)
                continue
            if job is None:
                self._stop.wait(self.poll)
                continue
            self._execute(job, slot)

    def _execute(self, job, slot: str) -> None:
        done = threading.Event()

        def renew() -> None:
            while not done.wait(self.lease / 3):
                try:
                    if not self.broker.heartbeat(job.id, slot, self.lease):
                        return
                except Exception:
                    pass

        threading.Thread(target=renew, daemon=True).start()
        try:
            fn = REGISTRY.get(job.task)
            if fn is None:
                raise LookupError(f"unknown task '{job.task}'")
            result = fn(**job.args)
            if self.broker.complete(job.id, slot, result):
                with self._count_lock:
                    self.processed += 1
        except Exception as exc:
            detail = f"{type(exc).__name__}: {exc}"
            self.broker.fail(job.id, slot, detail + "\n" + traceback.format_exc(limit=3))
            with self._count_lock:
                self.failed += 1
        finally:
            done.set()
