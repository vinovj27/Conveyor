from __future__ import annotations

import json
import random
import sqlite3
import threading
import time
from dataclasses import asdict, dataclass
from typing import Any, Callable

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    queue         TEXT    NOT NULL,
    task          TEXT    NOT NULL,
    args          TEXT    NOT NULL,
    priority      INTEGER NOT NULL DEFAULT 0,
    status        TEXT    NOT NULL DEFAULT 'queued',
    attempts      INTEGER NOT NULL DEFAULT 0,
    max_attempts  INTEGER NOT NULL DEFAULT 3,
    run_at        REAL    NOT NULL,
    lease_until   REAL,
    worker        TEXT,
    result        TEXT,
    error         TEXT,
    created_at    REAL    NOT NULL,
    finished_at   REAL
);
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs (status, queue, priority DESC, run_at, id);
CREATE INDEX IF NOT EXISTS jobs_lease ON jobs (status, lease_until);
"""

STATUSES = ("queued", "running", "done", "dead")


@dataclass
class Job:
    id: int
    queue: str
    task: str
    args: dict
    priority: int
    status: str
    attempts: int
    max_attempts: int
    run_at: float
    lease_until: float | None
    worker: str | None
    result: Any
    error: str | None
    created_at: float
    finished_at: float | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Job":
        data = dict(row)
        data["args"] = json.loads(data["args"])
        data["result"] = json.loads(data["result"]) if data["result"] is not None else None
        return cls(**data)

    def to_dict(self) -> dict:
        return asdict(self)


class Broker:
    """Durable job queue on SQLite with leases, retries, exponential backoff and a dead-letter state."""

    def __init__(self, path: str = "conveyor.db", clock: Callable[[], float] = time.time,
                 backoff_base: float = 1.0, backoff_cap: float = 300.0) -> None:
        self.clock = clock
        self.backoff_base = backoff_base
        self.backoff_cap = backoff_cap
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def enqueue(self, task: str, args: dict | None = None, *, queue: str = "default", priority: int = 0,
                delay: float = 0.0, max_attempts: int = 3) -> int:
        if not task:
            raise ValueError("task is required")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        now = self.clock()
        with self._lock:
            cur = self._db.execute(
                "INSERT INTO jobs (queue, task, args, priority, max_attempts, run_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (queue, task, json.dumps(args or {}), priority, max_attempts, now + delay, now))
            return cur.lastrowid

    def claim(self, worker: str, queues: list[str] | None = None, lease: float = 30.0) -> Job | None:
        """Atomically lease the highest-priority ready job. Expired leases are reclaimed first."""
        queues = queues or ["default"]
        now = self.clock()
        marks = ",".join("?" * len(queues))
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                self._reap_expired(now)
                row = self._db.execute(
                    f"""UPDATE jobs SET status = 'running', attempts = attempts + 1, worker = ?, lease_until = ?
                        WHERE id = (SELECT id FROM jobs WHERE status = 'queued' AND run_at <= ? AND queue IN ({marks})
                                    ORDER BY priority DESC, run_at, id LIMIT 1)
                        RETURNING *""",
                    (worker, now + lease, now, *queues)).fetchone()
                self._db.execute("COMMIT")
            except Exception:
                self._db.execute("ROLLBACK")
                raise
        return Job.from_row(row) if row else None

    def heartbeat(self, job_id: int, worker: str, lease: float = 30.0) -> bool:
        with self._lock:
            cur = self._db.execute(
                "UPDATE jobs SET lease_until = ? WHERE id = ? AND worker = ? AND status = 'running'",
                (self.clock() + lease, job_id, worker))
            return cur.rowcount == 1

    def complete(self, job_id: int, worker: str, result: Any = None) -> bool:
        """Mark a job done. Refused if the lease was lost and the job now belongs to someone else."""
        with self._lock:
            cur = self._db.execute(
                """UPDATE jobs SET status = 'done', result = ?, error = NULL, lease_until = NULL, finished_at = ?
                   WHERE id = ? AND worker = ? AND status = 'running'""",
                (json.dumps(result), self.clock(), job_id, worker))
            return cur.rowcount == 1

    def fail(self, job_id: int, worker: str, error: str) -> str | None:
        """Record a failure. Returns the job's new status: 'queued' for a retry, 'dead' when attempts run out."""
        now = self.clock()
        with self._lock:
            row = self._db.execute(
                "SELECT attempts, max_attempts FROM jobs WHERE id = ? AND worker = ? AND status = 'running'",
                (job_id, worker)).fetchone()
            if row is None:
                return None
            if row["attempts"] >= row["max_attempts"]:
                self._db.execute(
                    "UPDATE jobs SET status = 'dead', error = ?, lease_until = NULL, finished_at = ? WHERE id = ?",
                    (error[:2000], now, job_id))
                return "dead"
            self._db.execute(
                "UPDATE jobs SET status = 'queued', error = ?, lease_until = NULL, run_at = ? WHERE id = ?",
                (error[:2000], now + self.backoff(row["attempts"]), job_id))
            return "queued"

    def backoff(self, attempts: int) -> float:
        """Exponential backoff with full jitter: random in [base, base * 2^attempts], capped."""
        ceiling = min(self.backoff_cap, self.backoff_base * 2 ** attempts)
        return random.uniform(self.backoff_base, max(self.backoff_base, ceiling))

    def retry(self, job_id: int) -> bool:
        with self._lock:
            cur = self._db.execute(
                """UPDATE jobs SET status = 'queued', attempts = 0, run_at = ?, error = NULL, finished_at = NULL, worker = NULL
                   WHERE id = ? AND status = 'dead'""", (self.clock(), job_id))
            return cur.rowcount == 1

    def get(self, job_id: int) -> Job | None:
        with self._lock:
            row = self._db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return Job.from_row(row) if row else None

    def jobs(self, status: str | None = None, limit: int = 50) -> list[Job]:
        sql, params = "SELECT * FROM jobs", []
        if status:
            sql, params = sql + " WHERE status = ?", [status]
        with self._lock:
            rows = self._db.execute(sql + " ORDER BY id DESC LIMIT ?", (*params, limit)).fetchall()
        return [Job.from_row(r) for r in rows]

    def stats(self) -> dict:
        now = self.clock()
        with self._lock:
            counts = dict(self._db.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status").fetchall())
            per_task = self._db.execute(
                """SELECT task, COUNT(*) total, SUM(status = 'done') done, SUM(status = 'dead') dead,
                          AVG(CASE WHEN status = 'done' THEN finished_at - created_at END) avg_latency
                   FROM jobs GROUP BY task ORDER BY task""").fetchall()
            workers = self._db.execute(
                "SELECT worker, COUNT(*) FROM jobs WHERE status = 'running' AND lease_until > ? GROUP BY worker", (now,)).fetchall()
            recent = self._db.execute("SELECT COUNT(*) FROM jobs WHERE status = 'done' AND finished_at > ?", (now - 10,)).fetchone()[0]
            retried = self._db.execute("SELECT COUNT(*) FROM jobs WHERE attempts > 1").fetchone()[0]
        return {
            "counts": {s: counts.get(s, 0) for s in STATUSES},
            "tasks": [dict(r) for r in per_task],
            "active_workers": {w: n for w, n in workers},
            "throughput_per_s": round(recent / 10, 2),
            "retried": retried,
        }

    def _reap_expired(self, now: float) -> None:
        self._db.execute(
            """UPDATE jobs SET status = 'dead', error = 'lease expired on final attempt', lease_until = NULL, finished_at = ?
               WHERE status = 'running' AND lease_until < ? AND attempts >= max_attempts""", (now, now))
        self._db.execute(
            """UPDATE jobs SET status = 'queued', error = 'lease expired', lease_until = NULL
               WHERE status = 'running' AND lease_until < ?""", (now,))
