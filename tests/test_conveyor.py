import tempfile
import threading
import time
import unittest
from pathlib import Path

from conveyor import Broker, RemoteBroker, Worker, task
from conveyor.server import make_server

calls = []


@task("record")
def record(value):
    calls.append(value)
    return value * 2


class Clock:
    def __init__(self):
        self.now = 1_000.0

    def __call__(self):
        return self.now


class BrokerTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self._tmp.name) / "q.db")
        self.clock = Clock()
        self.b = Broker(self.path, clock=self.clock, backoff_base=1, backoff_cap=10)

    def tearDown(self):
        self.b.close()
        self._tmp.cleanup()

    def test_priority_then_fifo(self):
        low = self.b.enqueue("t", {"n": 1})
        high = self.b.enqueue("t", {"n": 2}, priority=10)
        low2 = self.b.enqueue("t", {"n": 3})
        order = [self.b.claim("w").id for _ in range(3)]
        self.assertEqual(order, [high, low, low2])
        self.assertIsNone(self.b.claim("w"))

    def test_delayed_jobs_wait_until_due(self):
        self.b.enqueue("t", delay=60)
        self.assertIsNone(self.b.claim("w"))
        self.clock.now += 61
        self.assertIsNotNone(self.b.claim("w"))

    def test_queues_are_isolated(self):
        self.b.enqueue("t", queue="emails")
        self.assertIsNone(self.b.claim("w", ["default"]))
        self.assertEqual(self.b.claim("w", ["emails", "default"]).queue, "emails")

    def test_each_job_is_claimed_exactly_once_under_contention(self):
        ids = {self.b.enqueue("t", {"i": i}) for i in range(300)}
        claimed, lock = [], threading.Lock()

        def grab(n):
            while (job := self.b.claim(f"w{n}")) is not None:
                with lock:
                    claimed.append(job.id)

        threads = [threading.Thread(target=grab, args=(n,)) for n in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sorted(claimed), sorted(ids))

    def test_expired_lease_is_redelivered_and_stale_worker_is_fenced(self):
        job_id = self.b.enqueue("t")
        self.b.claim("crashed", lease=5)
        self.clock.now += 6
        job = self.b.claim("healthy", lease=5)
        self.assertEqual((job.id, job.attempts, job.error), (job_id, 2, "lease expired"))
        self.assertFalse(self.b.complete(job_id, "crashed", "late"))
        self.assertTrue(self.b.complete(job_id, "healthy", "ok"))
        self.assertEqual(self.b.get(job_id).result, "ok")

    def test_heartbeat_keeps_lease_alive(self):
        job_id = self.b.enqueue("t")
        self.b.claim("w", lease=5)
        for _ in range(4):
            self.clock.now += 4
            self.assertTrue(self.b.heartbeat(job_id, "w", lease=5))
        self.assertIsNone(self.b.claim("other"))

    def test_retries_with_backoff_then_dead_letter(self):
        job_id = self.b.enqueue("t", max_attempts=3)
        for attempt in (1, 2):
            job = self.b.claim("w")
            self.assertEqual(job.attempts, attempt)
            self.assertEqual(self.b.fail(job_id, "w", "boom"), "queued")
            self.assertIsNone(self.b.claim("w"), "retry must wait for backoff")
            self.clock.now += 20
        self.b.claim("w")
        self.assertEqual(self.b.fail(job_id, "w", "boom"), "dead")
        self.assertEqual(self.b.stats()["counts"]["dead"], 1)
        self.assertTrue(self.b.retry(job_id))
        self.assertEqual(self.b.claim("w").attempts, 1)

    def test_lease_expiry_on_final_attempt_goes_to_dead_letter(self):
        job_id = self.b.enqueue("t", max_attempts=1)
        self.b.claim("w", lease=1)
        self.clock.now += 2
        self.assertIsNone(self.b.claim("w"))
        self.assertEqual(self.b.get(job_id).status, "dead")

    def test_backoff_grows_and_is_capped(self):
        self.assertLessEqual(max(self.b.backoff(1) for _ in range(200)), 2)
        self.assertLessEqual(max(self.b.backoff(10) for _ in range(200)), 10)
        self.assertGreater(max(self.b.backoff(3) for _ in range(200)), 4)

    def test_jobs_survive_restart(self):
        self.b.enqueue("t", {"keep": True})
        self.b.close()
        self.b = Broker(self.path, clock=self.clock)
        self.assertEqual(self.b.claim("w").args, {"keep": True})

    def test_validation(self):
        with self.assertRaises(ValueError):
            self.b.enqueue("")
        with self.assertRaises(ValueError):
            self.b.enqueue("t", max_attempts=0)


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.b = Broker(str(Path(self._tmp.name) / "q.db"), backoff_base=0.05, backoff_cap=0.2)

    def tearDown(self):
        self.b.close()
        self._tmp.cleanup()

    def wait_idle(self, broker, timeout=20):
        deadline = time.time() + timeout
        while time.time() < deadline:
            c = broker.stats()["counts"]
            if c["queued"] == 0 and c["running"] == 0:
                return c
            time.sleep(0.05)
        self.fail("queue did not drain")

    def test_worker_runs_jobs_retries_failures_and_dead_letters_bad_ones(self):
        for i in range(30):
            self.b.enqueue("count_primes", {"limit": 1000 + i})
        for _ in range(10):
            self.b.enqueue("flaky", {"fail_rate": 0.5, "work": 0}, max_attempts=10)
        bad = self.b.enqueue("always_fails", max_attempts=2)
        missing = self.b.enqueue("no_such_task", max_attempts=1)
        worker = Worker(self.b, "w", concurrency=4, poll=0.01).start()
        counts = self.wait_idle(self.b)
        worker.stop()
        self.assertEqual(self.b.get(bad).status, "dead")
        self.assertIn("unknown task", self.b.get(missing).error)
        self.assertEqual(counts["dead"], 2 + sum(1 for j in self.b.jobs("dead", 100) if j.task == "flaky"))
        self.assertEqual(self.b.jobs("done", 100)[-1].result, 168)

    def test_workers_over_http(self):
        server = make_server(self.b, 0)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        remote = RemoteBroker(f"http://127.0.0.1:{server.server_port}")
        try:
            calls.clear()
            ids = [remote.enqueue("record", {"value": i}) for i in range(25)]
            workers = [Worker(RemoteBroker(remote.url), f"w{n}", concurrency=2, poll=0.01).start() for n in range(3)]
            self.wait_idle(remote)
            for w in workers:
                w.stop()
            self.assertEqual(sorted(calls), list(range(25)))
            self.assertEqual(self.b.get(ids[7]).result, 14)
            with self.assertRaises(RuntimeError):
                remote._call("/api/enqueue", {"args": {}})
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
