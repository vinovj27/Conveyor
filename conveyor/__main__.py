import argparse
import json
import os
import random
import subprocess
import sys
import threading
import time
from pathlib import Path

from .broker import Broker
from .client import RemoteBroker
from .server import make_server
from .worker import Worker

DEFAULT_URL = os.environ.get("CONVEYOR_URL", "http://127.0.0.1:8700")


def cmd_broker(args) -> None:
    broker = Broker(args.db)
    server = make_server(broker, args.port, args.host)
    print(f"Conveyor broker on http://{args.host}:{server.server_port} · dashboard at / · data in {args.db}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()
        broker.close()


def cmd_worker(args) -> None:
    worker = Worker(RemoteBroker(args.url), args.name, args.queues.split(","), args.concurrency, args.lease)
    print(f"Worker {worker.name} · {args.concurrency} slots · queues {args.queues} · broker {args.url}", flush=True)
    worker.run_forever()


def cmd_enqueue(args) -> None:
    job_id = RemoteBroker(args.url).enqueue(args.task, json.loads(args.args), queue=args.queue, priority=args.priority,
                                            delay=args.delay, max_attempts=args.max_attempts)
    print(f"Enqueued job {job_id}")


def cmd_stats(args) -> None:
    print(json.dumps(RemoteBroker(args.url).stats(), indent=2))


def demo_jobs(broker, n: int) -> None:
    text = "the quick brown fox jumps over the lazy dog and the dog sleeps while the fox runs"
    for i in range(n):
        kind = i % 10
        if kind < 3:
            broker.enqueue("count_primes", {"limit": random.randint(50_000, 300_000)})
        elif kind < 5:
            broker.enqueue("hash_chain", {"seed": f"job-{i}", "rounds": 30_000})
        elif kind < 6:
            broker.enqueue("word_count", {"text": text, "top": 3}, priority=5)
        elif kind < 9:
            broker.enqueue("flaky", {"fail_rate": 0.4}, max_attempts=4)
        else:
            broker.enqueue("sleep", {"seconds": 1.5})
    broker.enqueue("always_fails", {"message": "malformed payload"}, max_attempts=2)


def cmd_demo(args) -> None:
    db = Path(args.db)
    for suffix in ("", "-wal", "-shm"):
        Path(str(db) + suffix).unlink(missing_ok=True)
    broker = Broker(str(db), backoff_base=0.3, backoff_cap=3)
    server = make_server(broker, args.port)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}"
    print(f"Dashboard: {url}", flush=True)

    demo_jobs(broker, args.jobs)
    procs = [subprocess.Popen([sys.executable, "-m", "conveyor", "worker", "--url", url, "--name", f"worker-{i + 1}",
                               "--concurrency", "3", "--lease", "3"], stdout=subprocess.DEVNULL)
             for i in range(args.workers)]
    print(f"Started {args.workers} worker processes on {args.jobs + 1} jobs", flush=True)

    killed = False
    start = time.time()
    try:
        while True:
            c = broker.stats()["counts"]
            print(f"\r  queued {c['queued']:>4}  running {c['running']:>3}  done {c['done']:>4}  dead {c['dead']:>2}", end="", flush=True)
            if args.chaos and not killed and c["done"] > args.jobs // 3:
                procs[0].kill()
                killed = True
                print(f"\n  Chaos: killed worker-1 mid-job; its leases expire and other workers pick the jobs up", flush=True)
            if c["queued"] == 0 and c["running"] == 0:
                break
            time.sleep(0.25)
        s = broker.stats()
        print(f"\nFinished in {time.time() - start:.1f}s · {s['counts']['done']} done · {s['retried']} needed retries · "
              f"{s['counts']['dead']} in the dead-letter queue", flush=True)
        if not args.exit:
            print(f"Dashboard still live at {url} · Ctrl+C to stop", flush=True)
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        print()
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            p.wait(10)
        server.shutdown()
        broker.close()


def main() -> None:
    parser = argparse.ArgumentParser(prog="conveyor", description="Distributed task queue")
    sub = parser.add_subparsers(dest="command", required=True)

    b = sub.add_parser("broker", help="run the broker and dashboard")
    b.add_argument("--db", default="conveyor.db")
    b.add_argument("--host", default="127.0.0.1")
    b.add_argument("--port", type=int, default=8700)
    b.set_defaults(func=cmd_broker)

    w = sub.add_parser("worker", help="run a worker process")
    w.add_argument("--url", default=DEFAULT_URL)
    w.add_argument("--name")
    w.add_argument("--queues", default="default")
    w.add_argument("--concurrency", type=int, default=2)
    w.add_argument("--lease", type=float, default=15.0)
    w.set_defaults(func=cmd_worker)

    e = sub.add_parser("enqueue", help="add a job")
    e.add_argument("task")
    e.add_argument("--args", default="{}", help='JSON object, e.g. \'{"limit": 100000}\'')
    e.add_argument("--queue", default="default")
    e.add_argument("--priority", type=int, default=0)
    e.add_argument("--delay", type=float, default=0.0)
    e.add_argument("--max-attempts", type=int, default=3)
    e.add_argument("--url", default=DEFAULT_URL)
    e.set_defaults(func=cmd_enqueue)

    st = sub.add_parser("stats", help="print queue statistics")
    st.add_argument("--url", default=DEFAULT_URL)
    st.set_defaults(func=cmd_stats)

    d = sub.add_parser("demo", help="broker + worker processes + sample jobs, with a worker crash")
    d.add_argument("--jobs", type=int, default=150)
    d.add_argument("--workers", type=int, default=3)
    d.add_argument("--port", type=int, default=8700)
    d.add_argument("--db", default="conveyor-demo.db")
    d.add_argument("--no-chaos", dest="chaos", action="store_false")
    d.add_argument("--exit", action="store_true", help="exit when all jobs finish")
    d.set_defaults(func=cmd_demo)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
