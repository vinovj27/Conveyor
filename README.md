# Conveyor

A distributed task queue built from scratch in Python, in the spirit of Celery and Sidekiq. A broker persists jobs in SQLite and hands them to worker processes over HTTP with leases, retries, exponential backoff, priorities, delayed jobs and a dead-letter queue. A live dashboard shows it all. Standard library only.

![Python](https://img.shields.io/badge/Python-3.10+-blue) ![Dependencies](https://img.shields.io/badge/dependencies-0-brightgreen) ![Tests](https://img.shields.io/badge/tests-13%20passing-brightgreen)

## See it work

```bash
python -m conveyor demo
```

This starts a broker, launches 3 worker processes, enqueues 150 jobs (CPU work, flaky network-style calls, slow jobs and one that always fails), then **kills a worker mid-job** to show recovery:

```
Dashboard: http://127.0.0.1:8700
Started 3 worker processes on 151 jobs
  Chaos: killed worker-1 mid-job; its leases expire and other workers pick the jobs up
Finished in 5.8s · 118 done · 20 needed retries · 3 in the dead-letter queue
```

Open the dashboard to watch the queue drain and retry dead-lettered jobs with one click.

## Guarantees

| Problem | How Conveyor handles it |
|---|---|
| Two workers grabbing the same job | Claims are a single atomic `UPDATE … RETURNING` inside `BEGIN IMMEDIATE`. A test with 8 threads racing over 300 jobs checks each is claimed exactly once. |
| A worker crashes mid-job | Every claim is a **lease**. Running workers renew it with heartbeats; if a worker dies, the lease expires and the job goes back to the queue. |
| A crashed worker comes back and reports late | Completions are **fenced** by worker ID, so a stale worker can't overwrite the result of the worker that took over. |
| A dependency is down | Failed jobs retry with **exponential backoff and full jitter**, capped, so retries don't stampede. |
| A job can never succeed | After `max_attempts` it moves to the **dead-letter queue** with its last error, where you can inspect and retry it. |
| The broker restarts | Jobs live in SQLite (WAL mode), so nothing in the queue is lost. |

Delivery is **at-least-once**: a job whose worker dies may run again, so tasks should be idempotent, as with any production queue.

## Run it yourself

Python 3.10 or newer, no installs needed.

```bash
python -m conveyor broker                                 # terminal 1: broker + dashboard on :8700
python -m conveyor worker --concurrency 4                 # terminal 2 (start as many as you like)
python -m conveyor enqueue count_primes --args '{"limit": 2000000}' --priority 5
python -m conveyor enqueue flaky --args '{"fail_rate": 0.7}' --max-attempts 5
python -m conveyor enqueue sleep --args '{"seconds": 3}' --delay 10
python -m conveyor stats
python -m unittest discover -s tests                      # run the tests
```

Or install the `conveyor` command with `pip install .`.

### Writing tasks

```python
from conveyor import task

@task()
def resize_image(path: str, width: int) -> str:
    ...
    return new_path
```

Import your module in the worker process and enqueue jobs by task name.

## Architecture

```
 producers ──HTTP──▶ ┌────────────────────────┐ ◀──HTTP── worker process 1 (N threads)
                     │ Broker  (SQLite, WAL)  │ ◀──HTTP── worker process 2
 dashboard ◀──────── │ leases · retries · DLQ │ ◀──HTTP── worker process 3 …
                     └────────────────────────┘
```

Job lifecycle: `queued → running → done`, or `running → queued` (retry after backoff, or lease expired) `→ … → dead`.

## Layout

```
conveyor/
├── broker.py     SQLite schema, atomic claim, leases, retries, backoff, stats
├── worker.py     threaded worker with heartbeat lease renewal
├── client.py     RemoteBroker: the same API over HTTP
├── server.py     JSON API + dashboard
├── tasks.py      @task registry and sample tasks
└── static/dashboard.html
tests/            ordering, contention, leases, fencing, retries, dead letters, HTTP end-to-end
```

## Next steps

- Scheduled (cron-style) recurring jobs
- Rate limits per queue
- A Postgres backend using `SELECT … FOR UPDATE SKIP LOCKED`
