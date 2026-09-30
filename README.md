# Keychain — Distributed Priority Queue Service

An in-memory, thread-safe priority queue service (Python + FastAPI). Producers enqueue messages with a priority (`HIGH`/`MEDIUM`/`LOW`) and optional TTL; consumers dequeue, process, and acknowledge them, with at-least-once delivery via a visibility-timeout + retry/DLQ model. Full problem spec: see the assignment PDF in this repo.

## Running it

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Interactive API docs: `http://127.0.0.1:8000/docs`

## API at a glance


| Method & Path                                 | Purpose                                                         |
| --------------------------------------------- | --------------------------------------------------------------- |
| `POST /queues`                                | Create a queue (name, visibility timeout, max retries)          |
| `GET /queues` / `GET /queues/{name}`          | List queues / get one queue's config                            |
| `POST /queues/{name}/messages`                | Enqueue (payload, priority, optional TTL)                       |
| `POST /queues/{name}/messages/dequeue`        | Dequeue highest-priority, oldest message (200, or 204 if empty) |
| `POST /queues/{name}/messages/{id}/ack`       | Acknowledge (body: receipt handle)                              |
| `GET /queues/{name}/dlq`                      | List dead-lettered messages                                     |
| `GET /queues/{name}/metrics` / `GET /metrics` | Metrics for one queue / all queues (JSON)                       |


Full request/response schemas are in `/docs` (auto-generated from the code) — not duplicated here.

## Design decisions & trade-offs

- **Per-priority min-heaps, keyed by a monotonic** `seq` — gives priority ordering with FIFO within a priority in O(log n); a redelivered message keeps its *original* `seq`, so it doesn't jump ahead of messages that waited the whole time.
- **No background threads.** Every operation (enqueue/dequeue/ack/metrics) lazily "reaps" expired state (visibility timeouts, TTLs) under its own lock before doing its own work. Simpler to reason about and test than a timer thread, at the cost of a very small amount of extra work per call.
- **Receipt handles.** Each dequeue issues a fresh receipt handle; ack must present the exact current one. This means a late ack from a consumer whose visibility timeout already expired is rejected instead of silently deleting a message someone else is now processing.
- **TTL expiry drops the message; it does not go to the DLQ.** DLQ is reserved for "exceeded retry limit" — a distinct failure mode from "nobody processed this in time."
- **In-memory, single-process implementation.** All queue state (ready heaps, in-flight messages, DLQ) lives in process memory rather than a database or external broker — this keeps the service simple and fast to build/test, at the cost of durability across restarts and horizontal scaling; see "Scalability story" and "Durability story" below for how that trade-off would be addressed in production.



## Concurrency model

One `threading.Lock` per queue, not a single global lock — operations on different queues never contend. A separate registry lock guards only queue creation/lookup and is released before any message operation runs, so it's held for microseconds. All API routes are sync (not `async def`), so FastAPI runs them on real threads, matching this lock-based design.

**Deliberately not more fine-grained than this.** A queue's `ready` heaps, `in_flight`, timeout heaps, and DLQ are updated together as part of single logical operations (e.g. dequeue moves a message from `ready` into `in_flight` and onto the visibility heap atomically) — splitting them into per-structure locks wouldn't add real parallelism (the same operation still needs all of them together) but would add a genuine new risk: getting lock-acquisition order wrong across methods and deadlocking. The registry lock and a queue's lock are also never held at the same time by any code path, so there's no cross-lock ordering to get wrong either. Verified under load: 5 producer threads enqueuing 200 messages total onto one queue, drained concurrently by 8 consumer threads — no lost messages, no exceptions, no corruption (see `tests/test_concurrency.py`).

## Scalability story

**Today:** a single process holds all state, so it can't be scaled out as-is.

**Production design:** move the queue state into Redis so the API servers become stateless and any instance can serve any request. The same structures map directly:


| In-memory (today)                       | Redis                                                                        |
| --------------------------------------- | ---------------------------------------------------------------------------- |
| Per-priority ready heap, keyed by `seq` | One sorted set per priority, score = `seq`                                   |
| Visibility-timeout heap                 | Sorted set, score = deadline                                                 |
| TTL heap                                | Sorted set, score = expiry time                                              |
| `_messages` dict                        | One hash per message (payload, delivery count, receipt handle, enqueue time) |
| Per-queue lock                          | A Lua script per operation (Redis runs each script atomically)               |


- **Sequence numbers:** `INCR` on a per-queue counter, done inside the enqueue script. This is strictly increasing, so FIFO within a priority still holds.
- **Time:** scripts use Redis's own clock (`TIME`), so visibility deadlines are consistent no matter which API server handled the call.
- **Reaping** (returning timed-out messages to ready, moving exhausted ones to the DLQ, dropping expired ones) happens in two places. A small background sweeper runs every second or so and keeps idle queues and metrics accurate. The dequeue script also does a small reap itself, so redelivery is still prompt if the sweeper is late. Both are atomic scripts with a cap on entries handled per call, so running them twice is harmless and neither blocks Redis for long.
- **Scaling Redis:** with Redis Cluster, all keys for a queue share a hash tag (`q:{name}:...`) so its scripts run on one shard.

**Limits:** this doesn't help a single very hot queue. Enqueue, dequeue and ack are all writes, so they must go to the one primary that owns the queue; read replicas only help with metrics and DLQ reads. To push one queue further, batch operations, use a bigger instance, or split the queue across shards (which weakens the ordering guarantee). Memory also bounds the backlog, so Redis should run with `noeviction` and the API should cap payload size and queue depth.

## Durability story

**Today:** at-least-once delivery holds only while the process is alive. A crash loses everything (ready, in-flight and DLQ).

**Production design:** Redis with AOF persistence (Redis writes every change to a log file on disk) and at least one replica.

- **What survives:** if Redis restarts, it reloads its data from the AOF file, so messages are not lost. If the Redis machine itself dies, a replica takes over.
- **What can still be lost:** replication is asynchronous, so the last moments of writes may not have reached the replica yet. A message enqueued just before a crash could be lost.
- **Closing that gap:** the API can wait for a replica to confirm each write (`WAIT`) before telling the producer it succeeded. This is safer but adds some latency.
- **No separate log in S3 or a database:** AOF and replication already do this job, and writing to a second system would add latency and two copies that can disagree. Periodic backups to S3 are still a good idea for disaster recovery.
- **Duplicates:** the queue guarantees at-least-once delivery, so a message can occasionally arrive twice. Consumers should handle that (for example by ignoring a message ID they've already processed).

**Alternative:** if "an acknowledged message is never lost" must be a hard guarantee and throughput is moderate, a Postgres-backed queue is a better fit. Dequeue uses `SELECT ... FOR UPDATE SKIP LOCKED`, the visibility timeout is a `visible_at` column, and commits are durable with a synchronous replica. The trade-offs are lower peak throughput and needing to keep table bloat under control.

## Node failure handling

- **API server dies:** nothing is lost, because servers hold no state. The load balancer routes to another instance. Messages that were in flight there are redelivered after their visibility timeout.
- **Redis primary dies:** a replica is promoted (Sentinel or Cluster failover). Writes that hadn't replicated yet may be lost, and acks that hadn't replicated may cause a message to be delivered again. Duplicates are allowed under at-least-once, so the delivery protocol doesn't change.
- **Consumer dies mid-processing:** the visibility timeout expires and the message is redelivered, as today.



## Testing approach

```bash
pytest       # 22 tests: tests/test_queue.py, test_concurrency.py
pytest --cov=app --cov-report=term-missing   # coverage: app/main.py untested at the HTTP layer
```

- `tests/test_queue.py` — unit tests directly against `MessageQueue`: priority ordering, FIFO within a priority, priority inversion, visibility-timeout redelivery, stale-receipt rejection, retry-limit → DLQ, TTL expiry (including while in flight), metrics counters, and a regression test for the mutable-reference bug mentioned above. Timeout tests use small real `sleep()`s rather than a fake clock (a deliberate simplicity trade-off).
- `tests/test_concurrency.py` — real OS threads as producers/consumers against one queue: (1) N producers + M consumers racing to enqueue/dequeue/ack, asserting every message is delivered exactly once with none lost; (2) consumers that randomly "crash" (dequeue, never ack), asserting every message is still eventually delivered and acked exactly once via redelivery, with no double-acks.
- `scripts/demo.py` — a standalone producer/consumer script against a *running* server (real HTTP, not in-process), with consumers randomly simulating crashes so redelivery is visible; run it with `uvicorn app.main:app` up in another terminal. Verified end-to-end: 60/60 messages enqueued and acked despite simulated crashes, including one message redelivered 4 times before finally succeeding.



## What I'd do with more time

- Extend visibility timeout on demand and requeue-from-DLQ endpoints
- Redis-backed storage (see "Scalability story") for durability, failover and stateless API servers
- Delete-queue and update-queue-config endpoints
- Load test to validate the p95 < 100ms target under real concurrency
- A background worker thread per queue, running `_reap()` on a timer instead of lazily on every call — would take redelivery/TTL-expiry work off the hot path of enqueue/dequeue/ack, at the cost of the "no background threads" simplicity trade-off described above; only worth it if load testing actually shows per-call reaping as a measurable latency cost
- Multi-tenancy / auth on the API

