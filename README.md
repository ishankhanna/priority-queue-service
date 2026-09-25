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

Today this is a single process. To scale out: shard queues across N stateless service instances by hashing the queue name (`hash(name) % N`) behind a routing layer, so all operations for a given queue always land on the same instance — each instance keeps owning its own in-memory queues, no cross-instance coordination needed for a single queue's operations. Rebalancing on scale-up/down would need consistent hashing plus a state-transfer step, since state is in-memory.

**This doesn't help a single hot queue.** Hashing by queue name pins one queue to exactly one instance regardless of `N` — a classic hot-partition problem, the same one any hash-sharded system (Kafka partitions, DynamoDB partition keys) runs into. Adding more instances only spreads *other* queues more thinly; it does nothing for the one queue that's actually overloaded. Fixing that would mean sub-sharding a single logical queue across multiple `MessageQueue` instances (e.g. by priority level or a hash of message ID) — but that's a real trade-off, not a free scale-out: this service's core guarantee is strict priority + FIFO-within-priority ordering enforced by one lock over one set of heaps per queue, so splitting a queue means either accepting weaker per-shard-only ordering or adding cross-shard merge logic on the consumer side. Before reaching for that complexity, it's worth measuring whether a single queue's lock (held only briefly per operation, with no I/O) actually becomes a bottleneck in practice, or whether vertical scaling of that one shard is enough.

## Durability story

Currently: at-least-once delivery holds only while the process is alive — a crash loses all in-memory state (ready, in-flight, and DLQ messages). A production version would add a write-ahead log (append-only, fsync'd on enqueue/ack) so state can be replayed on restart, plus replication to a standby so a single node's disk isn't a single point of failure.

## Node failure handling

Today: if the instance owning a queue goes down, that queue's messages are gone until restart (no failover). Production: replicate each queue's WAL to a standby that can take over as leader; consumers already tolerate a node going away mid-processing via the visibility-timeout mechanism, so failover mainly needs the *ready/in-flight state* to be recoverable, not the delivery protocol to change.

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
- Persistence (WAL) + replication for real durability/failover
- Delete-queue and update-queue-config endpoints
- Load test to validate the p95 < 100ms target under real concurrency
- A background worker thread per queue, running `_reap()` on a timer instead of lazily on every call — would take redelivery/TTL-expiry work off the hot path of enqueue/dequeue/ack, at the cost of the "no background threads" simplicity trade-off described above; only worth it if load testing actually shows per-call reaping as a measurable latency cost
- Multi-tenancy / auth on the API

