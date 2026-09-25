"""A single named, thread-safe, in-memory priority queue.

(See README for full rationale.)

- One min-heap per priority for ``ready`` messages, ordered by a monotonic
  ``seq`` -- gives priority ordering with FIFO within a priority.
  ``visibility_heap``/``ttl_heap`` are similar min-heaps ordered by
  deadline/expiry. No background threads: every public method locks and
  calls ``_reap(now)`` first, so redelivery, DLQ transitions, and TTL
  expiry happen as a side effect of normal operations.
- ``_messages`` (id -> Message) is the source of truth for existence;
  heap entries pointing to a missing message are stale and discarded
  lazily when encountered, rather than removed mid-heap.
- One `threading.Lock` per queue protects everything in it. We use just
  one lock, not several smaller ones, because most operations need to
  touch multiple structures at once anyway (e.g. `dequeue` updates
  `_ready`, `_in_flight`, and `_visibility_heap` together) -- so splitting
  the lock wouldn't help performance, only add a risk of deadlocks.
"""

import copy
import heapq
import itertools
import threading
import time
import uuid
from typing import Optional

from app.metrics import QueueMetrics
from app.models import (
    InvalidReceipt,
    Message,
    MessageNotFound,
    Priority,
    QueueConfig,
)

# Dequeue checks priorities in this order.
_PRIORITY_ORDER = (Priority.HIGH, Priority.MEDIUM, Priority.LOW)


class MessageQueue:
    def __init__(self, config: QueueConfig):
        self.config = config
        self._lock = threading.Lock()

        self._messages: dict[str, Message] = {}
        self._ready: dict[Priority, list[tuple[int, str]]] = {
            priority: [] for priority in _PRIORITY_ORDER
        }
        self._in_flight: dict[str, Message] = {}
        self._visibility_heap: list[tuple[float, str, str]] = []  # (deadline, msg_id, receipt)
        self._ttl_heap: list[tuple[float, str]] = []  # (expires_at, msg_id)
        self._dlq: list[Message] = []

        self._seq_counter = itertools.count()

        # Metrics counters, updated at the exact point of each state
        # transition so that reads are O(1) -- no scanning of messages or
        # heaps needed. `dlq_total` isn't tracked separately since `_dlq`
        # is append-only, so `len(self._dlq)` already is the cumulative
        # count.
        self._ready_count: dict[Priority, int] = {p: 0 for p in _PRIORITY_ORDER}
        self._enqueued_total = 0
        self._acked_total = 0
        self._expired_total = 0

    # ---- public API ----

    def enqueue(
        self, payload: str, priority: Priority, ttl_seconds: Optional[float] = None
    ) -> str:
        """Submit a message. Returns the new message's ID."""
        with self._lock:
            now = time.time()
            self._reap(now)

            msg_id = str(uuid.uuid4())
            seq = next(self._seq_counter)
            expires_at = now + ttl_seconds if ttl_seconds is not None else None

            message = Message(
                id=msg_id,
                payload=payload,
                priority=priority,
                seq=seq,
                enqueued_at=now,
                expires_at=expires_at,
            )
            self._messages[msg_id] = message
            heapq.heappush(self._ready[priority], (seq, msg_id))
            if expires_at is not None:
                heapq.heappush(self._ttl_heap, (expires_at, msg_id))

            self._ready_count[priority] += 1
            self._enqueued_total += 1

            return msg_id

    def dequeue(self) -> Optional[Message]:
        """Return the highest-priority, oldest available message, marking
        it in flight. Returns ``None`` if the queue has nothing ready."""
        with self._lock:
            now = time.time()
            self._reap(now)

            message = self._pop_next_ready()
            if message is None:
                return None

            message.delivery_count += 1
            message.receipt_handle = str(uuid.uuid4())
            message.visibility_deadline = now + self.config.visibility_timeout_seconds

            self._in_flight[message.id] = message
            heapq.heappush(
                self._visibility_heap,
                (message.visibility_deadline, message.id, message.receipt_handle),
            )

            # Return a snapshot, not the live internal object. `message`
            # keeps being mutated in place as it moves through its
            # lifecycle (e.g. redelivered with a new receipt_handle); a
            # caller holding a reference to the internal object would see
            # those later mutations reflected in a value they already
            # "received," which is both an encapsulation break and a
            # thread-safety hazard (mutation happening outside any lock the
            # caller could observe it under).
            return copy.copy(message)

    def ack(self, message_id: str, receipt_handle: str) -> None:
        """Acknowledge successful processing. Removes the message
        permanently. Raises ``MessageNotFound`` if the message doesn't
        exist, or ``InvalidReceipt`` if the receipt is stale (e.g. the
        visibility timeout already expired and the message was
        redelivered)."""
        with self._lock:
            now = time.time()
            self._reap(now)

            message = self._messages.get(message_id)
            if message is None:
                raise MessageNotFound(message_id)

            in_flight_message = self._in_flight.get(message_id)
            if in_flight_message is None or in_flight_message.receipt_handle != receipt_handle:
                raise InvalidReceipt(message_id)

            del self._in_flight[message_id]
            del self._messages[message_id]
            self._acked_total += 1

    def get_dlq(self) -> list[Message]:
        """Return a snapshot of dead-lettered messages."""
        with self._lock:
            now = time.time()
            self._reap(now)
            return [copy.copy(message) for message in self._dlq]

    def get_metrics(self) -> QueueMetrics:
        """Return a point-in-time snapshot of this queue's metrics."""
        with self._lock:
            now = time.time()
            self._reap(now)
            return QueueMetrics(
                name=self.config.name,
                ready_count=dict(self._ready_count),
                in_flight_count=len(self._in_flight),
                oldest_message_age_seconds=self._oldest_ready_age(now),
                enqueued_total=self._enqueued_total,
                acked_total=self._acked_total,
                dlq_total=len(self._dlq),
                expired_total=self._expired_total,
            )

    # ---- internal helpers (caller must hold self._lock) ----

    def _oldest_ready_age(self, now: float) -> Optional[float]:
        """Age in seconds of the oldest ready message across all
        priorities, or ``None`` if the queue has nothing ready.

        Permanently discards any stale heap entries encountered along the
        way (messages already removed from `_messages`, e.g. via TTL
        expiry) -- this is a real, not just a peek-only, cleanup, since
        those entries are genuinely garbage. The valid top of each heap is
        only peeked, never popped.
        """
        oldest: Optional[float] = None
        for priority in _PRIORITY_ORDER:
            heap = self._ready[priority]
            while heap and heap[0][1] not in self._messages:
                heapq.heappop(heap)
            if heap:
                _, msg_id = heap[0]  # peek only
                age = now - self._messages[msg_id].enqueued_at
                if oldest is None or age > oldest:
                    oldest = age
        return oldest

    def _pop_next_ready(self) -> Optional[Message]:
        """Pop and return the highest-priority, oldest ready message,
        skipping any stale heap entries left behind by TTL expiry."""
        for priority in _PRIORITY_ORDER:
            heap = self._ready[priority]
            while heap:
                _, msg_id = heap[0]
                message = self._messages.get(msg_id)
                if message is None:
                    heapq.heappop(heap)  # stale: expired via TTL, discard and keep looking
                    continue
                heapq.heappop(heap)
                self._ready_count[priority] -= 1
                return message
        return None

    def _reap(self, now: float) -> None:
        """Lazily apply visibility-timeout redelivery/DLQ transitions and
        TTL expiry. Called at the start of every public operation."""
        self._reap_visibility_timeouts(now)
        self._reap_ttl(now)

    def _reap_visibility_timeouts(self, now: float) -> None:
        heap = self._visibility_heap
        while heap and heap[0][0] <= now:
            _deadline, msg_id, receipt = heapq.heappop(heap)

            message = self._messages.get(msg_id)
            in_flight_message = self._in_flight.get(msg_id)
            if (
                message is None
                or in_flight_message is None
                or in_flight_message.receipt_handle != receipt
            ):
                # Stale entry: already acked, or superseded by a later
                # delivery (this exact receipt is no longer the live one).
                continue

            del self._in_flight[msg_id]

            if message.expires_at is not None and message.expires_at <= now:
                # TTL passed while in flight -- drop rather than redeliver.
                del self._messages[msg_id]
                self._expired_total += 1
                continue

            if message.delivery_count > self.config.max_retries:
                self._dlq.append(message)
                del self._messages[msg_id]
                continue

            # Redeliver: push back using the ORIGINAL seq so it regains its
            # original FIFO position within its priority, rather than
            # jumping ahead of messages that have been waiting the whole
            # time.
            heapq.heappush(self._ready[message.priority], (message.seq, msg_id))
            self._ready_count[message.priority] += 1

    def _reap_ttl(self, now: float) -> None:
        heap = self._ttl_heap
        while heap and heap[0][0] <= now:
            _expires_at, msg_id = heapq.heappop(heap)

            message = self._messages.get(msg_id)
            if message is None:
                continue  # already acked / already expired / already DLQ'd
            if msg_id in self._in_flight:
                # Currently being processed -- don't yank it out from under
                # a consumer. If it comes back to ready via visibility
                # timeout, expiry is re-checked there.
                continue

            # Genuinely idle in `ready` and its TTL has passed.
            del self._messages[msg_id]
            self._ready_count[message.priority] -= 1
            self._expired_total += 1
            # The corresponding (seq, msg_id) entry is left in the ready
            # heap; it's discarded lazily by `_pop_next_ready` when reached.
