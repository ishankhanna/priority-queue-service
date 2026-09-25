"""Registry of named queues.

``QueueService`` owns queue creation/lookup only. All message-level
operations (enqueue/dequeue/ack) are delegated to the individual
``MessageQueue``, which has its own lock -- so operations on different
queues never contend with each other, and this registry's lock is only
ever held briefly.

Thread-safety review (deliberately no finer-grained locking than this):

- This registry lock and a `MessageQueue`'s own lock are never held at the
  same time by any code path. `get_queue`/`create_queue` release the
  registry lock before returning; `get_all_metrics` releases it before
  calling into any individual queue. So there's no lock-ordering to get
  wrong, and therefore no deadlock risk from these two locks -- adding
  more locks here would only add that risk back for no benefit, since
  registry operations are already O(1) dict lookups held for microseconds.
"""

import threading

from app.metrics import QueueMetrics
from app.models import QueueAlreadyExists, QueueConfig, QueueNotFound
from app.queue import MessageQueue


class QueueService:
    def __init__(self):
        self._lock = threading.Lock()
        self._queues: dict[str, MessageQueue] = {}

    def create_queue(
        self,
        name: str,
        visibility_timeout_seconds: float = 30.0,
        max_retries: int = 3,
    ) -> QueueConfig:
        """Create a new named queue. Raises ``QueueAlreadyExists`` if the
        name is already taken."""
        with self._lock:
            if name in self._queues:
                raise QueueAlreadyExists(name)

            config = QueueConfig(
                name=name,
                visibility_timeout_seconds=visibility_timeout_seconds,
                max_retries=max_retries,
            )
            self._queues[name] = MessageQueue(config)
            return config

    def get_queue(self, name: str) -> MessageQueue:
        """Look up a queue by name. Raises ``QueueNotFound`` if it doesn't
        exist."""
        with self._lock:
            queue = self._queues.get(name)
        if queue is None:
            raise QueueNotFound(name)
        return queue

    def list_queues(self) -> list[str]:
        with self._lock:
            return list(self._queues.keys())

    def get_all_metrics(self) -> list[QueueMetrics]:
        """Return a metrics snapshot for every queue.

        Takes a brief snapshot of the registry (list of queue references)
        under the registry lock, then calls each queue's own `get_metrics`
        outside that lock -- so a slow read on one queue can never block
        registry lookups or other queues' metrics.
        """
        with self._lock:
            queues = list(self._queues.values())
        return [queue.get_metrics() for queue in queues]
