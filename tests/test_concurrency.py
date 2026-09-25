"""Concurrency tests: real OS threads acting as producers/consumers,
directly against MessageQueue/QueueService. These are the tests that
would catch lock-granularity bugs, lost updates, or double-delivery.
"""

import random
import threading
import time

from app.models import InvalidReceipt, Priority
from app.service import QueueService


def test_concurrent_producers_and_consumers_no_loss_no_duplication():
    """N producer threads enqueue onto one queue; M consumer threads race
    to drain it, acking immediately. Every message enqueued must end up
    delivered exactly once -- nothing lost, nothing double-delivered."""
    service = QueueService()
    # Visibility timeout generous enough that nothing times out mid-test;
    # this test is specifically about the ready/in-flight/ack path, not
    # redelivery (that's covered separately below).
    service.create_queue("stress", visibility_timeout_seconds=5, max_retries=5)
    queue = service.get_queue("stress")

    num_producers = 5
    messages_per_producer = 40
    total_expected = num_producers * messages_per_producer

    enqueued_ids = []
    enqueued_lock = threading.Lock()

    def produce(producer_id: int):
        priorities = (Priority.HIGH, Priority.MEDIUM, Priority.LOW)
        ids = [
            queue.enqueue(f"p{producer_id}-m{i}", priorities[i % 3])
            for i in range(messages_per_producer)
        ]
        with enqueued_lock:
            enqueued_ids.extend(ids)

    producers = [threading.Thread(target=produce, args=(i,)) for i in range(num_producers)]
    for t in producers:
        t.start()
    for t in producers:
        t.join()

    assert len(enqueued_ids) == total_expected
    assert len(set(enqueued_ids)) == total_expected  # all unique IDs

    delivered_ids = []
    delivered_lock = threading.Lock()

    def consume():
        local = []
        while True:
            message = queue.dequeue()
            if message is None:
                break
            queue.ack(message.id, message.receipt_handle)
            local.append(message.id)
        with delivered_lock:
            delivered_ids.extend(local)

    consumers = [threading.Thread(target=consume) for _ in range(8)]
    for t in consumers:
        t.start()
    for t in consumers:
        t.join()

    assert len(delivered_ids) == total_expected, "some messages were lost or a consumer over-drained"
    assert len(set(delivered_ids)) == total_expected, "some message was delivered more than once"
    assert set(delivered_ids) == set(enqueued_ids)

    metrics = queue.get_metrics()
    assert metrics.in_flight_count == 0
    assert sum(metrics.ready_count.values()) == 0


def test_concurrent_consumers_with_simulated_crashes_eventually_ack_all():
    """Some consumers deliberately dequeue and never ack (simulating a
    crash). Messages must eventually be redelivered via the visibility
    timeout and end up acked exactly once by whoever gets them next --
    proving at-least-once delivery holds, and exactly-once *acking* holds,
    even under concurrent redelivery."""
    service = QueueService()
    service.create_queue("crashy", visibility_timeout_seconds=0.2, max_retries=10)
    queue = service.get_queue("crashy")

    num_messages = 50
    ids = [queue.enqueue(f"m{i}", Priority.HIGH) for i in range(num_messages)]

    acked_ids = []
    acked_lock = threading.Lock()
    stop = threading.Event()

    def worker(worker_id: int):
        rng = random.Random(worker_id)
        while not stop.is_set():
            message = queue.dequeue()
            if message is None:
                time.sleep(0.02)
                continue
            if rng.random() < 0.3:
                continue  # simulate a crash: never ack, let it time out
            try:
                queue.ack(message.id, message.receipt_handle)
            except InvalidReceipt:
                # Lost the race: this message's visibility timeout fired
                # and it was redelivered to someone else before our ack
                # arrived. Correct behavior, not a bug -- just don't
                # count it as ours.
                continue
            with acked_lock:
                acked_ids.append(message.id)

    workers = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
    for t in workers:
        t.start()

    deadline = time.time() + 6  # generous window for several redelivery rounds
    while time.time() < deadline and len(acked_ids) < num_messages:
        time.sleep(0.05)
    stop.set()
    for t in workers:
        t.join(timeout=2)

    assert len(acked_ids) == num_messages, "not all messages were eventually delivered and acked"
    assert len(set(acked_ids)) == num_messages, "a message was acked more than once"
    assert set(acked_ids) == set(ids)

    metrics = queue.get_metrics()
    assert metrics.dlq_total == 0, "max_retries=10 should have been generous enough to avoid DLQ"
    assert metrics.in_flight_count == 0
