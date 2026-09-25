"""Unit tests for MessageQueue -- the core functional requirements.

Timeout-related tests use small real `visibility_timeout_seconds`/
`ttl_seconds` values plus `time.sleep()`, per the deliberate decision not
to use an injectable clock in `MessageQueue` (see app/queue.py).
"""

import time

import pytest

from app.models import InvalidReceipt, MessageNotFound, Priority, QueueAlreadyExists, QueueNotFound
from app.service import QueueService


def make_queue(**config):
    service = QueueService()
    service.create_queue("q", **config)
    return service.get_queue("q")


# ---- create queue ----


def test_create_duplicate_queue_raises():
    service = QueueService()
    service.create_queue("dup")
    with pytest.raises(QueueAlreadyExists):
        service.create_queue("dup")


def test_get_unknown_queue_raises():
    service = QueueService()
    with pytest.raises(QueueNotFound):
        service.get_queue("does-not-exist")


# ---- enqueue / priority ordering / FIFO ----


def test_priority_ordering():
    q = make_queue()
    q.enqueue("low", Priority.LOW)
    q.enqueue("medium", Priority.MEDIUM)
    q.enqueue("high", Priority.HIGH)

    assert q.dequeue().payload == "high"
    assert q.dequeue().payload == "medium"
    assert q.dequeue().payload == "low"


def test_fifo_within_same_priority():
    q = make_queue()
    q.enqueue("first", Priority.HIGH)
    q.enqueue("second", Priority.HIGH)
    q.enqueue("third", Priority.HIGH)

    assert q.dequeue().payload == "first"
    assert q.dequeue().payload == "second"
    assert q.dequeue().payload == "third"


def test_priority_inversion_does_not_happen():
    """A HIGH message enqueued AFTER a LOW one must still be dequeued
    first -- priority always wins over arrival order across priorities."""
    q = make_queue()
    q.enqueue("low-first", Priority.LOW)
    q.enqueue("high-second", Priority.HIGH)

    assert q.dequeue().payload == "high-second"
    assert q.dequeue().payload == "low-first"


def test_dequeue_on_empty_queue_returns_none():
    q = make_queue()
    assert q.dequeue() is None


def test_enqueue_returns_unique_message_ids():
    q = make_queue()
    assert q.enqueue("a", Priority.HIGH) != q.enqueue("b", Priority.HIGH)


# ---- acknowledge ----


def test_ack_removes_message_permanently():
    q = make_queue()
    q.enqueue("a", Priority.HIGH)
    m = q.dequeue()
    q.ack(m.id, m.receipt_handle)

    with pytest.raises(MessageNotFound):
        q.ack(m.id, m.receipt_handle)  # double-ack


def test_ack_with_wrong_receipt_is_rejected():
    q = make_queue()
    q.enqueue("a", Priority.HIGH)
    m = q.dequeue()

    with pytest.raises(InvalidReceipt):
        q.ack(m.id, "not-the-real-receipt")


def test_ack_unknown_message_raises_message_not_found():
    q = make_queue()
    with pytest.raises(MessageNotFound):
        q.ack("does-not-exist", "whatever")


# ---- visibility timeout / redelivery ----


def test_visibility_timeout_triggers_redelivery():
    q = make_queue(visibility_timeout_seconds=0.2, max_retries=3)
    q.enqueue("a", Priority.HIGH)

    first = q.dequeue()
    assert first.delivery_count == 1

    time.sleep(0.3)  # let visibility timeout expire without acking

    second = q.dequeue()
    assert second is not None
    assert second.id == first.id
    assert second.delivery_count == 2
    assert second.receipt_handle != first.receipt_handle


def test_stale_receipt_rejected_after_redelivery():
    q = make_queue(visibility_timeout_seconds=0.2, max_retries=3)
    q.enqueue("a", Priority.HIGH)

    first = q.dequeue()
    time.sleep(0.3)
    q.dequeue()  # redelivers with a new receipt handle

    with pytest.raises(InvalidReceipt):
        q.ack(first.id, first.receipt_handle)  # the OLD receipt, now stale


def test_redelivered_message_keeps_original_fifo_position():
    """A redelivered message must not jump ahead of messages that have
    been waiting the whole time -- it keeps its original seq."""
    q = make_queue(visibility_timeout_seconds=0.2, max_retries=3)
    q.enqueue("a", Priority.HIGH)
    q.enqueue("b", Priority.HIGH)

    q.dequeue()  # "a" now in flight
    time.sleep(0.3)  # "a" times out and is reaped back to ready

    redelivered = q.dequeue()
    assert redelivered.payload == "a"
    assert q.dequeue().payload == "b"


def test_dequeue_returns_snapshot_not_live_reference():
    """Regression test for a real bug found during development: dequeue
    must not return a reference to the internal Message object, since
    later mutation (e.g. redelivery) would otherwise silently change a
    value the caller already 'received'."""
    q = make_queue(visibility_timeout_seconds=0.2, max_retries=3)
    q.enqueue("a", Priority.HIGH)

    first = q.dequeue()
    original_receipt = first.receipt_handle

    time.sleep(0.3)
    q.dequeue()  # triggers redelivery, mutating the internal Message

    assert first.receipt_handle == original_receipt  # caller's copy is unchanged


# ---- retry limit / DLQ ----


def test_retry_limit_exceeded_moves_to_dlq():
    q = make_queue(visibility_timeout_seconds=0.2, max_retries=1)
    msg_id = q.enqueue("flaky", Priority.HIGH)

    for _ in range(2):  # delivered max_retries + 1 = 2 times, never acked
        m = q.dequeue()
        assert m.id == msg_id
        time.sleep(0.3)

    assert q.dequeue() is None  # no longer redelivered

    dlq = q.get_dlq()
    assert len(dlq) == 1
    assert dlq[0].id == msg_id


def test_max_retries_zero_means_delivered_exactly_once():
    q = make_queue(visibility_timeout_seconds=0.2, max_retries=0)
    msg_id = q.enqueue("one-shot", Priority.HIGH)

    q.dequeue()
    time.sleep(0.3)  # first timeout already exceeds max_retries=0

    assert q.dequeue() is None
    assert q.get_dlq()[0].id == msg_id


# ---- TTL expiry ----


def test_ttl_expiry_drops_ready_message():
    q = make_queue()
    msg_id = q.enqueue("expires-soon", Priority.HIGH, ttl_seconds=0.2)
    time.sleep(0.3)

    assert q.dequeue() is None
    assert all(m.id != msg_id for m in q.get_dlq())  # TTL expiry != DLQ


def test_ttl_expiry_while_in_flight_drops_on_redelivery_attempt():
    """TTL expiring while a message is in flight should drop it (not
    redeliver it) once its visibility timeout also expires."""
    q = make_queue(visibility_timeout_seconds=0.2, max_retries=5)
    msg_id = q.enqueue("expires-in-flight", Priority.HIGH, ttl_seconds=0.25)

    m = q.dequeue()
    assert m.id == msg_id
    time.sleep(0.4)  # both visibility timeout AND ttl have now passed

    assert q.dequeue() is None
    assert all(msg.id != msg_id for msg in q.get_dlq())


# ---- metrics (see app/metrics.py) ----


def test_metrics_reflect_ready_and_in_flight_counts():
    q = make_queue()
    q.enqueue("a", Priority.HIGH)
    q.enqueue("b", Priority.LOW)
    m = q.dequeue()

    metrics = q.get_metrics()
    assert metrics.ready_count == {Priority.HIGH: 0, Priority.MEDIUM: 0, Priority.LOW: 1}
    assert metrics.in_flight_count == 1
    assert metrics.enqueued_total == 2

    q.ack(m.id, m.receipt_handle)
    metrics = q.get_metrics()
    assert metrics.in_flight_count == 0
    assert metrics.acked_total == 1


def test_metrics_dlq_and_expired_counters():
    q = make_queue(visibility_timeout_seconds=0.2, max_retries=0)
    q.enqueue("flaky", Priority.HIGH)
    q.dequeue()
    time.sleep(0.3)

    metrics = q.get_metrics()  # triggers reap -> DLQ (max_retries=0)
    assert metrics.dlq_total == 1

    q.enqueue("ttl", Priority.LOW, ttl_seconds=0.2)
    time.sleep(0.3)

    metrics = q.get_metrics()
    assert metrics.expired_total == 1
