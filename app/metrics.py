"""Metrics representation.

Design note on throughput: rather than maintaining a custom sliding-window
rate counter, each queue exposes cumulative ``enqueued_total`` /
``acked_total`` counters. Any monitoring system can derive a rate from two
samples of a monotonically increasing counter taken a known time apart --
building an in-process ring buffer to compute "messages/sec" ourselves
would duplicate that for no real benefit here.
"""

from dataclasses import dataclass
from typing import Optional

from app.models import Priority


@dataclass
class QueueMetrics:
    """A snapshot of one queue's metrics at a point in time."""

    name: str
    ready_count: dict[Priority, int]
    in_flight_count: int
    oldest_message_age_seconds: Optional[float]
    enqueued_total: int
    acked_total: int
    dlq_total: int
    expired_total: int
