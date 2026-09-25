"""Core data model for the priority queue service.

Kept deliberately dependency-free (no FastAPI / Pydantic here) so this module
can be exercised as a plain Python library, independent of how it's exposed
over the network later.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Priority(str, Enum):
    """Message priority."""

    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


@dataclass
class QueueConfig:
    """Per-queue configuration set at creation time."""

    name: str
    visibility_timeout_seconds: float = 30.0
    max_retries: int = 3


@dataclass
class Message:
    """A single message's full lifecycle state.

    One ``Message`` instance is shared by reference between ``ready``,
    ``in_flight``, and ``dlq`` structures inside a queue -- there is only
    ever one object per message, mutated in place as it moves through the
    lifecycle. ``messages`` (id -> Message) is the source of truth for
    whether a message still exists at all.
    """

    id: str
    payload: str
    priority: Priority
    seq: int
    enqueued_at: float
    expires_at: Optional[float] = None

    # Set/updated on each dequeue; used to validate acks and detect stale
    # redeliveries.
    delivery_count: int = 0
    receipt_handle: Optional[str] = None
    visibility_deadline: Optional[float] = None


# ---- domain errors ----
#
# Kept as plain exceptions here (not HTTP-aware) so the queue/service layer
# has no knowledge of REST. The API layer maps these to status
# codes.


class QueueServiceError(Exception):
    """Base class for all domain errors raised by this service."""


class QueueNotFound(QueueServiceError):
    def __init__(self, name: str):
        super().__init__(f"Queue '{name}' does not exist")
        self.name = name


class QueueAlreadyExists(QueueServiceError):
    def __init__(self, name: str):
        super().__init__(f"Queue '{name}' already exists")
        self.name = name


class MessageNotFound(QueueServiceError):
    def __init__(self, message_id: str):
        super().__init__(f"Message '{message_id}' does not exist")
        self.message_id = message_id


class InvalidReceipt(QueueServiceError):
    """Raised when an ack's receipt handle doesn't match the message's
    current in-flight delivery -- e.g. the visibility timeout already
    expired and the message was redelivered to someone else."""

    def __init__(self, message_id: str):
        super().__init__(f"Invalid or stale receipt handle for message '{message_id}'")
        self.message_id = message_id
