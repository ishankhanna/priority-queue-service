"""REST API for the priority queue service.
"""

from typing import Optional

from fastapi import FastAPI, Response, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.metrics import QueueMetrics
from app.models import (
    Priority,
    QueueAlreadyExists,
    QueueConfig,
    QueueNotFound,
    MessageNotFound,
    InvalidReceipt,
    QueueServiceError,
)
from app.service import QueueService

app = FastAPI(title="Keychain Priority Queue Service")

# Single process-wide service instance.
service = QueueService()


# ---- request / response schemas ----


class CreateQueueRequest(BaseModel):
    # Restricted to URL-path-safe characters: the queue name becomes part
    # of every other route as a path segment (e.g. `/queues/{name}/...`),
    # so anything containing `/` would silently break routing rather than
    # failing loudly at creation time. Rejecting it here, at the one place
    # a name is chosen, is simpler than handling it at every route that
    # takes `{name}`.
    name: str = Field(min_length=1, max_length=200, pattern=r"^[A-Za-z0-9_-]+$")
    visibility_timeout_seconds: float = Field(default=30.0, gt=0)
    max_retries: int = Field(default=3, ge=0)


class QueueConfigResponse(BaseModel):
    name: str
    visibility_timeout_seconds: float
    max_retries: int

    @classmethod
    def from_config(cls, config: QueueConfig) -> "QueueConfigResponse":
        return cls(
            name=config.name,
            visibility_timeout_seconds=config.visibility_timeout_seconds,
            max_retries=config.max_retries,
        )


class EnqueueRequest(BaseModel):
    payload: str
    priority: Priority
    ttl_seconds: Optional[float] = Field(default=None, gt=0)


class EnqueueResponse(BaseModel):
    message_id: str


class DequeueResponse(BaseModel):
    message_id: str
    payload: str
    priority: Priority
    receipt_handle: str
    delivery_count: int


class AckRequest(BaseModel):
    receipt_handle: str


class DlqMessageResponse(BaseModel):
    message_id: str
    payload: str
    priority: Priority
    delivery_count: int
    enqueued_at: float


class QueueMetricsResponse(BaseModel):
    name: str
    ready_count: dict[Priority, int]
    in_flight_count: int
    oldest_message_age_seconds: Optional[float]
    enqueued_total: int
    acked_total: int
    dlq_total: int
    expired_total: int

    @classmethod
    def from_metrics(cls, metrics: QueueMetrics) -> "QueueMetricsResponse":
        return cls(
            name=metrics.name,
            ready_count=metrics.ready_count,
            in_flight_count=metrics.in_flight_count,
            oldest_message_age_seconds=metrics.oldest_message_age_seconds,
            enqueued_total=metrics.enqueued_total,
            acked_total=metrics.acked_total,
            dlq_total=metrics.dlq_total,
            expired_total=metrics.expired_total,
        )


# ---- domain error -> HTTP status mapping ----
#
# app/models.py and app/queue.py know nothing about HTTP; this is the one
# place that translates domain errors into status codes.

_ERROR_STATUS = {
    QueueNotFound: status.HTTP_404_NOT_FOUND,
    QueueAlreadyExists: status.HTTP_409_CONFLICT,
    MessageNotFound: status.HTTP_404_NOT_FOUND,
    InvalidReceipt: status.HTTP_409_CONFLICT,
}


@app.exception_handler(QueueServiceError)
def handle_queue_service_error(request, exc: QueueServiceError) -> JSONResponse:
    status_code = _ERROR_STATUS.get(type(exc), status.HTTP_400_BAD_REQUEST)
    return JSONResponse(status_code=status_code, content={"detail": str(exc)})


# ---- admin ----


@app.post("/queues", response_model=QueueConfigResponse, status_code=status.HTTP_201_CREATED)
def create_queue(request: CreateQueueRequest) -> QueueConfigResponse:
    config = service.create_queue(
        name=request.name,
        visibility_timeout_seconds=request.visibility_timeout_seconds,
        max_retries=request.max_retries,
    )
    return QueueConfigResponse.from_config(config)


@app.get("/queues", response_model=list[str])
def list_queues() -> list[str]:
    return service.list_queues()


@app.get("/queues/{name}", response_model=QueueConfigResponse)
def get_queue(name: str) -> QueueConfigResponse:
    queue = service.get_queue(name)
    return QueueConfigResponse.from_config(queue.config)


# ---- messages ----


@app.post(
    "/queues/{name}/messages",
    response_model=EnqueueResponse,
    status_code=status.HTTP_201_CREATED,
)
def enqueue(name: str, request: EnqueueRequest) -> EnqueueResponse:
    queue = service.get_queue(name)
    message_id = queue.enqueue(request.payload, request.priority, request.ttl_seconds)
    return EnqueueResponse(message_id=message_id)


@app.post("/queues/{name}/messages/dequeue", response_model=DequeueResponse)
def dequeue(name: str) -> DequeueResponse:
    queue = service.get_queue(name)
    message = queue.dequeue()
    if message is None:
        # Returning a Response directly bypasses response_model validation
        # for this branch, so a 204 can carry no body.
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return DequeueResponse(
        message_id=message.id,
        payload=message.payload,
        priority=message.priority,
        receipt_handle=message.receipt_handle,
        delivery_count=message.delivery_count,
    )


@app.post("/queues/{name}/messages/{message_id}/ack", status_code=status.HTTP_204_NO_CONTENT)
def ack(name: str, message_id: str, request: AckRequest) -> Response:
    queue = service.get_queue(name)
    queue.ack(message_id, request.receipt_handle)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/queues/{name}/dlq", response_model=list[DlqMessageResponse])
def get_dlq(name: str) -> list[DlqMessageResponse]:
    queue = service.get_queue(name)
    return [
        DlqMessageResponse(
            message_id=message.id,
            payload=message.payload,
            priority=message.priority,
            delivery_count=message.delivery_count,
            enqueued_at=message.enqueued_at,
        )
        for message in queue.get_dlq()
    ]


# ---- metrics ----


@app.get("/queues/{name}/metrics", response_model=QueueMetricsResponse)
def get_queue_metrics(name: str) -> QueueMetricsResponse:
    queue = service.get_queue(name)
    return QueueMetricsResponse.from_metrics(queue.get_metrics())


@app.get("/metrics", response_model=list[QueueMetricsResponse])
def get_all_metrics() -> list[QueueMetricsResponse]:
    """Metrics for every queue, in one JSON call -- e.g. for a dashboard or
    a scraper that doesn't want to enumerate queues first."""
    return [QueueMetricsResponse.from_metrics(m) for m in service.get_all_metrics()]
