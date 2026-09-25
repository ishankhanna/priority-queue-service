"""Producer/consumer demo.

Demonstrates the enqueue -> dequeue -> ack flow under real concurrency,
against a *running* instance of the service (real HTTP, not the in-process
library -- this is what "producers" and "consumers" as callers of the API
actually look like). Some consumers deliberately "crash" (dequeue but
never ack) to show visibility-timeout redelivery and, for a message that
keeps failing past its retry limit, eventual dead-lettering.

Run:
    # terminal 1
    uvicorn app.main:app

    # terminal 2
    python3 scripts/demo.py
"""

import random
import threading
import time

import httpx

BASE_URL = "http://127.0.0.1:8000"
QUEUE_NAME = "demo-queue"
NUM_PRODUCERS = 3
MESSAGES_PER_PRODUCER = 20
NUM_CONSUMERS = 4
CRASH_PROBABILITY = 0.25  # fraction of dequeues a consumer "fails" to ack
RUN_TIMEOUT_SECONDS = 15

client = httpx.Client(base_url=BASE_URL, timeout=5.0)

enqueued_count = 0
acked_count = 0
counts_lock = threading.Lock()


def setup_queue() -> None:
    r = client.post(
        "/queues",
        json={"name": QUEUE_NAME, "visibility_timeout_seconds": 2, "max_retries": 3},
    )
    if r.status_code not in (201, 409):  # 409 = already created by a previous run
        r.raise_for_status()


def producer(producer_id: int) -> None:
    global enqueued_count
    priorities = ["HIGH", "MEDIUM", "LOW"]
    for i in range(MESSAGES_PER_PRODUCER):
        payload = f"producer-{producer_id}-msg-{i}"
        r = client.post(
            f"/queues/{QUEUE_NAME}/messages",
            json={"payload": payload, "priority": priorities[i % 3]},
        )
        r.raise_for_status()
        with counts_lock:
            enqueued_count += 1


def consumer(consumer_id: int, stop_event: threading.Event) -> None:
    global acked_count
    rng = random.Random(consumer_id)
    while not stop_event.is_set():
        r = client.post(f"/queues/{QUEUE_NAME}/messages/dequeue")
        if r.status_code == 204:
            time.sleep(0.1)
            continue
        r.raise_for_status()
        message = r.json()

        if rng.random() < CRASH_PROBABILITY:
            # Simulate a crashed/slow consumer: don't ack. The visibility
            # timeout will expire and the message will be redelivered.
            print(
                f"[consumer-{consumer_id}] 'crashed' on {message['payload']!r} "
                f"(delivery #{message['delivery_count']})"
            )
            continue

        ack = client.post(
            f"/queues/{QUEUE_NAME}/messages/{message['message_id']}/ack",
            json={"receipt_handle": message["receipt_handle"]},
        )
        if ack.status_code == 204:
            with counts_lock:
                acked_count += 1
            print(
                f"[consumer-{consumer_id}] acked {message['payload']!r} "
                f"(delivery #{message['delivery_count']})"
            )
        elif ack.status_code == 409:
            # Lost the race: this message's visibility timeout already
            # fired and it was redelivered to someone else first.
            print(f"[consumer-{consumer_id}] stale receipt for {message['payload']!r}, skipping")


def print_summary() -> None:
    metrics = client.get(f"/queues/{QUEUE_NAME}/metrics").json()
    dlq = client.get(f"/queues/{QUEUE_NAME}/dlq").json()

    print("\n--- summary ---")
    print(f"metrics: {metrics}")
    print(f"dead-lettered messages ({len(dlq)}):")
    for m in dlq:
        print(f"  - {m['payload']!r} (delivered {m['delivery_count']} times)")


def main() -> None:
    setup_queue()

    producers = [threading.Thread(target=producer, args=(i,)) for i in range(NUM_PRODUCERS)]
    for t in producers:
        t.start()
    for t in producers:
        t.join()

    total_expected = NUM_PRODUCERS * MESSAGES_PER_PRODUCER
    print(f"Enqueued {enqueued_count}/{total_expected} messages. Starting consumers...\n")

    stop_event = threading.Event()
    consumers = [
        threading.Thread(target=consumer, args=(i, stop_event)) for i in range(NUM_CONSUMERS)
    ]
    for t in consumers:
        t.start()

    deadline = time.time() + RUN_TIMEOUT_SECONDS
    while time.time() < deadline and acked_count < total_expected:
        time.sleep(1)
    stop_event.set()
    for t in consumers:
        t.join(timeout=2)

    print(f"\nDone. Acked {acked_count}/{total_expected} messages.")
    print_summary()


if __name__ == "__main__":
    main()
