#!/usr/bin/env bash
# Get metrics for the demo queue.

BASE_URL="http://127.0.0.1:8000"
QUEUE_NAME="demo-queue"

curl -s -X GET "$BASE_URL/queues/$QUEUE_NAME/metrics" | jq .
