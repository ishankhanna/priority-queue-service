#!/usr/bin/env bash
# Create a queue.

BASE_URL="http://127.0.0.1:8000"
QUEUE_NAME="demo-queue"

curl -s -X POST "$BASE_URL/queues" \
  -H "Content-Type: application/json" \
  -d "{\"name\": \"$QUEUE_NAME\", \"visibility_timeout_seconds\": 30, \"max_retries\": 3}" \
  | jq .
