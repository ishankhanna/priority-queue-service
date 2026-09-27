#!/usr/bin/env bash
# Enqueue a message.

BASE_URL="http://127.0.0.1:8000"
QUEUE_NAME="demo-queue"
PAYLOAD="hello-world"
PRIORITY="HIGH" # LOW, MEDIUM, HIGH

curl -s -X POST "$BASE_URL/queues/$QUEUE_NAME/messages" \
  -H "Content-Type: application/json" \
  -d "{\"payload\": \"$PAYLOAD\", \"priority\": \"$PRIORITY\"}" \
  | jq .
