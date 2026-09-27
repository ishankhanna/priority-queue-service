#!/usr/bin/env bash
# Dequeue a message. Note the message_id and receipt_handle in the
# response -- ack.sh needs both.

BASE_URL="http://127.0.0.1:8000"
QUEUE_NAME="demo-queue"

curl -s -X POST "$BASE_URL/queues/$QUEUE_NAME/messages/dequeue" | jq .
