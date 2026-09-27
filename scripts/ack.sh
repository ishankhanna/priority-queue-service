#!/usr/bin/env bash
# Acknowledge a message.

BASE_URL="http://127.0.0.1:8000"
QUEUE_NAME="demo-queue"
MESSAGE_ID="paste-message-id-here"
RECEIPT_HANDLE="paste-receipt-handle-here"

curl -s -X POST "$BASE_URL/queues/$QUEUE_NAME/messages/$MESSAGE_ID/ack" \
  -H "Content-Type: application/json" \
  -d "{\"receipt_handle\": \"$RECEIPT_HANDLE\"}" \
  -w "\nHTTP %{http_code}\n"
