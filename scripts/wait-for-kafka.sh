#!/usr/bin/env bash
set -euo pipefail

HOST=${1:-kafka}
PORT=${2:-9092}
TIMEOUT=${3:-60}

echo "Waiting for Kafka at $HOST:$PORT (timeout ${TIMEOUT}s)..."
start=$(date +%s)
while true; do
  if timeout 1 bash -c "</dev/tcp/$HOST/$PORT" >/dev/null 2>&1; then
    echo "Kafka is available at $HOST:$PORT"
    exit 0
  fi
  now=$(date +%s)
  elapsed=$((now - start))
  if [ "$elapsed" -ge "$TIMEOUT" ]; then
    echo "Timed out waiting for Kafka at $HOST:$PORT" >&2
    exit 1
  fi
  sleep 1
done
