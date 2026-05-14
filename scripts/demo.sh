#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

echo "Starting containers..."
docker-compose up -d

echo "Running migrations..."
docker-compose run --rm api npm run migrate

echo "Clearing idempotency keys..."
docker-compose exec redis sh -c "redis-cli --scan --pattern 'idempotency:*' | xargs -r redis-cli del || echo 'no keys'"

echo "Starting retry poller (detached)..."
docker-compose run -d --rm api npm run retry-poller || true

UUID=${1:-$(node -e "console.log(require('crypto').randomUUID())")}
echo "Posting test event with eventId=$UUID"
curl -s -X POST http://localhost:3000/api/v1/notifications/events \
  -H 'Content-Type: application/json' \
  -d "{\"eventId\":\"$UUID\",\"type\":\"order.created\",\"priority\":\"HIGH\",\"payload\":{\"orderId\":123}}" \
  -w '\nHTTP_STATUS:%{http_code}\n'

echo "Tail worker logs (ctrl-c to exit)"
docker-compose logs --tail=200 -f worker
