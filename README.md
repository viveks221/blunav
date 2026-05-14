# Notification Service

This repo contains a notification delivery service that accepts domain events via an HTTP API, persists them, and dispatches per-channel deliveries asynchronously via Kafka.

Overview
- Accepts events via `POST /api/v1/notifications/events` and persists them in Postgres.
- Publishes an event message to Kafka (`notifications.high` or `notifications.low`) based on priority.
- Kafka consumers create `NotificationDelivery` rows and attempt sends using provider implementations.

Quick start (development)
1. Start services:
   ```bash
   docker-compose up -d
   ```
2. Run migrations (inside container):
   ```bash
   docker-compose run --rm api npm run migrate
   ```
3. Clear idempotency keys (optional):
   ```bash
   docker-compose exec redis sh -c "redis-cli --scan --pattern 'idempotency:*' | xargs -r redis-cli del || echo 'no keys'"
   ```
4. Start retry poller (detached):
   ```bash
   docker-compose run -d --rm api npm run retry-poller
   ```
5. Post a test event:
   ```bash
   curl -X POST http://localhost:3000/api/v1/notifications/events \
     -H 'Content-Type: application/json' \
     -d '{"eventId":"<uuid>","type":"order.created","priority":"HIGH","payload":{"orderId":123}}'
   ```
6. Tail worker logs:
   ```bash
   docker-compose logs --tail=200 -f worker
   ```

Files of interest
- `src/api/routes/notifications.js` — API that accepts events and publishes to Kafka.
- `src/workers/BaseWorker.js` — Delivery processing logic.
- `src/workers/kafkaConsumer.js` — Kafka consumer worker.
- `src/workers/retryPoller.js` — DB-based retry poller.
- `src/core/circuitBreaker.js` — Redis-backed circuit breaker.

Design notes and tradeoffs
- Postgres is the source-of-truth for notifications/deliveries — simple and queryable.
- Kafka provides scalable work queues and DLQ support.
- Redis is used for idempotency and circuit state.

Next steps
- Add integration tests, metrics, and production provider integrations.

See also: `instruction` for more detailed notes.
# Blunav Notification Service (scaffold)

This repository contains the scaffold for the notification service described in `blueprint.md`.

Phase 1 implemented: basic project scaffolding, configuration, logger, simple API endpoint.

Start locally (after `npm install`):

```bash
npm install
npm start
```

API:
- `GET /health` — health check
- `POST /api/v1/notifications/events` — accept an event (returns 202)

Next: implement DB, Kafka, workers, providers, retry poller, and tests.
