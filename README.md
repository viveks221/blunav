# Notification Service

This repository is a candidate implementation for the task in `task.md`: a high-throughput notification engine (Express + KafkaJS + Sequelize + Redis) that accepts events via HTTP, enforces idempotency, writes a transactional outbox row, publishes to Kafka, and processes deliveries via pluggable providers with per-channel state, retries, and circuit breakers.

This README is intentionally aligned to the `task.md` deliverables: HLD system diagram, architecture trade-offs, LLD notes (interface + priority queue), state tracking schema summary, and local run + curl example.

## 1 — High-Level Design (HLD)

### System Diagram (handles ~50,000 requests/min)
```mermaid
flowchart LR
  subgraph Clients
    C[Upstream services]
  end
  C -->|POST /api/v1/notifications/events| API[API (Express)]
  API -->|idempotency (Redis) + validate| DB[(Postgres)]
  DB -->|insert| Outbox[(notification_outbox)]
  Outbox -->|poll & claim| Publisher[Outbox Publisher]
  Publisher -->|produce| Kafka[(Kafka)]
  Kafka -->|consume| WorkerHigh[Worker (high-priority)]
  Kafka -->|consume| WorkerLow[Worker (low-priority)]
  WorkerHigh -->|create/update| DB
  WorkerLow -->|create/update| DB
  WorkerHigh -->|provider.send| Provider[Provider (EMAIL/SMS/PUSH)]
```

### HLD Notes
- Scale target: ~50k requests/min (~833 req/sec). Design choices favor horizontal scaling (multiple API replicas, multiple worker replicas per topic) and partitioned work (separate topics for priority).
- Durability: `notification_outbox` in Postgres is the canonical record; Kafka is used for replay and fan-out.
- Idempotency: `idempotency:accept:<eventId>` keys live in Redis (SET NX + TTL) to prevent duplicate acceptance.

## 2 — Architecture Design Trade-offs (concise)

- Kafka + Outbox vs Redis queues:
  - Chosen: Kafka + transactional outbox for durability and replay.
  - Trade-off: More infra (Zookeeper/Kafka) and operational complexity, but prevents message loss and enables reprocessing.

- Redis idempotency vs DB-only dedupe:
  - Chosen: Redis SET NX for low-latency prevention during acceptance.
  - Trade-off: Requires Redis availability; keys expire to avoid permanent locks.

- Priority implementation:
  - Chosen: Two topics `notifications.high` and `notifications.low` with dedicated consumer groups.
  - Trade-off: Operationally simpler and ensures priority isolation at the broker level.

- Retry model & circuit breaker:
  - Per-delivery state tracked in DB, with `retryPolicy` computing delays and a `retryPoller` for reclaiming/reauthoring queued deliveries.
  - Circuit breaker state stored in Redis so multiple worker replicas share breaker state.

## 3 — LLD: Interfaces, Priority, Partial-failure handling

- NotificationProviders interface (implemented by mocks in `src/providers`):

```js
// Provider must implement:
class Provider {
  // channel = 'EMAIL'|'SMS'|'PUSH'
  async send({ to, payload, metadata }) { /* returns { success: boolean, code, result } */ }
}
```

- Priority queue mechanism:
  - On acceptance, API inserts `notification_outbox` with `priority` and `topic` computed by `src/queue/priorityRouting.js`.
  - `outbox-publisher` claims rows and produces to the appropriate topic (`notifications.high` | `notifications.low`).
  - Worker replicas subscribe to one or both topics depending on configuration (`worker-high` / `worker-low` / `worker`).

- Partial-failure strategy:
  - Each channel (EMAIL, SMS, PUSH) is a separate `NotificationDelivery` row and has independent retries.
  - Worker marks delivery states: `QUEUED` -> `SENDING` -> `SENT` / `RETRYING` / `FAILED`.
  - `retryPolicy.calculateRetry` determines backoff; `retryPoller` re-enqueues retries as Kafka `DELIVERY_RETRY` messages.
  - On terminal error (e.g., invalid phone), mark `FAILED` immediately; do not retry.

## 4 — State tracking schema (summary)

- `notification_outbox` (model: `src/models/NotificationOutbox.js`)
  - `id, event_id, topic, payload, status (pending|processing|published|failed), publish_attempts, last_error, created_at`

- `notifications` (model: `src/models/Notification.js`)
  - `id (eventId), source_service, event_type, payload, created_at`

- `notification_deliveries` (model: `src/models/NotificationDelivery.js`)
  - `id, notification_id, channel, status, attempts, next_retry_at, last_error, provider_response`

These tables are the canonical source for state and observability.

## 5 — Run the stack locally (docker-compose + example)

Start the stack (Postgres, Redis, Kafka, API, publisher, workers):

```bash
docker-compose up -d
# Run DB migrations
docker-compose exec api npm run migrate
# Start services (if not already started by compose)
docker-compose up -d api outbox-publisher worker worker-high worker-low
```

Note on migrations: the API now runs database migrations automatically at startup. After the service connects to the database it will invoke the migration runner; if migrations fail the API will exit (fail-fast) so the problem can be addressed. You can still run migrations manually with:

```bash
docker-compose exec api npm run migrate
```

If you re-run tests or resend events during development, clear Redis idempotency keys to accept the same `eventId` again.

Example: send a notification event to the API (cURL)

```bash
curl -i -X POST http://localhost:3000/api/v1/notifications/events \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: my-test-key-123" \
  -d '{
    "eventId": "",
    "eventType": "order.confirmed",
    "sourceService": "order-svc",
    "payload": { "orderId": "abc-123", "email": "user@example.com", "phone": "+15555551234" }
  }'
```

Notes:
- If `eventId` is empty, the service will generate one (or derive from the idempotency key as configured).
- To re-test the same eventId, clear Redis idempotency keys or use a new `Idempotency-Key`.
