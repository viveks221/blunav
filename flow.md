# Request -> Outbox -> Kafka -> Consumer Flow

Overview
- A client POSTs an event to the API endpoint `/api/v1/notifications/events`.
- The API performs validation and idempotency checks (Redis). If accepted it inserts an outbox row in Postgres.
- An outbox publisher polls the outbox table, claims pending rows, and produces messages to Kafka.
- Kafka consumers (workers) subscribe to topics, consume messages, create Notification + NotificationDelivery rows, call providers, and update delivery state.

Detailed step-by-step
1. Client request
   - POST /api/v1/notifications/events with JSON body and header `idempotency-key`.
   - Example curl:
     curl -X POST http://localhost:3000/api/v1/notifications/events \
       -H "Content-Type: application/json" \
       -H "idempotency-key: your-key" \
       -d '{"type":"welcome","priority":"HIGH","payload":{"channels":["EMAIL"],"email":"user@example.com","message":"hi"}}'

2. API validation + idempotency
   - Validation done in [src/api/routes/notifications.js](src/api/routes/notifications.js).
   - Idempotency reserve/release uses Redis via [src/core/idempotency.js](src/core/idempotency.js).
   - If accepted, API inserts an outbox row into the `notification_outbox` table (see [src/models/NotificationOutbox.js](src/models/NotificationOutbox.js)).

3. Outbox publisher
   - Polls `notification_outbox` for `pending` or retryable `failed` rows, marks them `processing`, and then produces to Kafka.
   - Logic is in [src/workers/outboxPublisher.js](src/workers/outboxPublisher.js).
   - Producer behavior and logs live in [src/queue/producer.js](src/queue/producer.js).
   - Successful publish updates outbox row to `published`.

4. Kafka
   - Topic routing uses priority -> topic mapping in [src/queue/priorityRouting.js].
   - Kafka acknowledges produced messages; offsets are logged by the publisher.

5. Consumer (worker)
   - Consumers run [src/workers/kafkaConsumer.js] and dispatch to `BaseWorker` ([src/workers/BaseWorker.js]).
   - Worker creates or updates `notifications` and `notification_deliveries`, claims deliveries, sends via provider, and updates delivery status.
   - Providers live under `src/providers/` (e.g., MockEmailProvider).
   - Retry/backoff, DLQ and circuit-breaker behavior are implemented in `BaseWorker` and core modules.

Where to inspect runtime state and logs
- API logs: `docker-compose logs -f api`
- Outbox publisher logs: `docker-compose logs -f outbox-publisher`
- Worker logs: `docker-compose logs -f worker` (and `worker-high`, `worker-low`)
- Postgres (inspect rows):
  - Outbox: `SELECT * FROM notification_outbox ORDER BY created_at DESC LIMIT 20;`
  - Notifications: `SELECT * FROM notifications WHERE id = '<eventId>';`
  - Deliveries: `SELECT * FROM notification_deliveries WHERE notification_id = '<eventId>';`

Important behaviours to note
- Idempotency: `idempotency-key` prevents duplicate processing by reserving an acceptance key in Redis.
- Outbox semantics: insert-first (DB is source of truth) → publisher guarantees at-least-once to Kafka; rows track publish attempts and backoff.
- Consumers should be idempotent: `findOrCreate` guards and state transitions avoid double-sends.
- DLQ: when deliveries exceed attempts they are published to a DLQ topic for manual inspection.

Helpful files
- API route: [src/api/routes/notifications.js](src/api/routes/notifications.js)
- Outbox publisher: [src/workers/outboxPublisher.js](src/workers/outboxPublisher.js)
- Kafka producer: [src/queue/producer.js](src/queue/producer.js)
- Kafka consumer: [src/workers/kafkaConsumer.js](src/workers/kafkaConsumer.js)
- Delivery processing: [src/workers/BaseWorker.js](src/workers/BaseWorker.js)

If you want, I can add a small timeline generator (SQL query + pretty print) that, given an `eventId`, prints the outbox + notification + delivery timeline. Want that?

Core modules — purpose and where they fit in the flow

- `idempotency` (`src/core/idempotency.js`)
   - Purpose: prevent duplicate acceptance of logically identical requests by reserving an acceptance key in Redis.
   - Where used: API route before inserting the outbox row; if reserve fails the API rejects or returns retryable error.

- `eventIdentity` (`src/core/eventIdentity.js`)
   - Purpose: derive a stable `eventId` from an `idempotency-key` header (or generate a new UUID when none provided).
   - Where used: API assigns the `eventId` used as the deduplication key and stored in the outbox.

- `stateMachine` (`src/core/stateMachine.js`)
   - Purpose: define allowed status transitions for `NotificationDelivery` (e.g., `PENDING` → `SENDING` → `SENT` / `RETRYING` / `FAILED`).
   - Where used: `BaseWorker` uses `transition()` to move deliveries through lifecycle steps safely and detect races.

- `retryPolicy` (`src/core/retryPolicy.js`)
   - Purpose: classify errors as terminal vs retryable (`isTerminalError`) and compute exponential backoff for retries (`nextRetryDelay`).
   - Where used: `BaseWorker` to decide whether to mark delivery `FAILED`, schedule `RETRYING` with `next_retry_at`, or send to DLQ.

- `circuitBreaker` (`src/core/circuitBreaker.js`)
   - Purpose: fast-fail calls to an unhealthy provider after repeated failures to avoid hammering it; exposes `isOpen`, `recordSuccess`, `recordFailure` and open-duration.
   - Where used: `BaseWorker` checks circuit status before attempting sends and marks deliveries `RETRYING` when circuit is open.

Notes on observability
- To see idempotency behavior: inspect Redis keys used by `idempotency` (look for keys with prefix `idempotency:accept:`).
- To trace `eventId` derivation: API logs include the `idempotency-key` header and the assigned `eventId` (see `src/api/routes/notifications.js`).
- To follow delivery lifecycle: check `notification_deliveries` table and worker logs (we added `Delivery created/ensured`, `Dispatching delivery`, `Provider send success/fail` logs in `BaseWorker`).

Commands to inspect each system quickly
- Redis keys: `docker-compose exec redis redis-cli keys "idempotency:*"`
- Outbox rows: `docker-compose exec postgres psql -U postgres -d notifications_db -c "SELECT * FROM notification_outbox ORDER BY created_at DESC LIMIT 20;"`
- Deliveries: `docker-compose exec postgres psql -U postgres -d notifications_db -c "SELECT * FROM notification_deliveries WHERE notification_id = '<eventId>'"`
- Worker logs (watch while issuing requests): `docker-compose logs -f worker worker-high worker-low`

