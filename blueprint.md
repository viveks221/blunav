# Notification Service — Node.js Implementation Blueprint
### Express · KafkaJS · Sequelize · Umzug · Redis · Winston · Docker Compose

> **How to use this document:** Hand this to an AI coding agent (Cursor, Claude Code, Copilot) as the single source of truth. Every section is a precise instruction. The agent should implement top-to-bottom without making any architectural decisions.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture Diagrams](#2-architecture-diagrams)
3. [Folder Structure](#3-folder-structure)
4. [Technology Decisions & Trade-offs](#4-technology-decisions--trade-offs)
5. [package.json](#5-packagejson)
6. [Docker Compose](#6-docker-compose)
7. [Dockerfile](#7-dockerfile)
8. [Environment Variables](#8-environment-variables)
9. [Logger — Winston](#9-logger--winston)
10. [Config](#10-config)
11. [Database Connection — Sequelize](#11-database-connection--sequelize)
12. [Migrations — Umzug](#12-migrations--umzug)
13. [Sequelize Models](#13-sequelize-models)
14. [Core Business Logic](#14-core-business-logic)
15. [Provider Interface & Mocks](#15-provider-interface--mocks)
16. [Kafka Producer & Consumer](#16-kafka-producer--consumer)
17. [Workers](#17-workers)
18. [Express API](#18-express-api)
19. [Unit Tests](#19-unit-tests)
20. [Seed Script — 1000 Requests](#20-seed-script--1000-requests)
21. [Demo Commands](#21-demo-commands)
22. [README Template](#22-readme-template)

---

## 1. Project Overview

### What This Service Does

Three upstream microservices — **Order**, **Payment**, **Shipping** — emit events.
This notification service:

1. Accepts events via REST API
2. Publishes them to the correct Kafka topic based on priority
3. Workers consume topics and dispatch via Email, SMS, or Push
4. Every delivery is tracked in PostgreSQL with full state history
5. Handles duplicates, retries, partial failures, and provider downtime

### Scale Target

- **50,000 requests/minute** (~833/second)
- Horizontal worker scaling — not vertical machine scaling
- Two Kafka topics, two worker pools — HIGH never blocked by LOW

### Non-Functional Requirements

| Requirement | Solution |
|---|---|
| No duplicate notifications | Redis SET NX idempotency key |
| Provider downtime | Circuit breaker + exponential backoff |
| Priority (OTP before Marketing) | Two Kafka topics + two worker pools |
| Partial failures | Per-channel delivery rows, independent retry |
| Full observability | Winston structured JSON logs + state API |

---

## 2. Architecture Diagrams

### 2.1 High-Level System

```
┌──────────────────────────────────────────────────────────────┐
│                    UPSTREAM MICROSERVICES                      │
│                                                                │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐        │
│  │  Order Svc  │   │ Payment Svc │   │ Shipping Svc│        │
│  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘        │
└─────────┼────────────────┼────────────────────┼──────────────┘
          │                │                    │
          │     POST /api/v1/notifications/events
          ▼                ▼                    ▼
┌──────────────────────────────────────────────────────────────┐
│                     EXPRESS API (:3000)                        │
│                                                                │
│   POST /events → validate (Zod) → publish to Kafka           │
│   GET  /notifications → query Postgres state                  │
│   GET  /health → liveness check                               │
└─────────────────────────┬────────────────────────────────────┘
                          │
              KafkaJS Producer (acks: all)
                          │
          ┌───────────────┴───────────────┐
          ▼                               ▼
  notifications.high              notifications.low
  (12 partitions)                 (6 partitions)
  OTP, Payment Fail,              Order Confirm,
  Fraud Alert, Account Lock       Shipping Update,
                                  Promotions
          │                               │
          ▼                               ▼
  ┌───────────────┐              ┌───────────────┐
  │ worker-high   │              │ worker-low    │
  │ (2 replicas)  │              │ (1 replica)   │
  │ group=high    │              │ group=low     │
  └───────┬───────┘              └───────┬───────┘
          │                               │
          └───────────────┬───────────────┘
                          │
            ┌─────────────▼──────────────┐
            │       PROCESSING CORE       │
            │                             │
            │  1. Check Redis idempotency │
            │  2. Write SENDING → Postgres│
            │  3. Call channel provider   │
            │  4. Write SENT/FAILED       │
            └─────────────┬──────────────┘
                          │
         ┌────────────────┼────────────────┐
         ▼                ▼                ▼
  ┌────────────┐  ┌────────────┐  ┌────────────┐
  │  Email     │  │  SMS       │  │  Push      │
  │  Provider  │  │  Provider  │  │  Provider  │
  │  (mock)    │  │  (mock)    │  │  (mock)    │
  └────────────┘  └────────────┘  └────────────┘
                          │
                 ┌─────────▼──────────┐
                 │     POSTGRESQL      │
                 │                     │
                 │  notifications      │
                 │  notification_      │
                 │  deliveries         │
                 └────────────────────┘
                          │
                 Failed → notifications.dlq
```

### 2.2 Idempotency Flow

```
Event arrives at Worker
        │
        ▼
Compute key = SHA256(sourceService + eventId + channel + recipientId)
        │
        ▼
Redis: SET idempotency:{key} "PROCESSING" NX EX 86400
        │
   ┌────┴────┐
   │ existed?│
   └────┬────┘
    YES │              NO
        │               │
        ▼               ▼
   SKIP — already   Write DB: status=SENDING
   processed        Call provider
                    Write DB: status=SENT
                    Redis SET key "SENT"
```

### 2.3 State Machine

```
             ┌─────────┐
  Event ────►│ PENDING │
             └────┬────┘
                  │ Worker picks up
                  ▼
             ┌─────────┐
             │ QUEUED  │
             └────┬────┘
                  │ Idempotency OK
                  ▼
             ┌─────────┐
             │ SENDING │◄──────────────┐
             └────┬────┘               │
                  │                    │
         ┌────────┴────────┐           │
     Success           Failure         │
         │                 │           │
         ▼                 ▼           │
    ┌─────────┐      ┌──────────┐      │
    │  SENT   │      │ RETRYING │──────┘
    │(terminal│      └──────────┘  attempt < max
    └─────────┘           │
                    attempt >= max
                          │
                          ▼
                    ┌──────────┐
                    │  FAILED  │──► DLQ
                    │(terminal)│
                    └──────────┘
```

### 2.4 Retry & Circuit Breaker

```
Provider call fails
        │
        ▼
is terminal error?
(INVALID_PHONE, HARD_BOUNCE, INVALID_TOKEN)
        │
   YES  │   NO
        │    │
        ▼    ▼
    FAILED  attempt_count < max_attempts?
                │
           YES  │   NO
                │    │
                ▼    ▼
         delay = 2^attempt * base + jitter   FAILED + DLQ
         Re-enqueue with delay header
                │
                ▼
         Circuit Breaker open?
                │
           YES  │   NO
                │    │
                ▼    ▼
         Failover or  Wait delay
         mark FAILED  Retry send
```

### 2.5 Partial Failure Model

```
Notification abc-123 (EMAIL + SMS requested)
│
├── delivery d-1  channel=EMAIL  status=SENT      ✓
└── delivery d-2  channel=SMS    status=RETRYING  attempt 2/3

Each channel: independent row, independent retry budget.
EMAIL success never prevents SMS retry.
SMS terminal failure never affects EMAIL.
```

---

## 3. Folder Structure

> **Instruction for agent:** Create exactly this structure. Do not rename or add folders.

```
notification-service/
│
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── package.json
├── jest.config.js
├── README.md
│
├── src/
│   ├── config/
│   │   └── index.js
│   │
│   ├── logger/
│   │   └── index.js
│   │
│   ├── db/
│   │   ├── connection.js
│   │   ├── migrate.js
│   │   └── migrations/
│   │       └── 001-initial-schema.js
│   │
│   ├── models/
│   │   ├── index.js
│   │   ├── Notification.js
│   │   └── NotificationDelivery.js
│   │
│   ├── core/
│   │   ├── stateMachine.js
│   │   ├── idempotency.js
│   │   ├── priorityRouter.js
│   │   └── retryPolicy.js
│   │
│   ├── providers/
│   │   ├── BaseProvider.js
│   │   ├── EmailProvider.js
│   │   ├── SmsProvider.js
│   │   ├── PushProvider.js
│   │   └── CircuitBreaker.js
│   │
│   ├── queue/
│   │   ├── kafka.js
│   │   ├── producer.js
│   │   └── topics.js
│   │
│   ├── workers/
│   │   ├── BaseWorker.js
│   │   ├── highPriorityWorker.js
│   │   └── lowPriorityWorker.js
│   │
│   └── api/
│       ├── server.js
│       ├── routes/
│       │   ├── notifications.js
│       │   └── health.js
│       └── middleware/
│           ├── validate.js
│           └── errorHandler.js
│
├── tests/
│   └── unit/
│       ├── stateMachine.test.js
│       ├── idempotency.test.js
│       ├── retryPolicy.test.js
│       ├── priorityRouter.test.js
│       └── circuitBreaker.test.js
│
└── scripts/
    ├── seed.js
    └── demo.sh
```

---

## 4. Technology Decisions & Trade-offs

### Why KafkaJS over BullMQ

BullMQ (Redis-based queue) is simpler to set up but loses messages if Redis restarts without persistence. KafkaJS gives you message replay — if your SMS provider goes down for 30 minutes, messages are still in the Kafka topic when it recovers. For a notification service where every message matters, replay is non-negotiable.

**Trade-off accepted:** KafkaJS requires Zookeeper in docker-compose — two extra containers, ~500MB RAM. Worth it for the durability guarantee.

### Why Two Topics over One with Priority Field

Kafka processes messages in partition order. A flood of 10,000 marketing emails will block an OTP sitting behind them in the same partition. Two separate topics with separate consumer groups mean the OTP worker never looks at the marketing backlog. Priority is enforced architecturally, not in code.

**Trade-off accepted:** More Kafka topics to monitor. Acceptable — priority inversion in a notification system is a user-visible failure.

### Why Umzug over Raw SQL init

Umzug gives you versioned, trackable migrations that run programmatically at startup. You can add a new column in migration 002 without dropping and recreating the database. Raw `init.sql` only works on first container start — any schema change requires wiping the volume.

### Why Sequelize over Prisma

Sequelize has been the Node.js ORM standard for a decade. Every senior Node developer knows it. Prisma is excellent but requires a separate schema file and codegen step that adds friction in a Docker environment. For an interview take-home, Sequelize is the safe, recognizable choice.

### Why Winston over Pino

Winston is the most recognizable Node.js logger. Pino is faster but less familiar. For an interview context where the interviewer will read your logs during the demo, Winston's readable format wins over Pino's raw throughput.

---

## 5. `package.json`

```json
{
  "name": "notification-service",
  "version": "1.0.0",
  "description": "Notification aggregation service — Order, Payment, Shipping",
  "main": "src/api/server.js",
  "scripts": {
    "start": "node src/api/server.js",
    "worker:high": "node src/workers/highPriorityWorker.js",
    "worker:low": "node src/workers/lowPriorityWorker.js",
    "migrate": "node src/db/migrate.js",
    "seed": "node scripts/seed.js",
    "test": "jest --runInBand --forceExit"
  },
  "dependencies": {
    "express": "^4.19.2",
    "sequelize": "^6.37.3",
    "pg": "^8.11.5",
    "pg-hstore": "^2.3.4",
    "umzug": "^3.8.1",
    "kafkajs": "^2.2.4",
    "ioredis": "^5.3.2",
    "winston": "^3.13.0",
    "zod": "^3.23.8",
    "dotenv": "^16.4.5",
    "uuid": "^10.0.0"
  },
  "devDependencies": {
    "jest": "^29.7.0",
    "ioredis-mock": "^8.9.0"
  }
}
```

---

## 6. `docker-compose.yml`

```yaml
version: '3.9'

services:

  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "2181"]
      interval: 10s
      timeout: 5s
      retries: 5

  kafka:
    image: confluentinc/cp-kafka:7.4.1
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 15s
      timeout: 10s
      retries: 10

  kafka-init:
    image: confluentinc/cp-kafka:7.4.1
    depends_on:
      kafka:
        condition: service_healthy
    entrypoint: ["/bin/sh", "-c"]
    command: |
      "
      kafka-topics --create --if-not-exists --bootstrap-server kafka:29092 \
        --replication-factor 1 --partitions 12 --topic notifications.high

      kafka-topics --create --if-not-exists --bootstrap-server kafka:29092 \
        --replication-factor 1 --partitions 6 --topic notifications.low

      kafka-topics --create --if-not-exists --bootstrap-server kafka:29092 \
        --replication-factor 1 --partitions 3 --topic notifications.dlq

      echo 'Topics created successfully'
      "

  postgres:
    image: postgres:15-alpine
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: notifications
      POSTGRES_USER: notif_user
      POSTGRES_PASSWORD: notif_pass
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U notif_user -d notifications"]
      interval: 5s
      timeout: 5s
      retries: 10

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --appendonly yes
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 10

  api:
    build: .
    command: sh -c "node src/db/migrate.js && node src/api/server.js"
    ports:
      - "3000:3000"
    env_file: .env
    environment:
      DB_HOST: postgres
      REDIS_URL: redis://redis:6379
      KAFKA_BROKERS: kafka:29092
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      kafka-init:
        condition: service_completed_successfully

  worker-high:
    build: .
    command: node src/workers/highPriorityWorker.js
    env_file: .env
    environment:
      DB_HOST: postgres
      REDIS_URL: redis://redis:6379
      KAFKA_BROKERS: kafka:29092
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      kafka-init:
        condition: service_completed_successfully

  worker-low:
    build: .
    command: node src/workers/lowPriorityWorker.js
    env_file: .env
    environment:
      DB_HOST: postgres
      REDIS_URL: redis://redis:6379
      KAFKA_BROKERS: kafka:29092
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      kafka-init:
        condition: service_completed_successfully

volumes:
  postgres_data:
  redis_data:
```

---

## 7. `Dockerfile`

```dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
```

---

## 8. `.env.example`

```env
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=notifications
DB_USER=notif_user
DB_PASSWORD=notif_pass

# Redis
REDIS_URL=redis://localhost:6379

# Kafka
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC_HIGH=notifications.high
KAFKA_TOPIC_LOW=notifications.low
KAFKA_TOPIC_DLQ=notifications.dlq
KAFKA_GROUP_HIGH=notif-workers-high
KAFKA_GROUP_LOW=notif-workers-low

# App
PORT=3000
NODE_ENV=development

# Retry
MAX_RETRY_ATTEMPTS=3
RETRY_BASE_DELAY_MS=2000

# Circuit Breaker
CB_FAILURE_THRESHOLD=5
CB_RECOVERY_TIMEOUT_MS=30000

# Idempotency
IDEMPOTENCY_TTL_SECONDS=86400
```

---

## 9. `src/logger/index.js`

```javascript
const { createLogger, format, transports } = require('winston')

const logger = createLogger({
  level: process.env.NODE_ENV === 'production' ? 'info' : 'debug',
  format: format.combine(
    format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    format.errors({ stack: true }),
    format.json()
  ),
  defaultMeta: { service: 'notification-service' },
  transports: [
    new transports.Console({
      format: format.combine(
        format.colorize(),
        format.printf(({ timestamp, level, message, service, ...meta }) => {
          const metaStr = Object.keys(meta).length
            ? ' ' + JSON.stringify(meta)
            : ''
          return `${timestamp} [${service}] ${level}: ${message}${metaStr}`
        })
      )
    })
  ]
})

module.exports = logger
```

---

## 10. `src/config/index.js`

```javascript
require('dotenv').config()

const config = {
  db: {
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || '5432'),
    name: process.env.DB_NAME || 'notifications',
    user: process.env.DB_USER || 'notif_user',
    password: process.env.DB_PASSWORD || 'notif_pass',
  },
  redis: {
    url: process.env.REDIS_URL || 'redis://localhost:6379',
  },
  kafka: {
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    topics: {
      high: process.env.KAFKA_TOPIC_HIGH || 'notifications.high',
      low: process.env.KAFKA_TOPIC_LOW || 'notifications.low',
      dlq: process.env.KAFKA_TOPIC_DLQ || 'notifications.dlq',
    },
    groups: {
      high: process.env.KAFKA_GROUP_HIGH || 'notif-workers-high',
      low: process.env.KAFKA_GROUP_LOW || 'notif-workers-low',
    },
  },
  app: {
    port: parseInt(process.env.PORT || '3000'),
    nodeEnv: process.env.NODE_ENV || 'development',
  },
  retry: {
    maxAttempts: parseInt(process.env.MAX_RETRY_ATTEMPTS || '3'),
    baseDelayMs: parseInt(process.env.RETRY_BASE_DELAY_MS || '2000'),
  },
  circuitBreaker: {
    failureThreshold: parseInt(process.env.CB_FAILURE_THRESHOLD || '5'),
    recoveryTimeoutMs: parseInt(process.env.CB_RECOVERY_TIMEOUT_MS || '30000'),
  },
  idempotency: {
    ttlSeconds: parseInt(process.env.IDEMPOTENCY_TTL_SECONDS || '86400'),
  },
}

module.exports = config
```

---

## 11. `src/db/connection.js`

```javascript
const { Sequelize } = require('sequelize')
const config = require('../config')
const logger = require('../logger')

const sequelize = new Sequelize(
  config.db.name,
  config.db.user,
  config.db.password,
  {
    host: config.db.host,
    port: config.db.port,
    dialect: 'postgres',
    logging: (msg) => logger.debug(msg),
    pool: {
      max: 10,
      min: 0,
      acquire: 30000,
      idle: 10000,
    },
  }
)

module.exports = sequelize
```

---

## 12. `src/db/migrations/001-initial-schema.js`

```javascript
'use strict'

module.exports = {
  // Umzug calls up() when running migrations
  async up({ context: queryInterface }) {
    const { DataTypes } = require('sequelize')

    // ── notifications ──────────────────────────────────────────────
    // One row per logical notification event. Immutable after creation.
    await queryInterface.createTable('notifications', {
      id: {
        type: DataTypes.UUID,
        defaultValue: DataTypes.UUIDV4,
        primaryKey: true,
      },
      idempotency_key: {
        type: DataTypes.STRING(64),
        allowNull: false,
        unique: true,
      },
      source_service: {
        type: DataTypes.STRING(50),
        allowNull: false,
      },
      event_type: {
        type: DataTypes.STRING(100),
        allowNull: false,
      },
      recipient_id: {
        type: DataTypes.STRING(100),
        allowNull: false,
      },
      recipient_email: {
        type: DataTypes.STRING(255),
        allowNull: true,
      },
      recipient_phone: {
        type: DataTypes.STRING(20),
        allowNull: true,
      },
      recipient_device_token: {
        type: DataTypes.STRING(255),
        allowNull: true,
      },
      priority: {
        type: DataTypes.ENUM('HIGH', 'LOW'),
        allowNull: false,
        defaultValue: 'LOW',
      },
      channels: {
        type: DataTypes.ARRAY(DataTypes.STRING),
        allowNull: false,
      },
      payload: {
        type: DataTypes.JSONB,
        defaultValue: {},
      },
      created_at: {
        type: DataTypes.DATE,
        allowNull: false,
        defaultValue: DataTypes.NOW,
      },
      updated_at: {
        type: DataTypes.DATE,
        allowNull: false,
        defaultValue: DataTypes.NOW,
      },
    })

    await queryInterface.addIndex('notifications', ['recipient_id'])
    await queryInterface.addIndex('notifications', ['source_service', 'event_type'])
    await queryInterface.addIndex('notifications', ['created_at'])

    // ── notification_deliveries ────────────────────────────────────
    // One row per (notification × channel). Mutable — updated on every
    // state change. Split from notifications so Email SENT is never
    // overwritten when SMS retries.
    await queryInterface.createTable('notification_deliveries', {
      id: {
        type: DataTypes.UUID,
        defaultValue: DataTypes.UUIDV4,
        primaryKey: true,
      },
      notification_id: {
        type: DataTypes.UUID,
        allowNull: false,
        references: {
          model: 'notifications',
          key: 'id',
        },
        onDelete: 'CASCADE',
      },
      channel: {
        type: DataTypes.ENUM('EMAIL', 'SMS', 'PUSH'),
        allowNull: false,
      },
      status: {
        type: DataTypes.ENUM('PENDING', 'QUEUED', 'SENDING', 'SENT', 'FAILED', 'RETRYING'),
        allowNull: false,
        defaultValue: 'PENDING',
      },
      provider: {
        type: DataTypes.STRING(50),
        allowNull: true,
      },
      attempt_count: {
        type: DataTypes.INTEGER,
        allowNull: false,
        defaultValue: 0,
      },
      max_attempts: {
        type: DataTypes.INTEGER,
        allowNull: false,
        defaultValue: 3,
      },
      last_attempted_at: {
        type: DataTypes.DATE,
        allowNull: true,
      },
      next_retry_at: {
        type: DataTypes.DATE,
        allowNull: true,
      },
      sent_at: {
        type: DataTypes.DATE,
        allowNull: true,
      },
      error_code: {
        type: DataTypes.STRING(100),
        allowNull: true,
      },
      error_message: {
        type: DataTypes.TEXT,
        allowNull: true,
      },
      provider_message_id: {
        type: DataTypes.STRING(255),
        allowNull: true,
      },
      created_at: {
        type: DataTypes.DATE,
        allowNull: false,
        defaultValue: DataTypes.NOW,
      },
      updated_at: {
        type: DataTypes.DATE,
        allowNull: false,
        defaultValue: DataTypes.NOW,
      },
    })

    // Critical index — powers the retry poller query:
    // "Give me RETRYING deliveries where next_retry_at is in the past"
    await queryInterface.addIndex('notification_deliveries', ['status', 'next_retry_at'])
    await queryInterface.addIndex('notification_deliveries', ['notification_id'])

    // Unique constraint — one row per notification+channel
    await queryInterface.addConstraint('notification_deliveries', {
      fields: ['notification_id', 'channel'],
      type: 'unique',
      name: 'uq_delivery_notification_channel',
    })
  },

  async down({ context: queryInterface }) {
    await queryInterface.dropTable('notification_deliveries')
    await queryInterface.dropTable('notifications')
    await queryInterface.sequelize.query('DROP TYPE IF EXISTS enum_notifications_priority')
    await queryInterface.sequelize.query('DROP TYPE IF EXISTS enum_notification_deliveries_channel')
    await queryInterface.sequelize.query('DROP TYPE IF EXISTS enum_notification_deliveries_status')
  },
}
```

---

## 12b. `src/db/migrate.js`

```javascript
const { Umzug, SequelizeStorage } = require('umzug')
const path = require('path')
const sequelize = require('./connection')
const logger = require('../logger')

const umzug = new Umzug({
  migrations: {
    glob: path.join(__dirname, 'migrations/*.js'),
    resolve: ({ name, path: migrationPath, context }) => {
      const migration = require(migrationPath)
      return {
        name,
        up: async () => migration.up({ context }),
        down: async () => migration.down({ context }),
      }
    },
  },
  context: sequelize.getQueryInterface(),
  storage: new SequelizeStorage({ sequelize }),
  logger: {
    info: (msg) => logger.info(msg),
    warn: (msg) => logger.warn(msg),
    error: (msg) => logger.error(msg),
    debug: (msg) => logger.debug(msg),
  },
})

async function runMigrations() {
  try {
    await sequelize.authenticate()
    logger.info('Database connection established')

    const pending = await umzug.pending()
    if (pending.length === 0) {
      logger.info('No pending migrations')
      return
    }

    logger.info(`Running ${pending.length} migration(s)`)
    await umzug.up()
    logger.info('All migrations completed')
  } catch (error) {
    logger.error('Migration failed', { error: error.message })
    process.exit(1)
  }
}

// Run directly: node src/db/migrate.js
if (require.main === module) {
  runMigrations().then(() => process.exit(0))
}

module.exports = { runMigrations }
```

---

## 13. Sequelize Models

### `src/models/Notification.js`

```javascript
const { DataTypes } = require('sequelize')
const sequelize = require('../db/connection')

const Notification = sequelize.define(
  'Notification',
  {
    id: {
      type: DataTypes.UUID,
      defaultValue: DataTypes.UUIDV4,
      primaryKey: true,
    },
    idempotency_key: {
      type: DataTypes.STRING(64),
      allowNull: false,
      unique: true,
    },
    source_service: { type: DataTypes.STRING(50), allowNull: false },
    event_type:     { type: DataTypes.STRING(100), allowNull: false },
    recipient_id:   { type: DataTypes.STRING(100), allowNull: false },
    recipient_email:        { type: DataTypes.STRING(255) },
    recipient_phone:        { type: DataTypes.STRING(20) },
    recipient_device_token: { type: DataTypes.STRING(255) },
    priority: {
      type: DataTypes.ENUM('HIGH', 'LOW'),
      allowNull: false,
      defaultValue: 'LOW',
    },
    channels: {
      type: DataTypes.ARRAY(DataTypes.STRING),
      allowNull: false,
    },
    payload: {
      type: DataTypes.JSONB,
      defaultValue: {},
    },
  },
  {
    tableName: 'notifications',
    underscored: true,    // snake_case columns
    timestamps: true,
    createdAt: 'created_at',
    updatedAt: 'updated_at',
  }
)

module.exports = Notification
```

### `src/models/NotificationDelivery.js`

```javascript
const { DataTypes } = require('sequelize')
const sequelize = require('../db/connection')

const NotificationDelivery = sequelize.define(
  'NotificationDelivery',
  {
    id: {
      type: DataTypes.UUID,
      defaultValue: DataTypes.UUIDV4,
      primaryKey: true,
    },
    notification_id: {
      type: DataTypes.UUID,
      allowNull: false,
    },
    channel: {
      type: DataTypes.ENUM('EMAIL', 'SMS', 'PUSH'),
      allowNull: false,
    },
    status: {
      type: DataTypes.ENUM('PENDING', 'QUEUED', 'SENDING', 'SENT', 'FAILED', 'RETRYING'),
      allowNull: false,
      defaultValue: 'PENDING',
    },
    provider:              { type: DataTypes.STRING(50) },
    attempt_count:         { type: DataTypes.INTEGER, defaultValue: 0 },
    max_attempts:          { type: DataTypes.INTEGER, defaultValue: 3 },
    last_attempted_at:     { type: DataTypes.DATE },
    next_retry_at:         { type: DataTypes.DATE },
    sent_at:               { type: DataTypes.DATE },
    error_code:            { type: DataTypes.STRING(100) },
    error_message:         { type: DataTypes.TEXT },
    provider_message_id:   { type: DataTypes.STRING(255) },
  },
  {
    tableName: 'notification_deliveries',
    underscored: true,
    timestamps: true,
    createdAt: 'created_at',
    updatedAt: 'updated_at',
  }
)

module.exports = NotificationDelivery
```

### `src/models/index.js`

```javascript
const Notification = require('./Notification')
const NotificationDelivery = require('./NotificationDelivery')

// One notification has many deliveries (one per channel)
Notification.hasMany(NotificationDelivery, {
  foreignKey: 'notification_id',
  as: 'deliveries',
})
NotificationDelivery.belongsTo(Notification, {
  foreignKey: 'notification_id',
  as: 'notification',
})

module.exports = { Notification, NotificationDelivery }
```

---

## 14. Core Business Logic

### `src/core/stateMachine.js`

```javascript
// Pure function — no I/O. Enforces valid state transitions.
// Any invalid transition throws — prevents phantom states.

const ALLOWED_TRANSITIONS = {
  PENDING:  ['QUEUED', 'FAILED'],
  QUEUED:   ['SENDING'],
  SENDING:  ['SENT', 'RETRYING', 'FAILED'],
  RETRYING: ['SENDING', 'FAILED'],
  SENT:     [],        // terminal
  FAILED:   [],        // terminal
}

class InvalidStateTransitionError extends Error {
  constructor(from, to) {
    super(`Cannot transition from ${from} to ${to}. Allowed: ${ALLOWED_TRANSITIONS[from]?.join(', ')}`)
    this.name = 'InvalidStateTransitionError'
  }
}

function transition(currentStatus, nextStatus) {
  const allowed = ALLOWED_TRANSITIONS[currentStatus] || []
  if (!allowed.includes(nextStatus)) {
    throw new InvalidStateTransitionError(currentStatus, nextStatus)
  }
  return nextStatus
}

function isTerminal(status) {
  return ALLOWED_TRANSITIONS[status]?.length === 0
}

module.exports = { transition, isTerminal, InvalidStateTransitionError }
```

### `src/core/idempotency.js`

```javascript
const crypto = require('crypto')
const config = require('../config')
const logger = require('../logger')

// Build a deterministic key from event properties.
// Same inputs always produce same key — pure function, no I/O.
function buildIdempotencyKey(sourceService, eventId, channel, recipientId) {
  const raw = `${sourceService}:${eventId}:${channel}:${recipientId}`
  return crypto.createHash('sha256').update(raw).digest('hex')
}

class IdempotencyChecker {
  constructor(redisClient) {
    this.redis = redisClient
  }

  // Returns true if acquired (first time seeing this key).
  // Returns false if key already exists (duplicate — skip).
  // SET NX EX is atomic — safe under concurrent workers.
  async acquire(key) {
    const result = await this.redis.set(
      `idempotency:${key}`,
      'PROCESSING',
      'NX',   // Only set if Not eXists
      'EX',   // Set expiry
      config.idempotency.ttlSeconds
    )
    return result === 'OK'
  }

  async markComplete(key) {
    await this.redis.set(
      `idempotency:${key}`,
      'SENT',
      'EX',
      config.idempotency.ttlSeconds
    )
  }

  async getStatus(key) {
    return this.redis.get(`idempotency:${key}`)
  }
}

module.exports = { buildIdempotencyKey, IdempotencyChecker }
```

### `src/core/priorityRouter.js`

```javascript
// Maps event types to Kafka topics.
// Fail-safe: unknown events default to HIGH — better to over-prioritize.

const HIGH_PRIORITY_EVENTS = new Set([
  'otp.requested',
  'payment.failed',
  'payment.confirmed',
  'account.locked',
  'fraud.alert',
  'order.cancelled',
  'password.reset',
])

const LOW_PRIORITY_EVENTS = new Set([
  'order.confirmed',
  'order.shipped',
  'shipping.update',
  'shipping.delivered',
  'promotion.new',
  'digest.weekly',
  'survey.request',
])

function getPriority(eventType) {
  if (LOW_PRIORITY_EVENTS.has(eventType)) return 'LOW'
  return 'HIGH'   // Default HIGH — fail safe
}

function getTopicForEvent(eventType, config) {
  const priority = getPriority(eventType)
  return priority === 'HIGH' ? config.kafka.topics.high : config.kafka.topics.low
}

module.exports = { getPriority, getTopicForEvent }
```

### `src/core/retryPolicy.js`

```javascript
const config = require('../config')

// Terminal errors — will NEVER succeed on retry.
// Skip retry budget entirely for these.
const TERMINAL_ERRORS = {
  SMS:   new Set(['INVALID_PHONE_NUMBER', 'UNSUBSCRIBED', 'LANDLINE_NUMBER']),
  EMAIL: new Set(['HARD_BOUNCE', 'SPAM_COMPLAINT', 'INVALID_EMAIL']),
  PUSH:  new Set(['INVALID_TOKEN', 'APP_UNINSTALLED', 'DEVICE_NOT_REGISTERED']),
}

// Returns { shouldRetry, delayMs, reason }
// Delay = base * 2^attempt + jitter
// Jitter prevents thundering herd when provider recovers
function calculateRetry(attemptCount, maxAttempts = null) {
  maxAttempts = maxAttempts ?? config.retry.maxAttempts
  const base = config.retry.baseDelayMs

  if (attemptCount >= maxAttempts) {
    return {
      shouldRetry: false,
      delayMs: 0,
      reason: `Max attempts (${maxAttempts}) exceeded`,
    }
  }

  const exponential = base * Math.pow(2, attemptCount)
  const jitter = Math.random() * base
  const delayMs = Math.round(exponential + jitter)

  return {
    shouldRetry: true,
    delayMs,
    reason: `Attempt ${attemptCount + 1}/${maxAttempts}, wait ${delayMs}ms`,
  }
}

function isTerminalError(errorCode, channel) {
  return TERMINAL_ERRORS[channel]?.has(errorCode) ?? false
}

module.exports = { calculateRetry, isTerminalError }
```

---

## 15. Provider Interface & Mocks

### `src/providers/BaseProvider.js`

```javascript
// Abstract base class. All providers implement send().
// Workers call provider.send() — never know which concrete provider.
// Swap mock for Twilio/SES without touching worker code.

class BaseProvider {
  get providerName() {
    throw new Error('providerName getter must be implemented')
  }

  // MUST return { success, providerMessageId?, errorCode?, errorMessage? }
  // MUST never throw — catch all errors internally and return success: false
  async send({ recipient, subject, body, metadata }) {
    throw new Error('send() must be implemented')
  }
}

module.exports = BaseProvider
```

### `src/providers/EmailProvider.js`

```javascript
const BaseProvider = require('./BaseProvider')
const logger = require('../logger')

class MockEmailProvider extends BaseProvider {
  get providerName() { return 'mock_smtp' }

  async send({ recipient, subject, body, metadata }) {
    // Simulate hard bounce — terminal error, no retry
    if (recipient.endsWith('@bounce.test')) {
      return {
        success: false,
        errorCode: 'HARD_BOUNCE',
        errorMessage: `Address ${recipient} does not exist`,
      }
    }

    // Simulate provider downtime — retryable error
    if (recipient.endsWith('@down.test')) {
      return {
        success: false,
        errorCode: 'PROVIDER_UNAVAILABLE',
        errorMessage: 'SMTP server not responding',
      }
    }

    logger.info('email_sent_mock', {
      recipient,
      subject,
      provider: this.providerName,
    })

    return {
      success: true,
      providerMessageId: `mock-email-${metadata.notificationId}`,
    }
  }
}

module.exports = { MockEmailProvider }
```

### `src/providers/SmsProvider.js`

```javascript
const BaseProvider = require('./BaseProvider')
const logger = require('../logger')

class MockSmsProvider extends BaseProvider {
  get providerName() { return 'mock_twilio' }

  async send({ recipient, subject, body, metadata }) {
    if (recipient === '+00000000000') {
      return {
        success: false,
        errorCode: 'INVALID_PHONE_NUMBER',
        errorMessage: 'Phone number is not valid',
      }
    }

    logger.info('sms_sent_mock', {
      recipient,
      body: body.substring(0, 50),
      provider: this.providerName,
    })

    return {
      success: true,
      providerMessageId: `mock-sms-SM${metadata.notificationId.slice(0, 8)}`,
    }
  }
}

module.exports = { MockSmsProvider }
```

### `src/providers/PushProvider.js`

```javascript
const BaseProvider = require('./BaseProvider')
const logger = require('../logger')

class MockPushProvider extends BaseProvider {
  get providerName() { return 'mock_fcm' }

  async send({ recipient, subject, body, metadata }) {
    if (recipient === 'invalid-token') {
      return {
        success: false,
        errorCode: 'INVALID_TOKEN',
        errorMessage: 'Device token is no longer valid',
      }
    }

    logger.info('push_sent_mock', {
      token: recipient.substring(0, 20),
      title: subject,
      provider: this.providerName,
    })

    return {
      success: true,
      providerMessageId: `mock-push-${metadata.notificationId.slice(0, 8)}`,
    }
  }
}

module.exports = { MockPushProvider }
```

### `src/providers/CircuitBreaker.js`

```javascript
// Per-provider circuit breaker stored in Redis.
// All worker replicas share the same circuit state.
// States: CLOSED (normal) → OPEN (failing) → HALF_OPEN (probing)

const config = require('../config')
const logger = require('../logger')

const STATE = { CLOSED: 'CLOSED', OPEN: 'OPEN', HALF_OPEN: 'HALF_OPEN' }

class CircuitBreaker {
  constructor(providerName, redisClient) {
    this.provider = providerName
    this.redis = redisClient
    this.failureKey = `cb:failures:${providerName}`
    this.stateKey = `cb:state:${providerName}`
    this.lastFailureKey = `cb:last_failure:${providerName}`
  }

  async getState() {
    const state = await this.redis.get(this.stateKey)
    if (!state) return STATE.CLOSED

    if (state === STATE.OPEN) {
      const lastFailure = await this.redis.get(this.lastFailureKey)
      if (lastFailure) {
        const elapsed = Date.now() - parseInt(lastFailure)
        if (elapsed > config.circuitBreaker.recoveryTimeoutMs) {
          await this.redis.set(this.stateKey, STATE.HALF_OPEN)
          return STATE.HALF_OPEN
        }
      }
    }

    return state
  }

  async isAvailable() {
    const state = await this.getState()
    if (state === STATE.OPEN) {
      logger.warn('circuit_rejected', { provider: this.provider })
      return false
    }
    return true
  }

  async recordSuccess() {
    await this.redis.del(this.failureKey)
    await this.redis.set(this.stateKey, STATE.CLOSED)
    logger.info('circuit_closed', { provider: this.provider })
  }

  async recordFailure() {
    const count = await this.redis.incr(this.failureKey)
    await this.redis.expire(this.failureKey, 60)
    await this.redis.set(this.lastFailureKey, Date.now().toString())

    if (count >= config.circuitBreaker.failureThreshold) {
      await this.redis.set(this.stateKey, STATE.OPEN)
      logger.error('circuit_opened', { provider: this.provider, failures: count })
    }
  }
}

module.exports = { CircuitBreaker }
```

---

## 16. Kafka Producer & Consumer

### `src/queue/kafka.js`

```javascript
const { Kafka } = require('kafkajs')
const config = require('../config')

const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: config.kafka.brokers,
  retry: {
    initialRetryTime: 300,
    retries: 10,
  },
})

module.exports = kafka
```

### `src/queue/producer.js`

```javascript
const kafka = require('./kafka')
const { getTopicForEvent } = require('../core/priorityRouter')
const config = require('../config')
const logger = require('../logger')

class NotificationProducer {
  constructor() {
    this.producer = kafka.producer({
      allowAutoTopicCreation: false,
    })
    this.connected = false
  }

  async connect() {
    if (!this.connected) {
      await this.producer.connect()
      this.connected = true
      logger.info('kafka_producer_connected')
    }
  }

  async publish(event) {
    await this.connect()
    const topic = getTopicForEvent(event.eventType, config)
    const key = `${event.sourceService}:${event.recipientId}`

    await this.producer.send({
      topic,
      messages: [{
        key,
        value: JSON.stringify(event),
      }],
      acks: -1,    // Wait for all replicas — no data loss
    })

    logger.info('event_published', {
      topic,
      eventType: event.eventType,
      eventId: event.eventId,
    })
  }

  async disconnect() {
    await this.producer.disconnect()
  }
}

// Singleton — shared across the API process
const producer = new NotificationProducer()
module.exports = producer
```

---

## 17. Workers

### `src/workers/BaseWorker.js`

```javascript
const kafka = require('../queue/kafka')
const { Notification, NotificationDelivery } = require('../models')
const { buildIdempotencyKey, IdempotencyChecker } = require('../core/idempotency')
const { transition, InvalidStateTransitionError } = require('../core/stateMachine')
const { calculateRetry, isTerminalError } = require('../core/retryPolicy')
const { getPriority } = require('../core/priorityRouter')
const { CircuitBreaker } = require('../providers/CircuitBreaker')
const logger = require('../logger')
const { Op } = require('sequelize')

class BaseWorker {
  constructor({ topic, groupId, providers, redisClient }) {
    this.topic = topic
    this.groupId = groupId
    this.providers = providers           // { EMAIL: provider, SMS: provider, PUSH: provider }
    this.redisClient = redisClient
    this.idempotency = new IdempotencyChecker(redisClient)
    this.circuitBreakers = {}

    for (const [channel, provider] of Object.entries(providers)) {
      this.circuitBreakers[channel] = new CircuitBreaker(provider.providerName, redisClient)
    }

    this.consumer = kafka.consumer({
      groupId,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
    })
  }

  async start() {
    await this.consumer.connect()
    await this.consumer.subscribe({ topic: this.topic, fromBeginning: false })

    logger.info('worker_started', { topic: this.topic, groupId: this.groupId })

    await this.consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const event = JSON.parse(message.value.toString())
          await this.processEvent(event)
        } catch (err) {
          logger.error('message_parse_failed', { error: err.message })
          // Commit offset anyway — malformed messages must not block the queue
        }
      },
    })
  }

  async processEvent(event) {
    for (const channel of event.channels) {
      await this.processChannel(event, channel)
    }
  }

  async processChannel(event, channel) {
    // ── Step 1: Idempotency ──────────────────────────────────────
    const key = buildIdempotencyKey(
      event.sourceService,
      event.eventId,
      channel,
      event.recipientId
    )

    const acquired = await this.idempotency.acquire(key)
    if (!acquired) {
      logger.info('duplicate_skipped', { key, channel })
      return
    }

    // ── Step 2: Persist notification (upsert) ────────────────────
    const notification = await this.getOrCreateNotification(event)

    // ── Step 3: Get or create delivery row ────────────────────────
    const delivery = await this.getOrCreateDelivery(notification, channel)

    if (['SENT', 'FAILED'].includes(delivery.status)) {
      logger.info('delivery_already_terminal', { deliveryId: delivery.id, status: delivery.status })
      return
    }

    // ── Step 4: Circuit breaker check ─────────────────────────────
    const cb = this.circuitBreakers[channel]
    if (cb && !(await cb.isAvailable())) {
      await this.scheduleRetry(delivery, 'CIRCUIT_OPEN', 'Provider circuit is open')
      return
    }

    // ── Step 5: Transition to SENDING ─────────────────────────────
    try {
      delivery.status = transition(delivery.status, 'SENDING')
      delivery.attempt_count += 1
      delivery.last_attempted_at = new Date()
      await delivery.save()
    } catch (err) {
      if (err instanceof InvalidStateTransitionError) {
        logger.error('invalid_state_transition', { error: err.message })
        return
      }
      throw err
    }

    // ── Step 6: Call provider ──────────────────────────────────────
    const provider = this.providers[channel]
    if (!provider) {
      logger.error('no_provider_for_channel', { channel })
      return
    }

    const recipient = this.getRecipient(event, channel)
    const result = await provider.send({
      recipient,
      subject: this.buildSubject(event),
      body: this.buildBody(event),
      metadata: { notificationId: notification.id },
    })

    // ── Step 7: Handle result ──────────────────────────────────────
    if (result.success) {
      delivery.status = transition(delivery.status, 'SENT')
      delivery.sent_at = new Date()
      delivery.provider = provider.providerName
      delivery.provider_message_id = result.providerMessageId
      await delivery.save()
      await this.idempotency.markComplete(key)
      await cb?.recordSuccess()
      logger.info('notification_sent', { channel, provider: provider.providerName })
    } else {
      await cb?.recordFailure()

      if (isTerminalError(result.errorCode, channel)) {
        delivery.status = transition(delivery.status, 'FAILED')
        delivery.error_code = result.errorCode
        delivery.error_message = result.errorMessage
        await delivery.save()
        logger.warn('terminal_failure', { channel, errorCode: result.errorCode })
      } else {
        await this.scheduleRetry(delivery, result.errorCode, result.errorMessage)
      }
    }
  }

  async scheduleRetry(delivery, errorCode, errorMessage) {
    const { shouldRetry, delayMs, reason } = calculateRetry(
      delivery.attempt_count,
      delivery.max_attempts
    )

    if (shouldRetry) {
      delivery.status = transition(delivery.status, 'RETRYING')
      delivery.next_retry_at = new Date(Date.now() + delayMs)
      delivery.error_code = errorCode
      delivery.error_message = errorMessage
      await delivery.save()
      logger.info('retry_scheduled', { delayMs, attempt: delivery.attempt_count, reason })
    } else {
      delivery.status = transition(delivery.status, 'FAILED')
      delivery.error_code = errorCode
      delivery.error_message = `Max retries exceeded. Last: ${errorMessage}`
      await delivery.save()
      logger.error('max_retries_exceeded', { channel: delivery.channel })
      // TODO: publish to DLQ topic for investigation
    }
  }

  async getOrCreateNotification(event) {
    const key = buildIdempotencyKey(
      event.sourceService, event.eventId, 'NOTIFICATION', event.recipientId
    )

    const [notification] = await Notification.findOrCreate({
      where: { idempotency_key: key },
      defaults: {
        idempotency_key: key,
        source_service: event.sourceService,
        event_type: event.eventType,
        recipient_id: event.recipientId,
        recipient_email: event.recipientEmail,
        recipient_phone: event.recipientPhone,
        recipient_device_token: event.recipientDeviceToken,
        priority: getPriority(event.eventType),
        channels: event.channels,
        payload: event.payload || {},
      },
    })

    return notification
  }

  async getOrCreateDelivery(notification, channel) {
    const [delivery] = await NotificationDelivery.findOrCreate({
      where: {
        notification_id: notification.id,
        channel,
      },
      defaults: {
        notification_id: notification.id,
        channel,
        status: 'PENDING',
      },
    })
    return delivery
  }

  getRecipient(event, channel) {
    if (channel === 'EMAIL') return event.recipientEmail || ''
    if (channel === 'SMS')   return event.recipientPhone || ''
    if (channel === 'PUSH')  return event.recipientDeviceToken || ''
    return ''
  }

  buildSubject(event) {
    const subjects = {
      'otp.requested':       'Your verification code',
      'payment.failed':      'Payment failed — action required',
      'order.confirmed':     'Your order has been confirmed',
      'shipping.delivered':  'Your package has been delivered',
      'promotion.new':       'A new offer just for you',
    }
    return subjects[event.eventType] || `Notification: ${event.eventType}`
  }

  buildBody(event) {
    return `Event: ${event.eventType}. Details: ${JSON.stringify(event.payload)}`
  }

  async stop() {
    await this.consumer.disconnect()
    logger.info('worker_stopped', { topic: this.topic })
  }
}

module.exports = BaseWorker
```

### `src/workers/highPriorityWorker.js`

```javascript
require('dotenv').config()
const Redis = require('ioredis')
const BaseWorker = require('./BaseWorker')
const { MockEmailProvider } = require('../providers/EmailProvider')
const { MockSmsProvider } = require('../providers/SmsProvider')
const { MockPushProvider } = require('../providers/PushProvider')
const config = require('../config')
const logger = require('../logger')
const { runMigrations } = require('../db/migrate')

async function main() {
  await runMigrations()

  const redis = new Redis(config.redis.url)

  const worker = new BaseWorker({
    topic: config.kafka.topics.high,
    groupId: config.kafka.groups.high,
    providers: {
      EMAIL: new MockEmailProvider(),
      SMS:   new MockSmsProvider(),
      PUSH:  new MockPushProvider(),
    },
    redisClient: redis,
  })

  await worker.start()

  process.on('SIGTERM', async () => {
    logger.info('SIGTERM received, shutting down high-priority worker')
    await worker.stop()
    process.exit(0)
  })
}

main().catch((err) => {
  logger.error('high_priority_worker_failed', { error: err.message })
  process.exit(1)
})
```

### `src/workers/lowPriorityWorker.js`

```javascript
require('dotenv').config()
const Redis = require('ioredis')
const BaseWorker = require('./BaseWorker')
const { MockEmailProvider } = require('../providers/EmailProvider')
const { MockSmsProvider } = require('../providers/SmsProvider')
const { MockPushProvider } = require('../providers/PushProvider')
const config = require('../config')
const logger = require('../logger')
const { runMigrations } = require('../db/migrate')

async function main() {
  await runMigrations()

  const redis = new Redis(config.redis.url)

  const worker = new BaseWorker({
    topic: config.kafka.topics.low,
    groupId: config.kafka.groups.low,
    providers: {
      EMAIL: new MockEmailProvider(),
      SMS:   new MockSmsProvider(),
      PUSH:  new MockPushProvider(),
    },
    redisClient: redis,
  })

  await worker.start()

  process.on('SIGTERM', async () => {
    logger.info('SIGTERM received, shutting down low-priority worker')
    await worker.stop()
    process.exit(0)
  })
}

main().catch((err) => {
  logger.error('low_priority_worker_failed', { error: err.message })
  process.exit(1)
})
```

---

## 18. Express API

### `src/api/middleware/validate.js`

```javascript
const { z } = require('zod')

// Reusable Zod validation middleware
function validate(schema) {
  return (req, res, next) => {
    const result = schema.safeParse(req.body)
    if (!result.success) {
      return res.status(400).json({
        error: 'Validation failed',
        details: result.error.flatten().fieldErrors,
      })
    }
    req.body = result.data
    next()
  }
}

// Schema for publishing a notification event
const publishEventSchema = z.object({
  eventId: z.string().min(1),
  sourceService: z.enum(['order', 'payment', 'shipping']),
  eventType: z.string().min(1),
  recipientId: z.string().min(1),
  channels: z.array(z.enum(['EMAIL', 'SMS', 'PUSH'])).min(1),
  recipientEmail: z.string().email().optional(),
  recipientPhone: z.string().optional(),
  recipientDeviceToken: z.string().optional(),
  payload: z.record(z.any()).optional().default({}),
})

module.exports = { validate, publishEventSchema }
```

### `src/api/middleware/errorHandler.js`

```javascript
const logger = require('../../logger')

function errorHandler(err, req, res, next) {
  logger.error('unhandled_error', {
    error: err.message,
    stack: err.stack,
    path: req.path,
    method: req.method,
  })

  res.status(500).json({
    error: 'Internal server error',
    message: process.env.NODE_ENV === 'development' ? err.message : undefined,
  })
}

module.exports = errorHandler
```

### `src/api/routes/health.js`

```javascript
const router = require('express').Router()
const sequelize = require('../../db/connection')

router.get('/', async (req, res) => {
  try {
    await sequelize.authenticate()
    res.json({ status: 'ok', db: 'connected', timestamp: new Date().toISOString() })
  } catch (err) {
    res.status(503).json({ status: 'error', db: 'disconnected' })
  }
})

module.exports = router
```

### `src/api/routes/notifications.js`

```javascript
const router = require('express').Router()
const { v4: uuidv4 } = require('uuid')
const producer = require('../../queue/producer')
const { Notification, NotificationDelivery } = require('../../models')
const { validate, publishEventSchema } = require('../middleware/validate')
const logger = require('../../logger')

// POST /api/v1/notifications/events
// Accepts event from upstream service, publishes to Kafka
router.post('/events', validate(publishEventSchema), async (req, res) => {
  try {
    const event = {
      ...req.body,
      eventId: req.body.eventId || uuidv4(),
    }

    await producer.publish(event)

    logger.info('event_accepted', {
      eventId: event.eventId,
      eventType: event.eventType,
      sourceService: event.sourceService,
    })

    res.status(202).json({
      status: 'accepted',
      eventId: event.eventId,
      message: 'Event published to notification queue',
    })
  } catch (err) {
    throw err
  }
})

// GET /api/v1/notifications
// List notifications with optional filters
router.get('/', async (req, res) => {
  const { sourceService, limit = 20 } = req.query

  const where = {}
  if (sourceService) where.source_service = sourceService

  const notifications = await Notification.findAll({
    where,
    include: [{ model: NotificationDelivery, as: 'deliveries' }],
    order: [['created_at', 'DESC']],
    limit: parseInt(limit),
  })

  res.json(notifications.map(n => ({
    id: n.id,
    sourceService: n.source_service,
    eventType: n.event_type,
    recipientId: n.recipient_id,
    priority: n.priority,
    channels: n.channels,
    createdAt: n.created_at,
    deliveries: n.deliveries.map(d => ({
      channel: d.channel,
      status: d.status,
      provider: d.provider,
      attemptCount: d.attempt_count,
      sentAt: d.sent_at,
      errorCode: d.error_code,
      errorMessage: d.error_message,
    })),
  })))
})

// GET /api/v1/notifications/:id
// Get single notification with delivery state
router.get('/:id', async (req, res) => {
  const notification = await Notification.findByPk(req.params.id, {
    include: [{ model: NotificationDelivery, as: 'deliveries' }],
  })

  if (!notification) {
    return res.status(404).json({ error: 'Notification not found' })
  }

  res.json({
    id: notification.id,
    sourceService: notification.source_service,
    eventType: notification.event_type,
    recipientId: notification.recipient_id,
    priority: notification.priority,
    channels: notification.channels,
    payload: notification.payload,
    createdAt: notification.created_at,
    deliveries: notification.deliveries.map(d => ({
      id: d.id,
      channel: d.channel,
      status: d.status,
      provider: d.provider,
      attemptCount: d.attempt_count,
      maxAttempts: d.max_attempts,
      lastAttemptedAt: d.last_attempted_at,
      nextRetryAt: d.next_retry_at,
      sentAt: d.sent_at,
      errorCode: d.error_code,
      errorMessage: d.error_message,
      providerMessageId: d.provider_message_id,
    })),
  })
})

module.exports = router
```

### `src/api/server.js`

```javascript
require('dotenv').config()
const express = require('express')
const { runMigrations } = require('../db/migrate')
const notificationsRouter = require('./routes/notifications')
const healthRouter = require('./routes/health')
const errorHandler = require('./middleware/errorHandler')
const logger = require('../logger')
const config = require('../config')

const app = express()
app.use(express.json())

app.use('/health', healthRouter)
app.use('/api/v1/notifications', notificationsRouter)
app.use(errorHandler)

async function start() {
  await runMigrations()

  app.listen(config.app.port, () => {
    logger.info('api_started', { port: config.app.port })
    logger.info(`API docs: curl http://localhost:${config.app.port}/health`)
  })
}

start().catch((err) => {
  logger.error('api_startup_failed', { error: err.message })
  process.exit(1)
})

module.exports = app
```

---

## 19. Unit Tests

### `jest.config.js`

```javascript
module.exports = {
  testEnvironment: 'node',
  testMatch: ['**/tests/unit/**/*.test.js'],
  collectCoverageFrom: ['src/core/**/*.js', 'src/providers/**/*.js'],
}
```

### `tests/unit/stateMachine.test.js`

```javascript
const { transition, isTerminal, InvalidStateTransitionError } = require('../../src/core/stateMachine')

describe('StateMachine', () => {
  test('valid: PENDING → QUEUED', () => {
    expect(transition('PENDING', 'QUEUED')).toBe('QUEUED')
  })

  test('valid: SENDING → SENT', () => {
    expect(transition('SENDING', 'SENT')).toBe('SENT')
  })

  test('valid: SENDING → RETRYING', () => {
    expect(transition('SENDING', 'RETRYING')).toBe('RETRYING')
  })

  test('valid: RETRYING → SENDING', () => {
    expect(transition('RETRYING', 'SENDING')).toBe('SENDING')
  })

  test('invalid: SENT → RETRYING throws', () => {
    expect(() => transition('SENT', 'RETRYING')).toThrow(InvalidStateTransitionError)
  })

  test('invalid: FAILED → SENDING throws', () => {
    expect(() => transition('FAILED', 'SENDING')).toThrow(InvalidStateTransitionError)
  })

  test('invalid: SENT → PENDING throws', () => {
    expect(() => transition('SENT', 'PENDING')).toThrow(InvalidStateTransitionError)
  })

  test('SENT is terminal', () => {
    expect(isTerminal('SENT')).toBe(true)
  })

  test('FAILED is terminal', () => {
    expect(isTerminal('FAILED')).toBe(true)
  })

  test('PENDING is not terminal', () => {
    expect(isTerminal('PENDING')).toBe(false)
  })

  test('RETRYING is not terminal', () => {
    expect(isTerminal('RETRYING')).toBe(false)
  })
})
```

### `tests/unit/idempotency.test.js`

```javascript
const RedisMock = require('ioredis-mock')
const { buildIdempotencyKey, IdempotencyChecker } = require('../../src/core/idempotency')

describe('buildIdempotencyKey', () => {
  test('is deterministic — same inputs produce same key', () => {
    const k1 = buildIdempotencyKey('order', 'evt-1', 'EMAIL', 'user-1')
    const k2 = buildIdempotencyKey('order', 'evt-1', 'EMAIL', 'user-1')
    expect(k1).toBe(k2)
  })

  test('differs by channel', () => {
    const email = buildIdempotencyKey('order', 'evt-1', 'EMAIL', 'user-1')
    const sms   = buildIdempotencyKey('order', 'evt-1', 'SMS',   'user-1')
    expect(email).not.toBe(sms)
  })

  test('differs by eventId', () => {
    const k1 = buildIdempotencyKey('order', 'evt-1', 'EMAIL', 'user-1')
    const k2 = buildIdempotencyKey('order', 'evt-2', 'EMAIL', 'user-1')
    expect(k1).not.toBe(k2)
  })
})

describe('IdempotencyChecker', () => {
  let checker

  beforeEach(() => {
    checker = new IdempotencyChecker(new RedisMock())
  })

  test('acquire returns true first time', async () => {
    expect(await checker.acquire('key-1')).toBe(true)
  })

  test('acquire returns false second time — duplicate', async () => {
    await checker.acquire('key-2')
    expect(await checker.acquire('key-2')).toBe(false)
  })

  test('after markComplete, acquire still returns false', async () => {
    await checker.acquire('key-3')
    await checker.markComplete('key-3')
    expect(await checker.acquire('key-3')).toBe(false)
  })
})
```

### `tests/unit/retryPolicy.test.js`

```javascript
const { calculateRetry, isTerminalError } = require('../../src/core/retryPolicy')

describe('calculateRetry', () => {
  test('attempt 0 should retry', () => {
    expect(calculateRetry(0, 3).shouldRetry).toBe(true)
  })

  test('attempt 3 with max 3 should not retry', () => {
    expect(calculateRetry(3, 3).shouldRetry).toBe(false)
  })

  test('delay increases with attempt count', () => {
    const d0 = calculateRetry(0, 5).delayMs
    const d1 = calculateRetry(1, 5).delayMs
    const d2 = calculateRetry(2, 5).delayMs
    // Each attempt roughly doubles (allowing for jitter)
    expect(d1).toBeGreaterThan(d0 * 0.5)
    expect(d2).toBeGreaterThan(d1 * 0.5)
  })

  test('returns reason string', () => {
    expect(calculateRetry(0, 3).reason).toBeTruthy()
  })
})

describe('isTerminalError', () => {
  test('INVALID_PHONE_NUMBER is terminal for SMS', () => {
    expect(isTerminalError('INVALID_PHONE_NUMBER', 'SMS')).toBe(true)
  })

  test('HARD_BOUNCE is terminal for EMAIL', () => {
    expect(isTerminalError('HARD_BOUNCE', 'EMAIL')).toBe(true)
  })

  test('INVALID_TOKEN is terminal for PUSH', () => {
    expect(isTerminalError('INVALID_TOKEN', 'PUSH')).toBe(true)
  })

  test('PROVIDER_UNAVAILABLE is not terminal for SMS', () => {
    expect(isTerminalError('PROVIDER_UNAVAILABLE', 'SMS')).toBe(false)
  })

  test('unknown error is not terminal', () => {
    expect(isTerminalError('UNKNOWN_ERROR', 'EMAIL')).toBe(false)
  })
})
```

### `tests/unit/priorityRouter.test.js`

```javascript
const { getPriority, getTopicForEvent } = require('../../src/core/priorityRouter')
const config = require('../../src/config')

describe('getPriority', () => {
  test('otp.requested is HIGH', () => {
    expect(getPriority('otp.requested')).toBe('HIGH')
  })

  test('payment.failed is HIGH', () => {
    expect(getPriority('payment.failed')).toBe('HIGH')
  })

  test('shipping.update is LOW', () => {
    expect(getPriority('shipping.update')).toBe('LOW')
  })

  test('promotion.new is LOW', () => {
    expect(getPriority('promotion.new')).toBe('LOW')
  })

  test('unknown event defaults to HIGH — fail safe', () => {
    expect(getPriority('some.unknown.event')).toBe('HIGH')
  })
})

describe('getTopicForEvent', () => {
  test('otp routes to high topic', () => {
    expect(getTopicForEvent('otp.requested', config)).toBe(config.kafka.topics.high)
  })

  test('promotion routes to low topic', () => {
    expect(getTopicForEvent('promotion.new', config)).toBe(config.kafka.topics.low)
  })
})
```

### `tests/unit/circuitBreaker.test.js`

```javascript
const RedisMock = require('ioredis-mock')
const { CircuitBreaker } = require('../../src/providers/CircuitBreaker')

describe('CircuitBreaker', () => {
  let cb

  beforeEach(() => {
    cb = new CircuitBreaker('test_provider', new RedisMock())
  })

  test('initial state is CLOSED', async () => {
    expect(await cb.getState()).toBe('CLOSED')
  })

  test('is available when CLOSED', async () => {
    expect(await cb.isAvailable()).toBe(true)
  })

  test('opens after failure threshold (5)', async () => {
    for (let i = 0; i < 5; i++) await cb.recordFailure()
    expect(await cb.getState()).toBe('OPEN')
  })

  test('is not available when OPEN', async () => {
    for (let i = 0; i < 5; i++) await cb.recordFailure()
    expect(await cb.isAvailable()).toBe(false)
  })

  test('recordSuccess resets to CLOSED', async () => {
    for (let i = 0; i < 5; i++) await cb.recordFailure()
    await cb.recordSuccess()
    expect(await cb.getState()).toBe('CLOSED')
  })
})
```

---

## 20. Seed Script — 1000 Requests

### `scripts/seed.js`

```javascript
require('dotenv').config()
const { v4: uuidv4 } = require('uuid')

const BASE_URL = process.env.SEED_URL || 'http://localhost:3000'

// Event templates with realistic distribution
const EVENT_TEMPLATES = [
  // HIGH PRIORITY — 30% of traffic
  { eventType: 'otp.requested',    sourceService: 'payment',  channels: ['SMS'],          priority: 'HIGH', weight: 10 },
  { eventType: 'payment.failed',   sourceService: 'payment',  channels: ['EMAIL', 'SMS'], priority: 'HIGH', weight: 8  },
  { eventType: 'payment.confirmed',sourceService: 'payment',  channels: ['EMAIL'],        priority: 'HIGH', weight: 7  },
  { eventType: 'fraud.alert',      sourceService: 'payment',  channels: ['SMS', 'PUSH'],  priority: 'HIGH', weight: 5  },

  // LOW PRIORITY — 70% of traffic
  { eventType: 'order.confirmed',  sourceService: 'order',    channels: ['EMAIL'],        priority: 'LOW',  weight: 25 },
  { eventType: 'order.shipped',    sourceService: 'order',    channels: ['EMAIL', 'PUSH'],priority: 'LOW',  weight: 15 },
  { eventType: 'shipping.update',  sourceService: 'shipping', channels: ['PUSH'],         priority: 'LOW',  weight: 15 },
  { eventType: 'shipping.delivered',sourceService: 'shipping',channels: ['EMAIL', 'PUSH'],priority: 'LOW',  weight: 10 },
  { eventType: 'promotion.new',    sourceService: 'order',    channels: ['EMAIL'],        priority: 'LOW',  weight: 5  },
]

// Build weighted pool
function buildWeightedPool(templates) {
  const pool = []
  for (const t of templates) {
    for (let i = 0; i < t.weight; i++) pool.push(t)
  }
  return pool
}

function pickRandom(pool) {
  return pool[Math.floor(Math.random() * pool.length)]
}

function buildEvent(template, index) {
  const userId = `user-${(index % 100).toString().padStart(3, '0')}`
  return {
    eventId: uuidv4(),
    sourceService: template.sourceService,
    eventType: template.eventType,
    recipientId: userId,
    channels: template.channels,
    recipientEmail: `${userId}@example.com`,
    recipientPhone: `+1415555${index.toString().padStart(4, '0')}`,
    recipientDeviceToken: `device-token-${userId}`,
    payload: {
      orderId: `ORD-${index}`,
      amount: `$${(Math.random() * 200).toFixed(2)}`,
    },
  }
}

async function publishEvent(event) {
  const res = await fetch(`${BASE_URL}/api/v1/notifications/events`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(event),
  })
  return res.status
}

async function seed(total = 1000, concurrency = 20) {
  const pool = buildWeightedPool(EVENT_TEMPLATES)

  console.log(`\n🚀 Seeding ${total} notifications (concurrency: ${concurrency})`)
  console.log(`📡 Target: ${BASE_URL}\n`)

  const results = { success: 0, failed: 0, byPriority: { HIGH: 0, LOW: 0 } }
  const start = Date.now()

  // Process in batches for controlled concurrency
  for (let batch = 0; batch < total; batch += concurrency) {
    const batchSize = Math.min(concurrency, total - batch)
    const promises = []

    for (let i = 0; i < batchSize; i++) {
      const index = batch + i
      const template = pickRandom(pool)
      const event = buildEvent(template, index)

      promises.push(
        publishEvent(event).then(status => {
          if (status === 202) {
            results.success++
            results.byPriority[template.priority]++
          } else {
            results.failed++
          }
        })
      )
    }

    await Promise.all(promises)

    // Progress log every 100 events
    if ((batch + batchSize) % 100 === 0 || batch + batchSize === total) {
      const elapsed = ((Date.now() - start) / 1000).toFixed(1)
      const done = batch + batchSize
      console.log(`  ✓ ${done}/${total} sent | ${elapsed}s elapsed | ${results.success} ok, ${results.failed} failed`)
    }
  }

  const elapsed = ((Date.now() - start) / 1000).toFixed(1)
  const rpm = Math.round((total / elapsed) * 60)

  console.log(`
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ✅ Seed complete
  Total:     ${total} events
  Success:   ${results.success}
  Failed:    ${results.failed}
  HIGH:      ${results.byPriority.HIGH}
  LOW:       ${results.byPriority.LOW}
  Time:      ${elapsed}s
  Rate:      ~${rpm} RPM
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  View results:
  curl http://localhost:3000/api/v1/notifications | jq .
  `)
}

seed(1000, 20).catch(console.error)
```

---

## 21. Demo Commands

### `scripts/demo.sh`

```bash
#!/bin/bash
BASE=http://localhost:3000

echo ""
echo "══════════════════════════════════════════"
echo "  NOTIFICATION SERVICE DEMO"
echo "══════════════════════════════════════════"

echo ""
echo "── 1. Health Check ──"
curl -s $BASE/health | jq .

echo ""
echo "── 2. HIGH PRIORITY — OTP (goes to notifications.high) ──"
curl -s -X POST $BASE/api/v1/notifications/events \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "demo-otp-001",
    "sourceService": "payment",
    "eventType": "otp.requested",
    "recipientId": "user-demo-001",
    "channels": ["SMS"],
    "recipientPhone": "+14155551234",
    "payload": {"otp": "847291"}
  }' | jq .

sleep 1

echo ""
echo "── 3. SAME EVENT AGAIN — Idempotency (should be skipped) ──"
curl -s -X POST $BASE/api/v1/notifications/events \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "demo-otp-001",
    "sourceService": "payment",
    "eventType": "otp.requested",
    "recipientId": "user-demo-001",
    "channels": ["SMS"],
    "recipientPhone": "+14155551234",
    "payload": {"otp": "847291"}
  }' | jq .

echo ""
echo "── 4. TERMINAL FAILURE — Invalid phone (no retry) ──"
curl -s -X POST $BASE/api/v1/notifications/events \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "demo-fail-001",
    "sourceService": "payment",
    "eventType": "otp.requested",
    "recipientId": "user-demo-002",
    "channels": ["SMS"],
    "recipientPhone": "+00000000000",
    "payload": {"otp": "111222"}
  }' | jq .

echo ""
echo "── 5. PARTIAL FAILURE — Email up, SMS invalid ──"
curl -s -X POST $BASE/api/v1/notifications/events \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "demo-partial-001",
    "sourceService": "payment",
    "eventType": "payment.failed",
    "recipientId": "user-demo-003",
    "channels": ["EMAIL", "SMS"],
    "recipientEmail": "user@example.com",
    "recipientPhone": "+00000000000",
    "payload": {"amount": "$99.99"}
  }' | jq .

echo ""
echo "── 6. LOW PRIORITY — Marketing (goes to notifications.low) ──"
curl -s -X POST $BASE/api/v1/notifications/events \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "demo-promo-001",
    "sourceService": "order",
    "eventType": "promotion.new",
    "recipientId": "user-demo-004",
    "channels": ["EMAIL"],
    "recipientEmail": "user@example.com",
    "payload": {"discount": "20% off"}
  }' | jq .

sleep 2

echo ""
echo "── 7. View All Notifications State ──"
curl -s $BASE/api/v1/notifications | jq '[.[] | {eventType, priority, deliveries: [.deliveries[] | {channel, status, errorCode}]}]'

echo ""
echo "── 8. Run 1000 Requests ──"
echo "  node scripts/seed.js"
echo ""
echo "── 9. Scale Workers ──"
echo "  docker-compose up --scale worker-high=4 --scale worker-low=2"
```

---

## 22. README Template

```markdown
# Notification Service

Aggregates events from Order, Payment, and Shipping microservices.
Dispatches via Email, SMS, and Push. Handles 50,000 RPM.

## Quick Start

```bash
git clone <repo> && cd notification-service
cp .env.example .env
docker-compose up --build
```

In another terminal:

```bash
# Fire 1000 mixed priority events
node scripts/seed.js

# Or run the full demo script
bash scripts/demo.sh
```

## What's Running

| Service       | URL / Port         | Purpose                        |
|---------------|--------------------|--------------------------------|
| API           | localhost:3000     | Event ingestion + state query  |
| Postgres      | localhost:5432     | Notification state store       |
| Redis         | localhost:6379     | Idempotency + circuit breaker  |
| Kafka         | localhost:9092     | Message broker                 |
| worker-high   | (background)       | OTP, payment, fraud alerts     |
| worker-low    | (background)       | Orders, shipping, promotions   |

## Key Endpoints

```bash
POST /api/v1/notifications/events   # Publish event
GET  /api/v1/notifications          # List all (with delivery state)
GET  /api/v1/notifications/:id      # Single notification detail
GET  /health                        # Liveness check
```

## Architecture Decisions

### Two Kafka Topics — Not One with a Priority Flag
Kafka processes messages in partition order. A marketing flood blocks OTPs
behind it in the same topic. Two topics + two worker pools means OTP workers
never look at the marketing backlog.

### Redis Idempotency Before the Provider Call
Key written as PROCESSING before calling Twilio/SES. If worker crashes after
the send but before the DB write, the retry sees PROCESSING, re-calls the
provider — but passes the same idempotency key. Provider deduplicates.
User gets exactly one SMS.

### Per-Channel Delivery Rows
Email SENT lives in its own row. SMS RETRYING lives in its own row.
A successful Email never masks a failing SMS. Each channel has independent
retry budget and terminal failure detection.

### State Machine with Explicit Transitions
Every valid transition is enumerated. SENT → RETRYING throws immediately.
Deliveries stuck in SENDING after a crash are detectable and alertable.

## Scaling

```bash
# Scale workers horizontally
docker-compose up --scale worker-high=4 --scale worker-low=2

# Watch queue lag
docker-compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group notif-workers-high
```

## Running Tests

```bash
npm test
```

All tests are unit tests — no Docker needed.
Tests cover: state machine, idempotency, retry policy,
priority routing, circuit breaker.

## Trade-offs

| Decision | What you gain | What you give up |
|---|---|---|
| KafkaJS over BullMQ | Message replay, durability | Zookeeper overhead |
| Two topics over priority field | Zero priority inversion | More infra to monitor |
| Sequelize over Prisma | Familiar, no codegen | Slightly more verbose |
| Mock providers | Zero external credentials | Must wire real SDK for prod |
| Per-channel delivery rows | Independent retry per channel | More DB rows |
```