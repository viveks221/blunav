# Backend Challenge: High-Throughput Notification Engine

## Overview

Design and implement a notification service that aggregates events from multiple microservices (Order, Payment, Shipping) and dispatches them via Email, SMS, or Push.

## Core Requirements

1. **HLD (Architecture):** Provide a system diagram showing how you handle 50,000 requests per minute. Address how you prevent duplicate notifications (Idempotency) and how you handle provider downtime (Retry logic).
2. **LLD (Implementation):** Implement a working service in your language of choice.
    * Define a clear interface for `NotificationProviders`.
    * Implement a **Priority Queue** mechanism (e.g., OTPs must go before Marketing emails).
    * Show how you handle partial failures (e.g., Email is up, but SMS is down).
3. **Data Persistence:** Design a schema to track the "State" of every notification (Pending, Sent, Failed, Retrying).

## Submission Guidelines

* Include a `README.md` explaining your architectural trade-offs.
* Include a `docker-compose.yml` to spin up necessary infrastructure (DB, Message Broker).
* Unit tests for the core business logic.