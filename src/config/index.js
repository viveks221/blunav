import path from 'path';
import dotenv from 'dotenv';

dotenv.config({ path: path.resolve(process.cwd(), '.env') });

export default {
  port: process.env.PORT || 3001,
  nodeEnv: process.env.NODE_ENV || 'development',
  databaseUrl: process.env.DATABASE_URL || 'postgres://postgres:postgres@localhost:5432/notifications_db',
  redisUrl: process.env.REDIS_URL || 'redis://blunavnode-redis-1:6379',
  kafkaBrokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  logLevel: process.env.LOG_LEVEL || 'info',
  /** When true, API process runs the outbox poller loop (dev convenience). Docker sets false and uses `outbox-publisher` service. */
  outboxInlinePublisher: process.env.OUTBOX_INLINE_PUBLISHER !== 'false',
  outboxPollIntervalMs: parseInt(process.env.OUTBOX_POLL_INTERVAL_MS || '500', 10),
};
