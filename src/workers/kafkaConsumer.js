#!/usr/bin/env node
import { fileURLToPath } from 'url';
import { kafka } from '../queue/kafka.js';
import topics from '../queue/topics.js';
import logger from '../logger/index.js';
import BaseWorker from './BaseWorker.js';

const GROUP_ID = process.env.CONSUMER_GROUP || 'blunav-worker-group';
const MAX_CONNECT_ATTEMPTS = parseInt(process.env.KAFKA_CONNECT_ATTEMPTS || '10', 10);

const DEFAULT_TOPICS = [topics.NOTIFICATIONS_HIGH, topics.NOTIFICATIONS_LOW].join(',');

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function subscribeTopics() {
  const raw = process.env.KAFKA_SUBSCRIBE_TOPICS || DEFAULT_TOPICS;
  return raw.split(',').map((s) => s.trim()).filter(Boolean);
}

function parseEnvelope(value) {
  const raw = value ? value.toString() : null;
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch (err) {
    logger.error('Invalid message JSON, skipping', { err: err.message, raw });
    return null;
  }
}

async function dispatch(worker, envelope) {
  if (!envelope || typeof envelope !== 'object') return;
  const kind = envelope.kind || 'EVENT';

  if (kind === 'DELIVERY_RETRY') {
    if (!envelope.deliveryId) {
      logger.error('DELIVERY_RETRY missing deliveryId', { envelope });
      return;
    }
    await worker.processDeliveryRetry(envelope.deliveryId);
    return;
  }

  await worker.processEvent(envelope);
}

async function start() {
  console.log('WORKER: Creating Kafka consumer...');
  const consumer = kafka.consumer({ groupId: GROUP_ID });
  console.log('WORKER: Kafka consumer created, attempting connect...');
  let attempt = 0;
  while (attempt < MAX_CONNECT_ATTEMPTS) {
    console.log('WORKER: Connect attempt', attempt + 1);
    try {
      await consumer.connect();
      console.log('WORKER: Connected!');
      logger.info('Kafka consumer connected', { groupId: GROUP_ID });
      break;
    } catch (err) {
      console.log('WORKER: Connect failed:', err.message);
      attempt += 1;
      const backoff = Math.min(30000, Math.pow(2, attempt) * 100);
      logger.warn('Kafka consumer connect failed, retrying', { attempt, err: err.message, backoff });
      await sleep(backoff);
    }
  }

  if (attempt >= MAX_CONNECT_ATTEMPTS) {
    throw new Error('Unable to connect to Kafka after multiple attempts');
  }

  const topicNames = subscribeTopics();
  for (const name of topicNames) {
    await consumer.subscribe({ topic: name });
  }
  logger.info('Kafka consumer subscribed', { topics: topicNames });

  logger.info('Kafka consumer starting run loop', { groupId: GROUP_ID });

  const worker = new BaseWorker();

  const runConfig = {
    eachMessage: async ({ topic, partition, message }) => {
      const envelope = parseEnvelope(message.value);
      if (!envelope) return;

      try {
        logger.info('Consuming message', { topic, partition, offset: message.offset, kind: envelope.kind || 'EVENT' });
        await dispatch(worker, envelope);
      } catch (err) {
        logger.error('Failed to process message', { err: err.message, envelope });
      }
    },
  };

  await consumer.run(runConfig);

  logger.info('Kafka consumer run loop active', { groupId: GROUP_ID });

  const shutdown = async () => {
    logger.info('Shutting down consumer gracefully');
    try {
      const dis = consumer.disconnect();
      const to = new Promise(r => setTimeout(r, 8000));
      await Promise.race([dis, to]);
    } catch (e) {
      logger.error('Error during consumer disconnect', { err: e.message });
    } finally {
      process.exit(0);
    }
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}


if (process.argv[1] === fileURLToPath(import.meta.url)) {
  start().catch((err) => {
    console.error('WORKER: Fatal error:', err);
    process.exit(1);
  });
}

export { start };
