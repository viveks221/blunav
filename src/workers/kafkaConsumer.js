#!/usr/bin/env node
import { kafka } from '../queue/kafka.js';
import topics from '../queue/topics.js';
import logger from '../logger/index.js';
import BaseWorker from './BaseWorker.js';

const GROUP_ID = process.env.CONSUMER_GROUP || 'blunav-worker-group';
const MAX_CONNECT_ATTEMPTS = parseInt(process.env.KAFKA_CONNECT_ATTEMPTS || '10', 10);

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function start() {
  const consumer = kafka.consumer({ groupId: GROUP_ID });
  let attempt = 0;
  // retry connect with exponential backoff
  while (attempt < MAX_CONNECT_ATTEMPTS) {
    try {
      await consumer.connect();
      logger.info('Kafka consumer connected', { groupId: GROUP_ID });
      break;
    } catch (err) {
      attempt += 1;
      const backoff = Math.min(30000, Math.pow(2, attempt) * 100);
      logger.warn('Kafka consumer connect failed, retrying', { attempt, err: err.message, backoff });
      await sleep(backoff);
    }
  }

  if (attempt >= MAX_CONNECT_ATTEMPTS) {
    throw new Error('Unable to connect to Kafka after multiple attempts');
  }

  await consumer.subscribe({ topic: topics.NOTIFICATIONS_HIGH });
  await consumer.subscribe({ topic: topics.NOTIFICATIONS_LOW });

  const worker = new BaseWorker();

  const runConfig = {
    eachMessage: async ({ topic, partition, message }) => {
      const value = message.value ? message.value.toString() : null;
      if (!value) return;
      let event;
      try {
        event = JSON.parse(value);
      } catch (err) {
        logger.error('Invalid message JSON, skipping', { err: err.message, value });
        return;
      }

      try {
        logger.info('Consuming event', { topic, partition, offset: message.offset, eventId: event.eventId });
        await worker.processEvent(event);
      } catch (err) {
        logger.error('Failed to process event', { err: err.message, event });
        // do not rethrow; allow consumer to continue. Offset commit behavior is handled by kafkajs.
      }
    }
  };

  // start consumer run loop
  await consumer.run(runConfig);

  const shutdown = async () => {
    logger.info('Shutting down consumer gracefully');
    try {
      // attempt graceful disconnect with timeout
      const dis = consumer.disconnect();
      const to = new Promise(r => setTimeout(r, 5000));
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

if (require.main === module) {
  start().catch(err => {
    logger.error('Consumer failed to start', { err: err.message });
    process.exit(1);
  });
}

export { start };
