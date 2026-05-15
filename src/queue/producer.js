import { kafka } from './kafka.js';
let producer;
import logger from '../logger/index.js';

async function getProducer() {
  if (!producer) {
    producer = kafka.producer();
    try {
      await producer.connect();
      logger.info('Kafka producer connected');
    } catch (err) {
      logger.error('Kafka producer connect failed', { err: err.message });
      throw err;
    }
  }
  return producer;
}

async function send(topic, messages) {
  const p = await getProducer();
  // messages: array of { key, value }
  try {
    logger.debug('Producing message', { topic, count: messages.length });
    const res = await p.send({ topic, messages });
    logger.info('Produced message', { topic, result: res });
    return res;
  } catch (err) {
    logger.error('Failed to produce message', { topic, err: err.message });
    throw err;
  }
}

async function disconnectProducer() {
  if (!producer) return;
  try {
    await producer.disconnect();
    logger.info('Kafka producer disconnected');
  } finally {
    producer = null;
  }
}

export { send, getProducer, disconnectProducer };
