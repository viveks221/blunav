import { Kafka } from 'kafkajs';
import config from '../config/index.js';

export const kafka = new Kafka({
  clientId: 'blunav-node',
  brokers: config.kafkaBrokers,
});
