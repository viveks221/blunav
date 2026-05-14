import { kafka } from './kafka.js';
let producer;

async function getProducer() {
  if (!producer) {
    producer = kafka.producer();
    await producer.connect();
  }
  return producer;
}

async function send(topic, messages) {
  const p = await getProducer();
  // messages: array of { key, value }
  await p.send({ topic, messages });
}

export { send, getProducer };
