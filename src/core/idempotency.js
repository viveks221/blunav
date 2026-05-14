import Redis from 'ioredis';
import { createHash } from 'crypto';
import config from '../config/index.js';

const redis = new Redis(config.redisUrl);

function makeKey(event) {
  const hash = createHash('sha256').update(JSON.stringify(event)).digest('hex');
  return `idempotency:${hash}`;
}

async function acquire(event, ttlSeconds = 60) {
  const key = makeKey(event);
  const got = await redis.set(key, 'locked', 'NX', 'EX', ttlSeconds);
  return got === 'OK';
}

async function release(event) {
  const key = makeKey(event);
  await redis.del(key);
}

export default { acquire, release, makeKey };
