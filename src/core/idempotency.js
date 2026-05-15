import Redis from 'ioredis';
import config from '../config/index.js';

let redis;

function getRedis() {
  if (!redis) {
    redis = new Redis(config.redisUrl, { maxRetriesPerRequest: 2, enableReadyCheck: true });
  }
  return redis;
}

/** Redis key: first successful HTTP accept for this eventId (client must reuse eventId for dedup). */
function acceptKey(eventId) {
  return `idempotency:accept:${eventId}`;
}

/**
 * Reserve acceptance of an event in the API (SET NX).
 * @param {import('ioredis').Redis} [client] optional Redis client (tests)
 * @returns {Promise<boolean>} true if this is the first accept, false if duplicate eventId
 */
async function tryReserveAcceptance(eventId, ttlSeconds = 7 * 24 * 3600, client) {
  const key = acceptKey(eventId);
  const r = client || getRedis();
  const got = await r.set(key, '1', 'NX', 'EX', ttlSeconds);
  return got === 'OK';
}

/** Call if Kafka publish fails after tryReserveAcceptance returned true, so the client can retry. */
async function releaseAcceptance(eventId, client) {
  const r = client || getRedis();
  await r.del(acceptKey(eventId));
}

export default { tryReserveAcceptance, releaseAcceptance, acceptKey, getRedis };
