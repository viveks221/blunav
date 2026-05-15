import Redis from 'ioredis';

function defaultRedis() {
  return new Redis(process.env.REDIS_URL || 'redis://redis:6379');
}

function mkKey(prefix, name) {
  return `cb:${prefix}:${name}`;
}

export function createCircuitBreaker(redisClient, opts = {}) {
  const redis = redisClient || defaultRedis();
  const failureThreshold = opts.failureThreshold || parseInt(process.env.CB_FAILURE_THRESHOLD, 10) || 5;
  const windowSeconds = opts.windowSeconds || parseInt(process.env.CB_WINDOW_SECONDS, 10) || 60;
  const openDurationSeconds = opts.openDurationSeconds || parseInt(process.env.CB_OPEN_SECONDS, 10) || 60;

  async function isOpen(name) {
    const stateKey = mkKey('state', name);
    const state = await redis.get(stateKey);
    return !!state;
  }

  async function recordFailure(name) {
    const failuresKey = mkKey('failures', name);
    const stateKey = mkKey('state', name);
    const failures = await redis.incr(failuresKey);
    const ttl = await redis.ttl(failuresKey);
    if (ttl === -1) {
      await redis.expire(failuresKey, windowSeconds);
    }

    if (failures >= failureThreshold) {
      await redis.set(stateKey, 'OPEN', 'EX', openDurationSeconds);
    }
    return failures;
  }

  async function recordSuccess(name) {
    const failuresKey = mkKey('failures', name);
    const stateKey = mkKey('state', name);
    await redis.del(failuresKey);
    await redis.del(stateKey);
  }

  return {
    isOpen,
    recordFailure,
    recordSuccess,
    getOpenDurationMs: () => openDurationSeconds * 1000,
    getFailureThreshold: () => failureThreshold,
    _redis: redis,
    _opts: { failureThreshold, windowSeconds, openDurationSeconds },
  };
}

export default { createCircuitBreaker };
