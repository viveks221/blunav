import circuitModule from '../src/core/circuitBreaker.js';
const { createCircuitBreaker } = circuitModule.default || circuitModule;

class FakeRedis {
  constructor() {
    this.store = new Map();
    this.ttls = new Map();
  }
  async get(k) { return this.store.has(k) ? String(this.store.get(k)) : null; }
  async set(k, v, mode, ex, seconds) {
    this.store.set(k, v);
    if (mode === 'EX') this.ttls.set(k, Date.now() + (seconds * 1000));
    return 'OK';
  }
  async incr(k) {
    const val = parseInt(this.store.get(k) || '0', 10) + 1;
    this.store.set(k, String(val));
    return val;
  }
  async ttl(k) {
    if (!this.ttls.has(k)) return -1;
    const expiry = this.ttls.get(k);
    const diff = Math.ceil((expiry - Date.now()) / 1000);
    return diff > 0 ? diff : -2;
  }
  async expire(k, seconds) {
    this.ttls.set(k, Date.now() + (seconds * 1000));
    return 1;
  }
  async del(k) { this.store.delete(k); this.ttls.delete(k); return 1; }
}

describe('circuitBreaker', () => {
  test('opens circuit after threshold failures and resets on success', async () => {
    const fake = new FakeRedis();
    const cb = createCircuitBreaker(fake, { failureThreshold: 2, windowSeconds: 10, openDurationSeconds: 5 });
    const name = 'prov:EMAIL';

    expect(await cb.isOpen(name)).toBe(false);
    const f1 = await cb.recordFailure(name);
    expect(f1).toBe(1);
    expect(await cb.isOpen(name)).toBe(false);
    const f2 = await cb.recordFailure(name);
    expect(f2).toBe(2);
    // should now be open
    expect(await cb.isOpen(name)).toBe(true);

    // simulate expiry by deleting state
    await fake.del(`cb:state:${name}`);
    expect(await cb.isOpen(name)).toBe(false);

    // record success clears counters and state
    await cb.recordFailure(name);
    await cb.recordSuccess(name);
    expect(await fake.get(`cb:failures:${name}`)).toBeNull();
    expect(await fake.get(`cb:state:${name}`)).toBeNull();
  });
});
