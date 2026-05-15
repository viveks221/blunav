/**
 * Delivery lifecycle (DB statuses).
 * PENDING: created, not yet claimed.
 * QUEUED: claimed for retry re-enqueue from poller, ready for worker.
 * SENDING: outbound call in flight.
 * SENT / FAILED / RETRYING: terminal or waiting for retry.
 */
export const VALID_TRANSITIONS = {
  PENDING: ['QUEUED', 'SENDING', 'RETRYING'],
  QUEUED: ['SENDING', 'RETRYING'],
  SENDING: ['SENT', 'RETRYING', 'FAILED'],
  RETRYING: ['QUEUED', 'SENDING', 'FAILED'],
};

export function canTransition(from, to) {
  if (from === to) return true;
  const allowed = VALID_TRANSITIONS[from];
  return Array.isArray(allowed) && allowed.includes(to);
}

export function transition(current, next) {
  if (!canTransition(current, next)) {
    const err = new Error(`Invalid state transition: ${current} -> ${next}`);
    err.code = 'INVALID_TRANSITION';
    throw err;
  }
  return next;
}
