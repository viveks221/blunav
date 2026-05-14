export const VALID_TRANSITIONS = {
  PENDING: ['QUEUED'],
  QUEUED: ['SENDING'],
  SENDING: ['SENT', 'RETRYING', 'FAILED'],
  RETRYING: ['QUEUED', 'FAILED'],
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
