export const TERMINAL_ERRORS = new Set(['INVALID_PHONE', 'HARD_BOUNCE', 'INVALID_TOKEN']);

export function isTerminalError(code) {
  return TERMINAL_ERRORS.has(code);
}

export function nextRetryDelay(attempt, base = 1000, max = 60 * 1000) {
  const backoff = Math.min(max, Math.pow(2, attempt) * base);
  // add jitter +/-20%
  const jitter = Math.floor((Math.random() * 0.4 - 0.2) * backoff);
  return backoff + jitter;
}
