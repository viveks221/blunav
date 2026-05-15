import { v5 as uuidv5, v4 as uuidv4 } from 'uuid';

/** Stable UUID v5 namespace for deriving `eventId` from `Idempotency-Key` header. */
export const EVENT_ID_NAMESPACE = '6ba7b810-9dad-11d1-80b4-00c04fd430c8';

export function eventIdFromIdempotencyKey(key) {
  return uuidv5(String(key).trim(), EVENT_ID_NAMESPACE);
}

export { uuidv4 as newEventId };
