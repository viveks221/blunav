import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { UniqueConstraintError } from 'sequelize';
import models from '../../models/index.js';
import logger from '../../logger/index.js';
import idempotency from '../../core/idempotency.js';
import { eventIdFromIdempotencyKey, newEventId } from '../../core/eventIdentity.js';
import { topicForNotificationPriority } from '../../queue/priorityRouting.js';
import { eventBodySchema } from '../validation/notificationEvent.js';

const router = Router();

router.post('/events', async (req, res) => {
logger.info('Received notification event', { body: req.body, headers: req.headers });
  const parse = eventBodySchema.safeParse(req.body);
  if (!parse.success) {
    return res.status(400).json({ error: parse.error.format() });
  }

  const data = parse.data;
  const idemHeader = req.get('idempotency-key');
  const eventId = idemHeader ? eventIdFromIdempotencyKey(idemHeader) : (data.eventId || newEventId());

  let firstAccept;
  try {
    firstAccept = await idempotency.tryReserveAcceptance(eventId);
  } catch (err) {
    logger.error('Idempotency store error', { err: err.message, eventId });
    return res.status(503).json({ error: 'idempotency_unavailable' });
  }

  if (!firstAccept) {
    return res.status(200).json({ accepted: false, duplicate: true, eventId });
  }

  const topic = topicForNotificationPriority(data.priority);
  const envelope = {
    kind: 'EVENT',
    eventId,
    type: data.type,
    priority: data.priority,
    source: data.source || 'other',
    payload: data.payload,
  };

  try {
    await models.NotificationOutbox.create({
      id: uuidv4(),
      event_id: eventId,
      topic,
      envelope,
      status: 'pending',
    });
    return res.status(202).json({ accepted: true, eventId });
  } catch (err) {
    if (err instanceof UniqueConstraintError) {
      return res.status(200).json({ accepted: false, duplicate: true, eventId });
    }
    logger.error('Failed to persist outbox row', { err: err.message, eventId });
    try {
      await idempotency.releaseAcceptance(eventId);
    } catch (e) {
      logger.warn('Failed to roll back acceptance key', { eventId, err: e.message });
    }
    return res.status(503).json({ error: 'storage_unavailable' });
  }
});

export default router;
