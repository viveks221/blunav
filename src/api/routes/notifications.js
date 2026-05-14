import { Router } from 'express';
import { z } from 'zod';
import { v4 as uuidv4 } from 'uuid';
const router = Router();
import models from '../../models/index.js';
import { send } from '../../queue/producer.js';
import { NOTIFICATIONS_HIGH, NOTIFICATIONS_LOW } from '../../queue/topics.js';
import logger from '../../logger/index.js';

const eventSchema = z.object({
  eventId: z.string().uuid().optional(),
  type: z.string(),
  priority: z.enum(['HIGH', 'LOW']).optional().default('LOW'),
  payload: z.record(z.any()),
});

router.post('/events', async (req, res) => {
  const parse = eventSchema.safeParse(req.body);
  if (!parse.success) {
    return res.status(400).json({ error: parse.error.format() });
  }

  const data = parse.data;
  const eventId = data.eventId || uuidv4();

  try {
    // persist Notification
    await models.Notification.create({ id: eventId, type: data.type, priority: data.priority || 'LOW', payload: data.payload });

    // publish to Kafka topic based on priority
    const topic = (data.priority === 'HIGH') ? NOTIFICATIONS_HIGH : NOTIFICATIONS_LOW;
    const message = { eventId, type: data.type, priority: data.priority || 'LOW', payload: data.payload };
    await send(topic, [{ key: eventId, value: JSON.stringify(message) }]);

    return res.status(202).json({ accepted: true, eventId });
  } catch (err) {
    logger.error('Failed to accept event', { err: err.message });
    return res.status(500).json({ error: 'internal_error' });
  }
});

export default router;
