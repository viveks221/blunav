import { v4 as uuidv4 } from 'uuid';
import { getProvider } from '../providers/ProviderFactory.js';
import idempotency from '../core/idempotency.js';
import logger from '../logger/index.js';
import { transition } from '../core/stateMachine.js';
import { isTerminalError, nextRetryDelay } from '../core/retryPolicy.js';
import models from '../models/index.js';
import { send } from '../queue/producer.js';
import topics from '../queue/topics.js';
import circuitModule from '../core/circuitBreaker.js';

const { sequelize, Notification, NotificationDelivery } = models;
const { createCircuitBreaker } = circuitModule;

const circuitBreaker = createCircuitBreaker();

class BaseWorker {
  constructor() {}

  async processEvent(event) {
    const acquired = await idempotency.acquire(event);
    if (!acquired) {
      logger.info('Event skipped by idempotency', { event });
      return;
    }

    try {
      // persist notification
      const nid = event.eventId || uuidv4();
      const notification = await Notification.create({ id: nid, type: event.type, priority: event.priority || 'LOW', payload: event.payload });

      // create per-channel deliveries
      const channels = ['EMAIL', 'SMS'];
      for (const ch of channels) {
        const did = uuidv4();
        const del = await NotificationDelivery.create({ id: did, notification_id: nid, channel: ch, status: 'PENDING' });
        // attempt send (synchronous for now)
        await this.sendDelivery(del);
      }
    } finally {
      await idempotency.release(event);
    }
  }

  async sendDelivery(delivery) {
    const provider = getProvider(delivery.channel);
    const providerKey = `${provider.name || provider.constructor.name}:${delivery.channel}`;
    const MAX_ATTEMPTS = parseInt(process.env.MAX_DELIVERY_ATTEMPTS || '3', 10);

    // If the circuit is open for this provider, schedule a retry without claiming
    if (await circuitBreaker.isOpen(providerKey)) {
      const nextRetry = new Date(Date.now() + (circuitBreaker._opts.openDurationSeconds * 1000));
      try {
        await NotificationDelivery.update(
          { status: transition('QUEUED', 'RETRYING'), last_error: 'circuit_open', next_retry_at: nextRetry },
          { where: { id: delivery.id, status: ['PENDING', 'QUEUED'] } }
        );
      } catch (e) {
        logger.warn('Failed to mark delivery as RETRYING due to open circuit', { deliveryId: delivery.id, err: e.message });
      }
      logger.warn('Circuit open for provider, skipping send', { providerKey, deliveryId: delivery.id });
      return;
    }

    // Atomically claim the delivery and increment attempts to avoid races
    const t = await sequelize.transaction();
    let claimed;
    try {
      const [count, rows] = await NotificationDelivery.update(
        { status: transition('QUEUED', 'SENDING'), attempts: sequelize.literal('attempts + 1') },
        { where: { id: delivery.id, status: ['PENDING', 'QUEUED'] }, returning: true, transaction: t }
      );

      if (!count || !rows || rows.length === 0) {
        // someone else claimed or status changed; nothing to do
        await t.commit();
        logger.info('Delivery already claimed or in progress by another worker', { deliveryId: delivery.id });
        return;
      }

      claimed = rows[0];
      await t.commit();
    } catch (e) {
      try { await t.rollback(); } catch (_) {}
      logger.error('Failed to claim delivery', { deliveryId: delivery.id, err: e.message });
      return;
    }

    // perform the send outside the transaction
    try {
      const res = await provider.send({ to: 'recipient', body: '...' });
      // mark SENT
      try { await NotificationDelivery.update({ status: transition('SENDING', 'SENT') }, { where: { id: delivery.id } }); } catch (e) { logger.error('Failed to mark delivery SENT', { deliveryId: delivery.id, err: e.message }); }
      // successful send resets circuit state
      try { await circuitBreaker.recordSuccess(providerKey); } catch (e) { logger.warn('Circuit breaker success record failed', { err: e }); }
      logger.info('Delivery sent', { id: delivery.id, providerResponse: res });
    } catch (err) {
      // record the failure in circuit breaker
      try { await circuitBreaker.recordFailure(providerKey); } catch (e) { logger.warn('Circuit breaker failure record failed', { err: e }); }

      const code = err.code || 'UNKNOWN_ERROR';
      // get latest attempts
      let latest;
      try { latest = await NotificationDelivery.findByPk(delivery.id); } catch (e) { logger.error('Failed to fetch delivery after error', { deliveryId: delivery.id, err: e.message }); }
      const attempts = latest ? latest.attempts : (claimed.attempts || 0);

      if (isTerminalError(code)) {
        await NotificationDelivery.update({ status: transition('SENDING', 'FAILED'), last_error: err.message }, { where: { id: delivery.id } });
      } else {
        if (attempts >= MAX_ATTEMPTS) {
          // mark failed and publish to DLQ
          await NotificationDelivery.update({ status: transition('SENDING', 'FAILED'), last_error: err.message }, { where: { id: delivery.id } });
          try {
            const notification = await Notification.findByPk(delivery.notification_id);
            const payload = { deliveryId: delivery.id, notificationId: delivery.notification_id, channel: delivery.channel, lastError: err.message, attempts, notificationPayload: notification ? notification.payload : null };
            await send(NOTIFICATIONS_DLQ, [{ key: delivery.id, value: JSON.stringify(payload) }]);
          } catch (e) {
            logger.error('Failed publishing to DLQ', { err: e });
          }
        } else {
          const delay = nextRetryDelay(attempts);
          const nextRetry = new Date(Date.now() + delay);
          await NotificationDelivery.update({ status: transition('SENDING', 'RETRYING'), last_error: err.message, next_retry_at: nextRetry }, { where: { id: delivery.id } });
        }
      }
    }
  }
}

export default BaseWorker;
