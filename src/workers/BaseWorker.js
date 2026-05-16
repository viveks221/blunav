import { v4 as uuidv4 } from 'uuid';
import { Op } from 'sequelize';
import { getProvider } from '../providers/ProviderFactory.js';
import { buildProviderRequest } from '../providers/buildPayload.js';
import logger from '../logger/index.js';
import { transition } from '../core/stateMachine.js';
import { isTerminalError, nextRetryDelay } from '../core/retryPolicy.js';
import models from '../models/index.js';
import { send } from '../queue/producer.js';
import { NOTIFICATIONS_DLQ } from '../queue/topics.js';
import { createCircuitBreaker } from '../core/circuitBreaker.js';

const { sequelize, Notification, NotificationDelivery } = models;

const circuitBreaker = createCircuitBreaker();

const DEFAULT_CHANNELS = ['EMAIL', 'SMS'];

function channelsFromPayload(payload) {
  if (payload && Array.isArray(payload.channels) && payload.channels.length > 0) {
    return [...new Set(payload.channels)];
  }
  return DEFAULT_CHANNELS;
}

class BaseWorker {
  constructor() {}

  /**
   * Create or load notification + per-channel deliveries, then attempt sends.
   * Postgres is source of truth; Kafka may redeliver — unique (notification_id, channel) prevents dup rows.
   */
  async processEvent(event) {
    const eventId = event.eventId;
    if (!eventId) {
      logger.error('Event missing eventId', { event });
      return;
    }
    logger.info('Processing event', { eventId, type: event.type, priority: event.priority });

    const priority = event.priority || 'LOW';
    const type = event.type || 'unknown';
    const payload = event.payload && typeof event.payload === 'object' ? { ...event.payload } : {};
    if (event.source) {
      payload.source_service = event.source;
    }

    const [notification] = await Notification.findOrCreate({
      where: { id: eventId },
      defaults: {
        id: eventId,
        type,
        priority,
        payload,
      },
    });

    await notification.update({ type, priority, payload });

    const channels = channelsFromPayload(payload);
    logger.debug('Preparing deliveries', { eventId, channels });
    for (const ch of channels) {
      try {
        await NotificationDelivery.findOrCreate({
          where: { notification_id: eventId, channel: ch },
          defaults: {
            id: uuidv4(),
            status: 'PENDING',
          },
        });
        logger.info('Delivery created/ensured', { eventId, channel: ch });
      } catch (e) {
        logger.warn('Delivery findOrCreate race', { eventId, channel: ch, err: e.message });
      }
    }

    const deliveries = await NotificationDelivery.findAll({
      where: {
        notification_id: eventId,
        status: { [Op.in]: ['PENDING', 'QUEUED', 'RETRYING'] },
      },
    });
    logger.info('Deliveries to process', { eventId, count: deliveries.length });

    for (const d of deliveries) {
      logger.debug('Dispatching delivery', { deliveryId: d.id, notificationId: d.notification_id, channel: d.channel });
      await this.sendDelivery(d);
    }
  }

  /** Handle Kafka message `{ kind: 'DELIVERY_RETRY', deliveryId }` from retry poller. */
  async processDeliveryRetry(deliveryId) {
    const delivery = await NotificationDelivery.findByPk(deliveryId);
    if (!delivery) {
      logger.warn('DELIVERY_RETRY: delivery not found', { deliveryId });
      return;
    }
    if (!['PENDING', 'QUEUED', 'RETRYING'].includes(delivery.status)) {
      logger.info('DELIVERY_RETRY: skip non-actionable status', { deliveryId, status: delivery.status });
      return;
    }
    await this.sendDelivery(delivery);
  }

  async sendDelivery(delivery) {
    const fresh = await NotificationDelivery.findByPk(delivery.id);
    if (!fresh || ['SENT', 'FAILED'].includes(fresh.status)) {
      return;
    }

    const provider = getProvider(fresh.channel);
    const providerKey = `${provider.name || provider.constructor.name}:${fresh.channel}`;
    const MAX_ATTEMPTS = parseInt(process.env.MAX_DELIVERY_ATTEMPTS || '3', 10);

    if (await circuitBreaker.isOpen(providerKey)) {
      const nextRetry = new Date(Date.now() + circuitBreaker.getOpenDurationMs());
      try {
        const nextStatus = transition(fresh.status, 'RETRYING');
        await NotificationDelivery.update(
          {
            status: nextStatus,
            last_error: 'circuit_open',
            next_retry_at: nextRetry,
          },
          { where: { id: fresh.id, status: fresh.status } },
        );
      } catch (e) {
        logger.error('STATE_MACHINE circuit_open', { deliveryId: fresh.id, from: fresh.status, err: e.message });
      }
      logger.warn('Circuit open for provider, skipping send', { providerKey, deliveryId: fresh.id });
      return;
    }

    const t = await sequelize.transaction();
    let claimed;
    try {
      logger.info('Attempting to claim delivery', { deliveryId: fresh.id });
      const [count, rows] = await NotificationDelivery.update(
        { status: 'SENDING', attempts: sequelize.literal('attempts + 1') },
        {
          where: {
            id: fresh.id,
            status: { [Op.in]: ['PENDING', 'QUEUED', 'RETRYING'] },
          },
          returning: true,
          transaction: t,
        },
      );

      if (!count || !rows || rows.length === 0) {
        await t.commit();
        logger.info('Delivery already claimed or finished', { deliveryId: fresh.id });
        return;
      }

      claimed = rows[0];
      logger.info('Delivery claimed', { deliveryId: claimed.id, previousStatus: fresh.status, attempts: claimed.attempts });
      await t.commit();
    } catch (e) {
      try {
        await t.rollback();
      } catch { /* noop */ }
      logger.error('Failed to claim delivery', { deliveryId: fresh.id, err: e.message });
      return;
    }

    const notification = await Notification.findByPk(fresh.notification_id);
    const providerPayload = buildProviderRequest(fresh.channel, notification ? notification.payload : {});

    try {
      logger.info('Calling provider.send', { deliveryId: fresh.id, provider: providerKey, payloadPreview: providerPayload && Object.keys(providerPayload).slice(0,5) });
      const res = await provider.send(providerPayload);
      logger.info('Provider send success', { deliveryId: fresh.id, provider: providerKey, providerResponse: res });
      try {
        const nextStatus = transition('SENDING', 'SENT');
        const [cnt] = await NotificationDelivery.update(
          { status: nextStatus },
          { where: { id: fresh.id, status: 'SENDING' } },
        );
        if (!cnt) {
          logger.warn('Race: delivery not in SENDING when marking SENT', { deliveryId: fresh.id });
        }
      } catch (e) {
        logger.error('STATE_MACHINE SENDING->SENT', { deliveryId: fresh.id, err: e.message });
      }
      try {
        await circuitBreaker.recordSuccess(providerKey);
      } catch (e) {
        logger.warn('Circuit breaker success record failed', { err: e });
      }
      logger.info('Delivery sent', { id: fresh.id, providerResponse: res });
    } catch (err) {
      logger.error('Provider send failed', { deliveryId: fresh.id, provider: providerKey, err: err.message });
      try {
        await circuitBreaker.recordFailure(providerKey);
      } catch (e) {
        logger.warn('Circuit breaker failure record failed', { err: e });
      }

      const code = err.code || 'UNKNOWN_ERROR';
      let latest;
      try {
        latest = await NotificationDelivery.findByPk(fresh.id);
      } catch (e) {
        logger.error('Failed to fetch delivery after error', { deliveryId: fresh.id, err: e.message });
      }
      const attempts = latest ? latest.attempts : (claimed.attempts || 0);

      if (isTerminalError(code)) {
        try {
          const nextStatus = transition('SENDING', 'FAILED');
          await NotificationDelivery.update(
            { status: nextStatus, last_error: err.message },
            { where: { id: fresh.id, status: 'SENDING' } },
          );
        } catch (e) {
          logger.error('STATE_MACHINE SENDING->FAILED', { deliveryId: fresh.id, err: e.message });
        }
      } else if (attempts >= MAX_ATTEMPTS) {
        try {
          const nextStatus = transition('SENDING', 'FAILED');
          await NotificationDelivery.update(
            { status: nextStatus, last_error: err.message },
            { where: { id: fresh.id, status: 'SENDING' } },
          );
        } catch (e) {
          logger.error('STATE_MACHINE SENDING->FAILED max attempts', { deliveryId: fresh.id, err: e.message });
        }
        try {
          const payload = {
            deliveryId: fresh.id,
            notificationId: fresh.notification_id,
            channel: fresh.channel,
            lastError: err.message,
            attempts,
            notificationPayload: notification ? notification.payload : null,
          };
          const dlqRes = await send(NOTIFICATIONS_DLQ, [{ key: fresh.id, value: JSON.stringify(payload) }]);
          logger.info('Published to DLQ', { deliveryId: fresh.id, dlqResult: dlqRes });
        } catch (e) {
          logger.error('Failed publishing to DLQ', { err: e });
        }
      } else {
        try {
          const delay = nextRetryDelay(attempts);
          const nextRetry = new Date(Date.now() + delay);
          const nextStatus = transition('SENDING', 'RETRYING');
          await NotificationDelivery.update(
            { status: nextStatus, last_error: err.message, next_retry_at: nextRetry },
            { where: { id: fresh.id, status: 'SENDING' } },
          );
          logger.info('Scheduled retry for delivery', { deliveryId: fresh.id, attempts, nextRetryAt: nextRetry });
        } catch (e) {
          logger.error('STATE_MACHINE SENDING->RETRYING', { deliveryId: fresh.id, err: e.message });
        }
      }
    }
  }
}

export default BaseWorker;
