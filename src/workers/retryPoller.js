import logger from '../logger/index.js';
import models from '../models/index.js';
import { Op } from 'sequelize';
import { fileURLToPath } from 'url';
import * as producer from '../queue/producer.js';
import { topicForNotificationPriority } from '../queue/priorityRouting.js';

const { sequelize, NotificationDelivery, Notification } = models;

const POLL_INTERVAL_MS = parseInt(process.env.RETRY_POLLER_INTERVAL_MS, 10) || 5000;
const BATCH_SIZE = parseInt(process.env.RETRY_POLLER_BATCH_SIZE, 10) || 100;
const QUEUED_STALE_MINUTES = parseInt(process.env.QUEUED_STALE_MINUTES || '15', 10);

// Poll once and claim rows using SELECT ... FOR UPDATE SKIP LOCKED to avoid races
async function pollOnce() {
  const now = new Date();
  const tx = await sequelize.transaction();
  try {
    const deliveries = await NotificationDelivery.findAll({
      where: { status: 'RETRYING', next_retry_at: { [Op.lte]: now } },
      include: [{ model: Notification, as: 'notification' }],
      limit: BATCH_SIZE,
      lock: tx.LOCK.UPDATE,
      skipLocked: true,
      transaction: tx,
    });

    if (!deliveries.length) {
      await tx.commit();
      return;
    }

    // mark as QUEUED within the same transaction so other pollers won't see them
    const ids = deliveries.map(d => d.id);
    await NotificationDelivery.update(
      { status: 'QUEUED', next_retry_at: null },
      { where: { id: ids }, transaction: tx }
    );

    await tx.commit();

    // outside transaction, enqueue them
    for (const del of deliveries) {
      try {
        const notif = del.notification || await Notification.findByPk(del.notification_id);
        const topic = topicForNotificationPriority(notif?.priority || 'LOW');
        const envelope = { kind: 'DELIVERY_RETRY', deliveryId: del.id };
        await producer.send(topic, [{ key: del.id, value: JSON.stringify(envelope) }]);
        logger.info('Re-enqueued delivery to Kafka', { deliveryId: del.id, topic });
      } catch (err) {
        logger.error('Failed to re-enqueue delivery after claiming', { deliveryId: del.id, err: err.message });
        // if enqueue fails, set status back to RETRYING so it will be retried later
        try {
          await NotificationDelivery.update({ status: 'RETRYING' }, { where: { id: del.id } });
        } catch (e) {
          logger.error('Failed to reset delivery to RETRYING', { deliveryId: del.id, err: e.message });
        }
      }
    }
  } catch (err) {
    try { await tx.rollback(); } catch (e) { logger.error('Failed to rollback transaction', { err: e.message }); }
    logger.error('Retry poller transaction failed', { err: err.message });
  }
}

/** Reclaim deliveries stuck in QUEUED (e.g. Kafka publish succeeded but consumer never ran). */
async function reclaimStaleQueued() {
  const threshold = new Date(Date.now() - QUEUED_STALE_MINUTES * 60 * 1000);
  const [n] = await NotificationDelivery.update(
    {
      status: 'RETRYING',
      last_error: 'queued_watchdog_reclaim',
      next_retry_at: new Date(),
    },
    {
      where: {
        status: 'QUEUED',
        updated_at: { [Op.lt]: threshold },
      },
    },
  );
  if (n > 0) {
    logger.warn('QUEUED watchdog reclaimed stale rows', { count: n, staleMinutes: QUEUED_STALE_MINUTES });
  }
}

async function start() {
  logger.info('Starting retry poller', { intervalMs: POLL_INTERVAL_MS });
  // ensure producer connected
  await producer.getProducer();
  let running = true;

  const loop = async () => {
    while (running) {
      try {
        await reclaimStaleQueued();
        await pollOnce();
      } catch (err) {
        logger.error('Retry poller error', { err: err.message });
      }
      await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
    }
  };

  loop().catch(err => logger.error('Retry poller loop failed', { err: err.message }));

  const stop = () => { running = false; };
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  start().catch(err => {
    console.error(err);
    process.exit(1);
  });
}

export { start, pollOnce, reclaimStaleQueued };
