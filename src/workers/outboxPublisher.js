import { Op } from 'sequelize';
import { fileURLToPath } from 'url';
import models from '../models/index.js';
import { send, getProducer, disconnectProducer } from '../queue/producer.js';
import logger from '../logger/index.js';

const BATCH = parseInt(process.env.OUTBOX_PUBLISH_BATCH || '50', 10);
const POLL_MS = parseInt(process.env.OUTBOX_POLL_INTERVAL_MS || '500', 10);
const STALE_PROCESSING_MS = parseInt(process.env.OUTBOX_STALE_PROCESSING_MS || String(5 * 60 * 1000), 10);
const MAX_PUBLISH_ATTEMPTS = parseInt(process.env.OUTBOX_MAX_PUBLISH_ATTEMPTS || '25', 10);

function backoffMs(attempt) {
  return Math.min(120_000, 2000 * Math.pow(2, Math.min(attempt, 6)));
}

async function resetStaleProcessing() {
  const { NotificationOutbox } = models;
  const threshold = new Date(Date.now() - STALE_PROCESSING_MS);
  const [n] = await NotificationOutbox.update(
    { status: 'pending', last_error: 'stale_processing_reset' },
    { where: { status: 'processing', updated_at: { [Op.lt]: threshold } } },
  );
  if (n > 0) {
    logger.warn('Outbox: reset stale processing rows', { count: n });
  }
}

/**
 * Claim a batch of outbox rows and publish each to Kafka.
 * @returns {Promise<number>} number of rows published this tick
 */
export async function publishOnce() {
  const { sequelize, NotificationOutbox } = models;
  await resetStaleProcessing();

  const now = new Date();
  const t = await sequelize.transaction();
  let rows;
  try {
    rows = await NotificationOutbox.findAll({
      where: {
        [Op.or]: [
          { status: 'pending' },
          {
            status: 'failed',
            publish_attempts: { [Op.lt]: MAX_PUBLISH_ATTEMPTS },
            [Op.or]: [
              { next_publish_at: null },
              { next_publish_at: { [Op.lte]: now } },
            ],
          },
        ],
      },
      order: [['created_at', 'ASC']],
      limit: BATCH,
      lock: t.LOCK.UPDATE,
      skipLocked: true,
      transaction: t,
    });

    if (!rows.length) {
      await t.commit();
      return 0;
    }
    logger.info('Outbox claim', { count: rows.length, eventIds: rows.map((r) => r.event_id) });

    await NotificationOutbox.update(
      { status: 'processing' },
      { where: { id: rows.map((r) => r.id) }, transaction: t },
    );
    await t.commit();
  } catch (err) {
    try {
      await t.rollback();
    } catch { /* noop */ }
    logger.error('Outbox publish transaction failed', { err: err.message });
    return 0;
  }

  let published = 0;
  for (const row of rows) {
    try {
      await send(row.topic, [{ key: row.event_id, value: JSON.stringify(row.envelope) }]);
      await NotificationOutbox.update(
        { status: 'published', last_error: null, next_publish_at: null },
        { where: { id: row.id } },
      );
      published += 1;
      logger.info('Outbox published', { eventId: row.event_id, topic: row.topic });
    } catch (err) {
      const attempts = row.publish_attempts + 1;
      const delay = backoffMs(attempts);
      const nextAt = new Date(Date.now() + delay);
      await NotificationOutbox.update(
        {
          status: attempts >= MAX_PUBLISH_ATTEMPTS ? 'abandoned' : 'failed',
          publish_attempts: attempts,
          last_error: err.message,
          next_publish_at: attempts >= MAX_PUBLISH_ATTEMPTS ? null : nextAt,
        },
        { where: { id: row.id } },
      );
      logger.error('Outbox publish failed', { eventId: row.event_id, err: err.message, attempts });
    }
  }

  return published;
}

export async function start() {
  logger.info('Outbox publisher starting', { pollMs: POLL_MS, batch: BATCH });
  await getProducer();
  let running = true;

  const tick = async () => {
    while (running) {
      try {
        await publishOnce();
      } catch (err) {
        logger.error('Outbox publisher tick error', { err: err.message });
      }
      await new Promise((r) => setTimeout(r, POLL_MS));
    }
  };

  const loop = tick();
  loop.catch((err) => logger.error('Outbox publisher loop died', { err: err.message }));

  const shutdown = async () => {
    running = false;
    logger.info('Outbox publisher shutting down');
    try {
      await disconnectProducer();
    } catch (e) {
      logger.warn('Outbox publisher producer disconnect', { err: e.message });
    }
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  return { stop: () => { running = false; } };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  start().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
