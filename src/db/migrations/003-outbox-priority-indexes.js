/**
 * - Extend notification priority enum (Postgres).
 * - Transactional outbox for Kafka publish reliability.
 * - Indexes for retry poller and QUEUED watchdog.
 */

export async function up(queryInterface, Sequelize) {
  const qi = queryInterface;
  const sequelize = qi.sequelize;

  for (const label of ['CRITICAL', 'TRANSACTIONAL', 'MARKETING']) {
    await sequelize.query(`
      DO $$ BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM pg_enum e
          JOIN pg_type t ON e.enumtypid = t.oid
          WHERE t.typname = 'enum_notifications_priority' AND e.enumlabel = '${label}'
        ) THEN
          ALTER TYPE "enum_notifications_priority" ADD VALUE '${label}';
        END IF;
      END $$;
    `);
  }

  await qi.createTable('notification_outbox', {
    id: {
      type: Sequelize.UUID,
      primaryKey: true,
      allowNull: false,
    },
    event_id: {
      type: Sequelize.UUID,
      allowNull: false,
      unique: true,
    },
    topic: { type: Sequelize.STRING(128), allowNull: false },
    envelope: { type: Sequelize.JSONB, allowNull: false },
    status: {
      type: Sequelize.STRING(24),
      allowNull: false,
      defaultValue: 'pending',
    },
    publish_attempts: { type: Sequelize.INTEGER, allowNull: false, defaultValue: 0 },
    next_publish_at: { type: Sequelize.DATE, allowNull: true },
    last_error: { type: Sequelize.TEXT, allowNull: true },
    created_at: { type: Sequelize.DATE, allowNull: false, defaultValue: Sequelize.literal('NOW()') },
    updated_at: { type: Sequelize.DATE, allowNull: false, defaultValue: Sequelize.literal('NOW()') },
  });

  await qi.addIndex('notification_outbox', ['status', 'created_at'], {
    name: 'notification_outbox_status_created_idx',
  });

  await sequelize.query(`
    CREATE INDEX IF NOT EXISTS notification_deliveries_retry_poll_idx
    ON notification_deliveries (status, next_retry_at)
    WHERE status = 'RETRYING';
  `);

  await sequelize.query(`
    CREATE INDEX IF NOT EXISTS notification_deliveries_queued_stale_idx
    ON notification_deliveries (status, updated_at)
    WHERE status = 'QUEUED';
  `);
}

export async function down(queryInterface) {
  const sequelize = queryInterface.sequelize;
  await sequelize.query('DROP INDEX IF EXISTS notification_deliveries_queued_stale_idx;');
  await sequelize.query('DROP INDEX IF EXISTS notification_deliveries_retry_poll_idx;');
  await queryInterface.dropTable('notification_outbox');
}
