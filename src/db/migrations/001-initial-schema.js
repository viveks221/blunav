export async function up(queryInterface, Sequelize) {
  await queryInterface.createTable('notifications', {
    id: {
      type: Sequelize.UUID,
      primaryKey: true,
      allowNull: false,
    },
    type: { type: Sequelize.STRING(128), allowNull: false },
    priority: { type: Sequelize.ENUM('HIGH', 'LOW'), allowNull: false, defaultValue: 'LOW' },
    payload: { type: Sequelize.JSONB, allowNull: false },
    created_at: { type: Sequelize.DATE, allowNull: false, defaultValue: Sequelize.literal('NOW()') },
    updated_at: { type: Sequelize.DATE, allowNull: false, defaultValue: Sequelize.literal('NOW()') },
  });

  await queryInterface.createTable('notification_deliveries', {
    id: {
      type: Sequelize.UUID,
      primaryKey: true,
      allowNull: false,
    },
    notification_id: {
      type: Sequelize.UUID,
      allowNull: false,
      references: { model: 'notifications', key: 'id' },
      onDelete: 'CASCADE',
    },
    channel: { type: Sequelize.ENUM('EMAIL', 'SMS', 'PUSH'), allowNull: false },
    status: { type: Sequelize.STRING(32), allowNull: false, defaultValue: 'PENDING' },
    attempts: { type: Sequelize.INTEGER, allowNull: false, defaultValue: 0 },
    last_error: { type: Sequelize.TEXT, allowNull: true },
    next_retry_at: { type: Sequelize.DATE, allowNull: true },
    created_at: { type: Sequelize.DATE, allowNull: false, defaultValue: Sequelize.literal('NOW()') },
    updated_at: { type: Sequelize.DATE, allowNull: false, defaultValue: Sequelize.literal('NOW()') },
  });
}

export async function down(queryInterface, Sequelize) {
  await queryInterface.dropTable('notification_deliveries');
  await queryInterface.dropTable('notifications');
  await queryInterface.sequelize.query('DROP TYPE IF EXISTS "enum_notifications_priority"');
  await queryInterface.sequelize.query('DROP TYPE IF EXISTS "enum_notification_deliveries_channel"');
}
