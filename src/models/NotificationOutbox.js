export default (sequelize, DataTypes) => sequelize.define('NotificationOutbox', {
  id: { type: DataTypes.UUID, primaryKey: true, allowNull: false },
  event_id: { type: DataTypes.UUID, allowNull: false, unique: true },
  topic: { type: DataTypes.STRING(128), allowNull: false },
  envelope: { type: DataTypes.JSONB, allowNull: false },
  status: { type: DataTypes.STRING(24), allowNull: false, defaultValue: 'pending' },
  publish_attempts: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 0 },
  next_publish_at: { type: DataTypes.DATE, allowNull: true },
  last_error: { type: DataTypes.TEXT, allowNull: true },
}, {
  tableName: 'notification_outbox',
  underscored: true,
  timestamps: true,
  createdAt: 'created_at',
  updatedAt: 'updated_at',
});
