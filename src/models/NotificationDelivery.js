export default (sequelize, DataTypes) => {
  return sequelize.define('NotificationDelivery', {
    id: { type: DataTypes.UUID, primaryKey: true, allowNull: false },
    notification_id: { type: DataTypes.UUID, allowNull: false },
    channel: { type: DataTypes.ENUM('EMAIL', 'SMS', 'PUSH'), allowNull: false },
    status: { type: DataTypes.STRING(32), allowNull: false, defaultValue: 'PENDING' },
    attempts: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 0 },
    last_error: { type: DataTypes.TEXT, allowNull: true },
    next_retry_at: { type: DataTypes.DATE, allowNull: true },
  }, {
    tableName: 'notification_deliveries',
    underscored: true,
    timestamps: true,
    createdAt: 'created_at',
    updatedAt: 'updated_at',
  });
};
