export default (sequelize, DataTypes) => {
  return sequelize.define('Notification', {
    id: { type: DataTypes.UUID, primaryKey: true, allowNull: false },
    type: { type: DataTypes.STRING(128), allowNull: false },
    priority: { type: DataTypes.ENUM('HIGH', 'LOW'), allowNull: false, defaultValue: 'LOW' },
    payload: { type: DataTypes.JSONB, allowNull: false },
  }, {
    tableName: 'notifications',
    underscored: true,
    timestamps: true,
    createdAt: 'created_at',
    updatedAt: 'updated_at',
  });
};
