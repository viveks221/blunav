import { DataTypes } from 'sequelize';
import sequelize from '../db/connection.js';
import NotificationModel from './Notification.js';
import NotificationDeliveryModel from './NotificationDelivery.js';
import NotificationOutboxModel from './NotificationOutbox.js';

const Notification = NotificationModel(sequelize, DataTypes);
const NotificationDelivery = NotificationDeliveryModel(sequelize, DataTypes);
const NotificationOutbox = NotificationOutboxModel(sequelize, DataTypes);

Notification.hasMany(NotificationDelivery, { foreignKey: 'notification_id', as: 'deliveries' });
NotificationDelivery.belongsTo(Notification, { foreignKey: 'notification_id', as: 'notification' });

export default { sequelize, Notification, NotificationDelivery, NotificationOutbox };
