export async function up(queryInterface) {
  await queryInterface.addIndex('notification_deliveries', ['notification_id', 'channel'], {
    unique: true,
    name: 'notification_deliveries_notification_id_channel_uidx',
  });
}

export async function down(queryInterface) {
  await queryInterface.removeIndex('notification_deliveries', 'notification_deliveries_notification_id_channel_uidx');
}
