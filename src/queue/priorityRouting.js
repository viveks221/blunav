import { NOTIFICATIONS_HIGH, NOTIFICATIONS_LOW } from './topics.js';

/** Route notification priority to Kafka topic (two-lane throughput model). */
export function topicForNotificationPriority(priority) {
  switch (priority) {
    case 'CRITICAL':
    case 'HIGH':
    case 'TRANSACTIONAL':
      return NOTIFICATIONS_HIGH;
    case 'MARKETING':
    case 'LOW':
    default:
      return NOTIFICATIONS_LOW;
  }
}
