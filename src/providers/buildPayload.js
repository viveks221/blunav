/**
 * Map persisted notification payload to provider-specific send() arguments.
 * @param {'EMAIL'|'SMS'|'PUSH'} channel
 * @param {Record<string, unknown>} payload
 */
export function buildProviderRequest(channel, payload) {
  const p = payload && typeof payload === 'object' ? payload : {};
  const label = typeof p.type === 'string' ? p.type : 'notification';

  switch (channel) {
    case 'EMAIL':
      return {
        to: p.email,
        subject: typeof p.subject === 'string' ? p.subject : 'Notification',
        body: typeof p.body === 'string' ? p.body : JSON.stringify({ label, meta: p }),
      };
    case 'SMS':
      return {
        to: p.phone,
        message: typeof p.smsBody === 'string' ? p.smsBody : (typeof p.body === 'string' ? p.body : `Alert: ${label}`),
      };
    case 'PUSH':
      return {
        to: p.deviceToken,
        title: typeof p.title === 'string' ? p.title : 'Notification',
        body: typeof p.pushBody === 'string' ? p.pushBody : (typeof p.body === 'string' ? p.body : String(label)),
      };
    default:
      throw new Error(`Unknown channel: ${channel}`);
  }
}
