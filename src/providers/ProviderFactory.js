import MockEmailProvider from './MockEmailProvider.js';
import MockSmsProvider from './MockSmsProvider.js';
import MockPushProvider from './MockPushProvider.js';

function getProvider(channel) {
  switch (channel) {
    case 'EMAIL':
      return new MockEmailProvider();
    case 'SMS':
      return new MockSmsProvider();
    case 'PUSH':
      return new MockPushProvider();
    default:
      throw new Error(`Unknown provider channel: ${channel}`);
  }
}

export { getProvider };
