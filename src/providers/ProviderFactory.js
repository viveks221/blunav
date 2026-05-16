import MockEmailProvider from './MockEmailProvider.js';
import MockSmsProvider from './MockSmsProvider.js';
import MockPushProvider from './MockPushProvider.js';
import EmailProvider from './EmailProvider.js';

function getProvider(channel) {
  switch (channel) {
    case 'EMAIL':
      // Use real EmailProvider when configured or fall back to mock for tests/dev
      try {
        return new EmailProvider();
      } catch (e) {
        return new MockEmailProvider();
      }
    case 'SMS':
      return new MockSmsProvider();
    case 'PUSH':
      return new MockPushProvider();
    default:
      throw new Error(`Unknown provider channel: ${channel}`);
  }
}

export { getProvider };
