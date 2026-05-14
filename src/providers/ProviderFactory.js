import MockEmailProvider from './MockEmailProvider.js';
import MockSmsProvider from './MockSmsProvider.js';

function getProvider(channel) {
  switch (channel) {
    case 'EMAIL':
      return new MockEmailProvider();
    case 'SMS':
      return new MockSmsProvider();
    default:
      throw new Error(`Unknown provider channel: ${channel}`);
  }
}

export { getProvider };
