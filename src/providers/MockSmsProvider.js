import BaseProvider from './BaseProvider.js';

class MockSmsProvider extends BaseProvider {
  constructor() {
    super('mock-sms');
  }

  async send({ to, message }) {
    return { ok: true, id: `sms_${Date.now()}` };
  }
}

export default MockSmsProvider;
