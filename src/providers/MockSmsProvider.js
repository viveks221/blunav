import BaseProvider from './BaseProvider.js';

class MockSmsProvider extends BaseProvider {
  constructor() {
    super('mock-sms');
  }

  async send({ to: _to, message: _message }) {
    return { ok: true, id: `sms_${Date.now()}` };
  }
}

export default MockSmsProvider;
