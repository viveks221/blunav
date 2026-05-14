import BaseProvider from './BaseProvider.js';

class MockEmailProvider extends BaseProvider {
  constructor() {
    super('mock-email');
  }

  async send({ to, subject, body }) {
    // simulate delivery
    return { ok: true, id: `msg_${Date.now()}` };
  }
}

export default MockEmailProvider;
