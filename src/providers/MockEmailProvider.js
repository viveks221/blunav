import BaseProvider from './BaseProvider.js';

class MockEmailProvider extends BaseProvider {
  constructor() {
    super('mock-email');
  }

  async send({ to: _to, subject: _subject, body: _body }) {
    // simulate delivery
    return { ok: true, id: `msg_${Date.now()}` };
  }
}

export default MockEmailProvider;
