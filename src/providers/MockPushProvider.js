import BaseProvider from './BaseProvider.js';

class MockPushProvider extends BaseProvider {
  constructor() {
    super('mock-push');
  }

  async send({ to: _to, title: _title, body: _body }) {
    return { ok: true, id: `push_${Date.now()}` };
  }
}

export default MockPushProvider;
