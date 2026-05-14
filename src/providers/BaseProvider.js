class BaseProvider {
  constructor(name) {
    this.name = name;
  }

  async send(payload) {
    throw new Error('send() not implemented');
  }
}

export default BaseProvider;
