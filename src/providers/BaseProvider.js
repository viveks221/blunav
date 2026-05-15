class BaseProvider {
  constructor(name) {
    this.name = name;
  }

  async send(_payload) {
    throw new Error('send() not implemented');
  }
}

export default BaseProvider;
