const schema = require('./spec/hyperschema')
const c = require('compact-encoding')
const ProtomuxRPC = require('protomux-rpc')

const addMailboxEncoding = {
  requestEncoding: schema.resolveStruct('@blind-peer/request-mailbox'),
  responseEncoding: schema.resolveStruct('@blind-peer/response-mailbox')
}

const postEncoding = {
  requestEncoding: schema.resolveStruct('@blind-peer/request-post'),
  responseEncoding: c.none
}

module.exports = class BlindPeerClient {
  constructor (stream) {
    this.stream = stream
    this.rpc = new ProtomuxRPC(stream, {
      id: stream.remotePublicKey,
      valueEncoding: c.none
    })
  }

  async close () {
    await this.rpc.end()
  }

  addMailbox (data) {
    return this.rpc.request('add-mailbox', data, addMailboxEncoding)
  }

  postToMailbox ({ id, message }) {
    return this.rpc.request('post-to-mailbox', { id, message }, postEncoding)
  }
}
