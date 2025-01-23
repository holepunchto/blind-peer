const c = require('compact-encoding')
const ProtomuxRPC = require('protomux-rpc')
const { addMailboxEncoding, postEncoding } = require('blind-peer-encodings')

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
