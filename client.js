const schema = require('./spec/hyperschema')
const c = require('compact-encoding')
const ProtomuxRPC = require('protomux-rpc')

const addMailboxEncoding = {
  requestEncoding: schema.resolveStruct('@blind-mailbox/request-mailbox'),
  responseEncoding: schema.resolveStruct('@blind-mailbox/response-mailbox')
}

const postEncoding = {
  requestEncoding: schema.resolveStruct('@blind-mailbox/request-post'),
  responseEncoding: schema.resolveStruct('@blind-mailbox/response-post')
}

module.exports = class BlindPeerClient {
  constructor (stream) {
    this.stream = stream
    this.rpc = new ProtomuxRPC(stream, {
      id: stream.remotePublicKey,
      valueEncoding: c.none
    })
  }

  addMailbox (data) {
    return this.rpc.request('add-mailbox', data, addMailboxEncoding)
  }

  post ({ autobase, message }) {
    return this.rpc.request('post', { autobase, message }, postEncoding)
  }
}
