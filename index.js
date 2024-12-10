const { EventEmitter } = require('events')
const AutobaseLightWriter = require('autobase-light-writer')
const HyperDB = require('hyperdb')
const Corestore = require('corestore')
const definition = require('./spec/hyperdb')
const schema = require('./spec/hyperschema')
const path = require('path')
const Hyperswarm = require('hyperswarm')
const ProtomuxRPC = require('protomux-rpc')
const c = require('compact-encoding')

module.exports = class BlindPeer extends EventEmitter {
  constructor (storage) {
    super()

    this.db = HyperDB.rocks(path.join(storage, 'hyperdb'), definition)
    this.store = new Corestore(path.join(storage, 'corestore'))
    this.store.on('core-open', this._oncoreopen.bind(this))
    this.swarm = null
  }

  _onconnection (connection) {
    this.store.replicate(connection)

    const rpc = new ProtomuxRPC(connection, {
      id: this.swarm.keyPair.publicKey,
      valueEncoding: c.none
    })

    rpc.respond('add-mailbox', {
      requestEncoding: schema.resolveStruct('@blind-peer/request-mailbox'),
      responseEncoding: schema.resolveStruct('@blind-peer/response-mailbox')
    }, this._onrpcadd.bind(this))

    rpc.respond('post', {
      requestEncoding: schema.resolveStruct('@blind-peer/request-post'),
      responseEncoding: schema.resolveStruct('@blind-peer/response-post')
    }, this._onrpcpost.bind(this))
  }

  async _onrpcadd (req) {
    this.emit('add-mailbox-received', req)
    const res = await this.add(req)
    this.emit('add-mailbox-handled', req)

    return {
      autobase: res.autobase,
      writer: res.writer,
      open: !!res.blockEncryptionKey // TODO: get rid of the encryption for these guys with a manifest upgrade, then no attacks cause self-described
    }
  }

  async _onrpcpost (req) {
    this.emit('post-received', req)
    const res = await this.post(req)
    this.emit('post-handled', req)

    return res
  }

  async _oncoreopen (core) {
    try {
      const entry = await this.db.get('@blind-peer/mailbox', { autobase: core.key })
      if (!entry || !entry.blockEncryptionKey) return

      const w = new AutobaseLightWriter(this.store.namespace(entry.autobase), entry.autobase, {
        active: false,
        blockEncryptionKey: entry.blockEncryptionKey
      })

      for (const peer of core.peers) {
        w.local.replicate(peer.stream)
      }

      core.on('peer-add', (peer) => {
        w.local.replicate(peer.stream)
      })

      core.on('close', () => {
        w.close().catch(noop)
      })
    } catch (err) {
      console.log(err)
    }
  }

  get publicKey () {
    return this.swarm.server.publicKey
  }

  async listen ({ bootstrap } = {}) {
    this.swarm = new Hyperswarm({
      keyPair: await this.store.createKeyPair('blind-mailbox'),
      bootstrap
    })
    this.swarm.on('connection', this._onconnection.bind(this))
    return this.swarm.listen()
  }

  async get ({ autobase }) {
    return await this.db.get('@blind-peer/mailbox', { autobase })
  }

  async add ({ autobase, blockEncryptionKey = null }) {
    const prev = await this.db.get('@blind-peer/mailbox', { autobase })

    if (prev) {
      if (prev.blockEncryptionKey) return prev
      prev.blockEncryptionKey = blockEncryptionKey
      await this.db.insert('@blind-peer/mailbox', prev)
      await this.db.flush()
      return prev
    }

    const w = new AutobaseLightWriter(this.store.namespace(autobase), autobase, { active: false })
    await w.ready()
    const entry = { autobase, writer: w.local.key, blockEncryptionKey }
    await this.db.insert('@blind-peer/mailbox', entry)
    await this.db.flush()
    await w.close()

    return entry
  }

  async post ({ autobase, message }) {
    const entry = await this.db.get('@blind-peer/mailbox', { autobase })
    if (!entry || !entry.blockEncryptionKey) return false

    const w = new AutobaseLightWriter(this.store.namespace(autobase), autobase, {
      active: false,
      blockEncryptionKey: entry.blockEncryptionKey
    })
    await w.append(message)
    const length = w.local.length
    await w.close()

    return { length }
  }

  async close () {
    if (this.swarm !== null) await this.swarm.destroy()
    await this.store.close()
  }
}

function noop () {}
