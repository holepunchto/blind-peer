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
const DBLock = require('db-lock')
const PassiveWatcher = require('passive-core-watcher')

module.exports = class BlindPeer extends EventEmitter {
  constructor (storage) {
    super()

    this.db = HyperDB.rocks(path.join(storage, 'hyperdb'), definition)
    this.store = new Corestore(path.join(storage, 'corestore'))
    this.swarm = null

    this.passiveWatcher = new PassiveWatcher(this.store, {
      watch: this._isEstablishedMailbox.bind(this),
      open: this._onmailboxcore.bind(this)
    })

    this.passiveWatcher.on('oncoreopen-error', (e) => {
      console.error(`Unexpected oncoreopen error in blind-peer ${e.stack}`)
    })
    this._openLightWriters = new Set()

    this.lock = new DBLock({
      enter: () => {
        return this.db.transaction()
      },
      exit (tx) {
        return tx.flush()
      }
    })
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

    rpc.respond('post-to-mailbox', {
      requestEncoding: schema.resolveStruct('@blind-peer/request-post'),
      responseEncoding: c.none
    }, this._onrpcpost.bind(this))
  }

  async _onrpcadd (req) {
    this.emit('add-mailbox-request', req)
    const res = await this.addMailbox(req)
    this.emit('add-mailbox-response', req, res)

    return {
      writer: res.writer,
      open: !!res.blockEncryptionKey // TODO: get rid of the encryption for these guys with a manifest upgrade, then no attacks cause self-described
    }
  }

  async _onrpcpost (req) {
    this.emit('post-to-mailbox-request', req)
    await this.postToMailbox(req)
    this.emit('post-to-mailbox-response', req)
  }

  async _isEstablishedMailbox (core) {
    if (!core.opened) await core.ready()
    const entry = await this.db.get('@blind-peer/mailbox-by-autobase', { autobase: core.key })
    return entry && entry.blockEncryptionKey
  }

  async _onmailboxcore (weakSession) {
    try {
      const entry = await this.db.get('@blind-peer/mailbox-by-autobase', { autobase: weakSession.key })
      if (weakSession.closing) return

      const lightWriterStore = this.store.namespace(entry.id)
      const w = new AutobaseLightWriter(lightWriterStore, entry.autobase, {
        active: false,
        blockEncryptionKey: entry.blockEncryptionKey
      })
      this._openLightWriters.add(w)

      for (const peer of weakSession.peers) {
        w.local.replicate(peer.stream)
      }

      weakSession.on('peer-add', (peer) => {
        w.local.replicate(peer.stream)
      })

      weakSession.on('close', () => {
        w.close().then(() => this._openLightWriters.delete(w), noop)
      })
      await w.ready()
    } catch (e) {
      console.error(`Unexpectedb blind-peer onmailboxcore error: ${e.stack}`)
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

  async get ({ id }) {
    return await this.db.get('@blind-peer/mailbox', { id })
  }

  async addMailbox ({ id, autobase, blockEncryptionKey = null }) {
    const prev = await this.db.get('@blind-peer/mailbox', { id })

    if (prev) {
      if (prev.blockEncryptionKey) return prev // fully open, immut
      prev.blockEncryptionKey = blockEncryptionKey

      const tx = await this.lock.enter()
      await tx.insert('@blind-peer/mailbox', prev)
      await this.lock.exit()
      await this.passiveWatcher.ensureTracked(prev.autobase)
      return prev
    }

    const w = new AutobaseLightWriter(this.store.namespace(id), autobase, { active: false })
    await w.ready()
    const entry = { id, autobase, writer: w.local.key, blockEncryptionKey }

    const tx = await this.lock.enter()
    await tx.insert('@blind-peer/mailbox', entry)
    await this.lock.exit()
    await this.passiveWatcher.ensureTracked(entry.autobase)

    await w.close()

    return entry
  }

  async postToMailbox ({ id, message }) {
    const entry = await this.db.get('@blind-peer/mailbox', { id })
    if (!entry || !entry.blockEncryptionKey) throw new Error('Autobase not found')

    const w = new AutobaseLightWriter(this.store.namespace(id), entry.autobase, {
      active: false,
      blockEncryptionKey: entry.blockEncryptionKey
    })
    await w.append(message)
    await w.close()
  }

  async close () {
    this.passiveWatcher.destroy()
    if (this.swarm !== null) await this.swarm.destroy()
    await this.db.close()

    await Promise.all([...this._openLightWriters].map(w => w.close()))
    await this.store.close()
  }
}

function noop () {}
