const Corestore = require('corestore')
const Hypercore = require('hypercore')
const { OplogMessage } = require('autobase/lib/messages.js')
const RocksDB = require('rocksdb-native')
const ReadyResource = require('ready-resource')
const Hyperswarm = require('hyperswarm')
const ProtomuxRPC = require('protomux-rpc')
const c = require('compact-encoding')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const safetyCatch = require('safety-catch')
const schema = require('./spec/hyperschema')
const BlindPeerDB = require('./lib/db.js')

const AddCoreRequest = schema.getEncoding('@blind-peer/add-core-request')
const PostToMailboxRequest = schema.getEncoding('@blind-peer/post-to-mailbox-request')
const WakeupReply = schema.getEncoding('@blind-peer/wakeup-reply')
const Mailbox = schema.getEncoding('@blind-peer/mailbox')

class CoreWakeup {
  constructor (tracker) {
    this.core = tracker.core
    this.blindPeer = tracker.blindPeer
    this.db = tracker.blindPeer.db
    this.id = null
    this.extension = null
    this.start().catch(safetyCatch)
  }

  async _getLatestWakeup () {
    const referrer = this.core.key

    const query = {
      gte: { referrer },
      lte: { referrer },
      reverse: true,
      limit: 24
    }

    const latest = await this.db.find('@blind-peer/cores-by-referrer', query).toArray()
    return encodeWakeup(latest)
  }

  destroy () {
    if (this.destroyed) return
    this.destroyed = true

    if (this.id && this.blindPeer.activeWakeup.get(this.id) === this) {
      this.blindPeer.activeWakeup.delete(this.id)
    }
  }

  send (record, channel) {
    if (!this.extension) return

    const w = encodeWakeup([record])
    for (const peer of this.core.peers) {
      const mux = peer.stream.userData

      // already replicating with that peer
      if (mux._infos.get(channel)) {
        continue
      }

      this.extension.send(w, peer)
    }
  }

  async start () {
    // TODO: move this to the new general wakeup extension...
    this.extension = this.core.registerExtension('autobase', { onmessage: noop })

    await this.core.ready()
    if (this.destroyed) return

    this.id = this.core.id
    this.blindPeer.activeWakeup.set(this.id, this)

    this.core.on('peer-add', async (peer) => {
      try {
        const w = await this._getLatestWakeup()
        if (peer.removed) return
        this.extension.send(w, peer)
      } catch (e) { safetyCatch(e) }
    })

    if (!this.core.peers.length) return

    const w = await this._getLatestWakeup()
    for (const peer of this.core.peers) {
      this.extension.send(w, peer)
    }
  }
}

class CoreTracker {
  constructor (blindPeer, core) {
    this.blindPeer = blindPeer
    this.core = core
    this.referrer = null
    this.destroyed = false
    this.record = null
    this.wakeup = null
    this.activated = false
    this.updated = false
    this.id = null
    this.referrerId = null
    this.channel = null

    const onupdate = this._onupdate.bind(this)
    const onactive = this._onactive.bind(this)

    this.core.on('upload', onactive)
    this.core.on('download', onactive)
    this.core.on('truncate', onupdate)
    this.core.on('append', onupdate)
  }

  _onupdate () {
    this.updated = true

    if (this.record) {
      this.record.length = this.core.length
      this.record.bytesAllocated = this.core.byteLength
      this.blindPeer.db.updateCore(this.record, this.id)
      this.blindPeer._flushBackground()

      if (this.referrer) {
        const w = this.blindPeer.activeWakeup.get(this.referrer.id)
        if (w) w.send(this.record, this.channel)
      }
    }
  }

  _onactive () {
    this.activated = true

    if (this.record) {
      this.blindPeer.db.updateCore(this.record, this.id)
    }
  }

  async _onrecord () {
    this.core.download({ start: 0, end: -1 })
    if (!this.record.referrer) return

    // this triggers the referrer tracker to open, if not already
    this.referrer = this.blindPeer.store.get({ key: this.record.referrer, active: false })
    await this.referrer.ready()

    const referrerDkey = b4a.toString(this.referrer.discoveryKey, 'hex')
    const tracker = this.blindPeer.activeReplication.get(referrerDkey)
    if (tracker) tracker.startWakeup()
  }

  async _getLatestWakeup () {
    const referrer = this.core.key
    const query = {
      gte: { referrer },
      lte: { referrer },
      reverse: true,
      limit: 24
    }

    const latest = await this.blindPeer.db.find('@blind-peer/cores-by-referrer', query).toArray()
    return encodeWakeup(latest)
  }

  startWakeup () {
    if (this.wakeup) return
    this.wakeup = new CoreWakeup(this)
  }

  async refresh () {
    await this.core.ready()
    if (this.destroyed) return

    this.id = this.core.id
    this.channel = 'hypercore/alpha##' + b4a.toString(this.core.discoveryKey, 'hex')
    const record = await this.blindPeer.db.get('@blind-peer/cores', { key: this.core.key })
    if (this.destroyed || this.record || !record) return

    this.record = record
    await this._onrecord()

    if (this.updated) this._onupdate()
    if (this.activated) this._onactive()
  }

  destroy () {
    if (this.destroyed) return
    this.destroyed = true

    if (this.referrer) this.referrer.close().catch(safetyCatch)
    if (this.wakeup) this.wakeup.destroy()
  }
}

class BlindPeer extends ReadyResource {
  constructor (rocks, { swarm, store } = {}) {
    super()

    this.rocks = typeof rocks === 'string' ? new RocksDB(rocks) : rocks
    this.store = store || new Corestore(this.rocks)
    this.swarm = swarm || null
    this.ownsSwarm = !swarm
    this.ownsStore = !store
    this.db = null
    this.activeReplication = new Map()
    this.activeWakeup = new Map()
    this.flushInterval = null
  }

  get encryptionPublicKey () {
    return this.db.encryptionKeyPair.publicKey
  }

  get publicKey () {
    return this.swarm.keyPair.publicKey
  }

  async _open () {
    await this.store.ready()
    // legacy, we can remove once current ones are upgraded
    const { secretKey } = await this.store.createKeyPair('blind-mirror-swarm')
    this.db = new BlindPeerDB(this.rocks, { swarming: secretKey.subarray(0, 32), encryption: null })
    await this.db.ready()

    if (this.swarm === null) this.swarm = new Hyperswarm({ keyPair: this.db.swarmingKeyPair })
    this.swarm.on('connection', this._onconnection.bind(this))

    this.store.watch(this._oncoreopen.bind(this))

    this.flushInterval = setInterval(this._flushBackground.bind(this), 10_000)
  }

  async listen () {
    if (!this.opened) await this.ready()
    return this.swarm.listen()
  }

  static createMailbox (blindPeerEncryptionPublicKey, opts = {}) {
    const {
      encryptionKey,
      seed = crypto.randomBytes(32),
      referrer = null
    } = opts

    const keyPair = crypto.keyPair(seed)
    const manifest = {
      signers: [{
        publicKey: keyPair.publicKey
      }]
    }

    const key = Hypercore.key(manifest)
    const blockEncryptionKey = encryptionKey
      ? Hypercore.blockEncryptionKey(key, encryptionKey)
      : null

    const mailbox = {
      version: 0,
      seed,
      referrer,
      blockEncryptionKey
    }

    const buffer = c.encode(Mailbox, mailbox)
    const cipher = crypto.encrypt(buffer, blindPeerEncryptionPublicKey)

    return cipher
  }

  _onreferrerupdates (updates) {
    const pending = new Set()

    for (const u of updates) {
      const id = b4a.toString(u.referrer, 'hex')
      const w = this.activeWakeup.get(id)
      if (!w) continue

      w.queued.push(u)
      pending.add(w)
    }

    for (const w of pending) {
      w.flush()
    }
  }

  _oncoreopen (core) {
    const session = new Hypercore({ core, weak: true })
    const id = b4a.toString(core.discoveryKey, 'hex')
    const tracker = new CoreTracker(this, session)

    this.activeReplication.set(id, tracker)
    tracker.refresh().catch(safetyCatch)

    session.on('close', () => {
      tracker.destroy()
      if (this.activeReplication.get(id) === tracker) {
        this.activeReplication.delete(id)
      }
    })
  }

  _flushBackground () {
    if (this.db.updated()) this.db.flush().catch(safetyCatch)
  }

  _onconnection (conn) {
    if (this.ownsStore) this.store.replicate(conn)

    const rpc = new ProtomuxRPC(conn, {
      id: this.swarm.keyPair.publicKey,
      valueEncoding: c.none
    })

    rpc.respond('add-core', { requestEncoding: AddCoreRequest, responseEncoding: c.none }, this._onaddcore.bind(this, conn))
    rpc.respond('post-to-mailbox', { requestEncoding: PostToMailboxRequest, responseEncoding: c.none }, this._onposttomailbox.bind(this, conn))
  }

  async _activateCore (stream, record) {
    const core = this.store.get({ key: record.key, active: false })
    await core.ready()

    const tracker = this.activeReplication.get(b4a.toString(core.discoveryKey, 'hex'))
    if (tracker && !tracker.record) await tracker.refresh()

    if (stream.destroying) {
      await core.close()
      return
    }

    core.replicate(stream)
    stream.on('close', () => core.close().catch(safetyCatch))
  }

  async _onposttomailbox (stream, record) {
    if (!record.mailbox || !record.message) return
    if (!this.opened) await this.ready()

    const buffer = crypto.decrypt(record.mailbox, this.db.encryptionKeyPair)
    if (!buffer) return

    const { version, seed, referrer, blockEncryptionKey } = c.decode(Mailbox, buffer)
    if (version !== 0) return

    const keyPair = crypto.keyPair(seed)
    const manifest = { signers: [{ publicKey: keyPair.publicKey }] }
    const key = Hypercore.key(manifest)

    const coreRecord = {
      key,
      referrer,
      deprecatedAutobase: null,
      deprecatedAutobaseBlockKey: null,
      priority: record.mailbox.referrer ? 1 : 0,
      announce: false
    }

    this.db.addCore(coreRecord)

    await this.db.flush()

    this.emit('add-core', coreRecord, false)

    const core = this.store.get({ key, manifest, keyPair, active: false })
    await core.ready()

    if (blockEncryptionKey) await core.setEncryptionKey(blockEncryptionKey, { block: true })

    const message = !referrer
      ? record.message
      : c.encode(OplogMessage, {
        version: 1,
        maxSupportedVersion: 1,
        digest: null,
        checkpoint: null,
        node: {
          heads: [],
          batch: 1,
          value: record.message
        }
      })

    this.emit('post-to-mailbox', record)

    await core.append(message)
    await core.close()
  }

  async _onaddcore (stream, record) {
    if (!this.opened) await this.ready()

    record.priority = Math.min(record.priority, 1) // 2 is reserved for trusted peers
    record.announce = false // reserved for trusted peers

    this.db.addCore(record)
    await this.db.flush() // flush now as important data

    this.emit('add-core', record, true)

    await this._activateCore(stream, record)
  }

  async _close () {
    clearInterval(this.flushInterval)
    if (this.ownsSwarm) await this.swarm.destroy()
    await this.db.close()
    await this.store.close()
    await this.rocks.close()
  }
}

module.exports = BlindPeer

function noop () {}

function encodeWakeup (writers) {
  const state = { start: 0, end: 0, buffer: null }
  const m = { version: 1, type: 1, writers }
  WakeupReply.preencode(state, m)
  state.buffer = b4a.allocUnsafe(state.end)
  WakeupReply.encode(state, m)
  return state.buffer
}
