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
const Wakeup = require('protomux-wakeup')
const ScopeLock = require('scope-lock')
const IdEnc = require('hypercore-id-encoding')
const debounce = require('debounceify')

const BlindPeerDB = require('./lib/db.js')

const { AddCoreEncoding, PostToMailboxEncoding, Mailbox } = require('blind-peer-encodings')

class CoreTracker {
  constructor (blindPeer, core) {
    this.blindPeer = blindPeer
    this.core = core
    this.destroyed = false
    this.record = null
    this.activated = false
    this.updated = false
    this.id = null
    this.referrerDiscoveryKey = null
    this.channel = null
    this.downloadRange = null

    const onupdate = this._onupdate.bind(this)
    const onactive = this._onactive.bind(this)

    this.core.on('upload', onactive)
    this.core.on('download', onactive)
    this.core.on('truncate', onupdate)
    this.core.on('append', onupdate)
  }

  _onupdate () {
    this.updated = true
    if (!this.record) return

    this.record.length = this.core.length
    this.record.bytesAllocated = this.core.byteLength - this.record.bytesCleared
    this.blindPeer.db.updateCore(this.record, this.id)
    this.blindPeer.flush().catch(safetyCatch)

    if (this.referrer) this.announceToReferrer()
  }

  _onactive () {
    this.activated = true

    if (this.record) {
      this.blindPeer.db.updateCore(this.record, this.id)
    }

    this.blindPeer.emit('core-activity', this.core, this.record)
  }

  gc () { // TODO: support gc-ing till less than last block (required hypercore to support getting byteLength at arbitrary versions)
    const bytesCleared = this.core.byteLength
    const blocksCleared = this.core.length
    this.record.bytesAllocated = this.core.byteLength - bytesCleared
    this.record.blocksCleared = blocksCleared
    this.record.bytesCleared = bytesCleared

    if (this.downloadRange) this.downloadRange.destroy()
    this.downloadRange = this.core.download({ start: this.record.blocksCleared, end: -1 })

    this.core.clear(0, blocksCleared).catch(safetyCatch)
    this.blindPeer.db.updateCore(this.record, this.id)

    return bytesCleared
  }

  async refresh () {
    await this.core.ready()
    if (this.destroyed) return

    this.id = this.core.id
    this.channel = 'hypercore/alpha##' + b4a.toString(this.core.discoveryKey, 'hex')

    const record = await this.blindPeer.db.get('@blind-peer/cores', { key: this.core.key })
    if (this.destroyed || this.record || !record) return

    this.record = record
    this.core.download({ start: this.record.blocksCleared, end: -1 })

    if (this.updated) this._onupdate()
    if (this.activated) this._onactive()
  }

  announceToReferrer () {
    if (!this.record || !this.record.referrer) return
    if (!this.referrerDiscoveryKey) this.referrerDiscoveryKey = crypto.discoveryKey(this.record.referrer)

    const session = this.blindPeer.wakeup.getSession(this.referrerDiscoveryKey)
    if (!session) return

    const wakeup = [{ key: this.core.key, length: this.core.length }]

    for (const peer of this.core.peers) {
      const mux = peer.stream.userData

      // already replicating with that peer
      if (mux._infos.get(this.channel)) {
        continue
      }

      session.announceByStream(peer.stream, wakeup)
    }
  }

  destroy () {
    if (this.destroyed) return
    this.destroyed = true
  }
}

class WakeupHandler {
  constructor (db, key, discoveryKey) {
    this.db = db
    this.key = key
    this.discoveryKey = discoveryKey
    this.active = false
  }

  async onpeeradd (peer, session) {
    const referrer = this.key
    const query = {
      gte: { referrer },
      lte: { referrer },
      reverse: true,
      limit: 24
    }

    try {
      const latest = await this.db.find('@blind-peer/cores-by-referrer', query).toArray()
      if (peer.removed) return
      session.announce(peer, latest)
    } catch {
      // do nothing
    }
  }
}

class BlindPeer extends ReadyResource {
  constructor (rocks, { swarm, store, wakeup, maxBytes = 100_000_000_000, enableGc = true, trustedPubKeys, port } = {}) {
    super()

    this.rocks = typeof rocks === 'string' ? new RocksDB(rocks) : rocks
    this.store = store || new Corestore(this.rocks, { active: false })
    this.swarm = swarm || null
    this._port = port || null
    this.trustedPubKeys = new Set()
    for (const k of trustedPubKeys || []) this.addTrustedPubKey(k)

    this.wakeup = wakeup || new Wakeup(this._onwakeup.bind(this))
    this.ownsWakeup = !wakeup
    this.ownsSwarm = !swarm
    this.ownsStore = !store
    this.db = null
    this.activeReplication = new Map()
    this.activeWakeup = new Map()
    this.flushInterval = null
    this.maxBytes = maxBytes
    this.enableGc = enableGc
    this.lock = new ScopeLock({ debounce: true })
    this.announcedCores = new Map()

    this.stats = {
      bytesGcd: 0,
      coresAdded: 0
    }
  }

  get encryptionPublicKey () {
    return this.db.encryptionKeyPair.publicKey
  }

  get publicKey () {
    return this.swarm.keyPair.publicKey
  }

  get digest () {
    return this.db.digest
  }

  addTrustedPubKey (key) {
    this.trustedPubKeys.add(IdEnc.normalize(key))
  }

  _isTrustedPeer (key) {
    return this.trustedPubKeys.has(IdEnc.normalize(key))
  }

  async _open () {
    await this.store.ready()
    // legacy, we can remove once current ones are upgraded
    const { secretKey } = await this.store.createKeyPair('blind-mirror-swarm')
    this.db = new BlindPeerDB(this.rocks.session(), { swarming: secretKey.subarray(0, 32), encryption: null })
    await this.db.ready()

    if (this.swarm === null) {
      const swarmOpts = { keyPair: this.db.swarmingKeyPair }
      if (this._port) swarmOpts.port = this._port
      this.swarm = new Hyperswarm(swarmOpts)
    }
    this.swarm.on('connection', this._onconnection.bind(this))

    const announceProms = []
    for await (const record of this.db.createAnnouncingCoresStream()) {
      announceProms.push(this._announceCore(record.key))
    }
    await Promise.all(announceProms)

    this.store.watch(this._oncoreopen.bind(this))

    this.flushInterval = setInterval(this.flush.bind(this), 10_000)
  }

  async _onwakeup (discoveryKey, stream) {
    const auth = await this.store.storage.getAuth(discoveryKey)
    if (!auth) return

    const w = this.wakeup.session(auth.key, new WakeupHandler(this.db, auth.key, discoveryKey))
    w.addStream(stream)
  }

  async listen () {
    if (!this.opened) await this.ready()
    return this.swarm.listen()
  }

  needsGc () {
    return this.digest.bytesAllocated >= this.maxBytes
  }

  async _gc () { // Do not call directly (assumes lock)
    if (!this.needsGc()) return

    const bytesToClear = this.digest.bytesAllocated - this.maxBytes
    let bytesCleared = 0
    this.emit('gc-start', { bytesToClear })

    for await (const record of this.db.createGcCandidateReadStream()) {
      if (this.closing) return
      if (bytesCleared >= bytesToClear) break
      if (record.bytesAllocated === 0) continue
      if (record.announce) continue // We never clear these ATM, since we do no book keeping on the cleared length of announced  cores

      const { key } = record

      // Explicitly opening the core ensures an active replication
      // session exists
      const core = this.store.get({ key })
      await core.ready()
      if (this.closing) return
      const id = b4a.toString(core.discoveryKey, 'hex')

      try {
        const tracker = this.activeReplication.get(id)
        const coreBytesCleared = tracker.gc()
        bytesCleared += coreBytesCleared
      } finally {
        core.close().catch(safetyCatch)
      }
    }

    await this.db.flush()
    if (this.closing) return
    this.stats.bytesGcd += bytesCleared
    this.emit('gc-done', { bytesCleared })
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

  async flush () { // not allowed to throw
    if (!(await this.lock.lock())) return
    try {
      if (this.enableGc && this.needsGc()) await this._gc()
      if (this.db.updated()) await this.db.flush()
    } catch (e) {
      safetyCatch(e)
    } finally {
      this.lock.unlock()
    }
  }

  _onconnection (conn) {
    if (this.closing) return

    if (this.ownsStore) this.store.replicate(conn)
    if (this.ownsWakeup) this.wakeup.addStream(conn)

    const rpc = new ProtomuxRPC(conn, {
      id: this.swarm.keyPair.publicKey,
      valueEncoding: c.none
    })

    rpc.respond('add-core', AddCoreEncoding, this._onaddcore.bind(this, conn))
    rpc.respond('post-to-mailbox', PostToMailboxEncoding, this._onposttomailbox.bind(this, conn))
  }

  async _activateCore (stream, record) {
    const core = this.store.get({ key: record.key })
    await core.ready()

    const tracker = this.activeReplication.get(b4a.toString(core.discoveryKey, 'hex'))
    if (tracker && !tracker.record) await tracker.refresh()

    if (record.announce) {
      await this._announceCore(core.key)
    }

    if (stream.destroying) {
      await core.close()
      return
    }

    core.replicate(stream)
    stream.on('close', () => core.close().catch(safetyCatch))
  }

  async _announceCore (key) {
    const coreId = IdEnc.normalize(key)
    if (this.announcedCores.has(coreId)) return

    const core = this.store.get({ key })
    this.announcedCores.set(coreId, core)

    core.on('append', () => this.emit('core-append', core))
    core.on('download', debounce(() => {
      if (core.length === core.contiguousLength) {
        this.emit('core-downloaded', core)
      }
    }))

    await core.ready()
    this.swarm.join(core.discoveryKey)

    // WARNING: we do not yet handle the case where
    // data of an announced core is cleared
    core.download({ start: 0, end: -1 })

    this.emit('announce-core', core)
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

    await this.flush()

    this.emit('add-core', coreRecord, false)

    const core = this.store.get({ key, manifest, keyPair })
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
    if (record.announce !== false && !this._isTrustedPeer(stream.remotePublicKey)) {
      this.emit('downgrade-announce', { record, remotePublicKey: stream.remotePublicKey })
      record.announce = false
    }

    if (record.referrer) {
      // ensure referrer is allocated...
      const core = this.store.get({ key: record.referrer })
      await core.ready()
      await core.close()
    }

    this.db.addCore(record)
    await this.flush() // flush now as important data

    this.stats.coresAdded++
    this.emit('add-core', record, true)

    await this._activateCore(stream, record)

    const coreRecord = await this.db.getCoreRecord(record.key)
    return coreRecord
  }

  async _close () {
    clearInterval(this.flushInterval)
    if (this.ownsWakeup) this.wakeup.destroy()
    if (this.ownsSwarm) await this.swarm.destroy()
    await this.flush()
    await this.db.close()
    if (this.ownsStore) await this.store.close()
    await this.rocks.close()
  }

  registerMetrics (promClient) {
    const self = this
    new promClient.Gauge({ // eslint-disable-line no-new
      name: 'blind_peer_bytes_allocated',
      help: 'The amount of bytes allocated by the hyperdb (as reported in its digest)',
      collect () {
        this.set(self.digest.bytesAllocated)
      }
    })

    new promClient.Gauge({ // eslint-disable-line no-new
      name: 'blind_peer_cores_added',
      help: 'The total amount of add-core RPC requests that were processed',
      collect () {
        this.set(self.stats.coresAdded)
      }
    })

    new promClient.Gauge({ // eslint-disable-line no-new
      name: 'blind_peer_bytes_gcd',
      help: 'The total amount of bytes garbage collected since the process started',
      collect () {
        this.set(self.stats.bytesGcd)
      }
    })
  }
}

module.exports = BlindPeer
