const Corestore = require('corestore')
const Hypercore = require('hypercore')
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

const BlindPeerDB = require('./lib/db.js')

const {
  AddCoreEncoding,
  AddAutobaseCoresEncoding,
  DeleteCoreEncoding
} = require('blind-peer-encodings')

class CoreTracker {
  constructor(blindPeer, core) {
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
    this.announceToReferrerBound = this.announceToReferrer.bind(this)

    const onupdate = this._onupdate.bind(this)
    const onactive = this._onactive.bind(this)

    this.core.on('upload', onactive)
    this.core.on('download', onactive)
    this.core.on('truncate', onupdate)
    this.core.on('append', onupdate)
  }

  _onupdate() {
    this.updated = true
    if (!this.record) return

    this.record.length = this.core.length
    this.record.bytesAllocated = this.core.byteLength - this.record.bytesCleared
    this.blindPeer.db.updateCore(this.record, this.id)
    this.blindPeer.flush().then(this.announceToReferrerBound, safetyCatch)
  }

  _onactive() {
    this.activated = true

    if (this.record) {
      this.blindPeer.db.updateCore(this.record, this.id)
    }

    this.blindPeer.emit('core-activity', this.core, this.record)
  }

  gc() {
    if (!this.record) throw new Error('Record must be set before calling gc')

    // TODO: support gc-ing till less than last block (required hypercore to support getting byteLength at arbitrary versions)
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

  async refresh() {
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

  announceToReferrer() {
    if (!this.record || !this.record.referrer) return
    if (!this.referrerDiscoveryKey) {
      this.referrerDiscoveryKey = crypto.discoveryKey(this.record.referrer)
    }

    const sessions = this.blindPeer.wakeup.getSessions(null, {
      discoveryKey: this.referrerDiscoveryKey
    })
    if (sessions.length === 0) return

    const wakeup = [{ key: this.core.key, length: this.core.length }]

    for (const s of sessions) {
      for (const peer of s.peers) {
        const mux = peer.stream.userData

        // already replicating with that peer
        if (mux._infos.get(this.channel)) {
          continue
        }

        s.announceByStream(peer.stream, wakeup)
      }
    }
  }

  destroy() {
    if (this.destroyed) return
    this.destroyed = true
  }
}

class WakeupHandler {
  constructor(wakeup, db, key, discoveryKey) {
    this.wakeup = wakeup
    this.db = db
    this.key = key
    this.discoveryKey = discoveryKey
    this.active = false
    this.timeout = setTimeout(this._gc.bind(this), 15_000) // sanity
  }

  remoteAttached() {
    if (!this.timeout) return
    clearTimeout(this.timeout)
    this.timeout = null
  }

  onpeeradd(peer, session) {
    this.remoteAttached()
  }

  onpeerremove(peer, session) {
    this.remoteAttached()
    if (session.peers.length === 0) session.destroy()
  }

  async onpeeractive(peer, session) {
    const referrer = this.key
    const query = {
      gte: { referrer },
      lte: { referrer },
      reverse: true,
      limit: 32
    }

    try {
      const latest = await this.db.find('@blind-peer/cores-by-referrer', query).toArray()
      if (peer.removed) return
      session.announce(peer, latest)
    } catch {
      // do nothing
    }
  }

  _gc() {
    const sessions = this.wakeup.getSessions(this.key)
    if (!sessions.length) return
    const session = sessions[0]
    if (session.peers.length === 0) session.destroy()
  }

  ondestroy(session) {
    this.remoteAttached()
  }
}

class BlindPeer extends ReadyResource {
  constructor(
    rocks,
    {
      swarm,
      store,
      wakeup,
      maxBytes = 100_000_000_000,
      enableGc = true,
      trustedPubKeys,
      port,
      announcingInterval = 100,
      wakeupGcTickTime = null
    } = {}
  ) {
    super()

    this.rocks = typeof rocks === 'string' ? new RocksDB(rocks) : rocks
    this.store = store || new Corestore(this.rocks, { active: false })
    this.swarm = swarm || null
    this._port = port || 0
    this.announcingInterval = announcingInterval
    this.trustedPubKeys = new Set()
    for (const k of trustedPubKeys || []) this.addTrustedPubKey(k)

    this.wakeup = wakeup || new Wakeup(this._onwakeup.bind(this), { gcTickTime: wakeupGcTickTime })
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
      coresAdded: 0,
      activations: 0,
      wakeups: 0
    }
  }

  get encryptionPublicKey() {
    return this.db.encryptionKeyPair.publicKey
  }

  get publicKey() {
    return this.swarm.keyPair.publicKey
  }

  get digest() {
    return this.db.digest
  }

  get nrAnnouncedCores() {
    return this.announcedCores.size
  }

  addTrustedPubKey(key) {
    this.trustedPubKeys.add(IdEnc.normalize(key))
  }

  _isTrustedPeer(key) {
    return this.trustedPubKeys.has(IdEnc.normalize(key))
  }

  async _open() {
    await this.store.ready()

    // legacy, we can remove once current ones are upgraded
    const { secretKey } = await this.store.createKeyPair('blind-mirror-swarm')
    this.db = new BlindPeerDB(this.rocks.session(), {
      swarming: secretKey.subarray(0, 32),
      encryption: null
    })
    await this.db.ready()

    // We don't need to track our own db, so we set this handler after the db core opened
    this.store.watch(this._oncoreopen.bind(this))

    if (this.swarm === null) {
      const swarmOpts = { keyPair: this.db.swarmingKeyPair }
      if (this._port) {
        swarmOpts.port = typeof this._port === 'number' ? [this._port, this._port + 64] : this._port
      }
      this.swarm = new Hyperswarm(swarmOpts)
    }
    this.swarm.on('connection', this._onconnection.bind(this))

    this._announceCores().catch(safetyCatch) // announcing cores asynchronously
    this.flushInterval = setInterval(this.flush.bind(this), 10_000)
  }

  _getSession(key, discoveryKey) {
    const sessions = this.wakeup.getSessions(key)
    if (sessions.length) return sessions[0]

    const handler = new WakeupHandler(this.wakeup, this.db, key, discoveryKey)
    const session = this.wakeup.session(key, handler)
    if (session.peers.length) handler.remoteAttached()
    return session
  }

  async _onwakeup(discoveryKey, muxer) {
    this.stats.wakeups++

    const auth = await this.store.storage.getAuth(discoveryKey)
    if (!auth) return

    const stream = muxer.stream
    if (stream.destroying || stream.destroyed) return

    const session = this._getSession(auth.key, discoveryKey)

    if (session.hasStream(stream)) return
    session.addStream(stream)

    // if new peer, send back the active handler for this peer
    const peer = session.getPeer(stream)
    if (peer && peer.active) session.handlers.onpeeractive(peer, session)
  }

  async listen() {
    if (!this.opened) await this.ready()
    return this.swarm.listen()
  }

  needsGc() {
    return this.digest.bytesAllocated >= this.maxBytes
  }

  async _gc() {
    // Do not call directly (assumes lock)
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
        if (!tracker.record) await tracker.refresh()
        const coreBytesCleared = tracker.gc()
        bytesCleared += coreBytesCleared
      } finally {
        await core.close().catch(safetyCatch)
      }
    }

    await this.db.flush()
    if (this.closing) return
    this.stats.bytesGcd += bytesCleared
    this.emit('gc-done', { bytesCleared })
  }

  _onreferrerupdates(updates) {
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

  _oncoreopen(core) {
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
    session.on('invalid-request', (err, req, from) => {
      this.emit('invalid-request', session, err, req, from)
    })
  }

  async flush() {
    // not allowed to throw
    if (!(await this.lock.lock())) return
    try {
      if (this.enableGc && this.needsGc()) await this._gc()
      if (this.db.updated()) await this.db.flush()
    } catch (e) {
      this.emit('flush-error', e)
      safetyCatch(e)
    } finally {
      this.lock.unlock()
    }
  }

  _onconnection(conn) {
    if (this.closing) {
      conn.destroy()
      return
    }

    if (this.ownsStore) this.store.replicate(conn)
    if (this.ownsWakeup) this.wakeup.addStream(conn)

    const rpc = new ProtomuxRPC(conn, {
      id: this.swarm.keyPair.publicKey,
      valueEncoding: c.none
    })

    rpc.respond('add-core', AddCoreEncoding, this._onaddcore.bind(this, conn))
    rpc.respond('delete-core', DeleteCoreEncoding, this._ondeletecore.bind(this, conn))
    rpc.respond(
      'add-autobase-cores',
      AddAutobaseCoresEncoding,
      this._onaddautobasecores.bind(this, conn)
    )
  }

  async _activateCore(stream, record) {
    this.stats.activations++

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

  async _announceCores() {
    for await (const record of this.db.createAnnouncingCoresStream()) {
      if (this.closing) return
      await this._announceCore(record.key)
      if (this.closing) return
      await new Promise((resolve) => setTimeout(resolve, this.announcingInterval))
      if (this.closing) return
    }

    this.emit('announced-initial-cores')
  }

  async _announceCore(key) {
    const coreId = IdEnc.normalize(key)
    if (this.announcedCores.has(coreId)) return

    const core = this.store.get({ key })
    this.announcedCores.set(coreId, core)

    core.on('append', () => {
      this.emit('core-append', core)
    })
    core.on('download', () => {
      if (core.length === core.contiguousLength) {
        this.emit('core-downloaded', core)
      }
    })

    await core.ready()
    this.swarm.join(core.discoveryKey, { server: true, client: false })

    // WARNING: we do not yet handle the case where
    // data of an announced core is cleared
    core.download({ start: 0, end: -1 })

    this.emit('announce-core', core)
  }

  async _onaddcore(stream, record) {
    if (!this.opened) await this.ready()

    record.priority = Math.min(record.priority, 1) // 2 is reserved for trusted peers
    if (record.announce !== false && !this._isTrustedPeer(stream.remotePublicKey)) {
      this.emit('downgrade-announce', { record, remotePublicKey: stream.remotePublicKey })
      record.announce = false
    }

    // We only add it to the db the first time, except if announce changed
    // Note: not race condition safe, but it's no problem if we do add the same core twice
    const existing = await this.db.getCoreRecord(record.key)
    const upgradeToAnnounce = existing && !existing.announce && record.announce
    if (!existing || upgradeToAnnounce) {
      this.db.addCore(record)
      await this.flush() // flush now as important data
      this.emit('add-new-core', record, true, stream)
    }

    if (record.referrer) {
      // ensure referrer is allocated...
      // TODO: move to a dedicated wakeup collection, insted of using a core since we moved away from that
      // still works atm, cause dkey
      const muxer = stream.userData
      const core = this.store.get({ key: record.referrer })
      await core.ready()
      const discoveryKey = core.discoveryKey
      await core.close()

      await this._onwakeup(discoveryKey, muxer)
    }

    this.stats.coresAdded++
    this.emit('add-core', record, true, stream)

    await this._activateCore(stream, record)

    const coreRecord = await this.db.getCoreRecord(record.key)
    return coreRecord
  }

  async _onaddautobasecores(stream, request) {
    const priority = Math.min(request.priority, 1) // 2 is reserved for trusted peers
    const { cores, referrer } = request

    const records = new Map()
    const lengths = new Map()
    for (const c of cores) {
      const id = IdEnc.normalize(c.key)
      records.set(id, { key: c.key, referrer, priority, announce: false })
      lengths.set(id, c.length)
    }

    // Note: not race condition safe, but it's no problem if we do add the same core twice
    const dbRecords = new Map()
    const recordsToAdd = []
    await Promise.all(
      [...records.entries()].map(async ([id, record]) => {
        const dbRecord = await this.db.getCoreRecord(record.key)
        dbRecords.set(id, dbRecord)
        if (dbRecord === null) recordsToAdd.push(record)
      })
    )

    for (const record of recordsToAdd) this.db.addCore(record)

    if (recordsToAdd.length > 0) await this.flush() // flush now as important data

    // TODO: revisit this code (copy-pasted from add-core)
    const muxer = stream.userData
    const core = this.store.get({ key: referrer })
    await core.ready()
    const discoveryKey = core.discoveryKey
    await core.close()
    await this._onwakeup(discoveryKey, muxer)

    for (const r of recordsToAdd) this.emit('add-new-core', r, true, stream)

    const activateProms = []
    for (const [id, dbRecord] of dbRecords.entries()) {
      const noChange = dbRecord && lengths.get(id) === dbRecord.length
      if (noChange) continue
      this.emit('add-core', records.get(id), true, stream)
      activateProms.push(this._activateCore(stream, records.get(id)))
    }
    await Promise.all(activateProms)

    return null // TODO: decide return value
  }

  async _ondeletecore(stream, { key }) {
    if (!this._isTrustedPeer(stream.remotePublicKey)) {
      this.emit('delete-blocked', stream, { key })
      throw new Error('Only trusted peers can delete cores')
    }

    const existing = (await this.db.getCoreRecord(key)) !== null
    this.emit('delete-core', stream, { key, existing })
    if (!existing) return false

    const core = this.store.get({ key })
    await core.ready()

    const announced = this.announcedCores.has(core.id)
    if (announced) {
      this.swarm.leave(core.discoveryKey)
      try {
        // Closes the download session
        await this.announcedCores.get(core.id).close()
      } catch (e) {
        safetyCatch(e)
      }
      this.announcedCores.delete(core.id)
    }

    const hexId = b4a.toString(core.discoveryKey, 'hex')
    const tracker = this.activeReplication.get(hexId)
    if (tracker) {
      try {
        // cancel the download request and trigger the cleanup logic
        // which removes it from the active replication map
        await tracker.core.close()
      } catch (e) {
        safetyCatch(e)
      }
    }

    await core.clear(0, core.length)

    this.db.deleteCore(key)
    await this.flush()
    this.emit('delete-core-end', stream, { key, announced })
    return true
  }

  async _close() {
    clearInterval(this.flushInterval)
    if (this.ownsWakeup) this.wakeup.destroy()
    if (this.ownsSwarm) await this.swarm.destroy()
    await this.flush()
    await this.db.close()
    if (this.ownsStore) await this.store.close()
    await this.rocks.close()
  }

  registerMetrics(promClient) {
    this.wakeup.registerMetrics(promClient)

    const self = this
    new promClient.Gauge({
      // eslint-disable-line no-new
      name: 'blind_peer_bytes_allocated',
      help: 'The amount of bytes allocated by the hyperdb (as reported in its digest)',
      collect() {
        this.set(self.digest.bytesAllocated)
      }
    })

    new promClient.Gauge({
      // eslint-disable-line no-new
      name: 'blind_peer_cores',
      help: 'The amount of cores (as reported in its digest)',
      collect() {
        this.set(self.digest.cores)
      }
    })

    new promClient.Gauge({
      // eslint-disable-line no-new
      name: 'blind_peer_cores_added',
      help: 'The total amount of add-core RPC requests that have been processed',
      collect() {
        this.set(self.stats.coresAdded)
      }
    })

    new promClient.Gauge({
      // eslint-disable-line no-new
      name: 'blind_peer_bytes_gcd',
      help: 'The total amount of bytes garbage collected since the process started',
      collect() {
        this.set(self.stats.bytesGcd)
      }
    })

    new promClient.Gauge({
      // eslint-disable-line no-new
      name: 'blind_peer_core_activations',
      help: 'The total amount of hypercore activations since the process started',
      collect() {
        this.set(self.stats.activations)
      }
    })

    new promClient.Gauge({
      // eslint-disable-line no-new
      name: 'blind_peer_wakeups',
      help: 'The total amount of hypercore wakeups since the process started',
      collect() {
        this.set(self.stats.wakeups)
      }
    })

    new promClient.Gauge({
      // eslint-disable-line no-new
      name: 'blind_peer_db_flushes',
      help: 'The total amount of database flushes since the process started',
      collect() {
        this.set(self.db.stats.flushes)
      }
    })

    new promClient.Gauge({
      // eslint-disable-line no-new
      name: 'blind_peer_announced_cores',
      help: 'The amount of announced cores',
      collect() {
        this.set(self.nrAnnouncedCores)
      }
    })
  }
}

module.exports = BlindPeer
