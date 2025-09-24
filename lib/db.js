const HyperDB = require('hyperdb')
const crypto = require('hypercore-crypto')
const ReadyResource = require('ready-resource')
const IdEnc = require('hypercore-id-encoding')
const mutexify = require('mutexify/promise')
const { definition: spec } = require('blind-peer-encodings')
const debounceify = require('debounceify')

// LOW, NORMAL, HIGH
const MAX_PRIO = 2

module.exports = class BlindPeerDB extends ReadyResource {
  constructor (db, auth, { flushDelay = 2000, maxPendingUpdates = 512 }) {
    super()

    this.db = HyperDB.rocks(db, spec)
    this.digest = null
    this.auth = auth || null
    this.flushDelay = flushDelay
    this.maxPendingUpdates = maxPendingUpdates
    this.encryptionKeyPair = null
    this.swarmingKeyPair = null

    this.coresAdding = []
    this.coresDeleting = []
    this.coresUpdated = new Map()

    this._tx = null
    this._flushTimer = null
    this._lock = mutexify()
    this._updateMetadata = getFreshUpdateMetadata()

    this.stats = {
      flushes: 0
    }

    this.flush = debounceify(this._undebouncedFlush.bind(this))
    this.ready().catch(noop)
  }

  find (col, q) {
    return this._tx.find(col, q)
  }

  get (col, q) {
    return this._tx.get(col, q)
  }

  getCoreRecord (key) {
    return this.get('@blind-peer/cores', { key })
  }

  async _open () {
    await this.db.ready()

    const auth = await this.db.get('@blind-peer/auth')

    this.auth = auth || this.auth || { swarming: null, encryption: null }
    if (!this.auth.encryption) this.auth.encryption = crypto.randomBytes(32)
    if (!this.auth.swarming) this.auth.swarming = crypto.randomBytes(32)

    this.encryptionKeyPair = crypto.encryptionKeyPair(this.auth.encryption)
    this.swarmingKeyPair = crypto.keyPair(this.auth.swarming)

    const digest = await this.db.get('@blind-peer/digest')

    this.digest = digest || { referrers: 0, cores: 0, bytesAllocated: 0, flushed: false }
    this.digest.flushed = false

    await this.db.insert('@blind-peer/auth', this.auth)
    await this.db.insert('@blind-peer/digest', this.digest)
    await this.db.flush()

    this._tx = this.db.transaction()
    this._flushTimer = setTimeout(this.flush.bind(this), this.flushDelay)
  }

  async _undebouncedFlush () { // Does not throw
    if (!this.opened) await this.ready()
    if (this._flushTimer) clearTimeout(this._flushTimer)

    const release = await this._lock()
    try {
      const coresUpdated = this.coresUpdated
      this.coresUpdated = new Map()

      const tx = this._tx
      const time = Date.now()

      let bytesAllocated = 0

      for (const core of coresUpdated.values()) {
        const c = await tx.get('@blind-peer/cores', { key: core.key })
        if (!c) continue // state mismatch

        const updated = c.length !== core.length

        bytesAllocated += (core.bytesAllocated - c.bytesAllocated)

        c.length = core.length
        c.bytesAllocated = core.bytesAllocated
        c.bytesCleared = core.bytesCleared || 0
        if (updated) c.updated = time
        c.active = time
        c.blocksCleared = core.blocksCleared || 0

        await tx.insert('@blind-peer/cores', c)
      }

      if (tx.updates.size || bytesAllocated || this.closing) {
        this.digest.cores += this._updateMetadata.addedCores - this._updateMetadata.deletedCores
        this.digest.referrers += this._updateMetadata.addedReferrers
        this.digest.bytesAllocated += bytesAllocated + this._updateMetadata.bytesAllocated
        this.digest.flushed = !!this.closing

        await tx.insert('@blind-peer/digest', this.digest)
      }

      if (tx.updates.size > 0) this.stats.flushes++ // we only count those flushes where we update the db
      await tx.flush()
      this._tx = this.db.transaction()

      if (this.closing) return

      this._updateMetadata = getFreshUpdateMetadata()
      this._flushTimer = setTimeout(this.flush.bind(this), this.flushDelay)
    } catch (e) {
      // Flush errors (for example out of disk space) are irrecoverable.
      // Best to crash the process.
      this.emit('error', e)
    } finally {
      release()
    }
  }

  async hasCore (key) {
    key = IdEnc.decode(key)
    return await this._tx.get('@blind-peer/cores', { key }) !== null
  }

  async addCore (info) {
    const release = await this._lock()
    try {
      const time = Date.now()

      const key = info.key
      const existing = await this._tx.get('@blind-peer/cores', { key })
      if (existing) {
        if (!info.announce && (info.priority || 0) <= existing.priority) return // would be a downgrade, which we never want

        existing.priority = Math.min(
          Math.max(info.priority || 0, existing.priority)
        )
        existing.announce = info.announce || existing.announce
        existing.updated = time
        existing.active = time
        await this._tx.insert('@blind-peer/cores', existing)
      } else {
        await this._tx.insert('@blind-peer/cores', {
          key,
          length: 0,
          bytesAllocated: 0,
          updated: time,
          active: time,
          priority: Math.min(MAX_PRIO, info.priority || 0),
          announce: !!info.announce,
          referrer: info.referrer || null,
          blocksCleared: 0,
          bytesCleared: 0
        })

        this._updateMetadata.addedCores++
        if (info.referrer) this._updateMetadata.addedReferrers++
      }
    } finally {
      release()
    }

    if (this._tx.updates.size > this.maxPendingUpdates) this.flush() // in background
  }

  async deleteCore (key) {
    if (!this.opened) await this.ready()
    const release = await this._lock()
    try {
      const record = await this._tx.get('@blind-peer/cores', { key })
      if (!record) return
      this._updateMetadata.deletedCores++
      this._updateMetadata.bytesAllocated -= record.bytesAllocated
      await this._tx.delete('@blind-peer/cores', { key })
    } finally {
      release()
    }

    if (this._tx.updates.size > this.maxPendingUpdates) this.flush() // in background
  }

  updateCore (core, id) { // TODO: id is technically optional
    this.coresUpdated.set(id, core)
  }

  updated () {
    return this.coresUpdated.size > 0 || this.tx.updates.size > 0
  }

  createGcCandidateReadStream () {
    return this._tx.find('@blind-peer/cores-by-activity')
  }

  createAnnouncingCoresStream () {
    return this._tx.find('@blind-peer/cores-by-announce')
  }

  async _close () {
    await this.flush()
    await this.db.close()
  }
}

function noop () {}

function getFreshUpdateMetadata () {
  return {
    addedCores: 0,
    deletedCores: 0,
    addedReferrers: 0,
    bytesAllocated: 0
  }
}
