const HyperDB = require('hyperdb')
const crypto = require('hypercore-crypto')
const ReadyResource = require('ready-resource')
const ScopeLock = require('scope-lock')
const spec = require('../spec/hyperdb')

// LOW, NORMAL, HIGH
const MAX_PRIO = 2

module.exports = class BlindPeerDB extends ReadyResource {
  constructor (db, auth) {
    super()

    this.db = HyperDB.rocks(db, spec)
    this.digest = null
    this.auth = auth || null

    this.encryptionKeyPair = null
    this.swarmingKeyPair = null

    this.lock = new ScopeLock({ debounce: true })

    this.coresAdding = []
    this.coresUpdated = new Map()

    this.ready().catch(noop)
  }

  find (col, q) {
    return this.db.find(col, q)
  }

  get (col, q) {
    return this.db.get(col, q)
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
  }

  async _flush () {
    const coresAdding = this.coresAdding
    const coresUpdated = this.coresUpdated

    this.coresAdding = []
    this.coresUpdated = new Map()

    const tx = this.db.transaction()
    const time = Date.now()

    let addedCores = 0
    let addedReferrers = 0
    let bytesAllocated = 0

    for (const info of coresAdding) {
      const key = info.key
      const existing = await tx.get('@blind-peer/cores', { key })

      if (!existing) {
        await tx.insert('@blind-peer/cores', {
          key,
          length: 0,
          bytesAllocated: 0,
          updated: time,
          active: time,
          priority: Math.min(MAX_PRIO, info.priority || 0),
          announce: !!info.announce,
          referrer: info.referrer || null,
          downloadRangeStart: 0,
          bytesCleared: 0
        })

        addedCores++
        if (info.referrer) addedReferrers++
      }
    }

    for (const core of coresUpdated.values()) {
      const c = await tx.get('@blind-peer/cores', { key: core.key })
      if (!c) continue // state mismatch

      const updated = c.length !== core.length

      bytesAllocated += (core.bytesAllocated - c.bytesAllocated)

      c.length = core.length
      c.bytesAllocated = core.bytesAllocated
      c.bytesCleared = core.bytesCleared
      if (updated) c.updated = time
      c.active = time
      c.downloadRangeStart = core.downloadRangeStart || 0

      await tx.insert('@blind-peer/cores', c)
    }

    if (addedCores || addedReferrers || bytesAllocated || this.closing) {
      this.digest.cores += addedCores
      this.digest.referrers += addedReferrers
      this.digest.bytesAllocated += bytesAllocated
      this.digest.flushed = !!this.closing

      await tx.insert('@blind-peer/digest', this.digest)
    }

    await tx.flush()
  }

  addCore (info) {
    this.coresAdding.push(info)
  }

  updateCore (core, id) { // TODO: id is technically optional
    this.coresUpdated.set(id, core)
  }

  updated () {
    return this.coresAdding.length > 0 || this.coresUpdated.size > 0
  }

  createGcCandidateReadStream () {
    return this.db.find('@blind-peer/cores-by-activity')
  }

  async flush () {
    if (!this.opened) await this.ready()
    if (!(await this.lock.lock())) return
    try {
      await this._flush()
    } finally {
      this.lock.unlock()
    }
  }

  async _close () {
    await this.flush()
    await this.db.close()
  }
}

function noop () {}
