const HyperDB = require('hyperdb')
const crypto = require('hypercore-crypto')
const ReadyResource = require('ready-resource')
const IdEnc = require('hypercore-id-encoding')
const { definition: spec } = require('blind-peer-encodings')

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

    this.coresAdding = []
    this.coresDeleting = []
    this.coresUpdated = new Map()

    this.stats = {
      flushes: 0
    }

    this.ready().catch(noop)
  }

  find (col, q) {
    return this.db.find(col, q)
  }

  get (col, q) {
    return this.db.get(col, q)
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
  }

  async flush () { // The caller is responsible for ensuring this runs in a lock
    if (!this.opened) await this.ready()

    const coresAdding = this.coresAdding
    const coresUpdated = this.coresUpdated
    const coresDeleting = this.coresDeleting

    this.coresAdding = []
    this.coresUpdated = new Map()
    this.coresDeleting = []

    const tx = this.db.transaction()
    const time = Date.now()

    let addedCores = 0
    let deletedCores = 0
    let addedReferrers = 0
    let bytesAllocated = 0

    for (const key of coresDeleting) {
      const record = await tx.get('@blind-peer/cores', { key })
      deletedCores++
      bytesAllocated -= record.bytesAllocated
      await tx.delete('@blind-peer/cores', { key })
    }

    for (const info of coresAdding) {
      const key = info.key
      const existing = await tx.get('@blind-peer/cores', { key })
      // TODO: test this path
      if (existing) {
        if (!info.announce && (info.priority || 0) <= existing.priority) continue // would be a downgrade, which we never want

        existing.priority = Math.min(
          Math.max(info.priority || 0, existing.priority)
        )
        existing.announce = info.announce || existing.announce
        existing.updated = time
        existing.active = time
        await tx.insert('@blind-peer/cores', existing)
      } else {
        await tx.insert('@blind-peer/cores', {
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
      c.bytesCleared = core.bytesCleared || 0
      if (updated) c.updated = time
      c.active = time
      c.blocksCleared = core.blocksCleared || 0

      await tx.insert('@blind-peer/cores', c)
    }

    if (addedCores || deletedCores || addedReferrers || bytesAllocated || this.closing) {
      this.digest.cores += addedCores - deletedCores
      this.digest.referrers += addedReferrers
      this.digest.bytesAllocated += bytesAllocated
      this.digest.flushed = !!this.closing

      await tx.insert('@blind-peer/digest', this.digest)
    }

    if (tx.updates.size > 0) this.stats.flushes++ // we only count those flushes where we update the db
    await tx.flush()
  }

  async hasCore (key) {
    key = IdEnc.decode(key)
    return await this.db.get('@blind-peer/cores', { key }) !== null
  }

  addCore (info) {
    this.coresAdding.push(info)
  }

  deleteCore (key) {
    this.coresDeleting.push(key)
  }

  updateCore (core, id) { // TODO: id is technically optional
    this.coresUpdated.set(id, core)
  }

  updated () {
    return this.coresAdding.length > 0 || this.coresUpdated.size > 0 || this.coresDeleting.length > 0
  }

  createGcCandidateReadStream () {
    return this.db.find('@blind-peer/cores-by-activity')
  }

  createAnnouncingCoresStream () {
    return this.db.find('@blind-peer/cores-by-announce')
  }

  async _close () {
    await this.db.close()
  }
}

function noop () {}
