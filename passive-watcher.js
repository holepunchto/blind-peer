const ReadyResource = require('ready-resource')
const Hypercore = require('hypercore')
const b4a = require('b4a')
const HypCrypto = require('hypercore-crypto')
const IdEnc = require('hypercore-id-encoding')

class PassiveWatcher extends ReadyResource {
  constructor (corestore, shouldWatch = () => {}) {
    super()

    this.store = corestore
    this.shouldWatch = shouldWatch
    this._oncoreopenBound = this._oncoreopen.bind(this)
    this._openCores = new Map()
  }

  async _open () {
    this.store.watch(this._oncoreopenBound)
  }

  async _close () {
    this.store.unwatch(this._oncoreopenBound)
    await Promise.allSettled([...this._openCores.values()].map(c => c.close()))
  }

  async _oncoreopen (core) {
    try {
      if (await this.shouldWatch(core)) {
        await core.ready()
        await this.ensureTracked(core.key)
      }
    } catch (e) {
      this.emit('oncoreopen-error', e)
    }
  }

  async ensureTracked (publicKey) {
    publicKey = IdEnc.decode(publicKey)

    const discKey = b4a.toString(HypCrypto.discoveryKey(publicKey), 'hex')
    const core = this.store.cores.get(discKey)
    if (!core) return // not in corestore atm (will rerun when oncoreopen runs)
    if (this._openCores.has(discKey)) return // Already processed

    const session = new Hypercore({ core, weak: true })
    this._openCores.set(discKey, session)
    // Must be sync up till the line above, for the accounting

    await session.ready()
    if (session.closing) { // race condition (insta close)
      this._openCores.delete(discKey)
      return
    }

    session.on('close', () => {
      this._openCores.delete(discKey)
    })

    this.emit('new-hypercore', session)
  }
}

module.exports = PassiveWatcher
