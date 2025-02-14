const { EventEmitter } = require('events')
const AutobaseLightWriter = require('autobase-light-writer')
const HyperDB = require('hyperdb')
const Corestore = require('corestore')
const { definition, postEncoding } = require('blind-peer-encodings')
const path = require('path')
const Hyperswarm = require('hyperswarm')
const ProtomuxRPC = require('protomux-rpc')
const c = require('compact-encoding')
const DBLock = require('db-lock')
const PassiveWatcher = require('passive-core-watcher')
const hypCrypto = require('hypercore-crypto')
const b4a = require('b4a')

module.exports = class BlindPeer extends EventEmitter {
  constructor (storage) {
    super()

    this.db = HyperDB.rocks(path.join(storage, 'hyperdb'), definition)
    this.store = new Corestore(path.join(storage, 'corestore'))
    this.swarm = null

    this.swarmSeed = null
    this.enccryptionSeed = null

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

    rpc.respond('post-to-mailbox', postEncoding, this._onrpcpost.bind(this))
  }

  async _onrpcpost (req) {
    this.emit('post-to-mailbox-request', req)
    await this.postToMailbox(req)
    this.emit('post-to-mailbox-response', req)
  }

  async _isEstablishedMailbox (core) {
    if (!core.opened) await core.ready()
    const entry = await this.db.get('@blind-peer/mailbox-by-autobase', { autobase: core.key })
    return entry
  }

  async _onmailboxcore (weakSession) {
    try {
      const entry = await this.db.get('@blind-peer/mailbox-by-autobase', { autobase: weakSession.key })
      if (weakSession.closing) return

      const lightWriterStore = this.store.namespace(entry.id)
      const { keyPair, manifest } = entropyToKeyPairAndManifest(entry.id)

      const w = new AutobaseLightWriter(lightWriterStore, entry.autobase, {
        active: false,
        blockEncryptionKey: entry.blockEncryptionKey,
        manifest,
        keyPair
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
    // TODO: this can probably be simplified (it should be a single entry, isntead of a full db schema)
    let seeds = await this.db.get('@blind-peer/seeds', { id: 'seeds' })
    if (!seeds) { // First time we launch
      seeds = { swarm: hypCrypto.randomBytes(32), encryption: hypCrypto.randomBytes(32), id: 'seeds' }
      const tx = await this.lock.enter()
      await tx.insert('@blind-peer/seeds', seeds)
      await this.lock.exit()

      const loadedSeeds = await this.db.get('@blind-peer/seeds', { id: 'seeds' })
      if (!b4a.equals(loadedSeeds.swarm, seeds.swarm) || !b4a.equals(loadedSeeds.encryption, seeds.encryption)) {
        throw new Error('Logical error in seeds bootstrap code')
      }
    }

    this.swarmSeed = seeds.swarm
    this.encryptionSeed = seeds.encryption
    this.encryptionKeyPair = hypCrypto.encryptionKeyPair(this.encryptionSeed)

    this.swarm = new Hyperswarm({
      seed: this.swarmSeed,
      bootstrap
    })
    this.swarm.on('connection', this._onconnection.bind(this))
    return this.swarm.listen()
  }

  async get ({ id }) {
    return await this.db.get('@blind-peer/mailbox', { id })
  }

  async _addMailbox ({ id, autobase, blockEncryptionKey = null }) {
    const { keyPair, manifest } = entropyToKeyPairAndManifest(id)

    const w = new AutobaseLightWriter(
      this.store.namespace(id),
      autobase,
      {
        active: false,
        manifest,
        keyPair
      })
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
    try {
      // TODO: this might be race-condition prone. Think about potential issues
      let entry = await this.db.get('@blind-peer/mailbox', { id })
      const decrypted = hypCrypto.decrypt(id, this.encryptionKeyPair)
      const entropy = decrypted.subarray(0, 32)
      const autobase = decrypted.subarray(32, 64)
      const blockEncryptionKey = decrypted.byteLength > 64
        ? decrypted.subarray(64)
        : null

      if (!entry) {
        // Setting up mailbox
        entry = await this._addMailbox({ id: entropy, autobase, blockEncryptionKey })
      }

      const { keyPair, manifest } = entropyToKeyPairAndManifest(entropy)

      const w = new AutobaseLightWriter(this.store.namespace(id), entry.autobase, {
        active: false,
        blockEncryptionKey: entry.blockEncryptionKey,
        manifest,
        keyPair
      })
      await w.ready()
      await w.append(message)
      await w.close()
    } catch (e) {
      console.error(e)
    }
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

function entropyToKeyPairAndManifest (entropy) {
  const keyPair = hypCrypto.keyPair(entropy)
  const manifest = {
    signers: [{ publicKey: keyPair.publicKey }]
  }

  return { keyPair, manifest }
}
