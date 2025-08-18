#!/usr/bin/env node

const { command, flag } = require('paparam')
const goodbye = require('graceful-goodbye')
const idEnc = require('hypercore-id-encoding')
const Instrumentation = require('hyper-instrument')
const RegisterClient = require('autobase-discovery/client/register')
const safetyCatch = require('safety-catch')
const byteSize = require('tiny-byte-size')
const pino = require('pino')
const b4a = require('b4a')
const hypCrypto = require('hypercore-crypto')

const BlindPeer = require('.')

const SERVICE_NAME = 'blind-peer'
const DEFAULT_STORAGE_LIMIT_MB = 100_000

const cmd = command('blind-peer',
  flag('--storage|-s [path]', 'Storage path, defaults to ./blind-peer'),
  flag('--port|-p [int]', 'DHT Port to try to bind to. Only relevant when that port is not firewalled. (defaults to a random port)'),
  flag('--trusted-peer|-t [trusted-peer]', 'Public key of a trusted peer (allowed to set announce: true). Can be more than 1.').multiple(),
  flag('--debug|-d', 'Enable debug mode (more logs)').multiple(),
  flag(`--max-storage|-m [int]', 'Max storage usage, in Mb (defaults to ${DEFAULT_STORAGE_LIMIT_MB})`),
  flag('--autodiscovery-rpc-key [autodiscovery-rpc-key]', 'Public key where the autodiscovery service is listening. When set, the autodiscovery-seed must also be set. Can be hex or z32.'),
  flag('--autodiscovery-seed [autodiscovery-seed]', '64-byte seed used to authenticate to the autodiscovery service.  Can be hex or z32.'),
  flag('--autodiscovery-service-name [autodiscovery-service-name]', `Name under which to register the service (default ${SERVICE_NAME})`),
  flag('--scraper-public-key [scraper-public-key]', 'Public key of a dht-prometheus scraper.  Can be hex or z32.'),
  flag('--scraper-secret [scraper-secret]', 'Secret of the dht-prometheus scraper.  Can be hex or z32.'),
  flag('--scraper-alias [scraper-alias]', '(optional) Alias with which to register to the scraper'),
  flag('--repl [repl]', 'Expose a repl-swarm at the passed-in seed (32 bytes in hex or z32 notation). Use for debugging only.'),
  async function ({ flags }) {
    const debug = flags.debug
    const logger = pino({
      level: debug ? 'debug' : 'info',
      name: 'blind-peer'
    })
    logger.info('Starting blind peer')

    const storage = flags.storage || 'blind-peer'
    const port = flags.port ? parseInt(flags.port) : null

    const maxBytes = 1_000_000 * parseInt(flags.maxStorage || DEFAULT_STORAGE_LIMIT_MB)
    const trustedPubKeys = (flags.trustedPeer || []).map(k => idEnc.decode(k))

    const blindPeer = new BlindPeer(storage, { trustedPubKeys, maxBytes, port })

    blindPeer.on('flush-error', e => {
      logger.warn(`Error while flushing the db: ${e.stack}`)
    })

    blindPeer.on('add-core', (record, _, stream) => {
      try {
        logger.info(`add-core request received from peer ${streamToStr(stream)} for record ${recordToStr(record)}`)
      } catch (e) {
        logger.info(`Invalid add-core request received: ${e.stack}`)
        logger.info(record)
      }
    })

    blindPeer.on('downgrade-announce', ({ record, remotePublicKey }) => {
      try {
        logger.info(`Downgraded announce for peer ${idEnc.normalize(remotePublicKey)} because the peer is not trusted (Original: ${recordToStr(record)})`)
      } catch (e) {
        logger.error(`Unexpected error while logging downgrade-announce: ${e.stack}`)
      }
    })

    blindPeer.on('announce-core', core => {
      logger.info(`Started announcing core ${coreToInfo(core)}`)
    })
    blindPeer.on('core-downloaded', core => {
      logger.info(`Announced core fully downloaded: ${coreToInfo(core)}`)
    })
    blindPeer.on('core-append', core => {
      logger.info(`Detected announced-core length update: ${coreToInfo(core)}`)
    })

    blindPeer.on('gc-start', ({ bytesToClear }) => {
      logger.info(`Starting GC, trying to clear ${byteSize(bytesToClear)} (bytes allocated: ${byteSize(blindPeer.digest.bytesAllocated)} of ${byteSize(blindPeer.maxBytes)})`)
    })
    blindPeer.on('gc-done', ({ bytesCleared }) => {
      logger.info(`Completed GC, cleared ${byteSize(bytesCleared)} bytes (bytes allocated: ${byteSize(blindPeer.digest.bytesAllocated)} of ${byteSize(blindPeer.maxBytes)})`)
    })
    if (debug) {
      blindPeer.on('core-activity', (core) => {
        logger.debug(`Core activity for ${coreToInfo(core)}`)
      })
    }

    logger.info(`Using storage '${storage}'`)
    if (trustedPubKeys.length > 0) {
      logger.info(`Trusted public keys:\n  -${[...blindPeer.trustedPubKeys].map(idEnc.normalize).join('\n  -')}`)
    }

    let instrumentation = null
    goodbye(async () => {
      if (instrumentation) {
        logger.info('Closing instrumentation')
        await instrumentation.close()
      }
      logger.info('Shutting down blind peer')
      await blindPeer.close()
      logger.info('Shut down blind peer')
    })

    if (flags.repl) {
      const seed = idEnc.decode(flags.repl)
      logger.warn('Setting up REPL swarm, enabling remote access to this process')
      const replSwarm = require('repl-swarm')
      replSwarm({ seed, logSeed: false, blindPeer, instrumentation })
    }

    await blindPeer.ready() // needed to be able to access the swarm object
    blindPeer.swarm.on('ban', (peerInfo, err) => {
      logger.warn(`Banned peer: ${b4a.toString(peerInfo.publicKey, 'hex')}.\n${err.stack}`)
    })
    if (debug) {
      blindPeer.swarm.on('connection', (conn, peerInfo) => {
        const key = idEnc.normalize(peerInfo.publicKey)
        logger.debug(`Opened connection to ${key}`)
        conn.on('close', () => logger.debug(`Closed connection to ${key}`))
      })
    }

    await blindPeer.listen()

    logger.info(`Blind peer listening, local address is ${blindPeer.swarm.dht.localAddress().host}:${blindPeer.swarm.dht.localAddress().port}`)
    logger.info(`Bytes allocated: ${byteSize(blindPeer.digest.bytesAllocated)} of ${byteSize(blindPeer.maxBytes)}`)

    if (flags.autodiscoveryRpcKey) {
      const autodiscoveryRpcKey = idEnc.decode(flags.autodiscoveryRpcKey)
      const seed = idEnc.decode(flags.autodiscoverySeed)
      const serviceName = flags.autodiscoveryServiceName || SERVICE_NAME
      const registerClient = new RegisterClient(autodiscoveryRpcKey, blindPeer.swarm.dht, seed)

      // No need to block on this, so we run it in the background
      logger.info(`Registering own RPC key rpc key ${idEnc.normalize(blindPeer.publicKey)} with service '${serviceName}' at autodiscovery service ${idEnc.normalize(autodiscoveryRpcKey)} (using public key ${idEnc.normalize(registerClient.keyPair.publicKey)})`)
      registerClient.putService(blindPeer.publicKey, serviceName)
        .then(() => { logger.info('Successfully requested to be added to the autodiscovery service') })
        .catch(e => { logger.warn(`Failed to register to the autodiscovery service: ${e.stack}`) })
        .finally(() => { registerClient.close().catch(safetyCatch) })
    }

    if (flags.scraperPublicKey) {
      const swarm = blindPeer.swarm
      logger.info('Setting up instrumentation')

      const scraperPublicKey = idEnc.decode(flags.scraperPublicKey)
      const scraperSecret = idEnc.decode(flags.scraperSecret)

      let prometheusAlias = flags.scraperAlias
      if (prometheusAlias && prometheusAlias.length > 99) throw new Error('The Prometheus alias must have length less than 100')
      if (!prometheusAlias) {
        prometheusAlias = `blind-peer-${idEnc.normalize(swarm.keyPair.publicKey)}`.slice(0, 99)
      }

      instrumentation = new Instrumentation({
        swarm,
        corestore: blindPeer.store,
        scraperPublicKey,
        prometheusAlias,
        scraperSecret,
        prometheusServiceName: SERVICE_NAME
      })

      blindPeer.registerMetrics(instrumentation.promClient)
      instrumentation.registerLogger(logger)
      await instrumentation.ready()
    }

    logger.info(`Listening at ${idEnc.normalize(blindPeer.publicKey)}`)
    logger.info(`Encryption public key is ${idEnc.normalize(blindPeer.encryptionPublicKey)}`)
  }
)

function recordToStr (record) {
  const discKey = hypCrypto.discoveryKey(record.key)
  return `DB Record for discovery key ${idEnc.normalize(discKey)} with priority: ${record.priority}. Announcing? ${record.announce}`
}

function getAddress (stream) {
  return {
    host: stream.rawStream?.remoteHost || null,
    port: stream.rawStream?.remotePort || null
  }
}

function streamToStr (stream) {
  const pubKey = idEnc.normalize(stream.remotePublicKey)
  const { port, host } = getAddress(stream)
  const address = port && host ? `${host}:${port}` : 'No address'
  return `${pubKey} (${address}`
}

function coreToInfo (core) {
  const discKey = hypCrypto.discoveryKey(core.key)
  return `Discovery key ${idEnc.normalize(discKey)} (${core.contiguousLength} / ${core.length}, ${core.peers.length} peers)`
}

cmd.parse()
