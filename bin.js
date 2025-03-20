#!/usr/bin/env node

const { command, flag } = require('paparam')
const goodbye = require('graceful-goodbye')
const idEnc = require('hypercore-id-encoding')
const Instrumentation = require('hyper-instrument')
const RegisterClient = require('autobase-discovery/client/register')
const safetyCatch = require('safety-catch')

const BlindPeer = require('.')

const SERVICE_NAME = 'blind-peer'

const cmd = command('blind-peer',
  flag('--storage|-s [path]', 'Storage path, defaults to ./blind-peer'),
  flag('--autodiscovery-rpc-key [autodiscovery-rpc-key]', 'Public key where the autodiscovery service is listening. When set, the autodiscovery-seed must also be set. Can be hex or z32.'),
  flag('--autodiscovery-seed [autodiscovery-seed]', '64-byte seed used to authenticate to the autodiscovery service.  Can be hex or z32.'),
  flag('--autodiscovery-service-name [autodiscovery-service-name]', `Name under which to register the service (default ${SERVICE_NAME})`),
  flag('--scraper-public-key [scraper-public-key]', 'Public key of a dht-prometheus scraper.  Can be hex or z32.'),
  flag('--scraper-secret [scraper-secret]', 'Secret of the dht-prometheus scraper.  Can be hex or z32.'),
  flag('--scraper-alias [scraper-alias]', '(optional) Alias with which to register to the scraper'),
  async function ({ flags }) {
    console.info('Starting blind peer')

    const storage = flags.storage || 'blind-peer'
    const blindPeer = new BlindPeer(storage)

    blindPeer.on('post-to-mailbox', req => {
      try {
        console.log(`post-to-mailbox request received for mailbox: ${idEnc.normalize(req.mailbox)} with message ${idEnc.normalize(req.message)})`)
      } catch {
        console.log('Invalid post-to-mailbox request received')
        console.log(req)
      }
    })

    blindPeer.on('add-core', req => {
      try {
        console.log(`add-core request received for ${idEnc.normalize(req.key)} with referrer ${req.referrer && idEnc.normalize(req.referrer)}`)
      } catch {
        console.log('Invalid add-core request received')
        console.log(req)
      }
    })

    console.info(`Using storage '${storage}'`)

    let instrumentation = null

    goodbye(async () => {
      if (instrumentation) {
        console.info('Closing instrumentation')
        await instrumentation.close()
      }
      console.info('Shutting down blind peer')
      await blindPeer.close()
    })

    await blindPeer.listen()

    blindPeer.swarm.on('connection', (conn, peerInfo) => {
      const key = idEnc.normalize(peerInfo.publicKey)
      console.log(`Opened connection to ${key}`)
      conn.on('close', () => console.log(`Closed connection to ${key}`))
    })

    if (flags.autodiscoveryRpcKey) {
      const autodiscoveryRpcKey = idEnc.decode(flags.autodiscoveryRpcKey)
      const seed = idEnc.decode(flags.autodiscoverySeed)
      const serviceName = flags.autodiscoveryServiceName || SERVICE_NAME
      const registerClient = new RegisterClient(autodiscoveryRpcKey, blindPeer.swarm.dht, seed)

      // No need to block on this, so we run it in the background
      console.info(`Registering own RPC key rpc key ${idEnc.normalize(blindPeer.publicKey)} with service '${serviceName}' at autodiscovery service ${idEnc.normalize(autodiscoveryRpcKey)} (using public key ${idEnc.normalize(registerClient.keyPair.publicKey)})`)
      registerClient.putService(blindPeer.publicKey, serviceName)
        .then(() => { console.info('Successfully requested to be added to the autodiscovery service') })
        .catch(e => { console.warn(`Failed to register to the autodiscovery service: ${e.stack}`) })
        .finally(() => { registerClient.close().catch(safetyCatch) })
    }

    if (flags.scraperPublicKey) {
      const swarm = blindPeer.swarm
      console.info('Setting up instrumentation')

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

      instrumentation.registerLogger(console)
      await instrumentation.ready()
    }

    console.info(`Listening at ${idEnc.normalize(blindPeer.publicKey)}`)
    console.info(`Encryption public key is ${idEnc.normalize(blindPeer.encryptionPublicKey)}`)
  }
)

cmd.parse()
