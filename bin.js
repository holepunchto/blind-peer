#!/usr/bin/env node

const { command, flag } = require('paparam')
const goodbye = require('graceful-goodbye')
const idEnc = require('hypercore-id-encoding')
const Instrumentation = require('hyper-instrument')

const BlindPeer = require('.')

const cmd = command('blind-peer',
  flag('--storage|-s [path]', 'storage path, defaults to ./blind-peer'),
  flag('--scraper-public-key [scraper-public-key]', 'Public key of a dht-prometheus scraper'),
  flag('--scraper-secret [scraper-secret]', 'Secret of the dht-prometheus scraper'),
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

    if (flags.scraperPublicKey) {
      const swarm = blindPeer.swarm
      console.info('Setting up instrumentation')

      const scraperPublicKey = idEnc.decode(flags.scraperPublicKey)
      const scraperSecret = idEnc.decode(flags.scraperSecret)
      const prometheusServiceName = 'blind-peer'

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
        prometheusServiceName
      })

      instrumentation.registerLogger(console)
      await instrumentation.ready()
    }

    console.info(`Listening at ${idEnc.normalize(blindPeer.publicKey)}`)
    console.info(`Encryption public key is ${idEnc.normalize(blindPeer.encryptionPublicKey)}`)
  }
)

cmd.parse()
