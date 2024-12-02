#!/usr/bin/env node

const { command, flag } = require('paparam')
const goodbye = require('graceful-goodbye')
const idEnc = require('hypercore-id-encoding')
const BlindPeer = require('.')

const cmd = command('blind-peer',
  flag('--storage|-s [path]', 'storage path, defaults to ./blind-peer'),
  async function ({ flags }) {
    console.info('Starting blind peer')

    const storage = flags.storage || 'blind-peer'
    const blindPeer = new BlindPeer(storage)

    console.info(`Using storage '${storage}'`)

    goodbye(async () => {
      console.info('Shutting down blind peer')
      await blindPeer.close()
    })

    await blindPeer.listen()

    console.info(`Listening at ${idEnc.normalize(blindPeer.publicKey)}`)
  }
)

cmd.parse()
