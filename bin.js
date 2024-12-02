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

    blindPeer.on('add-mailbox-received', req => {
      try {
        console.log(`add mailbox received for autobase ${idEnc.normalize(req.autobase)}`)
      } catch {
        console.log('Invalid add-mailbox request received')
        console.log(req)
      }
    })

    blindPeer.on('post-received', req => {
      try {
        console.log(`Post received for autobase ${idEnc.normalize(req.autobase)}`)
      } catch {
        console.log('Invalid post request received')
        console.log(req)
      }
    })

    console.info(`Using storage '${storage}'`)

    goodbye(async () => {
      console.info('Shutting down blind peer')
      await blindPeer.close()
    })

    await blindPeer.listen()

    blindPeer.swarm.on('connection', (conn, peerInfo) => {
      const key = idEnc.normalize(peerInfo.publicKey)
      console.log(`Opened connection to ${key}`)
      conn.on('close', () => console.log(`Closed connection to ${key}`))
    })

    console.info(`Listening at ${idEnc.normalize(blindPeer.publicKey)}`)
  }
)

cmd.parse()
