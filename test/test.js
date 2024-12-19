const { once } = require('events')

const test = require('brittle')
const setupTestnet = require('hyperdht/testnet')
const Hyperswarm = require('hyperswarm')
const Autobase = require('autobase')
const Corestore = require('corestore')
const RAM = require('random-access-memory')
const b4a = require('b4a')
const hypCrypto = require('hypercore-crypto')
const tmpDir = require('test-tmp')
const BlindPeer = require('..')
const Client = require('../client')

const DEBUG = false
let clientCounter = 0 // For clean teardown order

test('client can use a blind-peer to add an autobase message', async t => {
  t.plan(1)

  const { bootstrap } = await getTestnet(t)

  const blindPeer = await setupBlindPeer(t, bootstrap)
  await blindPeer.swarm.flush()

  const { base, swarm: baseSwarm, mailboxId } = await setupAutobase(t, bootstrap, blindPeer.publicKey)
  baseSwarm.joinPeer(blindPeer.publicKey)
  await once(blindPeer, 'add-response') // ensure mailbox registered

  base.view.on('append', async () => {
    const message = await base.view.get(base.view.length - 1)
    t.alike(
      message,
      { hi: 'there' },
      'Message processed by autobase'
    )
  })

  const swarm = new Hyperswarm({ bootstrap })
  swarm.on('connection', async (conn) => {
    const client = new Client(conn)
    await client.postToMailbox({
      id: mailboxId,
      message: b4a.from(JSON.stringify({ hi: 'there' }))
    })
    await client.close()
    await swarm.destroy()
  })
  swarm.joinPeer(blindPeer.publicKey)
})

async function getTestnet (t) {
  const testnet = await setupTestnet()
  t.teardown(async () => {
    await testnet.destroy()
  }, { order: Infinity })

  return testnet
}

async function setupBlindPeer (t, bootstrap) {
  const storage = await tmpDir(t)
  const peer = new BlindPeer(storage)

  if (DEBUG) {
    peer.on('add-request', () => {
      console.log('add mailbox received')
    })
    peer.on('add-response', () => {
      console.log('add mailbox response')
    })
    peer.on('post-request', () => {
      console.log('post req received')
    })
    peer.on('post-response', () => {
      console.log('post response')
    })
  }

  const order = clientCounter++
  t.teardown(async () => {
    await peer.close()
  }, { order })

  await peer.listen({ bootstrap })
  if (DEBUG) {
    peer.swarm.on('connection', () => {
      console.log('Blind peer connection opened')
    })
  }

  return peer
}

async function setupAutobase (t, bootstrap, blindPeerKey) {
  const base = new Autobase(
    new Corestore(RAM.reusable()),
    null,
    {
      encryptionKey: Buffer.alloc(30).fill('secret'),
      ackInterval: 10,
      open (store) {
        return store.get('view', { valueEncoding: 'json' })
      },
      async apply (nodes, view, base) {
        for (const node of nodes) {
          const jsonValue = JSON.parse(node.value.toString())
          if (jsonValue.add) {
            await base.addWriter(Buffer.from(jsonValue.key, 'hex'), { indexer: false })
          } else {
            view.append(jsonValue)
          }
        }
      }
    }
  )

  await base.ready()

  const id = hypCrypto.randomBytes(32)

  const swarm = new Hyperswarm({ bootstrap })
  swarm.on('connection', async c => {
    if (DEBUG) console.log('autobase swarm connection opened')
    base.store.replicate(c)
    if (!c.remotePublicKey.equals(blindPeerKey)) return

    const peer = new Client(c)

    const info = await peer.addMailbox({ id, autobase: base.key })

    if (info.open === false) {
      const message = b4a.from(
        JSON.stringify({ add: true, key: info.writer.toString('hex') })
      )
      await base.append(message)
      await base.update()

      const core = base.store.get({ key: info.writer, active: false })
      await core.setEncryptionKey(base.encryptionKey)
      const req = { id, autobase: base.key, blockEncryptionKey: core.encryption.blockKey }
      await core.close()

      await peer.addMailbox(req)

      if (DEBUG) console.log('autobase opened mailbox')
    }
  })

  const order = clientCounter++
  t.teardown(async () => {
    await base.close()
    await swarm.destroy()
  }, { order })

  return { base, swarm, mailboxId: id }
}
