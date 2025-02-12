const test = require('brittle')
const setupTestnet = require('hyperdht/testnet')
const Hyperswarm = require('hyperswarm')
const Autobase = require('autobase')
const Corestore = require('corestore')
const b4a = require('b4a')
const hypCrypto = require('hypercore-crypto')
const tmpDir = require('test-tmp')
const BlindPeer = require('..')
const Client = require('@holepunchto/blind-peer-client/lib/client')

const DEBUG = false
let clientCounter = 0 // For clean teardown order

test('client can use a blind-peer to add an autobase message', async t => {
  t.plan(2)

  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.swarm.flush()

  const { base, swarm: baseSwarm, mailboxId } = await setupAutobase(t, bootstrap, blindPeer.publicKey)

  baseSwarm.joinPeer(blindPeer.publicKey)
  await new Promise(resolve => { // ensure mailbox fully registered
    blindPeer.on('add-mailbox-response', (req, res) => {
      // Set when the mailbox is fully open
      // (which with the autobase connection flow currently happens
      //  after 2 add-mailbox requests when starting from scratch)
      if (res.blockEncryptionKey) resolve()
    })
  })

  base.view.once('append', async () => {
    const message = await base.view.get(base.view.length - 1)
    t.alike(
      message,
      { hi: 'there' },
      'Message processed by autobase'
    )
  })

  const swarm = new Hyperswarm({ bootstrap })
  swarm.on('connection', async (conn) => {
    try {
      const client = new Client(conn)
      await client.postToMailbox({
        id: mailboxId,
        message: b4a.from(JSON.stringify({ hi: 'there' }))
      })
      swarm.leavePeer(blindPeer.publicKey)
      await client.close()
      await swarm.destroy()
    } catch (e) {
      console.error('unexpected error while posting to mailbox')
      console.error(e)
      t.fail('error while posting to mailbox')
      return
    }

    t.pass('Successfully posted to mailbox')
  })
  swarm.joinPeer(blindPeer.publicKey)
})

test('can send autobase message with restarted blind-peer', async t => {
  t.plan(2)

  const { bootstrap } = await getTestnet(t)

  let blindPeerStorage = null
  const { blindPeer: blindPeerRun1, storage } = await setupBlindPeer(t, bootstrap)
  blindPeerStorage = storage
  await blindPeerRun1.swarm.flush()

  const { base, swarm: baseSwarm, mailboxId } = await setupAutobase(t, bootstrap, blindPeerRun1.publicKey)

  baseSwarm.joinPeer(blindPeerRun1.publicKey)
  await new Promise(resolve => { // ensure mailbox fully registered
    blindPeerRun1.on('add-mailbox-response', (req, res) => {
      // Set when the mailbox is fully open
      // (which with the autobase connection flow currently happens
      //  after 2 add-mailbox requests when starting from scratch)
      if (res.blockEncryptionKey) resolve()
    })
  })

  await blindPeerRun1.close()

  const { blindPeer } = await setupBlindPeer(t, bootstrap, { storage: blindPeerStorage })
  await blindPeer.swarm.flush()

  base.view.once('append', async () => {
    const message = await base.view.get(base.view.length - 1)
    t.alike(
      message,
      { hi: 'there' },
      'Message processed by autobase'
    )
  })

  const swarm = new Hyperswarm({ bootstrap })
  swarm.on('connection', async (conn) => {
    try {
      const client = new Client(conn)
      await client.postToMailbox({
        id: mailboxId,
        message: b4a.from(JSON.stringify({ hi: 'there' }))
      })
      swarm.leavePeer(blindPeer.publicKey)
      await client.close()
      await swarm.destroy()
    } catch (e) {
      console.error('unexpected error while posting to mailbox')
      console.error(e)
      t.fail('error while posting to mailbox')
      return
    }

    t.pass('Successfully posted to mailbox')
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

async function setupBlindPeer (t, bootstrap, { storage } = {}) {
  if (!storage) storage = await tmpDir(t)
  const peer = new BlindPeer(storage)

  if (DEBUG) {
    peer.on('add-mailbox-request', (req) => {
      console.log('add-mailbox request received for autobase', req.autobase)
    })
    peer.on('add-mailbox-response', (req, resp) => {
      console.log('add-mailbox response for autobase', resp.autobase)
    })
    peer.on('post-to-mailbox-request', () => {
      console.log('post-to-mailbox req received')
    })
    peer.on('post-to-mailbox-response', () => {
      console.log('post to mailbox response')
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

  return { blindPeer: peer, storage }
}

async function setupAutobase (t, bootstrap, blindPeerKey) {
  const storage = await tmpDir(t)
  const base = new Autobase(
    new Corestore(storage),
    null,
    {
      encryptionKey: Buffer.alloc(30).fill('secret'),
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

    await peer.close()
  })

  const order = clientCounter++
  t.teardown(async () => {
    await swarm.destroy()
    await base.close()
  }, { order })

  return { base, swarm, mailboxId: id }
}
