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
const { createMailbox, getKeyFromEntropy } = require('blind-peer-encodings')

const DEBUG = false
let clientCounter = 0 // For clean teardown order

test('client can use a blind-peer to add an autobase message', async t => {
  t.plan(2)

  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.swarm.flush()

  const { base, swarm: baseSwarm, mailboxId } = await setupAutobase(t, bootstrap, blindPeer.publicKey, blindPeer.encryptionKeyPair.publicKey)

  baseSwarm.joinPeer(blindPeer.publicKey)

  const swarm = new Hyperswarm({ bootstrap })

  let found = false
  base.view.on('append', async () => {
    if (found) return
    if (base.view.length === 0) return
    const message = await base.view.get(base.view.length - 1)
    t.alike(
      message,
      { hi: 'there' },
      'Message processed by autobase'
    )
    found = true
  })

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
  t.plan(4)

  const { bootstrap } = await getTestnet(t)

  let blindPeerStorage = null
  const { blindPeer: blindPeerRun1, storage } = await setupBlindPeer(t, bootstrap)
  blindPeerStorage = storage
  await blindPeerRun1.swarm.flush()

  const { base, swarm: baseSwarm, mailboxId } = await setupAutobase(t, bootstrap, blindPeerRun1.publicKey, blindPeerRun1.encryptionKeyPair.publicKey)

  let found = false
  const msg1Prom = new Promise((resolve) => {
    base.view.on('append', async () => {
      if (found) return
      if (base.view.length === 0) return
      found = true
      const message = await base.view.get(base.view.length - 1)
      t.alike(
        message,
        { hi: 'there' },
        'Message processed by autobase'
      )
      resolve()
    })
  })

  const blindPeerPublicKey = blindPeerRun1.publicKey
  baseSwarm.joinPeer(blindPeerRun1.publicKey)

  {
    const swarm = new Hyperswarm({ bootstrap })
    swarm.on('connection', async (conn) => {
      if (DEBUG) console.log('Swarm received a connection')
      try {
        const client = new Client(conn)
        await client.postToMailbox({
          id: mailboxId,
          message: b4a.from(JSON.stringify({ hi: 'there' }))
        })
        swarm.leavePeer(blindPeerRun1.publicKey)
        await client.close()
        conn.destroy()
        await swarm.destroy()
      } catch (e) {
        console.error('unexpected error while posting to mailbox')
        console.error(e)
        t.fail('error while posting to mailbox')
        return
      }

      t.pass('Successfully posted to mailbox')
    })

    const blindPeerPublicKey = blindPeerRun1.publicKey
    swarm.joinPeer(blindPeerPublicKey)

    await msg1Prom
    await blindPeerRun1.close()
  }

  base.view.on('append', async () => {
    const message = await base.view.get(base.view.length - 1)
    if (!message.hi2) return
    t.alike(
      message,
      { hi2: 'there2' },
      'Message processed by autobase after restart'
    )
  })

  const { blindPeer } = await setupBlindPeer(t, bootstrap, { storage: blindPeerStorage })
  await blindPeer.swarm.flush()
  {
    const swarm = new Hyperswarm({ bootstrap })
    swarm.on('connection', async (conn) => {
      if (DEBUG) console.log('Swarm2 received a connection')
      try {
        const client = new Client(conn)
        await client.postToMailbox({
          id: mailboxId,
          message: b4a.from(JSON.stringify({ hi2: 'there2' }))
        })
        swarm.leavePeer(blindPeerPublicKey)
        await client.close()
        conn.destroy()
        await swarm.destroy()
      } catch (e) {
        console.error('unexpected error while posting to mailbox')
        console.error(e)
        t.fail('error while posting to mailbox')
        return
      }

      t.pass('Peer 2 successfully posted to mailbox')
    })

    swarm.joinPeer(blindPeerPublicKey)
  }
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

async function setupAutobase (t, bootstrap, blindPeerKey, blindPeerEncryptionPublicKey) {
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
          if (DEBUG) console.log('applying', jsonValue)
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

  const swarm = new Hyperswarm({ bootstrap })
  swarm.on('connection', async c => {
    if (DEBUG) console.log('autobase swarm connection opened')
    base.store.replicate(c)
    if (DEBUG) c.on('close', () => { console.log('autobase connection closed') })
  })

  const mailboxEntropy = hypCrypto.randomBytes(32)
  const hypercoreKey = getKeyFromEntropy(mailboxEntropy)

  const addWriterMsg = b4a.from(
    JSON.stringify({ add: true, key: hypercoreKey.toString('hex') })
  )
  await base.append(addWriterMsg)
  await base.update()

  const blockEncryptionKey = base.store.get({ key: hypercoreKey, active: false }).encryption.blockKey

  const mailboxId = createMailbox(
    blindPeerEncryptionPublicKey,
    {
      entropy: mailboxEntropy,
      autobaseKey: base.key,
      blockEncryptionKey
    }
  )

  const order = clientCounter++
  t.teardown(async () => {
    await swarm.destroy()
    await base.close()
  }, { order })

  return { base, swarm, mailboxId }
}
