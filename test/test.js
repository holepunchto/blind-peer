const test = require('brittle')
const setupTestnet = require('hyperdht/testnet')
const Corestore = require('corestore')
const tmpDir = require('test-tmp')
const { once } = require('events')
const b4a = require('b4a')
const Client = require('@holepunchto/blind-peering/lib/client')
const Hyperswarm = require('hyperswarm')
const BlindPeer = require('..')

const DEBUG = false
let clientCounter = 0 // For clean teardown order

test('client can use a blind-peer to add a core', async t => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  let coreKey = null
  const coreAddedProm = once(blindPeer, 'add-core')
  coreAddedProm.catch(() => {})
  let client = null
  {
    const { core, swarm } = await setupCoreHolder(t, bootstrap)
    swarm.joinPeer(blindPeer.publicKey) // TODO: clean flow for setting up corestore replication (blind-peer client works at dht level, so no swarm connection event)
    client = new Client(blindPeer.publicKey, { dht: swarm.dht })
    await client.ready()

    coreKey = core.key
    await client.addCore(coreKey)
  }

  const [record] = await coreAddedProm
  t.alike(record.key, coreKey, 'added the core')
  t.is(record.priority, 0, '0 Default priority')
  t.is(record.announce, false, 'default no announce')
  await client.close()

  {
    const { swarm, store } = await setupPeer(t, bootstrap)
    const core = store.get({ key: coreKey })
    await core.ready()
    swarm.joinPeer(blindPeer.publicKey, { dht: swarm.dht })
    await swarm.flush()
    const block = await core.get(1)
    t.is(b4a.toString(block), 'Block 1', 'Can download the core from the blind peer')
  }
})

async function getTestnet (t) {
  const testnet = await setupTestnet()
  t.teardown(async () => {
    await testnet.destroy()
  }, { order: Infinity })

  return testnet
}

async function setupPeer (t, bootstrap) {
  const storage = await tmpDir(t)
  const swarm = new Hyperswarm({ bootstrap })
  const store = new Corestore(storage)

  const order = clientCounter++
  swarm.on('connection', c => {
    if (DEBUG) console.log('(CORE HOLDER) connection opened')
    store.replicate(c)
    c.on('error', (e) => {
      if (DEBUG) console.warn(`Swarm error: ${e.stack}`)
    })
  })
  t.teardown(async () => {
    await swarm.destroy()
    await store.close()
  }, { order })

  return { swarm, store }
}

async function setupCoreHolder (t, bootstrap) {
  const { swarm, store } = await setupPeer(t, bootstrap)

  const core = store.get({ name: 'core' })
  await core.append('Block 0')
  await core.append('Block 1')
  swarm.join(core.discoveryKey)

  return { swarm, store, core }
}

async function setupBlindPeer (t, bootstrap, { storage } = {}) {
  if (!storage) storage = await tmpDir(t)

  const swarm = new Hyperswarm({ bootstrap })
  const peer = new BlindPeer(storage, { swarm })

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
    await swarm.destroy()
  }, { order })

  await peer.listen()
  if (DEBUG) {
    peer.swarm.on('connection', () => {
      console.log('Blind peer connection opened')
    })
  }

  return { blindPeer: peer, storage }
}
