const test = require('brittle')
const setupTestnet = require('hyperdht/testnet')
const Corestore = require('corestore')
const tmpDir = require('test-tmp')
const BlindPeer = require('..')
const Client = require('@holepunchto/blind-peering/lib/client')
const Hyperswarm = require('hyperswarm')

const DEBUG = false
let clientCounter = 0 // For clean teardown order

test('client can use a blind-peer to add a core', async t => {
  t.plan(3)
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm } = await setupCoreHolder(t, bootstrap)
  swarm.joinPeer(blindPeer.publicKey) // TODO: clean flow for setting up corestore replication (blind-peer client works at dht level, so no swarm connection event)
  const client = new Client(blindPeer.publicKey, { dht: swarm.dht })
  await client.ready()
  t.teardown(async () => {
    await client.close()
  }, { order: 0 })

  blindPeer.on('add-core', async (record) => {
    t.alike(record.key, core.key, 'added the core')
    t.is(record.priority, 0, '0 Default priority')
    t.is(record.announce, false, 'default no announce')
  })

  await client.addCore(core.key)
})

async function getTestnet (t) {
  const testnet = await setupTestnet()
  t.teardown(async () => {
    await testnet.destroy()
  }, { order: Infinity })

  return testnet
}

async function setupCoreHolder (t, bootstrap) {
  const storage = await tmpDir(t)
  const swarm = new Hyperswarm({ bootstrap })
  const store = new Corestore(storage)

  const order = clientCounter++
  swarm.on('connection', c => {
    console.log('core holder connection...')
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

  const core = store.get({ name: 'core' })
  await core.append('Block 0')
  await core.append('Block 1')
  swarm.join(core.discoveryKey)

  return { swarm, core }
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
