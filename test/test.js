const test = require('brittle')
const setupTestnet = require('hyperdht/testnet')
const Corestore = require('corestore')
const tmpDir = require('test-tmp')
const { once } = require('events')
const b4a = require('b4a')
const Client = require('@holepunchto/blind-peering')
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
    const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
    client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
    coreKey = core.key
    client.addCoreBackground(core)
  }

  const [record] = await coreAddedProm
  t.alike(record.key, coreKey, 'added the core')
  t.is(record.priority, 0, '0 Default priority')
  t.is(record.announce, false, 'default no announce')

  // TODO: expose an event in blind-peer which allows us to detect
  // when a core has updated
  await new Promise(resolve => setTimeout(resolve, 1000))
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

test('can lookup core after blind peer restart', async t => {
  const { bootstrap } = await getTestnet(t)

  let blindPeerStorage = null
  let coreKey = null

  {
    const { blindPeer, storage } = await setupBlindPeer(t, bootstrap)
    blindPeerStorage = storage
    await blindPeer.listen()
    await blindPeer.swarm.flush()

    const coreAddedProm = once(blindPeer, 'add-core')

    coreAddedProm.catch(() => {})
    let client = null
    {
      const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
      client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
      coreKey = core.key
      client.addCoreBackground(core)
    }

    const [record] = await coreAddedProm
    t.alike(record.key, coreKey, 'added the core')

    // TODO: expose an event in blind-peer which allows us to detect
    // when a core has updated
    await new Promise(resolve => setTimeout(resolve, 1000))
    await client.close()
    await blindPeer.close()
  }

  {
    const { blindPeer } = await setupBlindPeer(t, bootstrap, { storage: blindPeerStorage })
    await blindPeer.listen()
    await blindPeer.swarm.flush()

    const { swarm, store } = await setupPeer(t, bootstrap)
    const core = store.get({ key: coreKey })
    await core.ready()
    swarm.joinPeer(blindPeer.publicKey, { dht: swarm.dht })
    await swarm.flush()
    const block = await core.get(1)
    t.is(b4a.toString(block), 'Block 1', 'Can download the core from the restarted blind peer')
  }
})

test('garbage collection when space limit reached', async t => {
  const { bootstrap } = await getTestnet(t)

  const enableGc = false // We trigger it manually, so we can test the accounting
  const { blindPeer } = await setupBlindPeer(t, bootstrap, { enableGc, maxBytes: 10_000 })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const nrCores = 10
  const nrBlocks = 200
  const cores = []

  const { swarm, store } = await setupCoreHolder(t, bootstrap)
  {
    const client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
    t.teardown(async () => {
      await client.close()
    }, { order: 0 })

    // First batch--these will definitely be gc'd because they will have lower 'last activity'
    for (let i = 0; i < 5; i++) {
      const core = store.get({ name: `core-${i}` })
      cores.push(core)
      const blocks = []
      for (let j = 0; j < nrBlocks; j++) blocks.push(`core-${i}-block-${j}`)
      await core.append(blocks)
      client.addCoreBackground(core)
    }

    // TODO: expose an event in blind-peer which allows us to detect
    // when a core has updated
    await new Promise(resolve => setTimeout(resolve, 1000))

    // Second batch--some of these will be gc'd, but which exactly depends
    // on the order in which their blocks are downloaded (so we don't rely on it)
    for (let i = 5; i < nrCores; i++) {
      const core = store.get({ name: `core-${i}` })
      cores.push(core)
      const blocks = []
      for (let j = 0; j < nrBlocks; j++) blocks.push(`core-${i}-block-${j}`)
      await core.append(blocks)
      client.addCoreBackground(core)
    }
  }

  await new Promise(resolve => setTimeout(resolve, 1000))
  const initBytes = blindPeer.digest.bytesAllocated

  const [[{ bytesCleared }]] = await Promise.all([
    once(blindPeer, 'gc-done'),
    blindPeer.gc()
  ])

  const nowBytes = blindPeer.digest.bytesAllocated
  t.is(nowBytes < 10_000, true, 'gcd till below limit')
  t.is(nowBytes > 1000, true, 'did not gc too much')
  t.is(initBytes - bytesCleared, nowBytes, 'Bytes-cleared accounting correct')
  t.is(nowBytes < 10000, true, 'digest updated')

  t.is(cores[0].length, 200, 'sanity check')
  t.is(blindPeer.digest.bytesAllocated, nowBytes, 'sanity check')

  let updatedBytesAllocated = 0
  let updatedBytesCleared = 0
  for (const c of cores.slice(0, 5)) { // The cores which definitely got gc'd
    const rec = await blindPeer.db.getCoreRecord(c.key)
    updatedBytesAllocated += rec.bytesAllocated
    updatedBytesCleared += rec.bytesCleared
  }

  t.is(updatedBytesAllocated, 0, 'All cores got gcd')
  t.is(updatedBytesCleared > 10000, true, 'bytesCleared accounting correct')

  const origRecord = await blindPeer.db.getCoreRecord(cores[0].key)
  await cores[0].append('Block-200')
  await new Promise(resolve => setTimeout(resolve, 1000))

  const updatedRecord = await blindPeer.db.getCoreRecord(cores[0].key)

  t.is(origRecord.bytesAllocated, 0, 'sanity check')
  t.is(updatedRecord.bytesAllocated, 9, 'Downloads newly added blocks after gc, but not old ones')
  t.is(updatedRecord.bytesCleared, origRecord.bytesCleared, 'Sanity check on bytesCleared accounting')
  t.is(blindPeer.digest.bytesAllocated > nowBytes, true, 'downloaded the new block')
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

async function setupBlindPeer (t, bootstrap, { storage, maxBytes, enableGc } = {}) {
  if (!storage) storage = await tmpDir(t)

  const swarm = new Hyperswarm({ bootstrap })
  const peer = new BlindPeer(storage, { swarm, maxBytes, enableGc })

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
