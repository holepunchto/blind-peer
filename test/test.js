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

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
  coreKey = core.key
  client.addCoreBackground(core)

  const [record] = await coreAddedProm
  t.alike(record.key, coreKey, 'added the core')
  t.is(record.priority, 0, '0 Default priority')
  t.is(record.announce, false, 'default no announce')

  // TODO: expose an event in blind-peer which allows us to detect
  // when a core has updated
  await new Promise(resolve => setTimeout(resolve, 1000))
  await client.close()
  await swarm.destroy() // So the core holder stops announcing the core

  {
    const { swarm, store } = await setupPeer(t, bootstrap)
    const core = store.get({ key: coreKey })
    await core.ready()
    swarm.joinPeer(blindPeer.publicKey, { dht: swarm.dht })

    // TODO: revert to flushing when swarm.flush issue solved
    // await swarm.flush()
    await new Promise(resolve => setTimeout(resolve, 1000))

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

    // TODO: revert to flushing when swarm.flush issue solved
    // await swarm.flush()
    await new Promise(resolve => setTimeout(resolve, 1000))

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

    for (let i = 0; i < nrCores; i++) {
      const core = store.get({ name: `core-${i}` })
      cores.push(core)
      const blocks = []
      for (let j = 0; j < nrBlocks; j++) blocks.push(`core-${i}-block-${j}`)
      await core.append(blocks)
      client.addCoreBackground(core)
    }
  }

  // TODO: some event to ensure they're fully downloaded
  await new Promise(resolve => setTimeout(resolve, 2000))
  const initBytes = blindPeer.digest.bytesAllocated

  const [[{ bytesCleared }]] = await Promise.all([
    once(blindPeer, 'gc-done'),
    blindPeer._gc()
  ])

  const nowBytes = blindPeer.digest.bytesAllocated
  t.is(nowBytes < 10_000, true, 'gcd till below limit')
  t.is(nowBytes > 1000, true, 'did not gc too much')
  t.is(initBytes - bytesCleared, nowBytes, 'Bytes-cleared accounting correct')
  t.is(nowBytes < 10000, true, 'digest updated')
  t.is(blindPeer.digest.bytesAllocated, nowBytes, 'sanity check')

  let gcdCoreI = 0
  let origRecord = await blindPeer.db.getCoreRecord(cores[gcdCoreI].key)
  while (true) {
    origRecord = await blindPeer.db.getCoreRecord(cores[gcdCoreI].key)
    if (origRecord.bytesAllocated === 0) break
    gcdCoreI++
  }

  await cores[gcdCoreI].append('Block-200')
  await new Promise(resolve => setTimeout(resolve, 1000))

  const updatedRecord = await blindPeer.db.getCoreRecord(cores[gcdCoreI].key)

  t.is(origRecord.bytesAllocated, 0, 'sanity check')
  t.is(updatedRecord.bytesAllocated, 9, 'Downloads newly added blocks after gc, but not old ones')
  t.is(updatedRecord.bytesCleared, origRecord.bytesCleared, 'Sanity check on bytesCleared accounting')
  t.is(blindPeer.digest.bytesAllocated > nowBytes, true, 'downloaded the new block')
})

test('Trusted peers can set announce: true to have the blind peer announce it', async t => {
  const { bootstrap } = await getTestnet(t)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const { blindPeer } = await setupBlindPeer(t, bootstrap, { trustedPubKeys: [swarm.dht.defaultKeyPair.publicKey] })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const coreAddedProm = once(blindPeer, 'add-core')
  coreAddedProm.catch(() => {})

  const client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
  const coreKey = core.key
  await client.addCore(core, coreKey, { announce: true })

  const [record] = await coreAddedProm
  t.alike(record.key, coreKey, 'added the core')
  t.is(record.priority, 0, '0 Default priority')
  t.is(record.announce, true, 'announce set')

  // TODO: expose an event in blind-peer which allows us to detect
  // when a core has updated
  await new Promise(resolve => setTimeout(resolve, 1000))
  await client.close()
  await swarm.destroy() // So the core holder stops announcing the core

  {
    const { swarm, store } = await setupPeer(t, bootstrap)
    const core = store.get({ key: coreKey })
    await core.ready()
    swarm.join(core.discoveryKey)

    // TODO: revert to flushing when swarm.flush issue solved
    // await swarm.flush()
    await new Promise(resolve => setTimeout(resolve, 1000))

    const block = await core.get(1)
    t.is(b4a.toString(block), 'Block 1', 'The blind peer is swarming directly on the core (announce processed)')
  }
})

test('Untrusted peers cannot set announce: true', async t => {
  const { bootstrap } = await getTestnet(t)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const { blindPeer } = await setupBlindPeer(t, bootstrap, { trustedPubKeys: [] })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const coreAddedProm = once(blindPeer, 'add-core')
  coreAddedProm.catch(() => {})

  const client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
  const coreKey = core.key
  await client.addCore(core, coreKey, { announce: true })

  const [record] = await coreAddedProm
  t.alike(record.key, coreKey, 'added the core')
  t.is(record.priority, 0, '0 Default priority')
  t.is(record.announce, false, 'announce corrected to false')
  await swarm.destroy() // So the core holder stops announcing the core

  // TODO: expose an event in blind-peer which allows us to detect
  // when a core has updated
  await new Promise(resolve => setTimeout(resolve, 1000))
  await client.close()

  {
    const { swarm, store } = await setupPeer(t, bootstrap)
    const core = store.get({ key: coreKey })
    await core.ready()
    swarm.join(core.discoveryKey)

    // TODO: revert to flushing when swarm.flush issue solved
    // await swarm.flush()
    await new Promise(resolve => setTimeout(resolve, 1000))

    await t.exception(
      async () => {
        await core.get(1, { timeout: 500 })
      },
      /REQUEST_TIMEOUT/,
      'The blind peer is NOT swarming directly on the core (announce not processed)'
    )
  }
})

test('records with announce: true are announced upon startup', async t => {
  const { bootstrap } = await getTestnet(t)
  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const trustedPubKeys = [swarm.dht.defaultKeyPair.publicKey]

  let blindPeerStorage = null
  let coreKey = null

  {
    const { blindPeer, storage } = await setupBlindPeer(t, bootstrap, { trustedPubKeys })
    blindPeerStorage = storage

    await blindPeer.listen()
    await blindPeer.swarm.flush()

    const coreAddedProm = once(blindPeer, 'add-core')
    coreAddedProm.catch(() => {})

    const client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
    coreKey = core.key
    client.addCoreBackground(core, coreKey, { announce: true })

    const [record] = await coreAddedProm
    t.is(record.announce, true, 'announce set (sanity check)')

    // TODO: debug why, without this, we get an unhandled rejection
    await new Promise(resolve => setTimeout(resolve, 1000))

    await client.close()
    await blindPeer.close()
  }

  await swarm.destroy() // So the core holder stops announcing the core

  {
    const { swarm, store } = await setupPeer(t, bootstrap)
    const core = store.get({ key: coreKey })
    await core.ready()
    swarm.join(core.discoveryKey)
    await t.exception(
      async () => {
        await core.get(1, { timeout: 500 })
      },
      /REQUEST_TIMEOUT/,
      'Sanity check: core not available without blind peer'
    )

    const { blindPeer } = await setupBlindPeer(t, bootstrap, { storage: blindPeerStorage, trustedPubKeys })
    await blindPeer.listen()
    await blindPeer.swarm.flush()

    // TODO: revert to flushing when swarm.flush issue solved
    // await swarm.flush()
    await new Promise(resolve => setTimeout(resolve, 1000))

    const block = await core.get(1)
    t.is(b4a.toString(block), 'Block 1', 'Restarted blind peer announces the core')
  }
})

test('Trusted peers can update an existing record to start announcing it', async t => {
  const { bootstrap } = await getTestnet(t)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const { blindPeer } = await setupBlindPeer(t, bootstrap, { trustedPubKeys: [swarm.dht.defaultKeyPair.publicKey] })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
  const coreKey = core.key

  {
    const coreAddedProm = once(blindPeer, 'add-core')
    coreAddedProm.catch(() => {})
    await client.addCore(core, coreKey, { announce: false })

    const [record] = await coreAddedProm
    t.alike(record.key, coreKey, 'added the core')
    t.is(record.priority, 0, '0 Default priority')
    t.is(record.announce, false, 'announce not set')
  }

  {
    const coreAddedProm = once(blindPeer, 'add-core')
    coreAddedProm.catch(() => {})
    await client.addCore(store.get({ key: core.key }), coreKey, { announce: true })

    const [record] = await coreAddedProm
    t.is(record.announce, true, 'announce set in db')
  }

  await swarm.destroy()
  await client.close()
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

// Illustrates a bug
test.skip('Cores added by someone who does not have them are downloaded from other peers', async t => {
  const { bootstrap } = await getTestnet(t)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  swarm.on('connection', conn =>
    console.log('CORE HOLDER swarm key', conn.publicKey.toString('hex'))
  )
  const { swarm: peerSwarm, store: peerStore } = await setupPeer(t, bootstrap)
  const core2 = store.get({ name: 'core2' })
  await core2.append(['core2block0', 'core2block1'])
  console.log('disckey core11', core.discoveryKey.toString('hex'))
  console.log('disckey core2', core2.discoveryKey.toString('hex'))

  const { blindPeer } = await setupBlindPeer(t, bootstrap, { trustedPubKeys: [peerSwarm.dht.defaultKeyPair.publicKey] })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const client = new Client(peerSwarm, peerStore, { mediaMirrors: [blindPeer.publicKey] })
  const coreKey = core.key

  // 1) Add a core with announce: True
  // This works fine (it connects to the peer who owns the core and downloads it)
  {
    const coreAddedProm = once(blindPeer, 'add-core')
    coreAddedProm.catch(() => {})
    const reply = await client.addCore(store.get({ key: core.key }), coreKey, { announce: true })
    t.is(reply.announce, true, 'announce true')

    const [record] = await coreAddedProm
    t.is(record.announce, true, 'announce set in db')

    await new Promise(resolve => setTimeout(resolve, 1000))
    const bpCore = blindPeer.store.get({ key: core.key })
    await bpCore.ready()

    t.is(bpCore.length, 2, 'Got core metadata')
    t.is(bpCore.contiguousLength, 2, 'Downloaded core')
  }

  // Step 2: Add a second core with announce false, which is owned by the same peer
  console.log('\n\n\nADDING 2nd core now\n\n\n')

  {
    const coreAddedProm = once(blindPeer, 'add-core')
    coreAddedProm.catch(() => {})
    const reply = await client.addCore(store.get({ key: core2.key }), core2.key, { announce: false })
    t.is(reply.announce, false, 'announce false (sanity check)')
  }
  await new Promise(resolve => setTimeout(resolve, 1000))

  console.log('\n\n\nCHECKING now\n\n\n')

  const bpCore2 = blindPeer.store.get({ key: core2.key })
  await bpCore2.ready()

  console.log('nr peers', bpCore2.peers.length)
  console.log('blind peer streams', blindPeer.store.streamTracker)
  console.log('core length and contig length', bpCore2.length, bpCore2.contiguousLength)
  t.is(bpCore2.length, 2, 'blind peer saw the updated length')

  // Observed behaviour: no peer gets created, until the core is re-opened again
  // So there must be a race condition somewhere.
  // There is a connection between the peers (I think, to confirm)
  // so it is the peer creation itself which fails

  // If we wait a bit longer, the download will be processed
  // (because we opened a new session)
  // await new Promise(resolve => setTimeout(resolve, 1000))
  // console.log(bpCore2.peers.length)
  // console.log(blindPeer.store.streamTracker)
  // console.log(bpCore2.length, bpCore2.contiguousLength)

  await swarm.destroy()
  await client.close()
})

async function setupCoreHolder (t, bootstrap) {
  const { swarm, store } = await setupPeer(t, bootstrap)

  const core = store.get({ name: 'core' })
  await core.append('Block 0')
  await core.append('Block 1')
  swarm.join(core.discoveryKey)

  return { swarm, store, core }
}

async function setupBlindPeer (t, bootstrap, { storage, maxBytes, enableGc, trustedPubKeys } = {}) {
  if (!storage) storage = await tmpDir(t)

  const swarm = new Hyperswarm({ bootstrap })
  const peer = new BlindPeer(storage, { swarm, maxBytes, enableGc, trustedPubKeys })

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
