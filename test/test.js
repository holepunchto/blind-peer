const test = require('brittle')
const setupTestnet = require('hyperdht/testnet')
const Corestore = require('corestore')
const tmpDir = require('test-tmp')
const { once } = require('events')
const b4a = require('b4a')
const Client = require('blind-peering')
const BlindPeerMuxer = require('blind-peer-muxer')
const Hyperswarm = require('hyperswarm')
const promClient = require('bare-prom-client')
const Autobase = require('autobase')
const IdEnc = require('hypercore-id-encoding')
const ProtomuxRPCRouter = require('protomux-rpc-router')
const BlindPeerRouter = require('blind-peer-router')
const crypto = require('hypercore-crypto')
const BlindPeer = require('..')
const TopKWindow = require('../lib/top-k.js')

const DEBUG = false
let clientCounter = 0 // For clean teardown order
const clientOpts = { batchIdleWait: 250, batchMaxWait: 1000 }

test('client can use a blind-peer to add a core', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  let coreKey = null
  const coreAddedProm = once(blindPeer, 'add-core')

  coreAddedProm.catch(() => {})
  let client = null

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  coreKey = core.key
  client.addCoreBackground(core)

  const [record] = await coreAddedProm
  t.alike(record.key, coreKey, 'added the core')
  t.is(record.priority, 0, '0 Default priority')
  t.is(record.announce, false, 'default no announce')

  // TODO: expose an event in blind-peer which allows us to detect
  // when a core has updated
  await new Promise((resolve) => setTimeout(resolve, 1000))
  await client.close()
  await swarm.destroy() // So the core holder stops announcing the core

  {
    const { swarm, store } = await setupPeer(t, bootstrap)
    const core = store.get({ key: coreKey })
    await core.ready()
    swarm.joinPeer(blindPeer.publicKey, { dht: swarm.dht })

    // TODO: revert to flushing when swarm.flush issue solved
    // await swarm.flush()
    await new Promise((resolve) => setTimeout(resolve, 1000))

    const block = await core.get(1)
    t.is(b4a.toString(block), 'Block 1', 'Can download the core from the blind peer')
  }
})

test('client can use a blind-peer to add an autobase', async (t) => {
  const tFirstAdd = t.test()
  tFirstAdd.plan(2)

  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const {
    swarm: indexerSwarm,
    base: indexer,
    store: indexerStore
  } = await setupAutobaseHolder(t, bootstrap)
  await indexerSwarm.flush()

  const bases = []
  for (let i = 0; i < 2; i++) {
    const { swarm, base, store } = await setupAutobaseHolder(t, bootstrap, indexer.local.key)
    await swarm.flush()
    await Promise.all([
      once(base, 'is-indexer'),
      indexer.append({ add: b4a.toString(base.local.key, 'hex') })
    ])

    await base.append({ some: 'thing' })
    bases.push({ base, swarm, store })
  }

  await indexer.append({ some: 'thing' })
  for (const { base } of bases) {
    await base.append({ other: 'thing' })
  }

  await new Promise((resolve) => setTimeout(resolve, 1000)) // Give time to stabilise the signed lengths
  t.is(indexer.activeWriters.map.size, 3, '3 active writers (sanity check)')

  const nrCoresInAutobase = 6 // could change if autobase internals change

  // A first writer adds the autobase
  {
    const expectedAddedKeys = new Set([
      ...[...indexer.views()].map((v) => b4a.toString(v.key, 'hex')),
      ...[...indexer.activeWriters].map((w) => b4a.toString(w.core.key, 'hex'))
    ])
    t.is(expectedAddedKeys.size, nrCoresInAutobase, 'sanity check')

    let nrAdded = 0
    const addedKeys = new Set()
    const onaddcore = (record) => {
      nrAdded++
      addedKeys.add(b4a.toString(record.key, 'hex'))
      if (addedKeys.size > expectedAddedKeys.size) {
        t.fail('more keys added than expected')
      }
      if (addedKeys.size === expectedAddedKeys.size) {
        if (DEBUG) {
          console.log('total add core requests received', nrAdded, 'unique:', addedKeys.size)
        }
        tFirstAdd.alike(addedKeys, expectedAddedKeys, 'expected cores added')
        tFirstAdd.is(nrAdded, expectedAddedKeys.size, 'no duplicate add-core requests')
      }
    }
    blindPeer.on('add-core', onaddcore)

    const client = new Client(indexerSwarm.dht, indexerStore, {
      ...clientOpts,
      keys: [blindPeer.publicKey]
    })
    await client.addAutobase(indexer)
    await tFirstAdd

    // Give some time to sync
    await new Promise((resolve) => setTimeout(resolve, 500))
    blindPeer.off('add-core', onaddcore)
  }

  // Another writer adds the autobase as well.
  // No cores get added, because nothing changed
  {
    let nrAdded = 0
    const addedKeys = new Set()
    const onaddcore = (record) => {
      nrAdded++
      if (DEBUG) console.log('added core', nrAdded)
      addedKeys.add(b4a.toString(record.key, 'hex'))
    }
    blindPeer.on('add-core', onaddcore)
    const requestProcessed = once(blindPeer, 'add-cores-done')

    const client = new Client(bases[0].swarm.dht, bases[0].store, { keys: [blindPeer.publicKey] })
    await client.addAutobase(bases[0].base)
    await requestProcessed

    t.is(addedKeys.size, 0, 'no keys were added in the second run')
  }
})

test('adding autobase cores only results in replication sessions if there are length differences', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  let {
    swarm: indexerSwarm,
    base: indexer,
    store: indexerStore
  } = await setupAutobaseHolder(t, bootstrap)
  await indexerSwarm.flush()

  const bases = []
  for (let i = 0; i < 2; i++) {
    const { swarm, base, store } = await setupAutobaseHolder(t, bootstrap, indexer.local.key)
    await swarm.flush()
    await Promise.all([
      once(base, 'is-indexer'),
      indexer.append({ add: b4a.toString(base.local.key, 'hex') })
    ])

    await base.append({ some: 'thing' })
    bases.push({ base, swarm, store })
  }

  await indexer.append({ some: 'thing' })
  for (const { base } of bases) {
    await base.append({ some: 'thing' })
  }

  await new Promise((resolve) => setTimeout(resolve, 1000)) // Stabilise the views
  t.is(indexer.activeWriters.map.size, 3, '3 active writers (sanity check)')

  await Promise.all(bases.map(({ base }) => base.close())) // To avoid length updates due to acks etc.

  t.is(blindPeer.stats.activations, 0, 'sanity check')

  // A first writer adds the autobase
  {
    const client = new Client(indexerSwarm.dht, indexerStore, { keys: [blindPeer.publicKey] })
    t.teardown(async () => await client.close())
    await Promise.all([once(blindPeer, 'add-cores-done'), client.addAutobase(indexer)])

    t.is(blindPeer.stats.activations, 6, '3 views and all 3 indexer core activated')
    await new Promise((resolve) => setTimeout(resolve, 500)) // Give time to download the cores

    // 2nd time, everything is already known (no change in autobase state)
    // Re-opening needed, else it won't be added again by the client
    await indexer.close()
    {
      const { base } = await loadAutobase(indexerStore, null)
      indexer = base
    }

    await Promise.all([once(blindPeer, 'add-cores-done'), client.addAutobase(indexer)])
    await new Promise((resolve) => setTimeout(resolve, 500)) // Give time to stabilise

    t.is(blindPeer.stats.activations, 6, 'no cores changed so nothing activated')

    // third time, one core updates and is intantly sent
    // Re-opening needed, else it won't be added again by the client
    await indexer.close()
    {
      const { base } = await loadAutobase(indexerStore, null)
      indexer = base
    }

    await indexer.append({ 'a new': 'length' })

    await Promise.all([once(blindPeer, 'add-cores-done'), client.addAutobase(indexer)])
    await new Promise((resolve) => setTimeout(resolve, 500)) // Give time to finish gossiping lengths (normally redundant)

    const lengthAtEnd = (await blindPeer.store.storage.getInfos([indexer.local.discoveryKey]))[0]
      .head.length
    t.is(lengthAtEnd, indexer.local.length, 'after add core they both know the same length')
  }
})

test('view cores are added with no wakeup set', async (t) => {
  t.plan(4)

  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { swarm, base, store } = await setupAutobaseHolder(t, bootstrap)

  let nrAdded = 0
  console.log(base.views())
  const viewKeys = new Set(base.views().map((v) => IdEnc.normalize(v.key)))
  // A writer adds the autobase
  {
    const onaddcore = (record) => {
      console.log('record', record)
      nrAdded++

      if (viewKeys.has(IdEnc.normalize(record.key))) {
        t.is(record.wakeup, false, 'no wakeup for view key')
      } else {
        t.is(record.wakeup, true, 'wakeup for non-view key')
      }
    }
    blindPeer.on('add-core', onaddcore)

    const client = new Client(swarm.dht, store, {
      ...clientOpts,
      keys: [blindPeer.publicKey]
    })
    await client.addAutobase(base)
  }
})

test('Client stats correctness', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  {
    const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
    const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
    await Promise.all([once(blindPeer, 'add-cores-done'), client.addCore(core)])

    t.is(client.stats.addCore, 1, 'addCore stat')
    t.is(client.stats.addCoresTx, 1, 'addCoresTx stat')
    t.is(client.stats.addAutobase, 0, 'sanity check')
  }

  {
    const { base, swarm, store } = await setupAutobaseHolder(t, bootstrap)
    const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
    await Promise.all([once(blindPeer, 'add-cores-done'), client.addAutobase(base)])

    // addCore somtimes gets called extra by the client logic, so we can't test exact numbers for those
    t.is(client.stats.addCoresTx >= 1, true, 'addCoresTx stat')
    t.is(client.stats.addAutobase, 1, 'addAutobase stat')
  }

  t.is(blindPeer.stats.addCoresRx >= 2, true, 'sanity check')
  t.is(blindPeer.stats.muxerPaired >= 0, true, 'sanity check')
  t.is(blindPeer.stats.muxerErrors === 0, true, 'sanity check')
})

test('blind-peering respects max batch options', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  let {
    swarm: indexerSwarm,
    base: indexer,
    store: indexerStore
  } = await setupAutobaseHolder(t, bootstrap)
  await indexerSwarm.flush()

  const getLengths = (base) => [...base.activeWriters.map.values()].map((b) => b.core.length)

  const peers = []
  for (let i = 0; i < 6; i++) {
    peers.push(await getWakeupPeer(t, bootstrap, indexer, blindPeer))
  }
  t.is(indexer.activeWriters.map.size, 6, 'all active writers (sanity check)')

  // Give some time for them to gossip their lengths
  await new Promise((resolve) => setTimeout(resolve, 500))
  const initLengths = getLengths(indexer)
  t.is(blindPeer.stats.activations, 0, 'sanity check')

  // A first writer adds the autobase
  {
    const client = new Client(indexerSwarm.dht, indexerStore, {
      keys: [blindPeer.publicKey],
      maxBatchMin: 3,
      maxBatchMax: 7
    })
    t.teardown(async () => await client.close())
    await client.addAutobase(indexer)

    await new Promise((resolve) => setTimeout(resolve, 500))
    t.alike(getLengths(indexer), initLengths, 'sanity check: autobase cores did not change')
    t.is(blindPeer.stats.activations, 7, '3 views and 4 indexer cores activated')
  }
})

test('repeated add-core requests do not result in db updates', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  const client2 = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  const client3 = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })

  t.is(await blindPeer.db.getCoreRecord(core.key), null, 'sanity check')
  const coreKey = core.key
  await Promise.all([once(blindPeer, 'add-cores-done'), client.addCore(core)])
  const record = await blindPeer.db.getCoreRecord(core.key)

  t.alike(record.key, coreKey, 'added the core (sanity check)')

  // wait for it to be downloaded
  await new Promise((resolve) => setTimeout(resolve, 1000))
  const initFlushes = blindPeer.db.stats.flushes
  t.is(initFlushes > 0, true, 'sanity check')

  await client2.addCore(core)
  t.is(blindPeer.db.stats.flushes, initFlushes, 'did not flush db again')

  await client3.addCore(core, { priority: 1 })
  t.is(blindPeer.db.stats.flushes, initFlushes, 'flush db not called, even if record changed')
  await blindPeer.flush()
  const record3 = await blindPeer.db.getCoreRecord(core.key)
  t.is(record3.priority, 0, 'cannot change the record after it was added')

  await client.close()
  await client2.close()
  await client3.close()
})

test('relayThrough opt passed through', async (t) => {
  t.plan(1)
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const relayThrough = () => {
    t.pass('It was relayed')
    return false
  }
  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey], relayThrough })
  await client.addCore(core)
  await client.close()
})

test('can lookup core after blind peer restart', async (t) => {
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
      client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
      coreKey = core.key
      client.addCoreBackground(core)
    }

    const [record] = await coreAddedProm
    t.alike(record.key, coreKey, 'added the core')

    // TODO: expose an event in blind-peer which allows us to detect
    // when a core has updated
    await new Promise((resolve) => setTimeout(resolve, 1000))
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
    await new Promise((resolve) => setTimeout(resolve, 1000))

    const block = await core.get(1)
    t.is(b4a.toString(block), 'Block 1', 'Can download the core from the restarted blind peer')
  }
})

test('garbage collection when space limit reached', async (t) => {
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
    const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
    t.teardown(
      async () => {
        await client.close()
      },
      { order: 0 }
    )

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
  await new Promise((resolve) => setTimeout(resolve, 2000))
  const initBytes = blindPeer.digest.bytesAllocated

  const [[{ bytesCleared }]] = await Promise.all([once(blindPeer, 'gc-done'), blindPeer._gc()])

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
  await new Promise((resolve) => setTimeout(resolve, 1000))

  const updatedRecord = await blindPeer.db.getCoreRecord(cores[gcdCoreI].key)

  t.is(origRecord.bytesAllocated, 0, 'sanity check')
  t.is(updatedRecord.bytesAllocated, 9, 'Downloads newly added blocks after gc, but not old ones')
  t.is(
    updatedRecord.bytesCleared,
    origRecord.bytesCleared,
    'Sanity check on bytesCleared accounting'
  )
  t.is(blindPeer.digest.bytesAllocated > nowBytes, true, 'downloaded the new block')
})

test('can gc core that is not currently active', async (t) => {
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
    const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
    t.teardown(
      async () => {
        await client.close()
      },
      { order: 0 }
    )

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
  await new Promise((resolve) => setTimeout(resolve, 2000))

  await swarm.destroy()
  await store.close()
  // TODO: expose corestore gc tick time (it takes 4 ticks to gc weak cores)
  await new Promise((resolve) => setTimeout(resolve, 10000))

  t.is(blindPeer.activeReplication.size, 0, 'sanity check (core not active)')
  t.ok(blindPeer.digest.bytesAllocated > 10_000, 'sanity check')

  await Promise.all([once(blindPeer, 'gc-done'), blindPeer._gc()])

  const nowBytes = blindPeer.digest.bytesAllocated
  t.is(nowBytes < 10_000, true, 'gcd till below limit')
  t.is(nowBytes > 1000, true, 'did not gc too much')
})

test('Trusted peers can set announce: true to have the blind peer announce it', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    trustedPubKeys: [swarm.dht.defaultKeyPair.publicKey]
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const coreAddedProm = once(blindPeer, 'add-core')
  coreAddedProm.catch(() => {})

  t.is(blindPeer.activeReplication.size, 0, 'sanity check (no cores yet)')

  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  const coreKey = core.key
  await Promise.all([once(blindPeer, 'add-cores-done'), client.addCore(core, { announce: true })])

  const [record] = await coreAddedProm
  t.alike(record.key, coreKey, 'added the core')
  t.is(record.priority, 0, '0 Default priority')
  t.is(record.announce, true, 'announce set')

  t.is(blindPeer.activeReplication.size, 1, 'added to active replication set')

  // TODO: expose an event in blind-peer which allows us to detect
  // when a core has updated
  await new Promise((resolve) => setTimeout(resolve, 1000))
  await client.close()
  await swarm.destroy() // So the core holder stops announcing the core

  {
    const { swarm, store } = await setupPeer(t, bootstrap)
    const core = store.get({ key: coreKey })
    await core.ready()
    swarm.join(core.discoveryKey)

    // TODO: revert to flushing when swarm.flush issue solved
    // await swarm.flush()
    await new Promise((resolve) => setTimeout(resolve, 1000))

    const block = await core.get(1)
    t.is(
      b4a.toString(block),
      'Block 1',
      'The blind peer is swarming directly on the core (announce processed)'
    )
  }
})

test('Untrusted peers cannot set announce: true', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const { blindPeer } = await setupBlindPeer(t, bootstrap, { trustedPubKeys: [] })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const coreAddedProm = once(blindPeer, 'add-core')
  coreAddedProm.catch(() => {})

  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  const coreKey = core.key
  await client.addCore(core, { announce: true })

  // TODO: a flow for the client to figure out if it got downgraded

  const [record] = await coreAddedProm
  t.alike(record.key, coreKey, 'added the core')
  t.is(record.priority, 0, '0 Default priority')
  t.is(record.announce, false, 'announce corrected to false')
  await swarm.destroy() // So the core holder stops announcing the core

  // TODO: expose an event in blind-peer which allows us to detect
  // when a core has updated
  await new Promise((resolve) => setTimeout(resolve, 1000))
  await client.close()

  {
    const { swarm, store } = await setupPeer(t, bootstrap)
    const core = store.get({ key: coreKey })
    await core.ready()
    swarm.join(core.discoveryKey)

    // TODO: revert to flushing when swarm.flush issue solved
    // await swarm.flush()
    await new Promise((resolve) => setTimeout(resolve, 1000))

    await t.exception(
      async () => {
        await core.get(1, { timeout: 500 })
      },
      /REQUEST_TIMEOUT/,
      'The blind peer is NOT swarming directly on the core (announce not processed)'
    )
  }
})

test('records with announce: true are announced upon startup', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const trustedPubKeys = [swarm.dht.defaultKeyPair.publicKey]

  let blindPeerStorage = null
  let coreKey = null
  let replicatedDiscKeys = null
  {
    const { blindPeer, storage } = await setupBlindPeer(t, bootstrap, { trustedPubKeys })
    blindPeerStorage = storage

    await blindPeer.listen()
    await blindPeer.swarm.flush()

    const coreAddedProm = once(blindPeer, 'add-core')
    coreAddedProm.catch(() => {})

    const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
    coreKey = core.key
    client.addCoreBackground(core, { announce: true })

    const [record] = await coreAddedProm
    t.is(record.announce, true, 'announce set (sanity check)')

    // TODO: debug why, without this, we get an unhandled rejection
    await new Promise((resolve) => setTimeout(resolve, 1000))

    replicatedDiscKeys = [...blindPeer.activeReplication.keys()]
    t.alike(replicatedDiscKeys, [b4a.toString(core.discoveryKey, 'hex')])

    await client.close()
    await blindPeer.close()
  }

  await swarm.destroy() // So the core holder stops announcing the core

  {
    const { swarm, store } = await setupPeer(t, bootstrap)
    const core = store.get({ key: coreKey })
    await core.ready()
    const topic = swarm.join(core.discoveryKey)
    await t.exception(
      async () => {
        await core.get(1, { timeout: 500 })
      },
      /REQUEST_TIMEOUT/,
      'Sanity check: core not available without blind peer'
    )

    const { blindPeer } = await setupBlindPeer(t, bootstrap, {
      storage: blindPeerStorage,
      trustedPubKeys
    })
    await Promise.all([blindPeer.listen(), once(blindPeer, 'announced-initial-cores')])

    t.alike(
      [...blindPeer.activeReplication.keys()],
      replicatedDiscKeys,
      'announced core is tracked upon startup'
    )

    // TODO: revert to flushing when swarm.flush issue solved
    // await swarm.flush()
    await topic.refresh()

    const block = await core.get(1)
    t.is(b4a.toString(block), 'Block 1', 'Restarted blind peer announces the core')
  }
})

test('Trusted peers can update an existing record to start announcing it', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    trustedPubKeys: [swarm.dht.defaultKeyPair.publicKey]
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  const coreKey = core.key

  {
    const coreAddedProm = once(blindPeer, 'add-core')
    coreAddedProm.catch(() => {})
    await client.addCore(core, { announce: false })

    const [record] = await coreAddedProm
    t.alike(record.key, coreKey, 'added the core')
    t.is(record.priority, 0, '0 Default priority')
    t.is(record.announce, false, 'announce not set')
  }

  {
    const coreAddedProm = once(blindPeer, 'add-core')
    coreAddedProm.catch(() => {})
    await client.addCore(store.get({ key: core.key }), { announce: true })

    const [record] = await coreAddedProm
    t.is(record.announce, true, 'announce set in db')
    t.is((await blindPeer.db.getCoreRecord(record.key)).announce, true)
  }

  await swarm.destroy()
  await client.close()
})

// TODO: add delete to client
test.skip('Trusted peers can delete a core', async (t) => {
  const tEvents = t.test('events')
  tEvents.plan(7)

  const { bootstrap } = await getTestnet(t)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const trustedPubKeys = [swarm.dht.defaultKeyPair.publicKey]
  const { blindPeer } = await setupBlindPeer(t, bootstrap, { trustedPubKeys })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  let firstDelete = true
  blindPeer.on('delete-core', (stream, { key, existing }) => {
    if (firstDelete) {
      tEvents.alike(stream.remotePublicKey, trustedPubKeys[0], 'delete-core stream')
      tEvents.alike(key, core.key, 'delete-core key')
      tEvents.is(existing, true, 'delete-core existing')
      firstDelete = false
      return
    }
    tEvents.is(existing, false, 'delete-core existing when it is not')
  })
  blindPeer.on('delete-core-end', (stream, { key, announced }) => {
    tEvents.alike(stream.remotePublicKey, trustedPubKeys[0], 'delete-core-end stream')
    tEvents.alike(key, core.key, 'delete-core-end key')
    tEvents.is(announced, true, 'delete-core-end announced')
  })
  const coreAddedProm = once(blindPeer, 'add-core')
  coreAddedProm.catch(() => {})

  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  const coreKey = core.key
  await client.addCore(core, { announce: true })

  const [record] = await coreAddedProm
  t.alike(record.key, coreKey, 'added the core')
  t.is(await blindPeer.db.hasCore(coreKey), true, 'core in db')

  // give it time to download
  await new Promise((resolve) => setTimeout(resolve, 1000))

  t.is(blindPeer.db.digest.cores, 1, '1 core in digest (sanity check)')
  t.is(blindPeer.db.digest.bytesAllocated > 0, true, 'digest has bytes allocated of the core')

  const [res] = await client.deleteCore(coreKey)
  t.is(res, true, 'returns true if core existed and is now deleted')
  t.is(await blindPeer.db.hasCore(coreKey), false, 'core removed from db')
  t.is(blindPeer.db.digest.cores, 0, 'core removed from digest')
  t.is(blindPeer.db.digest.bytesAllocated === 0, true, 'digest no longer has bytes allocated')

  const [res2] = await client.deleteCore(coreKey)
  t.is(res2, false, 'returns false if core did not exist')

  await swarm.destroy()
  await client.close()
})

// TODO: add delete to client
test.skip('Untrusted peers cannot delete a core', async (t) => {
  t.plan(6)
  const { bootstrap } = await getTestnet(t)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    trustedPubKeys: [IdEnc.decode('a'.repeat(64))]
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  blindPeer.once('delete-blocked', (stream, { key }) => {
    t.alike(stream.remotePublicKey, swarm.dht.defaultKeyPair.publicKey, 'delete-blocked stream')
    t.alike(key, core.key, 'delete-blocked key')
  })

  const coreAddedProm = once(blindPeer, 'add-core')
  coreAddedProm.catch(() => {})

  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  const coreKey = core.key
  await client.addCore(core, coreKey)

  const [record] = await coreAddedProm
  t.alike(record.key, coreKey, 'added the core')
  t.is(await blindPeer.db.hasCore(coreKey), true, 'core in db')

  try {
    await client.deleteCore(coreKey)
  } catch (e) {
    t.is(e.cause.message.includes('Only trusted peers can delete cores'), true, 'expected err msg')
  }
  t.is(await blindPeer.db.hasCore(coreKey), true, 'core still in db')

  await swarm.destroy()
  await client.close()
})

test('Client can request multiple blind peers in one request', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const blindPeers = []
  for (let i = 0; i < 3; i++) {
    const { blindPeer } = await setupBlindPeer(t, bootstrap, {
      trustedPubKeys: [swarm.dht.defaultKeyPair.publicKey]
    })
    await blindPeer.listen()
    blindPeers.push(blindPeer)
  }

  await new Promise((resolve) => setTimeout(resolve, 500)) // TODO: swarm flushes

  const coreAddedProm = Promise.all(blindPeers.map((bp) => once(bp, 'add-core')))
  coreAddedProm.catch(() => {})

  const client = new Client(swarm.dht, store, { keys: blindPeers.map((bp) => bp.publicKey) })
  await client.addCore(core, { announce: true, pick: 3 })

  const [[record1], [record2], [record3]] = await coreAddedProm
  t.is(record1.announce, true, 'announce set')
  t.is(record2.announce, true, 'announce set')
  t.is(record3.announce, true, 'announce set')

  await client.close()
  await swarm.destroy()
})

test('client suspend/resume logic', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    trustedPubKeys: [swarm.dht.defaultKeyPair.publicKey]
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  const coreKey = core.key
  const { base } = await setupAutobaseHolder(t, bootstrap)
  await base.ready()

  {
    const coreAddedProm = once(blindPeer, 'add-core')
    coreAddedProm.catch(() => {})
    await client.addCore(core, { announce: false })

    const [record] = await coreAddedProm
    t.alike(record.key, coreKey, 'added the core')
  }
  await once(blindPeer, 'add-cores-done') // finish request

  {
    const coreAddedProm = once(blindPeer, 'add-cores-done')
    coreAddedProm.catch(() => {})
    await client.addAutobase(base, { announce: false })

    const [, req] = await coreAddedProm
    t.alike(req.referrer, base.key, 'sanity check')
    t.is(req.cores.length > 3, true, 'includes views/writers')
  }

  const getSuspendeds = () => [...client.blindPeers.values()].map((v) => v.suspended)

  t.alike(getSuspendeds(), [false], 'clients not yet suspended')
  t.is(client.suspended, false, 'not suspended')

  await client.suspend()

  t.alike(getSuspendeds(), [true], 'clients suspended')
  t.is(client.suspended, true, 'suspended')

  const tResume = t.test('resume')
  tResume.plan(2)
  blindPeer.on('add-cores-done', (_, req) => {
    if (!req.referrer) {
      tResume.pass('core resent after resume')
    } else {
      tResume.is(req.cores.length > 3, true, 'autobase re-sends views/writers on resume')
    }
  })
  await client.resume()

  await tResume

  t.alike(getSuspendeds(), [false], 'clients resumed')
  t.is(client.suspended, false, 'resumed')

  await swarm.destroy()
  await client.close()
})

test('client gc logic', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    trustedPubKeys: [swarm.dht.defaultKeyPair.publicKey]
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey], gcWait: 10 })
  const coreKey = core.key

  {
    const coreAddedProm = once(blindPeer, 'add-core')
    coreAddedProm.catch(() => {})
    await client.addCore(core, { announce: false })

    const [record] = await coreAddedProm
    t.alike(record.key, coreKey, 'added the core')
  }

  const ref = client.blindPeers.get(b4a.toString(blindPeer.publicKey, 'hex'))
  t.is(client.blindPeers.size, 1, 'not yet gcd (sanity check')
  t.is(ref.cores.size, 1, 'client has 1 core (sanity check')
  await core.close()
  await new Promise((resolve) => setTimeout(resolve, 1000))

  t.is(client.blindPeers.size, 0, 'gcd after sufficient gc ticks')
  t.is(ref.cores.size, 0, 'client no longer has the core')

  await swarm.destroy()
  await client.close()
})

test('client destroys pending timeouts on close', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { swarm, base, store } = await setupAutobaseHolder(t, bootstrap)

  await base.append({ some: 'thing' })

  const client = new Client(swarm.dht, store, {
    batchIdleWait: 1_000_000,
    batchMaxWait: 1_000_000,
    keys: [blindPeer.publicKey]
  })
  await client.addAutobase(base)
  client.close()

  await base.close()

  t.pass('unless the test run hangs for a really long time, this test passed')
})

test('invalid requests are emitted', async (t) => {
  t.plan(3)

  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  blindPeer.on('invalid-request', (core, err, req, from) => {
    t.is(err.code, 'INVALID_OPERATION', 'invalid-request event received')
  })

  await blindPeer.listen()
  await blindPeer.swarm.flush()

  let coreKey = null
  const coreAddedProm = once(blindPeer, 'add-core')

  coreAddedProm.catch(() => {})
  let client = null

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  coreKey = core.key
  client.addCoreBackground(core)

  const [record] = await coreAddedProm
  t.alike(record.key, coreKey, 'added the core')

  await new Promise((resolve) => setTimeout(resolve, 1000))
  await client.close()
  await swarm.destroy() // So the core holder stops announcing the core

  {
    const { swarm, store } = await setupPeer(t, bootstrap)
    const core = store.get({ key: coreKey })
    await core.ready()
    swarm.joinPeer(blindPeer.publicKey, { dht: swarm.dht })

    await new Promise((resolve) => setTimeout(resolve, 250))
    t.is(core.replicator.peers.length, 1, 'sanity check (we connected)')

    const invalidReq = {
      peer: core.replicator.peers[0],
      rt: 0,
      id: 1,
      fork: 0,
      block: { index: 0, nodes: 2 },
      hash: null,
      seek: { bytes: 1, padding: 1 }, // invalid to both seek and block when upgrading
      upgrade: { start: 0, length: 2 },
      manifest: false,
      priority: 1,
      timestamp: 1754412092523,
      elapsed: 0
    }
    core.replicator._inflight.add(invalidReq)
    core.replicator.peers[0].wireRequest.send(invalidReq)
  }
})

test('Prometheus metrics', async (t) => {
  // DEVNOTE: mostly copies the 'garbage collection when space limit reached' test
  const { bootstrap } = await getTestnet(t)

  const enableGc = false // We trigger it manually, so we can test the accounting
  const { blindPeer } = await setupBlindPeer(t, bootstrap, { enableGc, maxBytes: 10_000 })
  blindPeer.registerMetrics(promClient)
  t.teardown(() => {
    promClient.register.clear()
  })

  {
    const metrics = await promClient.register.metrics()
    t.ok(metrics.includes('blind_peer_bytes_allocated 0'), 'blind_peer_bytes_allocated included')
    t.ok(metrics.includes('blind_peer_bytes_gcd 0'), 'blind_peer_bytes_gcd included')
    t.ok(metrics.includes('blind_peer_cores_added 0'), 'blind_peer_cores_added included')
    t.ok(metrics.includes('blind_peer_cores 0'), 'blind_peer_cores included')
    t.ok(metrics.includes('blind_peer_core_activations 0'), 'blind_peer_core_activations included')
    t.ok(metrics.includes('blind_peer_wakeups 0'), 'blind_peer_wakeups')
    t.ok(metrics.includes('blind_peer_db_flushes 0'), 'blind_peer_db_flushes')
    t.ok(metrics.includes('blind_peer_announced_cores 0'), 'blind_peer_announced_cores')
    t.ok(metrics.includes('protomux_wakeup_topics_added 0'), 'protomux_wakeup_topics_added')
    t.ok(metrics.includes('blind_peer_rocks_gets'), 'blind_peer_rocks_gets')
    t.ok(metrics.includes('blind_peer_rocks_puts'), 'blind_peer_rocks_puts')
    t.ok(metrics.includes('blind_peer_rocks_deletes'), 'blind_peer_rocks_deletes')
    t.ok(metrics.includes('blind_peer_rocks_range_deletes'), 'blind_peer_rocks_range_deletes')
    t.ok(metrics.includes('blind_peer_rocks_read_batches'), 'blind_peer_rocks_read_batches')
    t.ok(metrics.includes('blind_peer_rocks_write_batches'), 'blind_peer_rocks_write_batches')
    t.ok(metrics.includes('blind_peer_add_cores_rx 0'), 'blind_peer_add_cores_rx')
    t.ok(metrics.includes('blind_peer_muxer_paired 0'), 'blind_peer_muxer_paired')
    t.ok(metrics.includes('blind_peer_muxer_errors 0'), 'blind_peer_muxer_error')
  }

  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const nrCores = 10
  const nrBlocks = 200
  const cores = []

  const { swarm, store } = await setupCoreHolder(t, bootstrap)
  {
    const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
    t.teardown(
      async () => {
        await client.close()
      },
      { order: 0 }
    )

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
  await new Promise((resolve) => setTimeout(resolve, 2000))

  const [[{ bytesCleared }]] = await Promise.all([once(blindPeer, 'gc-done'), blindPeer._gc()])

  const nowBytes = blindPeer.digest.bytesAllocated
  t.is(nowBytes < 10_000, true, 'gcd till below limit')

  {
    const getMetricValue = (text, name) => {
      return parseInt(text.split(name)[3]) // hack
    }
    const metrics = await promClient.register.metrics()
    t.is(getMetricValue(metrics, 'blind_peer_bytes_gcd'), bytesCleared, 'blind_peer_bytes_gcd')
    t.is(getMetricValue(metrics, 'blind_peer_cores_added'), nrCores, 'blind_peer_cores_added')
    t.is(
      getMetricValue(metrics, 'blind_peer_bytes_allocated'),
      nowBytes,
      'blind_peer_bytes_allocated'
    )
    t.is(getMetricValue(metrics, 'blind_peer_cores'), nrCores, 'blind_peer_cores')
    t.is(getMetricValue(metrics, 'blind_peer_db_flushes') > 0, true, 'blind_peer_db_flushes')
  }

  {
    const metrics = await promClient.register.metrics()
    const blindPeerRocksDeletes = getMetricValue('blind_peer_rocks_deletes')
    t.ok(blindPeerRocksDeletes > 0, `blind_peer_rocks_deletes ${blindPeerRocksDeletes}`)
    const blindPeerRocksRangeDeletes = getMetricValue('blind_peer_rocks_range_deletes')
    t.ok(
      blindPeerRocksRangeDeletes > 0,
      `blind_peer_rocks_range_deletes ${blindPeerRocksRangeDeletes}`
    )
    const blindPeerRocksGets = getMetricValue('blind_peer_rocks_gets')
    t.ok(blindPeerRocksGets > 0, `blind_peer_rocks_gets ${blindPeerRocksGets}`)
    const blindPeerRocksPuts = getMetricValue('blind_peer_rocks_puts')
    t.ok(blindPeerRocksPuts > 0, `blind_peer_rocks_puts ${blindPeerRocksPuts}`)
    const blindPeerRocksReadBatches = getMetricValue('blind_peer_rocks_read_batches')
    t.ok(
      blindPeerRocksReadBatches > 0,
      `blind_peer_rocks_read_batches ${blindPeerRocksReadBatches}`
    )
    const blindPeerRocksWriteBatches = getMetricValue('blind_peer_rocks_write_batches')
    t.ok(
      blindPeerRocksWriteBatches > 0,
      `blind_peer_rocks_write_batches ${blindPeerRocksWriteBatches}`
    )
    function getMetricValue(name) {
      return parseInt(metrics.split(`\n${name} `)[1].split('\n')[0]) // hack
    }
  }
})

test('TopKWindow tracks the top-k keys across a rolling window', async (t) => {
  const topK = new TopKWindow(2, 50, 2)
  await topK.ready()
  t.teardown(async () => {
    await topK.close()
  })

  topK.hit('a')
  topK.hit('a')
  topK.hit('b')
  await once(topK, 'rotated')

  topK.hit('c')
  topK.hit('c')
  topK.hit('c')
  topK.hit('d')
  await once(topK, 'rotated')

  t.alike(topK.topK, [
    { key: 'c', count: 3 },
    { key: 'a', count: 2 }
  ])
  t.is(topK.topKSum(), 5, 'sums the cached top-k counts')

  await once(topK, 'rotated')

  t.alike(topK.topK, [
    { key: 'c', count: 3 },
    { key: 'd', count: 1 }
  ])
  t.is(topK.topKSum(), 4, 'drops counts from the oldest bucket after rotation')

  await once(topK, 'rotated')

  t.alike(topK.topK, [])
  t.is(topK.topKSum(), 0, 'expires the full rolling window')
})

test('TopKWindow emits spike events during rotation only for entries that stay in top-k', async (t) => {
  const topK = new TopKWindow(1, 50, 2, 4)
  await topK.ready()
  t.teardown(async () => {
    await topK.close()
  })

  const spikes = []
  topK.on('spike', (key, count) => {
    spikes.push({ key, count })
  })

  topK.hit('a')
  topK.hit('a')
  topK.hit('a')
  topK.hit('a')
  topK.hit('a')
  topK.hit('a')

  topK.hit('b')
  topK.hit('b')
  topK.hit('b')
  topK.hit('b')
  topK.hit('b')

  topK.hit('c')
  topK.hit('c')
  topK.hit('c')
  topK.hit('c')

  t.alike(spikes, [], 'does not emit until rotation recalculates the rankings')

  await once(topK, 'rotated')

  t.alike(
    spikes,
    [
      { key: 'a', count: 6 },
      { key: 'b', count: 5 }
    ],
    'emits only the top-k threshold crossings and skips lower-ranked entries'
  )
})

test('Prometheus top-k metrics reflect add-cores traffic', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const topK = { bucketCount: 6, bucketTime: 100, k: 5 }
  const { blindPeer } = await setupBlindPeer(t, bootstrap, { topK })
  await blindPeer.swarm.flush()
  blindPeer.registerMetrics(promClient)
  t.teardown(() => {
    promClient.register.clear()
  })

  // we create 6 peers, with the 1st one send 1 request, 2nd one send 2 request ...
  const nrPeers = 6
  // with that the sum of top 5 request will be sum of 1+2+3+4+5+6 or (6*5)/2
  const totalRequests = (nrPeers * (nrPeers + 1)) / 2
  // with that the sum of top 5 request will be sum of 2+3+4+5+6 or totalRequest - 1
  const top5Requests = totalRequests - 1

  const muxers = []
  for (let i = 0; i < nrPeers; i++) {
    const { swarm, store } = await setupPeer(t, bootstrap)
    const core = store.get({ name: `top-k-core-${i}` })
    await core.ready()

    // `blind-peering` dedups repeated addCore calls per blind peer, so use
    // the raw muxer here to exercise repeated add-cores traffic.
    const muxer = await setupMuxer(t, swarm, store, blindPeer.publicKey)
    muxers.push({ muxer, core })
  }

  // wait for both of the top-k to rotated before schedule addCores,
  // to prevent them from scheduled into different rotate cycle
  await Promise.all([
    once(blindPeer.topKByPeer, 'rotated'),
    once(blindPeer.topKByReferrer, 'rotated')
  ])

  const allPromises = []

  for (let i = 0; i < nrPeers; i++) {
    for (let j = 0; j <= i; j++) {
      const { muxer, core } = muxers[i]
      allPromises.push(
        muxer.addCores({
          referrer: core.key,
          priority: 0,
          announce: false,
          cores: [{ key: core.key, length: core.length }]
        })
      )
    }
  }

  // wait for all the add cores to finish and the topK got rotated
  allPromises.push(once(blindPeer.topKByPeer, 'rotated'), once(blindPeer.topKByReferrer, 'rotated'))

  // wait to ensure all addCores request finished
  await Promise.all(allPromises)

  const metrics = await promClient.register.metrics()
  const getMetricValue = (name) => {
    return parseInt(metrics.split(`\n${name} `)[1].split('\n')[0])
  }

  t.is(getMetricValue('blind_peer_add_cores_rx'), totalRequests, 'tracked add-cores requests')
  t.is(
    getMetricValue('blind_peer_add_cores_top5_by_remote_key'),
    top5Requests,
    'top-5 remote peers'
  )
  t.is(getMetricValue('blind_peer_add_cores_top5_by_referrer'), top5Requests, 'top-5 referrers')
})

test('wakeup', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { base: indexer, swarm: indexerSwarm } = await setupAutobaseHolder(t, bootstrap)
  await new Promise((resolve) => setTimeout(resolve, 250)) // flush

  const peers = []
  const nrPeers = 3
  for (let i = 0; i < nrPeers; i++) {
    peers.push(await getWakeupPeer(t, bootstrap, indexer, blindPeer))
  }

  const initWireAnnounceTx = blindPeer.wakeup.stats.wireAnnounce.tx
  for (const { client, base } of peers) {
    await client.addAutobase(base)
  }
  await new Promise((resolve) => setTimeout(resolve, 1000))

  t.is(blindPeer.wakeup.stats.sessionsOpened, 1)
  t.ok(blindPeer.wakeup.stats.wireAnnounce.tx > initWireAnnounceTx, 'sent announce message')

  // Add non-swarming user
  {
    const initAnnounceTx = blindPeer.wakeup.stats.wireAnnounce.tx
    const { store, swarm } = await setupPeer(t, bootstrap)
    const { base } = await loadAutobase(store, indexer.local.key)

    // We want to test that the wakeup announce comes from
    // the blind-peer connection, so disable the wakeup protocol
    // between the indexer and this new writer
    const s1 = base.store.replicate(true)
    const s2 = indexer.store.replicate(false)
    s1.pipe(s2).pipe(s1)
    await Promise.all([
      indexer.append({ add: b4a.toString(base.local.key, 'hex') }),
      once(base, 'writable')
    ])
    const initAnnounceRxOther = base.wakeupProtocol.stats.wireAnnounce.rx
    const client = new Client(swarm.dht, store, {
      ...clientOpts,
      wakeup: base.wakeupProtocol,
      keys: [blindPeer.publicKey]
    })

    await Promise.all([client.addAutobase(base), once(blindPeer, 'add-cores-done')])

    t.ok(blindPeer.wakeup.stats.wireAnnounce.tx > initAnnounceTx, 'transmitted announce')
    t.is(blindPeer.wakeup.stats.sessionsOpened, 1, 'still using the same session')
    t.is(blindPeer.wakeup.stats.topicsAdded, 1, 'still using the same topic')
    t.ok(initAnnounceRxOther < base.wakeupProtocol.stats.wireAnnounce.rx, 'peer received announce')

    await client.close()
    await base.close()
    s1.destroy()
    s2.destroy()
  }

  await indexerSwarm.destroy()
  await Promise.all(peers.map((p) => p.swarm.destroy()))
  // Give topic time to gc
  await new Promise((resolve) => setTimeout(resolve, 1000))

  t.is(
    blindPeer.wakeup.stats.sessionsClosed,
    1,
    'session closed after all peers close their channel'
  )
  t.is(
    blindPeer.wakeup.stats.topicsGcd,
    1,
    'topic garbage collected after all peers close their channel'
  )
})

test('switch client mode depending on core lag', async (t) => {
  t.plan(2)

  const { bootstrap } = await getTestnet(t)

  const { swarm: peer1Swarm, store: peer1Store } = await setupPeer(t, bootstrap)
  const { swarm: peer2Swarm, store: peer2Store } = await setupPeer(t, bootstrap)

  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    replicationLagThreshold: 10,
    trustedPubKeys: [
      peer1Swarm.dht.defaultKeyPair.publicKey,
      peer2Swarm.dht.defaultKeyPair.publicKey
    ]
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const coreToAnnounce = peer1Store.get({ name: 'test' })
  await coreToAnnounce.ready()
  t.teardown(async () => {
    await coreToAnnounce.close()
  })

  for (let i = 0; i < 11; i++) {
    await coreToAnnounce.append(b4a.from(`block${i}`))
  }
  peer1Swarm.join(coreToAnnounce.discoveryKey, { server: true, client: false })

  const client2 = new Client(peer2Swarm.dht, peer2Store, { keys: [blindPeer.publicKey] })
  t.teardown(async () => {
    await client2.close()
  })
  const coreToAnnounce2 = peer2Store.get({ key: coreToAnnounce.key })
  await Promise.all([
    once(blindPeer, 'add-cores-done'),
    client2.addCore(coreToAnnounce2, { announce: true })
  ])

  blindPeer.on('core-client-mode-changed', (core, mode) => {
    t.alike(core.key, coreToAnnounce.key, 'core key')
    t.is(mode, false, 'client mode is false')
  })

  await once(blindPeer, 'core-downloaded')
})

test('add autobase calls router to resolve peers', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const swarmRouter = new Hyperswarm({ bootstrap })
  // in the first run, router needs blind peer keys, and blind peer needs router key,
  // so we need to create swarm and get router key before creating blind peer
  // note that this assumes router key is the same as swarm public key
  const routerKey = swarmRouter.keyPair.publicKey

  const { blindPeer } = await setupBlindPeer(t, bootstrap, { routerKey })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  await setupRouter(t, swarmRouter, [blindPeer])

  await new Promise((resolve) => setTimeout(resolve, 300))

  const {
    swarm: indexerSwarm,
    base: indexer,
    store: indexerStore
  } = await setupAutobaseHolder(t, bootstrap)

  const client = new Client(indexerSwarm.dht, indexerStore, {
    ...clientOpts,
    keys: [blindPeer.publicKey]
  })

  const prom = once(blindPeer, 'resolve-peers')
  client.addAutobaseBackground(indexer)
  const [res] = await prom

  const peerKey = res.result.peers[0].key
  t.alike(peerKey, blindPeer.publicKey, 'correct blind peer key')
})

test('resolve-peers-error emitted when router is unreachable', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const routerKey = crypto.keyPair().publicKey // random key, not from any router

  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    routerKey,
    routerPoolOpts: { totalTimeout: 1000, rpcTimeout: 500, retries: 1 }
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const {
    swarm: indexerSwarm,
    base: indexer,
    store: indexerStore
  } = await setupAutobaseHolder(t, bootstrap)

  const client = new Client(indexerSwarm.dht, indexerStore, { keys: [blindPeer.publicKey] })

  const prom = once(blindPeer, 'resolve-peers-error')
  client.addAutobaseBackground(indexer)
  const [res] = await prom

  t.alike(res.key, indexer.local.key, 'referrer is correct')
  t.ok(res.error, 'error is correct')
  t.is(
    res.error.message,
    'TOO_MANY_RETRIES: Too many failed attempts to reach a server',
    'error message is correct'
  )
})

async function setupCoreHolder(t, bootstrap) {
  const { swarm, store } = await setupPeer(t, bootstrap)

  const core = store.get({ name: 'core' })
  await core.append('Block 0')
  await core.append('Block 1')
  swarm.join(core.discoveryKey)

  return { swarm, store, core }
}

async function loadAutobase(store, autobaseBootstrap = null, { addIndexers = true } = {}) {
  const open = (store) => {
    return store.get('view', { valueEncoding: 'json' })
  }

  const apply = async (batch, view, base) => {
    for (const { value } of batch) {
      if (value.add) {
        const key = b4a.from(value.add, 'hex')
        await base.addWriter(key, { indexer: addIndexers })
        continue
      }

      if (view) await view.append(value)
    }
  }

  const base = new Autobase(store.namespace('base'), autobaseBootstrap, {
    open,
    apply,
    valueEncoding: 'json',
    ackInterval: 10,
    ackThreshold: 0
  })
  await base.ready()

  return { base }
}

async function setupBlindPeer(
  t,
  bootstrap,
  {
    storage,
    maxBytes,
    enableGc,
    trustedPubKeys,
    routerKey,
    routerPoolOpts,
    replicationLagThreshold,
    topK
  } = {}
) {
  if (!storage) storage = await tmpDir(t)

  const swarm = new Hyperswarm({ bootstrap })
  const peer = new BlindPeer(storage, {
    swarm,
    maxBytes,
    enableGc,
    trustedPubKeys,
    routerKey,
    routerPoolOpts,
    wakeupGcTickTime: 100,
    replicationLagThreshold,
    topK
  })

  const order = clientCounter++
  t.teardown(
    async () => {
      await peer.close()
      await swarm.destroy()
    },
    { order }
  )

  await peer.listen()
  if (DEBUG) {
    peer.swarm.on('connection', () => {
      console.log('Blind peer connection opened')
    })
  }

  return { blindPeer: peer, storage }
}

async function getTestnet(t) {
  const testnet = await setupTestnet()
  t.teardown(
    async () => {
      await testnet.destroy()
    },
    { order: Infinity }
  )

  return testnet
}

async function setupRouter(t, swarm, blindPeers) {
  const storage = await tmpDir(t)
  const store = new Corestore(storage)

  const order = clientCounter++

  const router = new ProtomuxRPCRouter()
  const service = new BlindPeerRouter(store, swarm, router, {
    blindPeers: blindPeers.map((item) => ({ key: item.publicKey }))
  })

  t.teardown(
    async () => {
      await service.close()
      await swarm.destroy()
      await store.close()
    },
    { order }
  )

  await service.ready()

  return { storage, store, swarm, router, service }
}

async function setupPeer(t, bootstrap) {
  const storage = await tmpDir(t)
  const swarm = new Hyperswarm({ bootstrap })
  const store = new Corestore(storage)

  const order = clientCounter++
  swarm.on('connection', (c) => {
    if (DEBUG) console.log('(CORE HOLDER) connection opened')
    store.replicate(c)
    c.on('error', (e) => {
      if (DEBUG) console.warn(`Swarm error: ${e.stack}`)
    })
  })
  t.teardown(
    async () => {
      await swarm.destroy()
      await store.close()
    },
    { order }
  )

  return { swarm, store }
}

async function setupMuxer(t, swarm, store, publicKey) {
  const stream = swarm.dht.connect(publicKey)
  store.replicate(stream)

  const muxer = new BlindPeerMuxer(stream)
  const order = clientCounter++
  t.teardown(
    async () => {
      muxer.close()
      stream.destroy()
    },
    { order }
  )

  await muxer.channel.fullyOpened()

  return muxer
}

async function setupAutobaseHolder(t, bootstrap, autobaseBootstrap = null) {
  const { swarm, store } = await setupPeer(t, bootstrap)
  const { wakeup, base } = await loadAutobase(store, autobaseBootstrap)
  swarm.join(base.discoveryKey)

  return { swarm, store, base, wakeup }
}

let writerI
async function getWakeupPeer(t, bootstrap, indexer, blindPeer) {
  const { store, swarm } = await setupPeer(t, bootstrap)

  const { base } = await loadAutobase(store, indexer.local.key, { addIndexers: false })
  swarm.join(base.discoveryKey)
  await Promise.all([
    indexer.append({ add: b4a.toString(base.local.key, 'hex') }),
    once(base, 'writable')
  ])

  const nr = writerI++
  await base.append(`Message from writer ${nr}`)
  const client = new Client(swarm.dht, store, {
    ...clientOpts,
    wakeup: base.wakeupProtocol,
    keys: [blindPeer.publicKey]
  })

  t.teardown(async () => {
    await client.close()
  })

  return { client, base, store, swarm, wakeup: base.wakeupProtocol }
}
