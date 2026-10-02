const test = require('brittle')
const { once } = require('events')
const b4a = require('b4a')
const Hyperswarm = require('hyperswarm')
const crypto = require('hypercore-crypto')
const rrp = require('resolve-reject-promise')
const {
  DEBUG,
  clientOpts,
  loadAutobase,
  setupBlindPeer,
  initBlindPeer,
  getTestnet,
  setupRouter,
  setupPeer,
  setupAutobaseHolder,
  getWakeupPeer,
  sleep,
  createClient
} = require('./helpers')

test('client can use a blind-peer to add an autobase', async (t) => {
  const tFirstAdd = t.test()
  tFirstAdd.plan(1)

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

    let done = false
    const onaddcore = (record) => {
      nrAdded++
      addedKeys.add(b4a.toString(record.key, 'hex'))
      if (addedKeys.size > expectedAddedKeys.size) {
        t.fail('more keys added than expected')
      }
      if (addedKeys.size === expectedAddedKeys.size && !done) {
        done = true // We don't want to test that a core never gets added twice here (too restrictive, and causes flakiness)
        if (DEBUG) {
          console.log('total add core requests received', nrAdded, 'unique:', addedKeys.size)
        }
        tFirstAdd.alike(addedKeys, expectedAddedKeys, 'expected cores added')
      }
    }
    blindPeer.on('add-core', onaddcore)

    const client = createClient(t, indexerSwarm.dht, indexerStore, {
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
  // No cores get re-added when they didn't change
  // Note: this test originally flaked because due to autobase acks,
  // some cores can change. So we merely test that at most 1 core gets added
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

    const client = createClient(t, bases[0].swarm.dht, bases[0].store, {
      keys: [blindPeer.publicKey]
    })
    await client.addAutobase(bases[0].base)
    await requestProcessed

    t.is(addedKeys.size <= 1, true, 'no more than 1 key was added in the second run')
  }
})

test('client can change blind-peer for an autobase', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { blindPeer: blindPeer2 } = await setupBlindPeer(t, bootstrap)
  await blindPeer2.listen()
  await blindPeer2.swarm.flush()

  const {
    swarm: indexerSwarm,
    base: indexer,
    store: indexerStore
  } = await setupAutobaseHolder(t, bootstrap)
  await indexerSwarm.flush()

  await indexer.append({ block: 0 })

  const client = createClient(t, indexerSwarm.dht, indexerStore, {
    keys: [blindPeer.publicKey]
  })
  await client.addAutobase(indexer)
  await indexer.append({ block: 1 })

  await new Promise((resolve) => setTimeout(resolve, 1000))

  client.setKeys([blindPeer2.publicKey])

  await new Promise((resolve) => setTimeout(resolve, 1000))

  await client.close()
  await indexerSwarm.destroy()

  await replicateAndAssert(blindPeer.publicKey, 'Can read from blindPeer1')
  await replicateAndAssert(blindPeer2.publicKey, 'Can read from blindPeer2')

  async function replicateAndAssert(blindPeerKey, message) {
    const { swarm: readerSwarm, store: readerStore } = await setupAutobaseHolder(
      t,
      bootstrap,
      indexer.local.key
    )
    await readerSwarm.flush()
    const core = readerStore.get({ key: indexer.views()[0].key, valueEncoding: 'json' })
    await core.ready()
    readerSwarm.joinPeer(blindPeerKey, { dht: readerSwarm.dht })

    await new Promise((resolve) => setTimeout(resolve, 1000))

    const block = await core.get(1)
    t.alike(block, { block: 1 }, `${message} - alike`)

    await readerSwarm.destroy()
  }
})

test('client can change multiple blind-peers for multiple autobases', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer: blindPeer1 } = await initBlindPeer(t, bootstrap)
  const { blindPeer: blindPeer2 } = await initBlindPeer(t, bootstrap)
  const { blindPeer: blindPeer3 } = await initBlindPeer(t, bootstrap)
  const { blindPeer: blindPeer4 } = await initBlindPeer(t, bootstrap)

  const { swarm, store } = await setupPeer(t, bootstrap)
  const base1 = await initAutobase()
  const base2 = await initAutobase('base2')
  await base2.append({ block: 3 })

  const client = createClient(t, swarm.dht, store, {
    keys: [blindPeer1.publicKey, blindPeer2.publicKey]
  })

  await client.addAutobase(base1, { pick: 1, target: blindPeer3.publicKey })
  await client.addAutobase(base2, { pick: 2 })
  await sleep(1000)

  const lengths = await Promise.all([
    getCoreLength(blindPeer1, base1),
    getCoreLength(blindPeer2, base1)
  ])

  // sanity check that it swarms as we expect before keys change
  // for base1, it is random on which blind-peer it will end up
  t.ok(lengths.indexOf(0) !== -1 && lengths.indexOf(2) !== -1, '1 blindPeer swarmed base1')
  t.is(await getCoreLength(blindPeer1, base2), 3, 'blindPeer1 swarmed base2')
  t.is(await getCoreLength(blindPeer2, base2), 3, 'blindPeer2 swarmed base2')

  client.setKeys([blindPeer3.publicKey, blindPeer4.publicKey])
  await sleep(1000)

  t.is(await getCoreLength(blindPeer3, base1), 2, 'blindPeer3 swarmed base1')
  t.is(await getCoreLength(blindPeer4, base1), 0, 'blindPeer4 did not swarm base1')
  t.is(await getCoreLength(blindPeer3, base2), 3, 'blindPeer3 swarmed base2')
  t.is(await getCoreLength(blindPeer4, base2), 3, 'blindPeer4 swarmed base2')

  async function getCoreLength(blindPeer, key) {
    const core = blindPeer.store.get({ key: key.view.key })
    await core.ready()
    return core.length
  }

  async function initAutobase(namespace = 'base') {
    const { base } = await loadAutobase(store, null, { namespace })
    await base.append({ block: 0 })
    await base.append({ block: 1 })

    return base
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
    const client = createClient(t, indexerSwarm.dht, indexerStore, { keys: [blindPeer.publicKey] })

    const { promise, resolve } = rrp()
    let addCoresDone = 0
    const onaddcores = () => {
      addCoresDone++
      if (addCoresDone === 2) {
        resolve()
        blindPeer.off('add-cores-done', onaddcores)
      }
    }
    blindPeer.on('add-cores-done', onaddcores)
    await Promise.all([promise, client.addAutobase(indexer)])

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

test('blind-peering respects max batch options for the writer cores', async (t) => {
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
    const client = createClient(t, indexerSwarm.dht, indexerStore, {
      keys: [blindPeer.publicKey],
      maxBatchMin: 1,
      maxBatchMax: 4
    })
    await client.addAutobase(indexer)

    await new Promise((resolve) => setTimeout(resolve, 500))
    t.alike(getLengths(indexer), initLengths, 'sanity check: autobase cores did not change')
    t.is(blindPeer.stats.activations, 7, '3 views and 4 indexer cores activated')
  }
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
    const client = createClient(t, swarm.dht, store, {
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

  const client = createClient(t, indexerSwarm.dht, indexerStore, {
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

  const client = createClient(t, indexerSwarm.dht, indexerStore, { keys: [blindPeer.publicKey] })

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
