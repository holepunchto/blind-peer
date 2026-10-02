const test = require('brittle')
const tmpDir = require('test-tmp')
const { once } = require('events')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const HyperDHTAddress = require('hyperdht-address')
const BlindPeer = require('..')
const {
  setupCoreHolder,
  setupBlindPeer,
  initBlindPeer,
  setupBlindPeers,
  getBlindPeerCoreLength,
  getTestnet,
  setupPeer,
  setupMuxer,
  setupAutobaseHolder,
  sleep,
  createClient
} = require('./helpers')

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
  client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
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

test('client can change to a new blind-peer', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { blindPeer: blindPeer2 } = await setupBlindPeer(t, bootstrap)
  await blindPeer2.listen()
  await blindPeer2.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
  const coreKey = core.key
  await client.addCore(core)

  // when a core has updated
  await new Promise((resolve) => setTimeout(resolve, 1000))

  client.setKeys([blindPeer2.publicKey])

  // give some time for new blindPeer2
  await new Promise((resolve) => setTimeout(resolve, 1000))

  {
    const { swarm, store } = await setupPeer(t, bootstrap)
    const core = store.get({ key: coreKey })
    await core.ready()
    swarm.joinPeer(blindPeer2.publicKey, { dht: swarm.dht })

    await new Promise((resolve) => setTimeout(resolve, 1000))

    const block = await core.get(1)
    t.is(b4a.toString(block), 'Block 1', 'Can download the core from the blind peer')
  }
})

test('client can migrate multiple cores to multiple blind-peers and preserve settings', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const [blindPeer1, blindPeer2, blindPeer3, blindPeer4, blindPeer5, blindPeer6] =
    await setupBlindPeers(t, bootstrap, 6)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const core2 = store.get({ name: 'core2' })
  await core2.append('Block 0')

  const coreKey = core.key
  const coreKey2 = core2.key

  const client = createClient(t, swarm.dht, store, {
    keys: [blindPeer1.publicKey, blindPeer2.publicKey, blindPeer3.publicKey]
  })
  await client.addCore(core, { priority: 1, pick: 1, target: blindPeer5.publicKey })
  await client.addCore(core2, { pick: 3 })

  // when a core has updated
  await new Promise((resolve) => setTimeout(resolve, 1000))

  const core1Results = await Promise.all([
    getBlindPeerCoreLength(blindPeer1, coreKey),
    getBlindPeerCoreLength(blindPeer2, coreKey),
    getBlindPeerCoreLength(blindPeer3, coreKey)
  ])

  // Sanity check that it is what we expect before keys change
  t.ok(core1Results.indexOf(2) === core1Results.lastIndexOf(2), '1 blindPeer swarmed for core1')
  t.ok(core1Results.indexOf(0) !== core1Results.lastIndexOf(0), '2 blindPeers not swarm for core1')

  t.is(await getBlindPeerCoreLength(blindPeer1, coreKey2), 1, 'blindPeer1 swarmed for core2')
  t.is(await getBlindPeerCoreLength(blindPeer2, coreKey2), 1, 'blindPeer2 swarmed for core2')
  t.is(await getBlindPeerCoreLength(blindPeer3, coreKey2), 1, 'blindPeer3 swarmed for core2')

  const bp5AddsCore1 = t.test()
  bp5AddsCore1.plan(1)
  const bp5AddsCore2 = t.test()
  bp5AddsCore2.plan(1)
  blindPeer5.on('add-core', (record) => {
    if (record.key.equals(coreKey)) {
      bp5AddsCore1.is(record.priority, 1, 'blindPeer5 added core1 with priority 1')
      return
    }
    if (record.key.equals(coreKey2)) {
      bp5AddsCore2.is(record.priority, 0, 'blindPeer5 added core2 with priority 0')
      return
    }
    bp5AddsCore1.fail('blindPeer5 should add only two cores')
  })

  client.setKeys([blindPeer4.publicKey, blindPeer5.publicKey, blindPeer6.publicKey])
  await new Promise((resolve) => setTimeout(resolve, 1000))

  await bp5AddsCore1
  await bp5AddsCore2

  t.is(await getBlindPeerCoreLength(blindPeer4, coreKey), 0, 'blindPeer4 not swarm for core1')
  t.is(await getBlindPeerCoreLength(blindPeer5, coreKey), 2, 'blindPeer5 swarmed for core1')
  t.is(await getBlindPeerCoreLength(blindPeer6, coreKey), 0, 'blindPeer6 not swarm for core1')

  t.is(await getBlindPeerCoreLength(blindPeer4, coreKey2), 1, 'blindPeer4 swarmed for core2')
  t.is(await getBlindPeerCoreLength(blindPeer5, coreKey2), 1, 'blindPeer5 swarmed for core2')
  t.is(await getBlindPeerCoreLength(blindPeer6, coreKey2), 1, 'blindPeer6 swarmed for core2')
})

test('blind-peer can set treeCache options for corestore', async (t) => {
  const dir = await tmpDir(t)
  const blindPeer = new BlindPeer(dir, { treeCache: { maxSize: 2 ** 17, maxAge: 1337 } })
  t.teardown(() => blindPeer.close())
  await blindPeer.ready()

  t.is(blindPeer.store.storage.treeCache.maxSize, 2 ** 17, 'got maxSize')
  t.is(blindPeer.store.storage.treeCache.maxAge, 1337, 'got maxAge')
  t.is(blindPeer.notificationErrorSnapshotDelay, 30_000, 'got snapshot delay default')
})

test('other clients help upload a core even if they did not add it', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await initBlindPeer(t, bootstrap)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const { swarm: swarm2, store: store2, core: core2 } = await setupCoreHolder(t, bootstrap)

  const coreCopy = store2.get(core.key)
  await coreCopy.ready()
  swarm.joinPeer(swarm2.keyPair.publicKey)
  await Promise.all([coreCopy.get(0), coreCopy.get(1)])

  await new Promise((resolve) => setTimeout(resolve, 500))
  t.is(coreCopy.contiguousLength, 2, 'sanity check: copy downloaded the core')

  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
  client.addCoreBackground(core)

  await Promise.all([once(blindPeer, 'add-core'), client.addCore(core)])

  // The second client is also talking to the blind peer, for its own cores
  const client2 = createClient(t, swarm2.dht, store2, { keys: [blindPeer.publicKey] })
  client2.addCoreBackground(core2)
  // Give time to upload
  await new Promise((resolve) => setTimeout(resolve, 500))
  t.is(
    coreCopy.peers.length,
    2,
    'sanity check: second peer is also replicating the copy with the blind peer'
  )

  // simulate a sudden disconnect of peer 1
  await client.close()

  const bpCopy = blindPeer.store.get(core.key)
  await bpCopy.ready()

  await core.append('another block')
  await new Promise((resolve) => setTimeout(resolve, 500))
  t.is(bpCopy.contiguousLength, 2, 'blind peer could not download the block')
  t.is(bpCopy.length, 3, 'blind peer could see new length through replication with the other peer')

  // Test: can another peer upload our block to the blind peer?
  await coreCopy.get(2)

  await new Promise((resolve) => setTimeout(resolve, 500))
  t.is(bpCopy.contiguousLength, 3, 'blind peer got the last block from the other peer')
})

test('client can use hyperdht addresses to add a core', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { blindPeer: blindPeer2 } = await setupBlindPeer(t, bootstrap)
  await blindPeer2.listen()
  await blindPeer2.swarm.flush()

  const { blindPeer: blindPeer3 } = await setupBlindPeer(t, bootstrap)
  await blindPeer3.listen()
  await blindPeer2.swarm.flush()

  const addedToAll = Promise.all([
    once(blindPeer, 'add-cores-done'),
    once(blindPeer2, 'add-cores-done'),
    once(blindPeer3, 'add-cores-done')
  ])

  let coreKey = null
  let client = null

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  // test both str and buffer keys, as well as the new style
  client = createClient(t, swarm.dht, store, {
    pick: 3,
    keys: [
      blindPeer2.publicKey.toString('hex'),
      blindPeer3.publicKey,
      HyperDHTAddress.encode(blindPeer.publicKey, bootstrap)
    ]
  })
  coreKey = core.key
  client.addCoreBackground(core)

  await addedToAll
  t.pass('added the core to all blind peers')

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

test('client only acceps valid keys', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const aaa = b4a.from('a'.repeat(64), 'hex')
  const bbb = b4a.from('b'.repeat(64), 'hex')
  const validKeys = [HyperDHTAddress.encode(aaa, bootstrap), bbb, 'c'.repeat(64)]

  const { swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = createClient(t, swarm.dht, store, { keys: validKeys })
  t.alike(
    new Set(client.keys),
    new Set([aaa, bbb, b4a.from('c'.repeat(64), 'hex')]),
    'uses expected keys'
  )
  t.alike(
    new Set(client.keys),
    new Set([aaa, bbb, b4a.from('c'.repeat(64), 'hex')]),
    'uses expected keys'
  )

  t.exception(() => createClient(t, swarm.dht, store, { keys: [...validKeys, 'a'.repeat(63)] }))
  t.exception(() =>
    createClient(t, swarm.dht, store, { keys: [...validKeys, b4a.from('a'.repeat(63))] })
  )
})

test('Client stats correctness', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  {
    const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
    const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
    await Promise.all([once(blindPeer, 'add-cores-done'), client.addCore(core)])

    t.is(client.stats.addCore, 1, 'addCore stat')
    t.is(client.stats.addCoresTx, 1, 'addCoresTx stat')
    t.is(client.stats.addAutobase, 0, 'sanity check')
  }

  {
    const { base, swarm, store } = await setupAutobaseHolder(t, bootstrap)
    const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
    await Promise.all([once(blindPeer, 'add-cores-done'), client.addAutobase(base)])

    // addCore somtimes gets called extra by the client logic, so we can't test exact numbers for those
    t.is(client.stats.addCoresTx >= 1, true, 'addCoresTx stat')
    t.is(client.stats.addAutobase, 1, 'addAutobase stat')
  }

  t.is(blindPeer.stats.addCoresRx >= 2, true, 'sanity check')
  t.is(blindPeer.stats.muxerPaired >= 0, true, 'sanity check')
  t.is(blindPeer.stats.muxerErrors === 0, true, 'sanity check')
})

test('repeated add-core requests do not result in db updates', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
  const client2 = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
  const client3 = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })

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
  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey], relayThrough })
  await client.addCore(core)
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
      client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
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

  const client = createClient(t, swarm.dht, store, { keys: blindPeers.map((bp) => bp.publicKey) })
  await client.addCore(core, { announce: true, pick: 3 })

  const [[record1], [record2], [record3]] = await coreAddedProm
  t.is(record1.announce, true, 'announce set')
  t.is(record2.announce, true, 'announce set')
  t.is(record3.announce, true, 'announce set')

  await client.close()
  await swarm.destroy()
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
  client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
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

  const client2 = createClient(t, peer2Swarm.dht, peer2Store, { keys: [blindPeer.publicKey] })
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

test('corestore replication defaults passive, but can be set active', async (t) => {
  const { bootstrap } = await getTestnet(t)

  {
    const { blindPeer } = await setupBlindPeer(t, bootstrap)
    await blindPeer.ready()
    t.is(blindPeer.store.active, false, 'default passive corestore')
  }

  {
    const { blindPeer } = await setupBlindPeer(t, bootstrap, { activeCorestore: true })
    await blindPeer.ready()
    t.is(blindPeer.store.active, true, 'can set active corestore')
  }
})

test('coreTracker does not leak when core closes before refresh completes', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const core = blindPeer.store.get({ name: 'leak-repro' })
  await core.ready()
  t.is(blindPeer.stats.coreTrackersCreated, 1, 'core trackers created stat')

  await core.close() // insta close to trigger race condition

  await core.core.close() // Force close, rather than relying on the gc (takes ~10s otherwise)

  t.is(blindPeer.activeReplication.size, 0, 'activeReplication entry removed after core closed')
  t.is(blindPeer.stats.coreTrackersDestroyed, 1, 'core trackers destroyed stat')
})

test('activating the same core repeatedly does not leak hypercore sessions and stream close listeners', async (t) => {
  // Repeated add-core requests happen when an autobase changes,
  // but to keep the tests simple we hack into the muxer directly
  // (the test is for the server side anyway)

  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap, { active: false })

  const connProm = once(blindPeer.swarm, 'connection')
  const muxer = await setupMuxer(t, swarm, store, blindPeer.publicKey)
  const [conn] = await connProm

  const initListeners = conn.listenerCount('close')

  for (let i = 0; i < 5; i++) {
    await core.append(`Block ${i + 1}`) // ensure length differs so needsActivation is set
    await Promise.all([
      once(blindPeer, 'add-cores-done'),
      muxer.addCores({
        cores: [{ key: core.key, length: core.length }]
      })
    ])
  }

  t.is(blindPeer.stats.activations, 5, 'each add-cores triggered an activation (sanity check)')

  t.is(conn.listenerCount('close') - initListeners, 1, `no close listener leak`)

  const bpCore = blindPeer.store.get(core.key)
  await bpCore.ready()
  t.is(bpCore.sessions.length, 2, 'no new session per request')
  t.is(blindPeer.getActiveReplicationSessions(), 1, 'blind peers own view correct')
  t.is(blindPeer.stats.activatedReplications, 1, 'blind peers own stat correct')
  await Promise.all([new Promise((resolve) => conn.once('close', resolve)), muxer.stream.destroy()])
  t.is(blindPeer.getActiveReplicationSessions(), 0, 'blind peers own stat correct')
})

test('db flush updates correctly for existing records', async (t) => {
  const addCore = async (info) => {
    // slight wait between flushes, so that record timestamps always increase
    // more than 1ms due to the flakiness with ms rounding
    await new Promise((resolve) => setTimeout(resolve, 10))
    blindPeer.db.addCore(info)
    await blindPeer.flush()
  }

  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await setupBlindPeer(t, bootstrap, { enableGc: false, listen: false })
  await blindPeer.ready()

  const key = crypto.randomBytes(32)
  await addCore({ key, priority: 0 })

  const initialRecord = await blindPeer.db.getCoreRecord(key)
  // sanity check initial values on new record
  t.is(initialRecord.priority, 0, 'initial priority 0')
  t.is(initialRecord.announce, false, 'initial announce false')
  t.ok(initialRecord.updated > 0, 'initial updated stamp not 0')
  t.ok(initialRecord.active > 0, 'initial active stamp not 0')
  t.is(initialRecord.active, initialRecord.updated, 'initial active and updated stamps match')
  t.is(initialRecord.blocksCleared, 0, 'initial blocksCleared 0')
  t.is(initialRecord.bytesCleared, 0, 'initial bytesCleared 0')

  await addCore({ key, priority: 3, blocksCleared: 5, bytesCleared: 10 })

  const updatedRecord = await blindPeer.db.getCoreRecord(key)
  t.is(updatedRecord.priority, 2, 'new priority clamped down 2')
  t.is(updatedRecord.announce, false, 'new announce is same')
  t.ok(updatedRecord.updated > initialRecord.updated, 'new updated stamp increased')
  t.ok(updatedRecord.active > initialRecord.active, 'new active stamp increased')
  t.is(updatedRecord.active, updatedRecord.updated, 'new active and updated stamps match')
  t.is(updatedRecord.blocksCleared, 5, 'new blocksCleared 5')
  t.is(updatedRecord.bytesCleared, 10, 'new bytesCleared 10')

  await addCore({ key, priority: 1 })
  {
    const record = await blindPeer.db.getCoreRecord(key)
    t.alike(record, updatedRecord, 'did not update for lower priority')
  }

  await addCore({ key, priority: 3 })
  {
    const record = await blindPeer.db.getCoreRecord(key)
    t.alike(record, updatedRecord, 'did not update for higher priority outside the clamp range')
  }

  await addCore({ key, priority: 1, announce: true, bytesCleared: 0 })
  const updatedRecord2 = await blindPeer.db.getCoreRecord(key)
  t.is(updatedRecord2.priority, 2, 'new priority clamped up to 2')
  t.ok(updatedRecord2.updated > updatedRecord.updated, 'new updated stamp increased')
  t.ok(updatedRecord2.active > updatedRecord.active, 'new active stamp increased')
  t.is(updatedRecord2.blocksCleared, 5, 'new blocksCleared is same')
  t.is(updatedRecord2.bytesCleared, 0, 'new bytesCleared 0')

  await addCore({ key, announce: true })
  {
    const record = await blindPeer.db.getCoreRecord(key)
    t.ok(record.updated > updatedRecord2.updated, 'announce "true" always updates')
  }
})

test('client sends handshake', async (t) => {
  t.plan(3)
  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await initBlindPeer(t, bootstrap)
  blindPeer.on('add-core', (_, __, stream) => {
    const handshake = stream.userData.getLastChannel({ protocol: 'blind-peer' }).handshake
    t.is(typeof handshake?.blindPeeringVersion, 'string')
    t.is(handshake?.clientName, 'app')
    t.is(handshake?.clientVersion, '3.2.1')
  })

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = createClient(t, swarm.dht, store, {
    keys: [blindPeer.publicKey],
    client: { name: 'app', version: '3.2.1' }
  })
  client.addCoreBackground(core)
})

test('adding a core does not switch it to active mode', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await new Promise((resolve) => setTimeout(resolve, 250))

  const coreAddedProm = once(blindPeer, 'add-core')

  coreAddedProm.catch(() => {})
  let client = null

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const { swarm: swarm2, store: store2 } = await setupPeer(t, bootstrap, { active: false })
  const coreCopy = store2.get({ key: core.key })
  await coreCopy.ready()
  swarm2.joinPeer(blindPeer.publicKey, { dht: swarm.dht })

  client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
  client.addCoreBackground(core)

  await new Promise((resolve) => setTimeout(resolve, 250))

  {
    const { swarm, store } = await setupPeer(t, bootstrap, { active: false })
    const coreCopy2 = store.get({ key: core.key })
    await coreCopy2.ready()
    swarm.joinPeer(blindPeer.publicKey, { dht: swarm.dht })

    await new Promise((resolve) => setTimeout(resolve, 500))

    await t.exception(
      () => coreCopy2.get(1, { timeout: 250 }),
      /REQUEST_TIMEOUT/,
      'did not gossip the core on new channel (blind peer still passive)'
    )
    await t.exception(
      () => coreCopy.get(1, { timeout: 250 }),
      /REQUEST_TIMEOUT/,
      'did not gossip the core on existing channel (blind peer still passive)'
    )
  }
})

test('per referrer rate limit sheds load', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const perReferrerRateLimitParams = { capacity: 2, intervalMs: 100 }
  const { blindPeer } = await setupBlindPeer(t, bootstrap, { perReferrerRateLimitParams })

  const { swarm, store } = await setupPeer(t, bootstrap)

  const cores = []
  for (let i = 0; i < 4; i++) {
    const core = store.get({ name: `core${i}` })
    await core.append('block1')
    await core.ready()
    cores.push(core)
  }
  const [core, core2, core3, core4] = cores

  const muxer = await setupMuxer(t, swarm, store, blindPeer.publicKey)
  for (let i = 0; i < 3; i++) {
    muxer.addCores({
      referrer: core.key,
      cores: [{ key: core.key, length: core.length }]
    })
  }

  await sleep(250)
  t.is(blindPeer.stats.referrerRateLimited, 1, 'rate limited')
  t.is(blindPeer.stats.addCoresRx, 3)

  // limit reset by now

  for (let i = 0; i < 3; i++) {
    muxer.addCores({
      referrer: core.key,
      cores: [{ key: core.key, length: core.length }]
    })
    muxer.addCores({
      referrer: core2.key,
      cores: [{ key: core2.key, length: core2.length }]
    })
  }

  await sleep(250)
  t.is(blindPeer.stats.referrerRateLimited, 3, 'rate limits keys separately')
  t.is(blindPeer.stats.addCoresRx, 9)

  // limit reset by now

  muxer.addCores({
    referrer: core.key,
    cores: [{ key: core.key, length: core.length }]
  })
  muxer.addCores({
    referrer: core.key,
    cores: [{ key: core3.key, length: core3.length }]
  })
  muxer.addCores({
    referrer: core.key,
    cores: [{ key: core4.key, length: core4.length }]
  })

  await sleep(250)
  t.is(blindPeer.stats.referrerRateLimited, 4, 'rate limited')
  t.is(blindPeer.stats.addCoresRx, 12)
  t.is(await blindPeer.db.hasCore(core3.key), true, 'core 3 got added')
  t.is(await blindPeer.db.hasCore(core4.key), false, 'core 4 got skipped due to rate limit')

  await sleep(250)

  t.is(blindPeer.perReferrerRateLimit.tokens.size, 0, 'gc works')
})

test('enables alwaysLatestBlock on corestore', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.ready()
  t.is(blindPeer.store.alwaysLatestBlock, true)
})
