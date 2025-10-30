const test = require('brittle')
const setupTestnet = require('hyperdht/testnet')
const Corestore = require('corestore')
const tmpDir = require('test-tmp')
const { once } = require('events')
const b4a = require('b4a')
const Client = require('blind-peering')
const Hyperswarm = require('hyperswarm')
const promClient = require('prom-client')
const Autobase = require('autobase')
const IdEnc = require('hypercore-id-encoding')
const Wakeup = require('protomux-wakeup')
const BlindPeer = require('..')

const DEBUG = false
let clientCounter = 0 // For clean teardown order

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
  client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
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
  const tSecondAdd = t.test()
  tSecondAdd.plan(2)

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
    await base.append({ some: 'thing' })
  }

  t.is(indexer.activeWriters.map.size, 3, '3 active writers (sanity check)')

  // A first writer adds the autobase
  {
    const expectedAddedKeys = new Set([
      b4a.toString(indexer.local.key, 'hex'),
      ...[...indexer.views()].map((v) => b4a.toString(v.key, 'hex'))
    ])

    let nrAdded = 0
    const addedKeys = new Set()
    const onaddcore = (record) => {
      nrAdded++
      addedKeys.add(b4a.toString(record.key, 'hex'))
      if (addedKeys.size > expectedAddedKeys.size) {
        t.fail('more keys added than expected')
      }
      if (addedKeys.size === expectedAddedKeys.size) {
        if (DEBUG)
          console.log('total add core requests received', nrAdded, 'unique:', addedKeys.size)
        tFirstAdd.alike(addedKeys, expectedAddedKeys, 'expected cores added')
        tFirstAdd.is(nrAdded, expectedAddedKeys.size, 'no duplicate add-core requests')
      }
    }
    blindPeer.on('add-core', onaddcore)

    const client = new Client(indexerSwarm, indexerStore, { mirrors: [blindPeer.publicKey] })
    await client.addAutobase(indexer)
    await tFirstAdd

    // Give some time for extra requests to cause test failure
    await new Promise((resolve) => setTimeout(resolve, 100))
    blindPeer.off('add-core', onaddcore)
  }

  // Another writer adds the autobase as well
  {
    const expectedAddedKeys = new Set([
      b4a.toString(bases[0].base.local.key, 'hex'),
      ...[...bases[0].base.views()].map((v) => b4a.toString(v.key, 'hex'))
    ])

    let nrAdded = 0
    const addedKeys = new Set()
    const onaddcore = (record) => {
      nrAdded++
      addedKeys.add(b4a.toString(record.key, 'hex'))
      if (addedKeys.size > expectedAddedKeys.size) {
        t.fail('more keys added than expected')
      }
      if (addedKeys.size === expectedAddedKeys.size) {
        if (DEBUG)
          console.log('total add core requests received', nrAdded, 'unique:', addedKeys.size)
        tSecondAdd.alike(addedKeys, expectedAddedKeys, 'expected cores added')
        tSecondAdd.is(nrAdded, expectedAddedKeys.size, 'no duplicate add-core requests')
      }
    }
    blindPeer.on('add-core', onaddcore)

    const client = new Client(bases[0].swarm, bases[0].store, { mirrors: [blindPeer.publicKey] })
    await client.addAutobase(bases[0].base)
    await tSecondAdd

    // Give some time for extra requests to cause test failure
    await new Promise((resolve) => setTimeout(resolve, 100))
    blindPeer.off('add-core', onaddcore)
  }
})

test('repeated add-core requests do not result in db updates', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
  const client2 = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
  const client3 = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })

  const coreKey = core.key
  const [record] = await client.addCore(core)
  t.alike(record.key, coreKey, 'added the core (sanity check)')

  // wait for it to be downloaded
  await new Promise((resolve) => setTimeout(resolve, 1000))
  const initFlushes = blindPeer.db.stats.flushes
  t.is(initFlushes > 0, true, 'sanity check')

  const [record2] = await client2.addCore(core)
  t.is(blindPeer.db.stats.flushes, initFlushes, 'did not flush db again')
  t.alike(record2.key, record.key, 'sanity check: got record')

  const [record3] = await client3.addCore(core, undefined, { priority: 1 })
  t.is(blindPeer.db.stats.flushes, initFlushes, 'flush db not called, even if record changed')
  t.is(record3.priority, 0, 'cannot change the record after it was added')

  await client.close()
  await client2.close()
  await client3.close()
})

test('relayThrough opt passed through', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const relayThrough = () => false
  const client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey], relayThrough })
  await client.addCore(core)

  t.alike(
    [...client.blindPeersByKey.values()][0].peer.relayThrough,
    relayThrough,
    'passed through to clients'
  )

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
      client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
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
    const client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
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

  const client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
  const coreKey = core.key
  const res = await client.addCore(core, coreKey, { announce: true })

  t.is(res.length, 1, 'addCore returns a result list')
  t.is(res[0].announce, true, 'blind peer confirms it is announcing')
  t.is(blindPeer.nrAnnouncedCores, 1, 'nrAnnouncedCores correct')

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

  const client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
  const coreKey = core.key
  const res = await client.addCore(core, coreKey, { announce: true })

  t.is(res[0].announce, false, 'blind peer communicates the request got downgraded')

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

    const client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
    coreKey = core.key
    client.addCoreBackground(core, coreKey, { announce: true })

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
    await blindPeer.listen()
    await blindPeer.swarm.flush()

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
    t.is((await blindPeer.db.getCoreRecord(record.key)).announce, true)
  }

  await swarm.destroy()
  await client.close()
})

test('Trusted peers can delete a core', async (t) => {
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

  const client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
  const coreKey = core.key
  await client.addCore(core, coreKey, { announce: true })

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

test('Untrusted peers cannot delete a core', async (t) => {
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

  const client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
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

  const client = new Client(swarm, store, { mediaMirrors: blindPeers.map((bp) => bp.publicKey) })
  const coreKey = core.key
  const res = await client.addCore(core, coreKey, { announce: true, pick: 3 })

  t.is(res.length, 3, 'addCore returns a result list')
  t.is(res[0].announce, true, 'blind peer confirms it is announcing')
  t.is(res[1].announce, true, 'blind peer confirms it is announcing')
  t.is(res[2].announce, true, 'blind peer confirms it is announcing')

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

  const client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
  const coreKey = core.key

  {
    const coreAddedProm = once(blindPeer, 'add-core')
    coreAddedProm.catch(() => {})
    await client.addCore(core, coreKey, { announce: false })

    const [record] = await coreAddedProm
    t.alike(record.key, coreKey, 'added the core')
  }

  const getSuspendeds = () => [...client.blindPeersByKey.values()].map((v) => v.peer.suspended)

  t.alike(getSuspendeds(), [false], 'clients not yet suspended')
  t.is(client.suspended, false, 'not suspended')

  await client.suspend()

  t.alike(getSuspendeds(), [true], 'clients suspended')
  t.is(client.suspended, true, 'suspended')

  await client.resume()

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

  const client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey], gcWait: 10 })
  const coreKey = core.key

  {
    const coreAddedProm = once(blindPeer, 'add-core')
    coreAddedProm.catch(() => {})
    await client.addCore(core, coreKey, { announce: false })

    const [record] = await coreAddedProm
    t.alike(record.key, coreKey, 'added the core')
  }

  const ref = client.blindPeersByKey.get(b4a.toString(blindPeer.publicKey, 'hex'))
  t.is(client.blindPeersByKey.size, 1, 'not yet gcd (sanity check')
  t.is(ref.cores.size, 1, 'client has 1 core (sanity check')
  t.is(client.mirroring.size, 1, 'mirroring includes core (sanity checK)')
  await core.close()
  await new Promise((resolve) => setTimeout(resolve, 1000))

  t.is(client.blindPeersByKey.size, 0, 'gcd after sufficient gc ticks')
  t.is(ref.cores.size, 0, 'client no longer has the core')
  t.is(client.mirroring.size, 0, 'no longer mirroring the core')

  await swarm.destroy()
  await client.close()
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
  client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
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
  }

  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const nrCores = 10
  const nrBlocks = 200
  const cores = []

  const { swarm, store } = await setupCoreHolder(t, bootstrap)
  {
    const client = new Client(swarm, store, { mediaMirrors: [blindPeer.publicKey] })
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
})

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

test.solo('wakeup', async (t) => {
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

  const peers = []
  const nrPeers = 3
  for (let i = 0; i < nrPeers; i++) {
    peers.push(await getWakeupPeer(t, bootstrap, indexer, blindPeer))
  }

  for (const { client, base } of peers) {
    await client.addAutobase(base)
  }

  t.is(blindPeer.wakeup.stats.sessionsOpened, 1)
  console.log(blindPeer.wakeup.stats)

  const initWireAnnounceTx = blindPeer.wakeup.stats.wireAnnounce.tx
  await peers[0].base.append('A new message')
  await new Promise(resolve => setTimeout(resolve, 250))
  t.ok(blindPeer.wakeup.stats.wireAnnounce.tx > initWireAnnounceTx, 'sent announce message')

  console.log(blindPeer.wakeup.stats)

  await peers[0].base.close()
  peers[0].base = (
    await loadAutobase(peers[0].store, indexer.local.key, {
      wakeup: peers[0].wakeup,
      addIndexers: false
    })
  ).base
  await new Promise((resolve) => setTimeout(resolve, 250))
  console.log(blindPeer.wakeup.stats)

  /*
  await new Promise(resolve => setTimeout(resolve, 1000))
  console.log(blindPeer.wakeup.stats)

  console.log('\nADD NON-SWARMING USER\n')
  const { store, swarm } = await setupPeer(t, bootstrap)
  const wakeup = new Wakeup(() => {
    console.log('WAKE UP CB CALLED')
  })
  const { base } = await loadAutobase(store, indexer.local.key, { wakeup })
  const client = new Client(swarm, store, {
    wakeup,
    mirrors: [blindPeer.publicKey]
  })
  const s1 = base.replicate(true)
  const s2 = indexer.replicate(false)
  s1.pipe(s2).pipe(s1)

  console.log(blindPeer.stats)

  await client.addAutobase(base)
  console.log(blindPeer.wakeup.stats)
  await new Promise(resolve => setTimeout(resolve, 1000))
  console.log(blindPeer.wakeup.stats)

  await Promise.all([
    indexer.append({ add: b4a.toString(base.local.key, 'hex') }),
    once(base, 'writable')
  ])
  await base.append('A new message')

  console.log([...peers[0].wakeup.topics.values()][0].active())
  peers.map(p => [...peers[0].wakeup.topics.values()][0].inactive())
  
  for (let i = 0; i < 3; i++) {
    console.log('adding a batch of messages')
    await peers[0].base.append('A new message')
    await peers[1].base.append('A new message')
    await peers[2].base.append('A new message')
    await new Promise((resolve) => setTimeout(resolve, 200))

    // console.log('peer wakeup stats', peers[0].wakeup.stats)
    // console.log('peer wakeup stats', peers[1].wakeup.stats)
    // console.log('peer wakeup stats', peers[2].wakeup.stats)

    console.log('blind peer wakeup stats', blindPeer.wakeup.stats)
    console.log(blindPeer.stats)
  } */
})

test('wakeup race condition', async (t) => {
  t.timeout(120000)
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { base: indexerBase1 } = await setupAutobaseHolder(t, bootstrap, null, {
    addIndexers: false
  })
  const { base: indexerBase2 } = await setupAutobaseHolder(t, bootstrap, null, {
    addIndexers: false
  })

  await new Promise((resolve) => setTimeout(resolve, 250)) // flush

  const {
    base: peerBase1,
    store,
    swarm,
    wakeup
  } = await getWakeupPeer(t, bootstrap, indexerBase1, blindPeer)

  console.log('setup peer 2')
  const { base: peerBase2 } = await loadAutobase(store, indexerBase2.local.key, { wakeup })
  swarm.join(peerBase2.discoveryKey)
  await Promise.all([
    indexerBase2.append({ add: b4a.toString(peerBase2.local.key, 'hex') }),
    once(peerBase2, 'writable')
  ])

  console.log('setup done')

  /*
  const peers = []
  const nrPeers = 3
  for (let i = 0; i < nrPeers; i++) {
    peers.push(await getWakeupPeer(t, bootstrap, indexer, blindPeer))
  }

  console.log('\nADDING AUTOBASE TO BLIND PEER\n')
  for (const { client, base } of peers) {
    await client.addAutobase(base)
  }

  console.log('\nADDED ALL\n')
  await peers[0].base.append('A new message')
  await peers[0].base.append('A new message')
  await peers[0].base.append('A new message')

  await new Promise(resolve => setTimeout(resolve, 1000))

  console.log('\nADD NON-SWARMING USER\n')
  const { store, swarm } = await setupPeer(t, bootstrap)
  const wakeup = new Wakeup(() => {
    console.log('WAKE UP CB CALLED')
  })
  const { base } = await loadAutobase(store, indexer.local.key, { wakeup })
  const client = new Client(swarm, store, {
    wakeup,
    mirrors: [blindPeer.publicKey]
  })

  await client.addAutobase(base)
  await new Promise(resolve => setTimeout(resolve, 10000))
  */
})

// Illustrates a bug
test.skip('Cores added by someone who does not have them are downloaded from other peers', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  swarm.on('connection', (conn) =>
    console.log('CORE HOLDER swarm key', conn.publicKey.toString('hex'))
  )
  const { swarm: peerSwarm, store: peerStore } = await setupPeer(t, bootstrap)
  const core2 = store.get({ name: 'core2' })
  await core2.append(['core2block0', 'core2block1'])
  console.log('disckey core11', core.discoveryKey.toString('hex'))
  console.log('disckey core2', core2.discoveryKey.toString('hex'))

  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    trustedPubKeys: [peerSwarm.dht.defaultKeyPair.publicKey]
  })
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

    await new Promise((resolve) => setTimeout(resolve, 1000))
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
    const reply = await client.addCore(store.get({ key: core2.key }), core2.key, {
      announce: false
    })
    t.is(reply.announce, false, 'announce false (sanity check)')
  }
  await new Promise((resolve) => setTimeout(resolve, 1000))

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

async function setupCoreHolder(t, bootstrap) {
  const { swarm, store } = await setupPeer(t, bootstrap)

  const core = store.get({ name: 'core' })
  await core.append('Block 0')
  await core.append('Block 1')
  swarm.join(core.discoveryKey)

  return { swarm, store, core }
}

async function loadAutobase(
  store,
  autobaseBootstrap = null,
  { addIndexers = true, wakeup = null } = {}
) {
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
    wakeup,
    open,
    apply,
    valueEncoding: 'json',
    ackInterval: 10,
    ackThreshold: 0
  })
  await base.ready()

  return { base }
}

async function setupBlindPeer(t, bootstrap, { storage, maxBytes, enableGc, trustedPubKeys } = {}) {
  if (!storage) storage = await tmpDir(t)

  const swarm = new Hyperswarm({ bootstrap })
  const peer = new BlindPeer(storage, { swarm, maxBytes, enableGc, trustedPubKeys })

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

async function setupAutobaseHolder(t, bootstrap, autobaseBootstrap = null) {
  const { swarm, store } = await setupPeer(t, bootstrap)
  const { wakeup, base } = await loadAutobase(store, autobaseBootstrap)
  swarm.join(base.discoveryKey)

  return { swarm, store, base, wakeup }
}

let writerI
async function getWakeupPeer(t, bootstrap, indexer, blindPeer) {
  const { store, swarm } = await setupPeer(t, bootstrap)
  const wakeup = new Wakeup(() => {
    console.log('WAKE UP CB CALLED')
  })

  const { base } = await loadAutobase(store, indexer.local.key, { addIndexers: false, wakeup })
  swarm.join(base.discoveryKey)
  await Promise.all([
    indexer.append({ add: b4a.toString(base.local.key, 'hex') }),
    once(base, 'writable')
  ])

  const nr = writerI++
  await base.append(`Message from writer ${nr}`)
  const client = new Client(swarm, store, {
    wakeup,
    mirrors: [blindPeer.publicKey]
  })
  return { client, base, store, swarm, wakeup }
}
