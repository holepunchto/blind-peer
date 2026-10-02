const test = require('brittle')
const { once } = require('events')
const b4a = require('b4a')
const IdEnc = require('hypercore-id-encoding')
const {
  setupCoreHolder,
  setupBlindPeer,
  getTestnet,
  setupPeer,
  createClient
} = require('./helpers')

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

  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
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

  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
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

    const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
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

  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
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

  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
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

  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
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
})
