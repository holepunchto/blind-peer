const test = require('brittle')
const setupTestnet = require('hyperdht/testnet')
const HyperDHT = require('hyperdht')
const Corestore = require('corestore')
const tmpDir = require('test-tmp')
const { once } = require('events')
const b4a = require('b4a')
const Client = require('blind-peering')
const BlindPeerMuxer = require('blind-peer-muxer')
const Hyperswarm = require('hyperswarm')
const promClient = require('bare-prom-client')
const Autobase = require('autobase')
const Autobee = require('autobee')
const IdEnc = require('hypercore-id-encoding')
const ProtomuxRPC = require('protomux-rpc')
const ProtomuxRPCRouter = require('protomux-rpc-router')
const BlindPeerRouter = require('blind-peer-router')
const crypto = require('hypercore-crypto')
const HyperDHTAddress = require('hyperdht-address')
const { ADMIN_CHANNEL_ID, AdminQueryTopKEncoding } = require('blind-peer-encodings')
const blindPush = require('blind-push')
const BlindPushGateway = require('blind-push-gateway')
const rrp = require('resolve-reject-promise')

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

test('client can change to a new blind-peer', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { blindPeer: blindPeer2 } = await setupBlindPeer(t, bootstrap)
  await blindPeer2.listen()
  await blindPeer2.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
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

  const client = new Client(swarm.dht, store, {
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

test('client can ask a blind-peer to create and forward a push notification', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { gateway, sentMessages } = await setupPushGateway(t, bootstrap)
  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    pushGatewayKeys: [gateway.publicKey]
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  await core.setUserData('referrer', core.key)
  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  t.teardown(async () => {
    await client.close()
  })

  await Promise.all([once(blindPeer, 'add-cores-done'), client.addCore(core)])
  await Promise.all([
    once(blindPeer, 'notification-sent'),
    client.sendNotification(core, { extra: b4a.from('extra') })
  ])

  t.is(sentMessages.length, 1, 'gateway received one forwarded push')

  const rawPayload = b4a.from(sentMessages[0].android.data.payload, 'base64')
  const notification = blindPush.decode(rawPayload)
  t.alike(notification.discoveryKey, core.discoveryKey, 'room discovery key forwarded')

  const result = await blindPush.readNotification(
    core.state.storage.store,
    core.key,
    notification.payload
  )

  t.ok(result, 'forwarded payload can be verified')
  t.alike(result.extra, b4a.from('extra'), 'notification extra')
  t.alike(result.result.key, core.key, 'verified payload targets the sender core')
  t.is(result.result.block.index, core.length - 1, 'verified payload contains the latest block')
  t.is(blindPeer.stats.notificationsRx, 1, 'blind-peer notification rx stat')
  t.is(blindPeer.stats.notificationsSent, 1, 'blind-peer notification sent stat')
  t.is(client.stats.notificationsTx, 1, 'blind-peering notification tx stat')
})

test('sendNotification does not leak core sessions', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { gateway } = await setupPushGateway(t, bootstrap)
  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    pushGatewayKeys: [gateway.publicKey]
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  t.teardown(() => client.close())

  await Promise.all([once(blindPeer, 'add-cores-done'), client.addCore(core)])

  const probe = blindPeer.store.get({ key: core.key })
  await probe.ready()
  const sessionsBefore = probe.sessions.length

  await Promise.all([once(blindPeer, 'notification-sent'), client.sendNotification(core)])

  t.is(probe.sessions.length, sessionsBefore, 'core session count is unchanged')
  await probe.close()
})

test('send push notification when not yet connected to blind peer', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { gateway, sentMessages } = await setupPushGateway(t, bootstrap)
  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    pushGatewayKeys: [gateway.publicKey]
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  await core.setUserData('referrer', core.key)

  const initClient = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  await Promise.all([once(blindPeer, 'add-cores-done'), initClient.addCore(core)])
  await initClient.close()

  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  t.is(sentMessages.length, 0, 'sanity check')

  await Promise.all([
    once(blindPeer, 'notification-sent'),
    client.sendNotification(core, { extra: b4a.from('extra') })
  ])

  t.is(sentMessages.length, 1, 'gateway received one forwarded push')
})

test('sets up core replication on notification if not present and the core is outdated', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { gateway, sentMessages } = await setupPushGateway(t, bootstrap)
  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    pushGatewayKeys: [gateway.publicKey]
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  // needs both sides to have a passive corestore, otherwise this side will
  // set up hypercore replication for the core always
  const { swarm: swarm2, store: store2 } = await setupPeer(t, bootstrap, { active: false })
  await new Promise((resolve) => setTimeout(resolve, 500))
  swarm2.joinPeer(swarm.keyPair.publicKey)
  const coreCopy = store2.get(core.key)
  coreCopy.download({ start: 0, end: -1 })

  const initClient = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  await Promise.all([once(blindPeer, 'add-cores-done'), initClient.addCore(core)])
  await initClient.close()

  await Promise.all([core.append('another block'), once(coreCopy, 'append')])

  const client = new Client(swarm2.dht, store2, { keys: [blindPeer.publicKey] })

  blindPeer.on('notification-error', (e) => {
    console.error(e)
    t.fail('notification should work')
  })

  await coreCopy.get(2) // ensure synced
  t.is(coreCopy.length, core.length, 'sanity check')

  await Promise.all([once(blindPeer, 'notification-sent'), client.sendNotification(coreCopy)])

  t.is(sentMessages.length, 1, 'gateway received one forwarded push')
})

test('send push notification falls back when closest blind peer times out', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { gateway, sentMessages } = await setupPushGateway(t, bootstrap)
  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    pushGatewayKeys: [gateway.publicKey]
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  await core.setUserData('referrer', core.key)

  const initClient = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  await Promise.all([once(blindPeer, 'add-cores-done'), initClient.addCore(core)])
  await initClient.close()

  const deadKey = HyperDHT.keyPair().publicKey
  const client = new Client(swarm.dht, store, {
    keys: [deadKey, blindPeer.publicKey],
    pick: 2
  })
  t.teardown(async () => {
    await initClient.close()
  })

  t.is(sentMessages.length, 0, 'sanity check')

  const start = Date.now()
  await Promise.all([
    once(blindPeer, 'notification-sent').then(() => console.log('something 1')),
    initClient.sendNotification(core, {
      keys: [deadKey, blindPeer.publicKey],
      target: deadKey, // try the dead key before the actual blind peer
      extra: b4a.from('extra')
    })
  ])

  t.ok(Date.now() - start >= 5000, 'waited for closest dead peer to time out')
  t.is(sentMessages.length, 1, 'fallback blind peer forwarded one push')
})

test('push notification timeout when getting block does not close the connection and emits a delayed error snapshot', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { gateway } = await setupPushGateway(t, bootstrap)
  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    pushGatewayKeys: [gateway.publicKey],
    notificationTimeout: 1000,
    notificationErrorSnapshotDelay: 100
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm: initSwarm, store: initStore } = await setupCoreHolder(t, bootstrap)

  const initClient = new Client(initSwarm.dht, initStore, { keys: [blindPeer.publicKey] })
  await Promise.all([once(blindPeer, 'add-cores-done'), initClient.addCore(core)])
  await initClient.close()

  // We'll add the core of a different peer, so the blind peer can't get the block
  const swarm = new Hyperswarm({ bootstrap })
  const store = new Corestore(await t.tmp())

  blindPeer.swarm.on('connection', (conn) => {
    conn.on('error', (err) => {
      t.fail('connection should not error')
      console.error(err)
    })
  })

  await core.append('Block2')

  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  const notificationError = once(blindPeer, 'notification-error')
  const snapshotPromise = once(blindPeer, 'notification-error-snapshot')
  const [[error]] = await Promise.all([notificationError, client.sendNotification(core)])
  t.is(error.code, 'REQUEST_TIMEOUT', 'emitted the original error')

  const [snapshot] = await snapshotPromise
  t.ok(snapshot.coreInfoBefore, 'captured core info before notification')
  t.ok(snapshot.coreInfoOnError, 'captured core info when notification failed')
  t.ok(snapshot.coreInfoAfterDelay, 'captured core info after snapshot delay')

  // some time for swarm error to trigger if any
  await new Promise((resolve) => setTimeout(resolve, 500))

  t.pass('notification error emitted, but conn did not close')

  await blindPeer.close()
  await swarm.destroy()
  await store.close()
})

test('blind-peering handles not ready cores for push notifications', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { gateway, sentMessages } = await setupPushGateway(t, bootstrap)
  const { blindPeer } = await initBlindPeer(t, bootstrap, {
    pushGatewayKeys: [gateway.publicKey]
  })

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  await core.setUserData('referrer', core.key)

  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  t.teardown(async () => await client.close())

  await Promise.all([once(blindPeer, 'add-cores-done'), client.addCore(core)])

  {
    const core = store.get({ name: 'core' })
    await core.ready()
    await Promise.all([once(blindPeer, 'notification-sent'), client.sendNotification(core)])
    t.is(sentMessages.length, 1, 'push gateway received the notification when core was ready')
  }

  {
    const core = store.get({ name: 'core' })
    await Promise.all([once(blindPeer, 'notification-sent'), client.sendNotification(core)])
    t.is(sentMessages.length, 2, 'push gateway received the notification when core was not ready')
  }
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

  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  client.addCoreBackground(core)

  await Promise.all([once(blindPeer, 'add-core'), client.addCore(core)])

  // The second client is also talking to the blind peer, for its own cores
  const client2 = new Client(swarm2.dht, store2, { keys: [blindPeer.publicKey] })
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

  await client2.close()
})

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

    const client = new Client(bases[0].swarm.dht, bases[0].store, { keys: [blindPeer.publicKey] })
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

  const client = new Client(indexerSwarm.dht, indexerStore, {
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

  const blindPeer1 = await initBlindPeer()
  const blindPeer2 = await initBlindPeer()
  const blindPeer3 = await initBlindPeer()
  const blindPeer4 = await initBlindPeer()

  const { swarm, store } = await setupPeer(t, bootstrap)
  const base1 = await initAutobase()
  const base2 = await initAutobase('base2')
  await base2.append({ block: 3 })

  const client = new Client(swarm.dht, store, {
    keys: [blindPeer1.publicKey, blindPeer2.publicKey]
  })

  await client.addAutobase(base1, { pick: 1, target: blindPeer3.publicKey })
  await client.addAutobase(base2, { pick: 2 })

  await new Promise((resolve) => setTimeout(resolve, 1000))

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
  await new Promise((resolve) => setTimeout(resolve, 1000))

  t.is(await getCoreLength(blindPeer3, base1), 2, 'blindPeer3 swarmed base1')
  t.is(await getCoreLength(blindPeer4, base1), 0, 'blindPeer4 did not swarm base1')
  t.is(await getCoreLength(blindPeer3, base2), 3, 'blindPeer3 swarmed base2')
  t.is(await getCoreLength(blindPeer4, base2), 3, 'blindPeer4 swarmed base2')

  async function initBlindPeer() {
    const { blindPeer } = await setupBlindPeer(t, bootstrap)
    await blindPeer.listen()
    await blindPeer.swarm.flush()

    return blindPeer
  }

  async function getCoreLength(blindPeer, key) {
    const core = blindPeer.store.get({ key: key.local.key })
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

test('client can use a blind-peer to add an autobee', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { swarm, store, bee } = await setupAutobeeHolder(t, bootstrap)
  await bee.append(JSON.stringify({ block: 1 }))

  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  t.teardown(async () => await client.close())

  const addedKeys = []
  const onaddcore = (record) => {
    addedKeys.push(b4a.toString(record.key, 'hex'))
  }
  blindPeer.on('add-core', onaddcore)

  await client.addAutobase(bee)

  await new Promise((resolve) => setTimeout(resolve, 500))

  await client.close()
  await bee.close()
  await swarm.destroy()

  const expectedKeys = [
    b4a.toString(bee.key, 'hex'),
    ...bee.views().map((x) => b4a.toString(x.key, 'hex'))
  ]
  t.alike(addedKeys.sort(), expectedKeys.sort(), 'correct cores were added')

  {
    const { swarm, bee: reader } = await setupAutobeeHolder(t, bootstrap, bee.key)
    await swarm.flush()

    let node = await reader.view.get(Buffer.from('latest'))
    t.absent(node, 'no data before joining blind-peer')

    swarm.joinPeer(blindPeer.publicKey, { dht: swarm.dht })

    await new Promise((resolve) => setTimeout(resolve, 1000))

    node = await reader.view.get(Buffer.from('latest'))
    t.alike(JSON.parse(node.value), { block: 1 }, 'get data from blind-peer')
  }
})

test('client can use a blind-peer to add an autobee with additionalViews', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { swarm, store, bee } = await setupAutobeeHolder(t, bootstrap)
  await bee.append(JSON.stringify({ block: 1 }))

  const { bee: bee2 } = await setupAutobeeHolder(t, bootstrap, bee.key)
  await bee.append(JSON.stringify({ addWriter: bee2.local.id }))
  await new Promise((resolve) => setTimeout(resolve, 500))
  // need to write something or the views() will be []
  await bee2.append(JSON.stringify({ block: 2 }))

  const { bee: bee3 } = await setupAutobeeHolder(t, bootstrap, bee.key)
  await bee.append(JSON.stringify({ addWriter: bee3.local.id }))
  await new Promise((resolve) => setTimeout(resolve, 500))
  await bee3.append(JSON.stringify({ block: 3 }))

  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  t.teardown(async () => await client.close())

  const addedKeys = []
  const onaddcore = (record) => {
    addedKeys.push(b4a.toString(record.key, 'hex'))
  }
  blindPeer.on('add-core', onaddcore)

  const writerViews = await bee.getWriterViews(bee.getExternalWriters()[0])
  await client.addAutobase(bee, { additionalViews: writerViews })
  await new Promise((resolve) => setTimeout(resolve, 500))

  // add all the writers, views of itself, and views of bee2 only, no bee3
  const expectedKeys = [
    b4a.toString(bee.key, 'hex'),
    ...bee.getExternalWriters().map((x) => b4a.toString(x, 'hex')),
    ...bee.views().map((x) => b4a.toString(x.key, 'hex')),
    ...bee2.views().map((x) => b4a.toString(x.key, 'hex'))
  ]
  t.alike(addedKeys.sort(), expectedKeys.sort(), 'correct cores were added')
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
  client = new Client(swarm.dht, store, {
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
  const client = new Client(swarm.dht, store, { keys: validKeys })
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

  t.exception(() => new Client(swarm.dht, store, { keys: [...validKeys, 'a'.repeat(63)] }))
  t.exception(
    () => new Client(swarm.dht, store, { keys: [...validKeys, b4a.from('a'.repeat(63))] })
  )
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
    const client = new Client(indexerSwarm.dht, indexerStore, {
      keys: [blindPeer.publicKey],
      maxBatchMin: 1,
      maxBatchMax: 4
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

test('gc correctly counts cleared bytes for cores that were gced before', async (t) => {
  async function appendBlocks(core, n) {
    const blocks = []
    for (let i = 0; i < n; i++) blocks.push(b4a.alloc(1))
    await core.append(blocks)
  }

  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    enableGc: false,
    maxBytes: 15
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { swarm, store } = await setupPeer(t, bootstrap)
  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })

  const coreA = store.get({ name: 'a' })
  const coreB = store.get({ name: 'b' })
  await appendBlocks(coreA, 10) // 10 bytes, priority 0 -> first gc candidate
  await appendBlocks(coreB, 10) // 10 bytes, priority 1 -> second gc candidate

  await Promise.all([once(blindPeer, 'add-cores-done'), client.addCore(coreA, { priority: 0 })])
  await Promise.all([once(blindPeer, 'add-cores-done'), client.addCore(coreB, { priority: 1 })])

  await new Promise((resolve) => setTimeout(resolve, 1000))

  {
    t.is(blindPeer.digest.bytesAllocated, 20, 'digest bytesAllocated 20 initially')
    const recordA = await blindPeer.db.getCoreRecord(coreA.key)
    const recordB = await blindPeer.db.getCoreRecord(coreB.key)
    t.is(recordA.bytesAllocated, 10, 'coreA bytesAllocated 10 initially')
    t.is(recordA.bytesCleared, 0, 'coreA bytesCleared 0 initially')
    t.is(recordB.bytesAllocated, 10, 'coreB bytesAllocated 10 initially')
    t.is(recordB.bytesCleared, 0, 'coreB bytesCleared 0 initially')
    t.is(blindPeer.stats.bytesGcd, 0, 'total bytesGcd 0 initially')
  }
  // first gc, clears coreA, freeing 10 bytes
  {
    const [[{ bytesCleared }]] = await Promise.all([once(blindPeer, 'gc-done'), blindPeer._gc()])
    t.is(blindPeer.digest.bytesAllocated, 10, 'digest bytesAllocated 10 after 1 gc')
    t.is(bytesCleared, 10, 'bytesCleared 10')
    const recordA = await blindPeer.db.getCoreRecord(coreA.key)
    const recordB = await blindPeer.db.getCoreRecord(coreB.key)
    t.is(recordA.bytesAllocated, 0, 'coreA cleared after gc')
    t.is(recordA.bytesCleared, 10, 'coreA cleared after gc')
    t.is(recordB.bytesAllocated, 10, 'coreB stayed after gc')
    t.is(recordB.bytesCleared, 0, 'coreB stayed after gc')
    t.is(blindPeer.stats.bytesGcd, 10, 'total bytesGcd 10 after gc')
  }

  t.is(blindPeer.needsGc(), false, 'no need to gc again after gc')

  // grow A a little (1 byte) and B a lot (6 bytes), back over max bytes
  await appendBlocks(coreA, 1)
  await appendBlocks(coreB, 6)

  await new Promise((resolve) => setTimeout(resolve, 1000))
  {
    t.is(blindPeer.digest.bytesAllocated, 17, 'digest bytesAllocated 17 after cores append')
    const recordA = await blindPeer.db.getCoreRecord(coreA.key)
    const recordB = await blindPeer.db.getCoreRecord(coreB.key)
    t.is(recordA.bytesAllocated, 1, 'coreA bytesAllocated 1 after gc and readd')
    t.is(recordA.bytesCleared, 10, 'coreA bytesCleared 10 after gc and readd')
    t.is(recordB.bytesAllocated, 16, 'coreB bytesAllocated 16 after gc and readd')
    t.is(recordB.bytesCleared, 0, 'coreB bytesCleared 0 after gc and readd')
  }

  // second gc, clearing just coreA is not enough now
  // it would free 1 byte, still above max bytes of 15
  {
    const [[{ bytesCleared }]] = await Promise.all([once(blindPeer, 'gc-done'), blindPeer._gc()])
    t.is(blindPeer.digest.bytesAllocated, 0, 'digest bytesAllocated 0 after 2 gc')
    t.is(bytesCleared, 17, 'clear all 17 bytes')
    const recordA = await blindPeer.db.getCoreRecord(coreA.key)
    const recordB = await blindPeer.db.getCoreRecord(coreB.key)
    t.is(recordA.bytesAllocated, 0, 'coreA bytesAllocated 0 after gc 2')
    t.is(recordA.bytesCleared, 11, 'coreA bytesCleared 11 after gc 2')
    t.is(recordB.bytesAllocated, 0, 'coreB bytesAllocated 0 after gc 2')
    t.is(recordB.bytesCleared, 16, 'coreB bytesCleared 16 after gc 2')
    t.is(blindPeer.stats.bytesGcd, 27, 'total bytesGcd 27 after gc')
  }

  t.is(blindPeer.needsGc(), false, 'no need to gc again after gc 2')
})

test('priority 2 add-cores redownloads blocks cleared by gc', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const enableGc = false
  const { blindPeer } = await setupBlindPeer(t, bootstrap, { enableGc, maxBytes: 1 })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  for (let i = 2; i < 10; i++) {
    await core.append(`Block ${i}`)
  }

  const muxer = await setupMuxer(t, swarm, store, blindPeer.publicKey)
  await Promise.all([
    once(blindPeer, 'add-cores-done'),
    muxer.addCores({
      referrer: core.key,
      priority: 0,
      announce: false,
      cores: [{ key: core.key, length: core.length }]
    })
  ])

  // wait a bit for downloading blocks
  await new Promise((resolve) => setTimeout(resolve, 1_000))

  const expectedBytes = core.byteLength
  {
    const record = await blindPeer.db.getCoreRecord(core.key)
    t.is(record.bytesAllocated, expectedBytes, 'gc cleared allocated bytes')
    t.is(record.blocksCleared, 0, 'gc marked all blocks cleared')
    t.is(record.bytesCleared, 0, 'gc marked all bytes cleared')
    t.is(blindPeer.stats.coreResetDownload, 0, 'no core got reset download yet')
  }

  await Promise.all([once(blindPeer, 'gc-done'), blindPeer._gc()])
  {
    const record = await blindPeer.db.getCoreRecord(core.key)
    t.is(record.bytesAllocated, 0, 'gc cleared allocated bytes')
    t.is(record.blocksCleared, core.length, 'gc marked all blocks cleared')
    t.is(record.bytesCleared, expectedBytes, 'gc marked all bytes cleared')
  }

  {
    const blindCore = blindPeer.store.get({ key: core.key })
    await blindCore.ready()
    t.is(blindCore.contiguousLength, 0, 'block content is gone after gc')
    await blindCore.close()
  }

  await Promise.all([
    once(blindPeer, 'add-cores-done'),
    muxer.addCores({
      referrer: core.key,
      priority: 2,
      announce: false,
      cores: [{ key: core.key, length: core.length }]
    })
  ])

  {
    const record = await blindPeer.db.getCoreRecord(core.key)
    t.is(record.priority, 2, 'sanity check')
    t.is(record.blocksCleared, 0, 'priority 2 resets cleared block metadata')
    t.is(record.bytesCleared, 0, 'priority 2 resets cleared byte metadata')
    t.is(blindPeer.stats.coreResetDownload, 1, 'core got reset')
  }

  await new Promise((resolve) => setTimeout(resolve, 1_000))

  {
    const blindCore = blindPeer.store.get({ key: core.key })
    await blindCore.ready()
    t.is(blindCore.contiguousLength, 10, 'block content comeback after priority 2')
    await blindCore.close()
  }

  await Promise.all([
    once(blindPeer, 'add-cores-done'),
    muxer.addCores({
      referrer: core.key,
      priority: 2,
      announce: false,
      cores: [{ key: core.key, length: core.length }]
    })
  ])
  {
    const record = await blindPeer.db.getCoreRecord(core.key)
    t.is(record.priority, 2, 'sanity check')
    t.is(blindPeer.stats.coreResetDownload, 1, 'core did not reset after addCore again')
  }
})

test('gc stats', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const enableGc = false // We trigger it manually, so we can test the accounting
  const { blindPeer } = await setupBlindPeer(t, bootstrap, { enableGc, maxBytes: 10 })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  t.teardown(
    async () => {
      await client.close()
    },
    { order: 0 }
  )

  const cores = []
  for (let i = 0; i < 3; i++) {
    const core = store.get({ name: `core-${i}` })
    cores.push(core)
    const blocks = []
    for (let j = 0; j < 6; j++) blocks.push(b4a.alloc(1))
    await core.append(blocks)
  }

  client.addCoreBackground(cores[1], { priority: 1 })
  client.addCoreBackground(cores[0], { priority: 0 })

  // time to download
  await new Promise((resolve) => setTimeout(resolve, 1000))

  t.is(blindPeer.digest.bytesAllocated, 12, 'sanity check on bytes allocated')
  await Promise.all([once(blindPeer, 'gc-done'), blindPeer._gc()])
  t.is(blindPeer.digest.bytesAllocated, 6, 'sanity check 1 core got gcd')

  t.is(blindPeer.stats.gc.prio0Gcd, 1, 'prio0')
  t.is(blindPeer.stats.gc.prio1Gcd, 0, 'prio1')
  t.is(blindPeer.stats.gc.prio2Gcd, 0, 'prio2')
  t.is(blindPeer.stats.gc.coresGcd, 1, 'coresGcd')
  t.is(blindPeer.stats.gc.firstTimeCoresGcd, 1, 'firstTimeCoresGcd')

  const blocks = []
  for (let j = 0; j < 6; j++) blocks.push(b4a.alloc(1))

  await cores[0].append(blocks)

  // time to download
  await new Promise((resolve) => setTimeout(resolve, 1000))

  await Promise.all([once(blindPeer, 'gc-done'), blindPeer._gc()])
  t.is(blindPeer.digest.bytesAllocated, 6, 'sanity check 1 core got gcd')

  t.is(blindPeer.stats.gc.prio0Gcd, 2, 'prio0')
  t.is(blindPeer.stats.gc.coresGcd, 2, 'coresGcd')
  t.is(blindPeer.stats.gc.firstTimeCoresGcd, 1, 'firstTimeCoresGcd')

  client.addCoreBackground(cores[2], { priority: 2 })
  // time to download
  await new Promise((resolve) => setTimeout(resolve, 1000))

  await Promise.all([once(blindPeer, 'gc-done'), blindPeer._gc()])
  t.is(blindPeer.stats.gc.prio0Gcd, 2, 'prio0')
  t.is(blindPeer.stats.gc.prio1Gcd, 1, 'prio1')
  t.is(blindPeer.stats.gc.prio2Gcd, 0, 'prio2')
  t.is(blindPeer.stats.gc.coresGcd, 3, 'coresGcd')
  t.is(blindPeer.stats.gc.firstTimeCoresGcd, 2, 'firstTimeCoresGcd')

  await cores[2].append(blocks)
  // time to download
  await new Promise((resolve) => setTimeout(resolve, 1000))

  await Promise.all([once(blindPeer, 'gc-done'), blindPeer._gc()])
  t.is(blindPeer.stats.gc.prio2Gcd, 1, 'prio2')
  t.is(blindPeer.stats.gc.coresGcd, 4, 'coresGcd')
  t.is(blindPeer.stats.gc.firstTimeCoresGcd, 3, 'firstTimeCoresGcd')
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
  await base.append('something')

  {
    const coreAddedProm = once(blindPeer, 'add-core')
    coreAddedProm.catch(() => {})
    await client.addCore(core, { announce: false })

    const [record] = await coreAddedProm
    t.alike(record.key, coreKey, 'added the core')
  }
  await once(blindPeer, 'add-cores-done') // finish request

  {
    let nrHandled = 0
    let coresAdded = 0
    const { promise, resolve } = rrp()
    const onreq = (_, req) => {
      nrHandled++
      coresAdded += req.cores.length
      if (nrHandled > 2) t.fail('too many rpc requests')
      if (req.referrer) {
        t.alike(req.referrer, base.key, 'sanity check')
      }

      if (nrHandled === 2) resolve()
    }

    blindPeer.on('add-cores-done', onreq)
    await Promise.all([client.addAutobase(base, { announce: false }), promise])

    blindPeer.off('add-cores-done', onreq)
    t.is(coresAdded > 3, true, 'includes views/writers')
  }

  const getSuspendeds = () => [...client.blindPeers.values()].map((v) => v.suspended)

  t.alike(getSuspendeds(), [false], 'clients not yet suspended')
  t.is(client.suspended, false, 'not suspended')

  await client.suspend()

  t.alike(getSuspendeds(), [true], 'clients suspended')
  t.is(client.suspended, true, 'suspended')

  const tResumeAutobase = t.test('resume autobase')
  tResumeAutobase.plan(2)
  const tResumeCore = t.test('resume core')
  tResumeCore.plan(1)

  blindPeer.on('add-cores-done', (_, req) => {
    if (!req.referrer) {
      if (req.cores.length === 1) tResumeCore.pass('core resent after resume')
      else if (req.cores.length === 3) tResumeAutobase.pass('views resent')
      else t.fail('unexpected request')
    } else {
      tResumeAutobase.is(req.cores.length, 1, 'autobase re-sends writers on resume')
    }
  })
  await client.resume()

  await tResumeAutobase
  await tResumeCore

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

test('client gc accounts for pending notifications', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await initBlindPeer(t, bootstrap)
  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const client = new Client(swarm.dht, store, {
    keys: [blindPeer.publicKey],
    gcWait: 10_000
  })
  t.teardown(async () => await client.close())

  await Promise.all([once(blindPeer, 'add-cores-done'), client.addCore(core)])
  const peer = client.blindPeers.values().next().value

  // block mid-sendNotification to test state mid-flight
  const { promise, resolve } = rrp()
  const send = peer.channel.sendNotification.bind(peer.channel)
  peer.channel.sendNotification = async (request) => {
    await promise
    return send(request)
  }

  t.absent(client._gc.has(peer), 'peer is not gc candidate while it has a core')
  await core.close()
  t.ok(client._gc.has(peer), 'peer entered gc after core was closed')

  const sendNotification = client.sendNotification(store.get({ name: 'core' }))
  await sleep(100)

  t.is(peer.pendingNotifications, 1, 'peer has pending notification')
  t.absent(client._gc.has(peer), 'peer left gc')

  resolve()
  await sendNotification

  t.is(peer.pendingNotifications, 0, 'peer has no pending notifications')
  t.ok(client._gc.has(peer), 'peer entered gc after notification was sent')
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

test('client addCore dedups repeated adds but only when needed', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  // We need corestore: false mode to trigger the edge case where neither side activated the core
  // and it needs re-activation
  const { core, swarm, store } = await setupCoreHolder(t, bootstrap, { active: false })

  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  t.teardown(() => client.close())

  await client.addCore(core)
  await new Promise((resolve) => setTimeout(resolve, 500))
  t.is(client.stats.addCore, 1, 'one add')
  t.is(client.stats.addCoresTx, 1, 'one tx')
  t.is(blindPeer.stats.addCoresRx, 1, 'one rx')
  t.is(blindPeer.stats.activations, 1, 'one activation')

  await client.addCore(core)
  await new Promise((resolve) => setTimeout(resolve, 500))
  t.is(client.stats.addCore, 1, 'dedups active add')
  t.is(client.stats.addCoresTx, 1, 'no duplicate tx')
  t.is(blindPeer.stats.addCoresRx, 1, 'no duplicate rx')
  t.is(blindPeer.stats.activations, 1, 'no duplicate activation')

  // simulate reconnect
  await client.suspend()
  await client.resume()

  await client.addCore(core)
  await new Promise((resolve) => setTimeout(resolve, 500))
  t.is(client.stats.addCore, 2, 're-adds after reconnect')
  t.is(client.stats.addCoresTx, 2, 'reconnect tx')
  t.is(blindPeer.stats.addCoresRx, 2, 'reconnect rx')
  t.is(blindPeer.stats.activations, 1, 'activation unchanged')

  await core.append('additional block')

  await client.addCore(core)
  await new Promise((resolve) => setTimeout(resolve, 500))
  t.is(client.stats.addCore, 3, 'adds changed core')
  t.is(client.stats.addCoresTx, 3, 'changed core tx')
  t.is(blindPeer.stats.addCoresRx, 3, 'changed core rx')
  t.is(blindPeer.stats.activations, 2, 'new activation')
})

test.solo('client addCore dedups new cores on existing connection', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await initBlindPeer(t, bootstrap)
  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  t.teardown(() => client.close())

  await Promise.all([client.addCore(core), once(blindPeer, 'add-cores-done')])
  t.is(client.stats.addCoresTx, 1, 'sanity: 1tx for first added core')

  const core2 = store.get({ name: 'core2' })
  client.addCoreBackground(core2)
  client.addCoreBackground(core2)
  await sleep(500)

  t.is(client.stats.addCoresTx, 2, 'dedup new core')
})

test.solo('client addCore dedups inactive cores when needed', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await initBlindPeer(t, bootstrap)
  const { swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  t.teardown(() => client.close())

  const core = store.get({ name: 'inactiveCore', active: false })
  await Promise.all([client.addCore(core), once(blindPeer, 'add-cores-done')])

  await client.suspend()
  await Promise.all([client.resume(), once(blindPeer, 'add-cores-done')])
  t.is(client.stats.addCoresTx, 2, 'sanity: 1tx for initial add and 1tx for reconnect')

  client.addCoreBackground(core)
  client.addCoreBackground(core)
  await sleep(500)

  t.is(client.stats.addCoresTx, 2, 'dedup inactive core after reconnect')

  await core.append('block')
  client.addCoreBackground(core)
  client.addCoreBackground(core)
  await sleep(500)

  t.is(client.stats.addCoresTx, 3, '1tx is made after core changed')
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
    t.ok(metrics.includes('blind_peer_gc_prio_0 0'), 'blind_peer_gc_prio_0 included')
    t.ok(metrics.includes('blind_peer_gc_prio_1 0'), 'blind_peer_gc_prio_1 included')
    t.ok(metrics.includes('blind_peer_gc_prio_2 0'), 'blind_peer_gc_prio_2 included')
    t.ok(metrics.includes('blind_peer_gc_cores_total 0'), 'blind_peer_gc_cores_total included')
    t.ok(
      metrics.includes('blind_peer_gc_cores_first_time_total 0'),
      'blind_peer_gc_cores_first_time_total included'
    )
    t.ok(metrics.includes('blind_peer_cores_added 0'), 'blind_peer_cores_added included')
    t.ok(metrics.includes('blind_peer_cores 0'), 'blind_peer_cores included')
    t.ok(metrics.includes('blind_peer_core_activations 0'), 'blind_peer_core_activations included')
    t.ok(
      metrics.includes('blind_peer_active_replication_sessions 0'),
      'blind_peer_active_replication_sessions included'
    )
    t.ok(
      metrics.includes('blind_peer_replication_sessions_opened 0'),
      'blind_peer_replication_sessions_opened included'
    )
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
    t.ok(metrics.includes('blind_peer_corestore_active 0'), 'blind_peer_corestore_active')
    t.ok(
      metrics.includes('blind_peer_push_notifications_active 0'),
      'blind_peer_push_notifications_active'
    )
    t.ok(metrics.includes('blind_peer_push_notifications_rx 0'), 'blind_peer_push_notifications_rx')
    t.ok(
      metrics.includes('blind_peer_push_notifications_sent 0'),
      'blind_peer_push_notifications_sent'
    )
    t.ok(
      metrics.includes('blind_peer_push_notifications_errors 0'),
      'blind_peer_push_notifications_errors'
    )

    t.ok(metrics.includes('blind_peer_core_trackers_created 0'), 'blind_peer_core_trackers_created')
    t.ok(
      metrics.includes('blind_peer_core_trackers_destroyed 0'),
      'blind_peer_core_trackers_destroyed'
    )
    t.ok(metrics.includes('blind_peer_core_reset_download 0'), 'blind_peer_core_reset_download')
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

test('push notification metrics include client pool stats when configured', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap, { pushGatewayKeys: ['a'.repeat(64)] })
  blindPeer.registerMetrics(promClient)
  t.teardown(() => {
    promClient.register.clear()
  })

  const metrics = await promClient.register.metrics()
  t.ok(metrics.includes('blind_peer_push_notifications_active 1'))
  t.ok(metrics.includes('blind_peer_push_notifications_make_request_attempted 0'))
  t.ok(metrics.includes('blind_peer_push_notifications_make_request_failed'))
  t.ok(metrics.includes('blind_peer_push_notifications_make_request_succeed 0'))
  t.ok(metrics.includes('blind_peer_push_notifications_try_attempted'))
  t.ok(metrics.includes('blind_peer_push_notifications_try_failed'))
  t.ok(metrics.includes('blind_peer_push_notifications_try_succeeded'))
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
    once(blindPeer.topKByReferrer, 'rotated'),
    once(blindPeer.topKByIp, 'rotated')
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
  allPromises.push(
    once(blindPeer.topKByPeer, 'rotated'),
    once(blindPeer.topKByReferrer, 'rotated'),
    once(blindPeer.topKByIp, 'rotated')
  )

  // wait to ensure all addCores request finished
  await Promise.all(allPromises)

  const metrics = await promClient.register.metrics()
  const getMetricValue = (name) => {
    return parseInt(metrics.split(`\n${name} `)[1].split('\n')[0])
  }

  t.is(getMetricValue('blind_peer_add_cores_rx'), totalRequests, 'tracked add-cores requests')
  t.is(blindPeer.topKByIp.spikeThreshold, null, 'remote IP top-k does not emit spike alerts')
  t.is(
    getMetricValue('blind_peer_add_cores_top5_by_remote_key'),
    top5Requests,
    'top-5 remote peers'
  )
  t.is(getMetricValue('blind_peer_add_cores_top5_by_referrer'), top5Requests, 'top-5 referrers')
  // since we're doing simple testing where all requests come from one IP, this is just a sanity check
  t.is(getMetricValue('blind_peer_add_cores_top5_by_remote_ip'), totalRequests, 'top-5 remote IPs')
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

test('trusted peers can query top-k over admin RPC', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const adminKeyPair = crypto.keyPair()
  const referrer = store.get({ name: 'referrer' })
  await referrer.ready()
  await referrer.append('referrer block')

  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    topK: {
      bucketCount: 2,
      bucketTime: 50,
      k: 5
    },
    trustedPubKeys: [adminKeyPair.publicKey]
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })
  t.teardown(async () => {
    await client.close()
  })

  await Promise.all([
    once(blindPeer, 'add-cores-done'),
    client.addCore(core, { referrer: referrer.key })
  ])
  await Promise.all([
    once(blindPeer.topKByPeer, 'rotated'),
    once(blindPeer.topKByReferrer, 'rotated'),
    once(blindPeer.topKByIp, 'rotated')
  ])

  const adminClient = await setupAdminClient(t, {
    bootstrap,
    serverPublicKey: blindPeer.publicKey,
    keyPair: adminKeyPair
  })
  const response = await adminClient.request('query-top-k', null, AdminQueryTopKEncoding)

  t.alike(response.peerPublicKey, blindPeer.topKByPeer.topK)
  t.alike(response.referrer, blindPeer.topKByReferrer.topK)
  t.alike(response.ip, blindPeer.topKByIp.topK)
})

test('untrusted peers cannot query top-k over admin RPC', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const nonAdminKeyPair = crypto.keyPair()

  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    trustedPubKeys: [IdEnc.decode('a'.repeat(64))]
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const adminClient = await setupAdminClient(t, {
    bootstrap,
    serverPublicKey: blindPeer.publicKey,
    keyPair: nonAdminKeyPair
  })

  try {
    await adminClient.request('query-top-k', null, AdminQueryTopKEncoding)
    t.fail('expected query-top-k to reject an untrusted peer')
  } catch (e) {
    t.is(
      e.cause.message,
      'Only trusted peers can query top-k',
      'query-top-k rejects untrusted admin RPC requests'
    )
  }
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

test('notification racing an in-flight add-cores waits for the core instead of erroring', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { gateway, sentMessages } = await setupPushGateway(t, bootstrap)
  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    pushGatewayKeys: [gateway.publicKey],
    retryRecordLookupTimeout: 500
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  await core.setUserData('referrer', core.key)

  const muxer = await setupMuxer(t, swarm, store, blindPeer.publicKey)

  blindPeer.on('notification-error', (e) => {
    console.error(e)
    t.fail('notification errored')
  })

  const request = {
    block: { key: core.key, index: core.length - 1 },
    destination: {
      key: core.key,
      discoveryKey: crypto.discoveryKey(core.key)
    }
  }

  muxer.addCores({
    cores: [{ key: core.key, length: core.length }]
  })
  await Promise.all([once(blindPeer, 'notification-sent'), muxer.sendNotification(request)])

  t.is(sentMessages.length, 1, 'notification forwarded after the core landed')
})

test('notification for an unknown core errors', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { gateway, sentMessages } = await setupPushGateway(t, bootstrap)
  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    pushGatewayKeys: [gateway.publicKey],
    retryRecordLookupTimeout: 500
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const muxer = await setupMuxer(t, swarm, store, blindPeer.publicKey)

  const request = {
    block: { key: core.key, index: core.length - 1 },
    destination: {
      key: core.key,
      discoveryKey: crypto.discoveryKey(core.key)
    }
  }

  const start = Date.now()
  await Promise.all([once(blindPeer, 'notification-error'), muxer.sendNotification(request)])

  t.ok(Date.now() - start >= 500, 'waited the retry timeout before erroring')
  t.is(sentMessages.length, 0, 'nothing forwarded for an unknown core')
})

test('notification errors when no push service available, but does not crash the connection', async (t) => {
  const tError = t.test('notification error')
  tError.plan(1)

  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    pushGatewayKeys: ['a'.repeat(64)],
    pushGatewayPoolOpts: { rpcTimeout: 100 }
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const muxer = await setupMuxer(t, swarm, store, blindPeer.publicKey)

  await Promise.all([
    once(blindPeer, 'add-cores-done'),
    muxer.addCores({
      cores: [{ key: core.key, length: core.length }]
    })
  ])

  const request = {
    block: { key: core.key, index: core.length - 1 },
    destination: {
      key: core.key,
      discoveryKey: crypto.discoveryKey(core.key)
    }
  }

  blindPeer.on('notification-error', (e) => {
    tError.is(e.code, 'TOO_MANY_RETRIES')
  })
  muxer.sendNotification(request)

  await tError

  const core2 = store.get({ name: 'core2' })
  await core2.append('block')

  await Promise.all([
    once(blindPeer, 'add-cores-done'),
    muxer.addCores({
      cores: [{ key: core2.key, length: core2.length }]
    })
  ])

  t.pass('muxer did not close (can still send requests')
})

test('client does not spam reconnect when connection closes immediately after opening', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await new Promise((resolve) => setTimeout(resolve, 500))

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  blindPeer.swarm.on('connection', (conn) => conn.destroy())
  const client = new Client(swarm.dht, store, {
    keys: [blindPeer.publicKey]
  })
  t.teardown(async () => {
    await client.close()
  })

  await client.addCore(core)
  await new Promise((resolve) => setTimeout(resolve, 200))

  // 1 connect for the first attempt. It retries when the connection closes
  // so 2 connects. Then it hangs on the backoff, so it doesn't increment more
  t.is([...client.blindPeers.values()][0].connects, 2, 'did not reconnect spam')
})

test('backoff decreases after successful connect', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await new Promise((resolve) => setTimeout(resolve, 500))

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  let isFirstConn = true
  blindPeer.swarm.on('connection', (conn) => {
    if (isFirstConn) conn.destroy()
    isFirstConn = false
  })

  const client = new Client(swarm.dht, store, {
    keys: [blindPeer.publicKey],
    backoffResetWait: 200
  })
  t.teardown(async () => {
    await client.close()
  })

  await client.addCore(core)
  await new Promise((resolve) => setTimeout(resolve, 100))

  const bp = [...client.blindPeers.values()][0]
  t.ok(bp.backoff.count > 0, 'not reset yet')

  // We need to wait for the backoff to finish (max 1.5s) and the reset to kick (200ms)
  // This time the connection stays open, so the reset happens
  await new Promise((resolve) => setTimeout(resolve, 2000))
  t.is(bp.backoff.count, 0, 'reset now')
})

test('client picks blind peers when they have no groups', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const blindPeers = await setupBlindPeers(t, bootstrap, 4)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = new Client(swarm.dht, store, {
    blindPeers: [
      { key: blindPeers[0].publicKey },
      { key: blindPeers[1].publicKey },
      { key: blindPeers[2].publicKey },
      { key: blindPeers[3].publicKey }
    ]
  })
  t.teardown(() => client.close())

  await client.addCore(core, { pick: 2 })
  await new Promise((resolve) => setTimeout(resolve, 1000))

  const lengths = await Promise.all(
    blindPeers.map((blindPeer) => getBlindPeerCoreLength(blindPeer, core.key))
  )
  t.is(lengths.filter((length) => length > 0).length, 2, 'added the core to two blind peers')
})

test('client picks the blind peer closest to the target when they have no groups', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const blindPeers = await setupBlindPeers(t, bootstrap, 4)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = new Client(swarm.dht, store, {
    blindPeers: [
      { key: blindPeers[0].publicKey },
      { key: blindPeers[1].publicKey },
      { key: blindPeers[2].publicKey },
      { key: blindPeers[3].publicKey }
    ]
  })
  t.teardown(() => client.close())

  // a blind peer is always the closest one to its own key
  await client.addCore(core, { pick: 1, target: blindPeers[3].publicKey })
  await new Promise((resolve) => setTimeout(resolve, 1000))

  const lengths = await Promise.all(
    blindPeers.map((blindPeer) => getBlindPeerCoreLength(blindPeer, core.key))
  )
  t.alike(lengths, [0, 0, 0, 2], 'added the core to the targeted blind peer only')
})

test('client picks blind peers from different groups', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const blindPeers = await setupBlindPeers(t, bootstrap, 4)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = new Client(swarm.dht, store, {
    blindPeers: [
      { key: blindPeers[0].publicKey, group: 'a' },
      { key: blindPeers[1].publicKey, group: 'a' },
      { key: blindPeers[2].publicKey, group: 'a' },
      { key: blindPeers[3].publicKey, group: 'b' }
    ]
  })
  t.teardown(() => client.close())

  await client.addCore(core, { pick: 2, target: blindPeers[0].publicKey })
  await new Promise((resolve) => setTimeout(resolve, 1000))

  const lengths = await Promise.all(
    blindPeers.map((blindPeer) => getBlindPeerCoreLength(blindPeer, core.key))
  )
  t.alike(lengths, [2, 0, 0, 2], 'added the core to one blind peer of each group')
})

test('client balances blind peers across groups when picking more than there are groups', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const blindPeers = await setupBlindPeers(t, bootstrap, 6)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = new Client(swarm.dht, store, {
    blindPeers: [
      { key: blindPeers[0].publicKey, group: 'a' },
      { key: blindPeers[1].publicKey, group: 'a' },
      { key: blindPeers[2].publicKey, group: 'a' },
      { key: blindPeers[3].publicKey, group: 'b' },
      { key: blindPeers[4].publicKey, group: 'b' },
      { key: blindPeers[5].publicKey, group: 'b' }
    ]
  })
  t.teardown(() => client.close())

  await client.addCore(core, { pick: 4, target: blindPeers[0].publicKey })
  await new Promise((resolve) => setTimeout(resolve, 1000))

  const lengths = await Promise.all(
    blindPeers.map((blindPeer) => getBlindPeerCoreLength(blindPeer, core.key))
  )
  const groupA = lengths.slice(0, 3).filter((length) => length > 0)
  const groupB = lengths.slice(3).filter((length) => length > 0)

  t.ok(lengths[0] > 0, 'targeted blind peer picked')
  t.is(groupA.length, 2, 'added the core to two blind peers of group a')
  t.is(groupB.length, 2, 'added the core to two blind peers of group b')
})

async function setupCoreHolder(t, bootstrap, { active } = {}, coreOpts = {}) {
  const { swarm, store } = await setupPeer(t, bootstrap, { active })

  const core = store.get({ name: 'core', ...coreOpts })
  await core.append('Block 0')
  await core.append('Block 1')
  swarm.join(core.discoveryKey)

  return { swarm, store, core }
}

async function loadAutobase(
  store,
  autobaseBootstrap = null,
  { addIndexers = true, namespace = 'base' } = {}
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

  const base = new Autobase(store.namespace(namespace), autobaseBootstrap, {
    open,
    apply,
    valueEncoding: 'json',
    ackInterval: 10,
    ackThreshold: 0
  })
  await base.ready()

  return { base }
}

async function loadAutobee(t, store, key = null) {
  async function apply(nodes, view, host) {
    for (const node of nodes) {
      const op = JSON.parse(node.value)

      if (op.addWriter) host.addWriter(op.addWriter)
      if (op.removeWriter) host.removeWriter(op.removeWriter)

      const w = view.write()
      w.tryPut(Buffer.from('latest'), node.value)
      await w.flush()
    }
  }

  const bee = new Autobee(store.namespace('autobee'), key, { apply })
  t.teardown(async () => await bee.close())
  await bee.ready()

  return { bee }
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
    topK,
    activeCorestore,
    pushGatewayKeys,
    pushGatewayPoolOpts,
    notificationTimeout,
    notificationErrorSnapshotDelay,
    retryRecordLookupTimeout
  } = {}
) {
  if (!storage) storage = await tmpDir(t)

  const adminRouter = new ProtomuxRPCRouter()
  const peer = new BlindPeer(storage, {
    bootstrap,
    maxBytes,
    enableGc,
    trustedPubKeys,
    routerKey,
    routerPoolOpts,
    pushGatewayKeys,
    pushGatewayPoolOpts,
    wakeupGcTickTime: 100,
    replicationLagThreshold,
    topK,
    adminRouter,
    activeCorestore,
    notificationTimeout,
    notificationErrorSnapshotDelay,
    retryRecordLookupTimeout
  })

  const order = clientCounter++
  t.teardown(
    async () => {
      await peer.close()
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

async function initBlindPeer(t, bootstrap, opts) {
  const result = await setupBlindPeer(t, bootstrap, opts)
  await result.blindPeer.listen()
  await result.blindPeer.swarm.flush()
  return result
}

async function setupBlindPeers(t, bootstrap, amount) {
  const blindPeers = []

  for (let i = 0; i < amount; i++) {
    const { blindPeer } = await initBlindPeer(t, bootstrap)
    blindPeers.push(blindPeer)
  }

  return blindPeers
}

async function getBlindPeerCoreLength(blindPeer, key) {
  const core = blindPeer.store.get({ key })
  await core.ready()
  return core.length
}

test('sendNotification does not create a second ref to an already-added blind peer when using HyperDHT addresses', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { gateway } = await setupPushGateway(t, bootstrap)

  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    pushGatewayKeys: [gateway.publicKey]
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const client = new Client(swarm.dht, store, {
    keys: [HyperDHTAddress.encode(blindPeer.publicKey, bootstrap)]
  })
  t.teardown(async () => {
    await client.close()
  })

  await Promise.all([once(blindPeer, 'add-core'), client.addCore(core)])

  t.is(client.blindPeers.size, 1, 'sanity check')

  await Promise.all([
    once(blindPeer, 'notification-sent'),
    client.sendNotification(core, { extra: b4a.from('extra') })
  ])

  t.is(client.blindPeers.size, 1, 'sendNotification reused the existing ref')
})

test('repeated addCore when not connected does not result in repeated infos and cores', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })

  t.is(core.listenerCount('close'), 0, 'core 0 "close" listeners initially')

  client.addCoreBackground(core, { pick: 5 })
  // You'd normally never call it again with a different value.
  // We do it here to have an easy assertion later
  client.addCoreBackground(core, { pick: 10 })
  await once(blindPeer, 'add-cores-done')
  await new Promise((resolve) => setTimeout(resolve, 100)) // Give some more time for (incorrect) extra requests

  const peer = client.blindPeers.get(b4a.toString(blindPeer.publicKey, 'hex'))

  t.is(peer.cores.size, 1, '1 core is added despite adding it twice')
  t.is(core.listenerCount('close'), 1, 'just 1 core "close" listener (not added again)')
  t.is(
    peer.cores.values().next().value.pick,
    5,
    'info object is from the first add (we never re-define the info)'
  )

  await client.close()
})

test('destroying a peer in blind-peering clears core listeners', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.swarm.flush()

  const { swarm, store, core } = await setupCoreHolder(t, bootstrap)
  const core2 = store.get({ name: 'core2' })
  await core2.append('block-0')

  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })

  t.is(core.listenerCount('close'), 0, 'core 0 "close" listeners initially')
  t.is(core2.listenerCount('close'), 0, 'core2 0 "close" listeners initially')

  await client.addCore(core)
  await client.addCore(core2)

  t.is(core.listenerCount('close'), 1, 'core 1 "close" listener after adding')
  t.is(core2.listenerCount('close'), 1, 'core2 1 "close" listener after adding')

  await core2.close()

  t.is(core2.listenerCount('close'), 0, 'core2 0 listeners after core2 close')

  const peer = client.blindPeers.get(b4a.toString(blindPeer.publicKey, 'hex'))

  await client.close()

  t.is(peer.destroyed, true, 'closing blind-peering destroyed the peer')
  t.is(core.listenerCount('close'), 0, 'core 0 "close" listeners after peer is destroyed')
  t.is(peer.cores.size, 0, 'destroy() clears the cores map of the peer')
})

test('destroying peer in blind-peering clears autobase listeners', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.swarm.flush()

  const { swarm, store, base } = await setupAutobaseHolder(t, bootstrap)
  t.teardown(() => base.close())
  await base.append({ hello: 'world' })

  const client = new Client(swarm.dht, store, { keys: [blindPeer.publicKey] })

  t.is(base.listenerCount('close'), 0, 'base 0 "close" listeners initially')
  t.is(base.listenerCount('writer'), 0, 'base 0 "writer" liteners initially')
  t.is(base.core.listenerCount('migrate'), 0, 'base core 0 "migrate" liteners initially')

  await client.addAutobase(base)

  t.is(base.listenerCount('close'), 1, 'base 1 "close" listener after adding')
  t.is(base.listenerCount('writer'), 1, 'base 1 "writer" liteners after adding')
  t.is(base.core.listenerCount('migrate'), 1, 'base core 1 "migrate" listener after adding')

  const peer = client.blindPeers.get(b4a.toString(blindPeer.publicKey, 'hex'))

  await client.close()

  t.is(peer.destroyed, true, 'closing blind-peering destroyed the peer')
  t.is(base.listenerCount('close'), 0, 'base 0 "close" listeners after peer is destroyed')
  t.is(base.listenerCount('writer'), 0, 'base 0 "writer" listeners after peer is destroyed')
  t.is(
    base.core.listenerCount('migrate'),
    0,
    'base core 0 "migrate" listeners after peer is destroyed'
  )
  t.is(peer.bases.size, 0, 'destroy() clears the bases map of the peer')
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

async function setupAdminClient(t, { bootstrap = null, serverPublicKey, keyPair }) {
  const dht = new HyperDHT({ bootstrap, keyPair })
  t.teardown(() => dht.destroy(), { order: 4000 })

  const stream = dht.connect(serverPublicKey)
  stream.on('error', () => {})
  await stream.opened

  const rpc = new ProtomuxRPC(stream, {
    id: ADMIN_CHANNEL_ID,
    valueEncoding: null
  })

  await rpc.fullyOpened()

  return rpc
}

async function setupPushGateway(t, bootstrap) {
  const sentMessages = []
  const dht = new HyperDHT({ bootstrap })
  const router = new ProtomuxRPCRouter()
  // push service stub to simulate real fcm send
  const pushServiceStub = {
    send: async (message) => {
      sentMessages.push(message)
    }
  }
  const gateway = new BlindPushGateway(dht, router, pushServiceStub)

  t.teardown(
    async () => {
      await gateway.close()
      await dht.destroy()
    },
    { order: clientCounter++ }
  )

  await gateway.ready()

  return { gateway, sentMessages }
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

async function setupPeer(t, bootstrap, { active } = {}) {
  const storage = await tmpDir(t)
  const swarm = new Hyperswarm({ bootstrap })
  const store = new Corestore(storage, { active })

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
    () => {
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

async function setupAutobeeHolder(t, bootstrap, key = null) {
  const { swarm, store } = await setupPeer(t, bootstrap)
  const { bee } = await loadAutobee(t, store, key)
  swarm.join(bee.discoveryKey)

  return { swarm, store, bee }
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

function sleep(delay = 1000) {
  return new Promise((resolve) => setTimeout(resolve, delay))
}
