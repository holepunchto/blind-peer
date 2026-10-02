const test = require('brittle')
const HyperDHT = require('hyperdht')
const Corestore = require('corestore')
const { once } = require('events')
const b4a = require('b4a')
const Hyperswarm = require('hyperswarm')
const crypto = require('hypercore-crypto')
const HyperDHTAddress = require('hyperdht-address')
const blindPush = require('blind-push')
const {
  setupCoreHolder,
  setupBlindPeer,
  initBlindPeer,
  setupPushGateway,
  getTestnet,
  setupPeer,
  setupMuxer,
  sleep,
  createClient
} = require('./helpers')

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
  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })

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

test('sendNotification distributes requests across connected blind peers', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { gateway, sentMessages } = await setupPushGateway(t, bootstrap)

  const { blindPeer: blindPeer1 } = await initBlindPeer(t, bootstrap, {
    pushGatewayKeys: [gateway.publicKey]
  })
  const { blindPeer: blindPeer2 } = await initBlindPeer(t, bootstrap, {
    pushGatewayKeys: [gateway.publicKey]
  })

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  await core.setUserData('referrer', core.key)

  const client = createClient(t, swarm.dht, store, {
    keys: [blindPeer1.publicKey, blindPeer2.publicKey],
    pick: 2,
    notificationRateLimit: null
  })

  await Promise.all([
    once(blindPeer1, 'add-cores-done'),
    once(blindPeer2, 'add-cores-done'),
    client.addCore(core)
  ])

  t.is(client.blindPeers.size, 2, 'client has two blind peers')
  t.ok(
    Array.from(client.blindPeers.values()).every((peer) => peer.connected),
    'both blind peers are connected'
  )

  for (let i = 0; i < 64; i++) {
    client.sendNotificationBackground(core)

    await Promise.race([
      once(blindPeer1, 'notification-sent'),
      once(blindPeer2, 'notification-sent')
    ])
  }

  t.ok(blindPeer1.stats.notificationsSent > 0, 'first blind peer sent a notification')
  t.ok(blindPeer2.stats.notificationsSent > 0, 'second blind peer sent a notification')
  t.is(
    blindPeer1.stats.notificationsSent + blindPeer2.stats.notificationsSent,
    64,
    'each request used one blind peer'
  )
  t.is(sentMessages.length, 64, 'gateway received every notification')
  t.is(client.stats.notificationsTx, 64, 'client counted every notification')
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
  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })

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

  const initClient = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
  await Promise.all([once(blindPeer, 'add-cores-done'), initClient.addCore(core)])
  await initClient.close()

  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
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

  const initClient = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
  await Promise.all([once(blindPeer, 'add-cores-done'), initClient.addCore(core)])
  await initClient.close()

  await Promise.all([core.append('another block'), once(coreCopy, 'append')])

  const client = createClient(t, swarm2.dht, store2, { keys: [blindPeer.publicKey] })

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

  const initClient = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
  await Promise.all([once(blindPeer, 'add-cores-done'), initClient.addCore(core)])
  await initClient.close()

  const deadKey = HyperDHT.keyPair().publicKey
  const client = createClient(t, swarm.dht, store, {
    keys: [deadKey, blindPeer.publicKey],
    pick: 2
  })

  t.is(sentMessages.length, 0, 'sanity check')

  const start = Date.now()
  await Promise.all([
    once(blindPeer, 'notification-sent').then(() => console.log('something 1')),
    client.sendNotification(core, {
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

  const initClient = createClient(t, initSwarm.dht, initStore, { keys: [blindPeer.publicKey] })
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

  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
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

  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })

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

test('client sendNotification gets rate limited', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await initBlindPeer(t, bootstrap)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = createClient(t, swarm.dht, store, {
    keys: [blindPeer.publicKey],
    notificationRateLimit: { capacity: 2, interval: 750, timeout: 1250 }
  })

  client.sendNotificationBackground(core)
  client.sendNotificationBackground(core)
  client.sendNotificationBackground(core)
  const lastSend = client.sendNotification(core)

  await sleep(500)
  t.is(client.stats.notificationsTx, 2, 'burst 2')

  await sleep(500)
  t.is(client.stats.notificationsTx, 3, 'send 1 for passed interval')

  await t.exception(async () => await lastSend, /Timed out/, 'throw for time out')
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

test('sendNotification does not create a second ref to an already-added blind peer when using HyperDHT addresses', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { gateway } = await setupPushGateway(t, bootstrap)

  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    pushGatewayKeys: [gateway.publicKey]
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const client = createClient(t, swarm.dht, store, {
    keys: [HyperDHTAddress.encode(blindPeer.publicKey, bootstrap)]
  })

  await Promise.all([once(blindPeer, 'add-core'), client.addCore(core)])

  t.is(client.blindPeers.size, 1, 'sanity check')

  await Promise.all([
    once(blindPeer, 'notification-sent'),
    client.sendNotification(core, { extra: b4a.from('extra') })
  ])

  t.is(client.blindPeers.size, 1, 'sendNotification reused the existing ref')
})
