const test = require('brittle')
const { once } = require('events')
const b4a = require('b4a')
const rrp = require('resolve-reject-promise')
const {
  setupCoreHolder,
  setupBlindPeer,
  initBlindPeer,
  setupBlindPeers,
  getBlindPeerCoreLength,
  getTestnet,
  setupAutobaseHolder,
  sleep,
  createClient
} = require('./helpers')

test('client suspend/resume logic', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    trustedPubKeys: [swarm.dht.defaultKeyPair.publicKey]
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
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
})

test('client gc logic', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    trustedPubKeys: [swarm.dht.defaultKeyPair.publicKey]
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey], gcWait: 10 })
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
})

test('client gc accounts for pending notifications', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await initBlindPeer(t, bootstrap)
  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)

  const client = createClient(t, swarm.dht, store, {
    keys: [blindPeer.publicKey],
    gcWait: 10_000
  })

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

  const client = createClient(t, swarm.dht, store, {
    batchIdleWait: 1_000_000,
    batchMaxWait: 1_000_000,
    keys: [blindPeer.publicKey]
  })
  await client.addAutobase(base)
  await client.close()

  await base.close()

  t.pass('unless the test run hangs for a really long time, this test passed')
})

test('client addCore dedups repeated adds but only when needed', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  // We need corestore: false mode to trigger the edge case where neither side activated the core
  // and it needs re-activation
  const { core, swarm, store } = await setupCoreHolder(t, bootstrap, { active: false })

  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })
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
  t.is(client.stats.addCore, 1, 'dedups after reconnect')
  t.is(client.stats.addCoresTx, 2, 'reconnect tx')
  t.is(blindPeer.stats.addCoresRx, 2, 'reconnect rx')
  t.is(blindPeer.stats.activations, 1, 'activation unchanged')

  await core.append('additional block')

  await client.addCore(core)
  await new Promise((resolve) => setTimeout(resolve, 500))
  t.is(client.stats.addCore, 2, 'adds changed core')
  t.is(client.stats.addCoresTx, 3, 'changed core tx')
  t.is(blindPeer.stats.addCoresRx, 3, 'changed core rx')
  t.is(blindPeer.stats.activations, 2, 'new activation')
})

test('client addCore dedups new cores on existing connection', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await initBlindPeer(t, bootstrap)
  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })

  await Promise.all([client.addCore(core), once(blindPeer, 'add-cores-done')])
  t.is(client.stats.addCoresTx, 1, 'sanity: 1tx for first added core')

  // existing blind-peer connection, but newly added core
  // with consecutive addCore not giving time to start replication
  const core2 = store.get({ name: 'core2' })
  client.addCoreBackground(core2)
  client.addCoreBackground(core2)
  await sleep(500)

  t.is(client.stats.addCoresTx, 2, 'dedup new core')
})

test('client addCore dedups inactive cores when needed', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await initBlindPeer(t, bootstrap)
  const { swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })

  const core = store.get({ name: 'inactiveCore', active: false })
  await Promise.all([client.addCore(core), once(blindPeer, 'add-cores-done')])

  await client.suspend()
  await Promise.all([client.resume(), once(blindPeer, 'add-cores-done')])
  t.is(client.stats.addCoresTx, 2, 'sanity: 1tx for initial add and 1tx for reconnect')

  await client.addCore(core)
  await sleep(500)
  await client.addCore(core)
  await sleep(500)

  t.is(client.stats.addCoresTx, 2, 'dedup inactive core after reconnect')

  await core.append('block')
  await client.addCore(core)
  await sleep(500)
  await client.addCore(core)
  await sleep(500)

  t.is(client.stats.addCoresTx, 3, '1tx is made after core changed')
})

test('client does not spam reconnect when connection closes immediately after opening', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await new Promise((resolve) => setTimeout(resolve, 500))

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  blindPeer.swarm.on('connection', (conn) => conn.destroy())
  const client = createClient(t, swarm.dht, store, {
    keys: [blindPeer.publicKey]
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

  const client = createClient(t, swarm.dht, store, {
    keys: [blindPeer.publicKey],
    backoffResetWait: 200
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
  const client = createClient(t, swarm.dht, store, {
    blindPeers: [
      { key: blindPeers[0].publicKey },
      { key: blindPeers[1].publicKey },
      { key: blindPeers[2].publicKey },
      { key: blindPeers[3].publicKey }
    ]
  })

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
  const client = createClient(t, swarm.dht, store, {
    blindPeers: [
      { key: blindPeers[0].publicKey },
      { key: blindPeers[1].publicKey },
      { key: blindPeers[2].publicKey },
      { key: blindPeers[3].publicKey }
    ]
  })

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
  const client = createClient(t, swarm.dht, store, {
    blindPeers: [
      { key: blindPeers[0].publicKey, group: 'a' },
      { key: blindPeers[1].publicKey, group: 'a' },
      { key: blindPeers[2].publicKey, group: 'a' },
      { key: blindPeers[3].publicKey, group: 'b' }
    ]
  })

  await client.addCore(core, { pick: 2, target: blindPeers[0].publicKey })
  await new Promise((resolve) => setTimeout(resolve, 1000))

  const lengths = await Promise.all(
    blindPeers.map((blindPeer) => getBlindPeerCoreLength(blindPeer, core.key))
  )
  t.alike(lengths, [2, 0, 0, 2], 'added the core to one blind peer of each group')
})

test.solo(
  'client balances blind peers across groups when picking more than there are groups',
  async (t) => {
    const start = Date.now()
    const log = (msg) => console.log(`[${Date.now() - start}ms] ${msg}`)
    const state = (r) => (r.closed ? 'closed' : r.closing ? 'closing' : 'open')
    let blindPeers = []
    let holder = null

    const dump = setTimeout(() => {
      log(
        `blind peers: ${blindPeers.map((b) => `${state(b)}/swarm ${b.swarm.destroyed}`).join(', ')}`
      )
      if (holder) log(`holder: swarm ${holder.swarm.destroyed}, store ${state(holder.store)}`)
    }, 28000)
    dump.unref()

    const { bootstrap } = await getTestnet(t)
    log('testnet ready')
    blindPeers = await setupBlindPeers(t, bootstrap, 6)
    log('blind peers ready')

    holder = await setupCoreHolder(t, bootstrap)
    const { core, swarm, store } = holder
    log('core holder ready')

    const client = createClient(t, swarm.dht, store, {
      blindPeers: [
        { key: blindPeers[0].publicKey, group: 'a' },
        { key: blindPeers[1].publicKey, group: 'a' },
        { key: blindPeers[2].publicKey, group: 'a' },
        { key: blindPeers[3].publicKey, group: 'b' },
        { key: blindPeers[4].publicKey, group: 'b' },
        { key: blindPeers[5].publicKey, group: 'b' }
      ]
    })

    await client.addCore(core, { pick: 4, target: blindPeers[0].publicKey })
    log('addCore done')
    await new Promise((resolve) => setTimeout(resolve, 1000))

    const lengths = await Promise.all(
      blindPeers.map((blindPeer) => getBlindPeerCoreLength(blindPeer, core.key))
    )
    log('lengths read')
    const groupA = lengths.slice(0, 3).filter((length) => length > 0)
    const groupB = lengths.slice(3).filter((length) => length > 0)

    t.ok(lengths[0] > 0, 'targeted blind peer picked')
    t.is(groupA.length, 2, 'added the core to two blind peers of group a')
    t.is(groupB.length, 2, 'added the core to two blind peers of group b')
    log('assertions done')
  }
)

test('repeated addCore when not connected does not result in repeated infos and cores', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })

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
})

test('destroying a peer in blind-peering clears core listeners', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.swarm.flush()

  const { swarm, store, core } = await setupCoreHolder(t, bootstrap)
  const core2 = store.get({ name: 'core2' })
  await core2.append('block-0')

  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })

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
  const { blindPeer } = await initBlindPeer(t, bootstrap)
  const { swarm, store, base } = await setupAutobaseHolder(t, bootstrap)
  t.teardown(() => base.close())
  await base.append({ hello: 'world' })

  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })

  t.is(base.listenerCount('close'), 0, 'base 0 "close" listeners initially')
  t.is(base.listenerCount('writer'), 0, 'base 0 "writer" listeners initially')
  t.is(base.listenerCount('anchor'), 0, 'base 0 "anchor" listeners initially')
  t.is(base.listenerCount('appending'), 0, 'base 0 "appending" listeners initially')
  t.is(base.core.listenerCount('migrate'), 0, 'base core 0 "migrate" listeners initially')

  await client.addAutobase(base)

  t.is(base.listenerCount('close'), 1, 'base 1 "close" listener after adding')
  t.is(base.listenerCount('writer'), 1, 'base 1 "writer" listeners after adding')
  t.is(base.listenerCount('anchor'), 1, 'base 1 "anchor" listeners after adding')
  t.is(base.listenerCount('appending'), 1, 'base 1 "appending" listeners after adding')
  t.is(base.core.listenerCount('migrate'), 1, 'base core 1 "migrate" listener after adding')

  const peer = client.blindPeers.get(b4a.toString(blindPeer.publicKey, 'hex'))

  await client.close()

  t.is(peer.destroyed, true, 'closing blind-peering destroyed the peer')
  t.is(base.listenerCount('close'), 0, 'base 0 "close" listeners after peer is destroyed')
  t.is(base.listenerCount('writer'), 0, 'base 0 "writer" listeners after peer is destroyed')
  t.is(base.listenerCount('anchor'), 0, 'base 0 "anchor" listeners after peer is destroyed')
  t.is(base.listenerCount('appending'), 0, 'base 0 "appending" listeners after peer is destroyed')
  t.is(
    base.core.listenerCount('migrate'),
    0,
    'base core 0 "migrate" listeners after peer is destroyed'
  )
  t.is(peer.bases.size, 0, 'destroy() clears the bases map of the peer')
})
