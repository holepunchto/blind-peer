const test = require('brittle')
const { once } = require('events')
const promClient = require('bare-prom-client')
const IdEnc = require('hypercore-id-encoding')
const crypto = require('hypercore-crypto')
const { AdminQueryTopKEncoding } = require('blind-peer-encodings')
const TopKWindow = require('../lib/top-k.js')
const {
  setupCoreHolder,
  setupBlindPeer,
  setupAdminClient,
  getTestnet,
  setupPeer,
  setupMuxer,
  createClient,
  waitForCoresDownloaded
} = require('./helpers')

test.solo('Prometheus metrics', async (t) => {
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
    t.ok(metrics.includes('blind_peer_referrer_rate_limited 0'), 'blind_peer_referrer_rate_limited')
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
    const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })

    for (let i = 0; i < nrCores; i++) {
      const core = store.get({ name: `core-${i}` })
      cores.push(core)
      const blocks = []
      for (let j = 0; j < nrBlocks; j++) blocks.push(`core-${i}-block-${j}`)
      await core.append(blocks)
      client.addCoreBackground(core)
    }
  }

  await waitForCoresDownloaded(blindPeer, cores)

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

  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })

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
