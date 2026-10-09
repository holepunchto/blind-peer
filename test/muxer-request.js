const test = require('brittle')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const { setupCoreHolder, setupBlindPeer, getTestnet, setupMuxer } = require('./helpers')

test('muxer requestAddCores resolves with the per-core response', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const muxer = await setupMuxer(t, swarm, store, blindPeer.publicKey)

  const res = await muxer.requestAddCores({ cores: [{ key: core.key, length: core.length }] })
  t.is(res.cores.length, 1, 'one entry per requested core')
  t.ok(b4a.equals(res.cores[0].key, core.key), 'response refers to the requested core')
  t.is(res.cores[0].length, 0, 'blind peer did not have the core yet')
  t.is(res.cores[0].activated, true, 'new core is activated')
})

test('muxer requestAddCores rejects with RATE_LIMITED when rate limited', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const perReferrerRateLimitParams = { capacity: 1, intervalMs: 10_000 }
  const { blindPeer } = await setupBlindPeer(t, bootstrap, { perReferrerRateLimitParams })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const muxer = await setupMuxer(t, swarm, store, blindPeer.publicKey)

  const request = { referrer: core.key, cores: [{ key: core.key, length: core.length }] }

  const first = await muxer.requestAddCores(request)
  t.is(first.cores.length, 1, 'first request processed')

  try {
    await muxer.requestAddCores(request)
    t.fail('must error')
  } catch (e) {
    t.is(e.code, 'RATE_LIMITED')
  }
  t.is(blindPeer.stats.referrerRateLimited, 1)
})

test('muxer requestSendNotification resolves once processed', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await setupBlindPeer(t, bootstrap)
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const muxer = await setupMuxer(t, swarm, store, blindPeer.publicKey)

  const request = {
    block: { key: core.key, index: core.length - 1 },
    destination: { key: core.key, discoveryKey: crypto.discoveryKey(core.key) }
  }

  t.is(await muxer.requestSendNotification(request), null, 'resolves with an empty response')
  t.is(blindPeer.stats.notificationsRx, 1)
})

test('muxer requestSendNotification for an unknown core rejects without closing the channel', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    pushGatewayKeys: ['a'.repeat(64)]
  })
  await blindPeer.listen()
  await blindPeer.swarm.flush()

  const { core, swarm, store } = await setupCoreHolder(t, bootstrap)
  const muxer = await setupMuxer(t, swarm, store, blindPeer.publicKey)

  const request = {
    block: { key: core.key, index: core.length - 1 },
    destination: { key: core.key, discoveryKey: crypto.discoveryKey(core.key) }
  }

  try {
    await muxer.requestSendNotification(request)
    t.fail('must error')
  } catch (e) {
    t.is(e.code, 'UNKNOWN_CORE')
  }

  const res = await muxer.requestAddCores({ cores: [{ key: core.key, length: core.length }] })
  t.is(res.cores.length, 1, 'channel still usable')
})
