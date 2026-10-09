const test = require('brittle')
const { once } = require('events')
const b4a = require('b4a')
const {
  setupCoreHolder,
  setupBlindPeer,
  getTestnet,
  setupPeer,
  setupMuxer,
  createClient,
  waitForCoresDownloaded,
  runGc,
  initBlindPeer,
  sleep
} = require('./helpers')

test('garbage collection when space limit reached', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const enableGc = false // We trigger it manually, so we can test the accounting
  const { blindPeer } = await initBlindPeer(t, bootstrap, { enableGc, maxBytes: 10_000 })

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
  const initBytes = blindPeer.digest.bytesAllocated

  const [[{ bytesCleared }]] = await runGc(blindPeer)

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
  await sleep(1000)

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
  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })

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
    const [[{ bytesCleared }]] = await runGc(blindPeer)
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
    const [[{ bytesCleared }]] = await runGc(blindPeer)
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

  await runGc(blindPeer)
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
  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })

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
  await runGc(blindPeer)
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

  await runGc(blindPeer)
  t.is(blindPeer.digest.bytesAllocated, 6, 'sanity check 1 core got gcd')

  t.is(blindPeer.stats.gc.prio0Gcd, 2, 'prio0')
  t.is(blindPeer.stats.gc.coresGcd, 2, 'coresGcd')
  t.is(blindPeer.stats.gc.firstTimeCoresGcd, 1, 'firstTimeCoresGcd')

  client.addCoreBackground(cores[2], { priority: 2 })
  // time to download
  await new Promise((resolve) => setTimeout(resolve, 1000))

  await runGc(blindPeer)
  t.is(blindPeer.stats.gc.prio0Gcd, 2, 'prio0')
  t.is(blindPeer.stats.gc.prio1Gcd, 1, 'prio1')
  t.is(blindPeer.stats.gc.prio2Gcd, 0, 'prio2')
  t.is(blindPeer.stats.gc.coresGcd, 3, 'coresGcd')
  t.is(blindPeer.stats.gc.firstTimeCoresGcd, 2, 'firstTimeCoresGcd')

  await cores[2].append(blocks)
  // time to download
  await new Promise((resolve) => setTimeout(resolve, 1000))

  await runGc(blindPeer)
  t.is(blindPeer.stats.gc.prio2Gcd, 1, 'prio2')
  t.is(blindPeer.stats.gc.coresGcd, 4, 'coresGcd')
  t.is(blindPeer.stats.gc.firstTimeCoresGcd, 3, 'firstTimeCoresGcd')
})

test('can gc core that is not currently active', async (t) => {
  const { bootstrap } = await getTestnet(t)

  const enableGc = false // We trigger it manually, so we can test the accounting
  const { blindPeer } = await initBlindPeer(t, bootstrap, { enableGc, maxBytes: 10_000 })

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

  await swarm.destroy()
  await store.close()
  // TODO: expose corestore gc tick time (it takes 4 ticks to gc weak cores)
  await sleep(10000)

  t.is(blindPeer.activeReplication.size, 0, 'sanity check (core not active)')
  t.ok(blindPeer.digest.bytesAllocated > 10_000, 'sanity check')

  await runGc(blindPeer)

  const nowBytes = blindPeer.digest.bytesAllocated
  t.is(nowBytes < 10_000, true, 'gcd till below limit')
  t.is(nowBytes > 1000, true, 'did not gc too much')
})
