const test = require('brittle')
const b4a = require('b4a')
const {
  clientOpts,
  initBlindPeer,
  getTestnet,
  setupAutobeeHolder,
  sleep,
  waitForEvents,
  waitUntil,
  assertUntilIs,
  createClient
} = require('./helpers')

test('client can use a blind-peer to add an autobee', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await initBlindPeer(t, bootstrap)
  const { swarm, store, bee } = await setupAutobeeHolder(t, bootstrap)
  await bee.append(JSON.stringify({ block: 1 }))

  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })

  const addedKeys = []
  const onaddcore = (record) => {
    addedKeys.push(b4a.toString(record.key, 'hex'))
  }
  blindPeer.on('add-core', onaddcore)

  await client.addAutobase(bee)
  await waitUntil(t, () => addedKeys.length >= 3, 'the blind peer added the cores')
  await sleep(500) // no further cores should get added

  const expectedKeys = [
    b4a.toString(bee.key, 'hex'),
    b4a.toString(bee.bee.core.key, 'hex'),
    b4a.toString(bee.system.bee.core.key, 'hex')
  ]

  t.alike(addedKeys.sort(), expectedKeys.sort(), 'correct cores were added')

  await client.close()
  await bee.close()
  await swarm.destroy()

  {
    const { swarm, bee: reader } = await setupAutobeeHolder(t, bootstrap, bee.key)
    await swarm.flush()

    const node = await reader.view.get(Buffer.from('latest'))
    t.absent(node, 'no data before joining blind-peer')

    swarm.joinPeer(blindPeer.publicKey, { dht: swarm.dht })
    await assertUntilIs(
      t,
      async () => {
        const node = await reader.view.get(Buffer.from('latest'))
        return node && JSON.parse(node.value).block
      },
      1,
      'get data from blind-peer'
    )
  }
})

test('client can use a blind-peer to add an autobee (multiple writers)', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await initBlindPeer(t, bootstrap)
  const { swarm, store, bee } = await setupAutobeeHolder(t, bootstrap)
  await bee.append(JSON.stringify({ block: 1 }))

  // bee2 joins before bee1 is added to blind-peer
  const { bee: bee2 } = await setupAutobeeHolder(t, bootstrap, bee.key)
  await bee.append(JSON.stringify({ addWriter: bee2.local.id }))
  await sleep(500)
  // need to write something or the views() will be []
  await bee2.append(JSON.stringify({ block: 2 }))

  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })

  const addedKeys = []
  const onaddcore = (record) => {
    addedKeys.push(b4a.toString(record.key, 'hex'))
  }
  blindPeer.on('add-core', onaddcore)

  await client.addAutobase(bee)
  await waitUntil(t, () => addedKeys.length >= 3, 'the blind peer added the cores')

  // bee3 joins after bee1 is added to blind-peer
  const { bee: bee3 } = await setupAutobeeHolder(t, bootstrap, bee.key)
  await bee.append(JSON.stringify({ addWriter: bee3.local.id }))
  await sleep(500)
  await bee3.append(JSON.stringify({ block: 3 }))
  await sleep(500)

  const expectedKeys = [
    b4a.toString(bee.key, 'hex'),
    b4a.toString(bee.bee.core.key, 'hex'),
    b4a.toString(bee.system.bee.core.key, 'hex')
  ]

  t.alike(addedKeys.sort(), expectedKeys.sort(), 'correct cores were added')
})

test('client adds views if autobee was initially empty (no views)', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await initBlindPeer(t, bootstrap)
  const { swarm, store, bee } = await setupAutobeeHolder(t, bootstrap)

  const client = createClient(t, swarm.dht, store, { keys: [blindPeer.publicKey] })

  const addedKeys = []
  const onaddcore = (record) => {
    addedKeys.push(b4a.toString(record.key, 'hex'))
  }
  blindPeer.on('add-core', onaddcore)

  await client.addAutobase(bee)
  await bee.append(JSON.stringify({ block: 1 }))
  await waitUntil(t, () => addedKeys.length >= 3, 'the blind peer added the cores')
  await sleep(500) // no further cores should get added

  const expectedKeys = [
    b4a.toString(bee.key, 'hex'),
    b4a.toString(bee.bee.core.key, 'hex'),
    b4a.toString(bee.system.bee.core.key, 'hex')
  ]
  t.alike(addedKeys.sort(), expectedKeys.sort(), 'correct cores were added')
})

test('client coalesces bumps before and after becoming trusted', async (t) => {
  const { bootstrap } = await getTestnet(t)
  const { blindPeer } = await initBlindPeer(t, bootstrap)
  const { swarm, store, bee } = await setupAutobeeHolder(t, bootstrap)
  await bee.append(JSON.stringify({ block: 1 }))

  let appendingCounter = 0
  let trusted = false
  bee.isLocalTrusted = () => Promise.resolve(trusted)

  const coresCalls = countCoresCalls()
  const client = createClient(t, swarm.dht, store, { ...clientOpts, keys: [blindPeer.publicKey] })

  await Promise.all([waitForEvents(blindPeer, 'add-cores-done', 2), client.addAutobase(bee)])

  t.alike(coresCalls, { normal: 1, recursive: 0 }, 'initial normal call')

  emitAppending()
  emitAppending()
  emitAppending()
  await sleep(1000) // appending events are bumped after 500ms timeout

  // 1 for first event, 1 for the coalesced events
  t.alike(coresCalls, { normal: 3, recursive: 0 }, 'untrusted triggers coalesced')

  trusted = true

  emitAppending()
  await sleep(1000)

  // 1 normal call the first time, and first recursive call
  t.alike(coresCalls, { normal: 4, recursive: 1 }, 'switched to recursive once trusted')

  emitAppending()
  emitAppending()
  emitAppending()
  await sleep(1000)

  // only recursive calls, 1 for first event, 1 for the coalesced events
  t.alike(coresCalls, { normal: 4, recursive: 3 }, 'trusted triggers coalesced')

  function emitAppending() {
    bee.emit('appending', [{ trusted: [{ key: b4a.alloc(32, appendingCounter++), length: 1 }] }])
  }

  function countCoresCalls() {
    const calls = { normal: 0, recursive: 0 }
    const cores = bee.cores.bind(bee)
    bee.cores = (opts = {}) => {
      calls[opts.recursive ? 'recursive' : 'normal']++
      return cores(opts)
    }
    return calls
  }
})
