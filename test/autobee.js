const test = require('brittle')
const b4a = require('b4a')
const { initBlindPeer, getTestnet, setupAutobeeHolder, sleep, createClient } = require('./helpers')

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
  await sleep(500)

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

    let node = await reader.view.get(Buffer.from('latest'))
    t.absent(node, 'no data before joining blind-peer')

    swarm.joinPeer(blindPeer.publicKey, { dht: swarm.dht })
    await sleep(1000)

    node = await reader.view.get(Buffer.from('latest'))
    t.alike(JSON.parse(node.value), { block: 1 }, 'get data from blind-peer')
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
  await sleep(500)

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
  await sleep(500)
  await bee.append(JSON.stringify({ block: 1 }))
  // TODO: the sleep here should be lowered once we update
  // blind-peering to not delay `onmigrate` for autobee
  await sleep(1500)

  const expectedKeys = [
    b4a.toString(bee.key, 'hex'),
    b4a.toString(bee.bee.core.key, 'hex'),
    b4a.toString(bee.system.bee.core.key, 'hex')
  ]
  t.alike(addedKeys.sort(), expectedKeys.sort(), 'correct cores were added')
})
