const setupTestnet = require('hyperdht/testnet')
const HyperDHT = require('hyperdht')
const Corestore = require('corestore')
const tmpDir = require('test-tmp')
const { once } = require('events')
const b4a = require('b4a')
const Client = require('blind-peering')
const BlindPeerMuxer = require('blind-peer-muxer')
const Hyperswarm = require('hyperswarm')
const Autobase = require('autobase')
const Autobee = require('autobee')
const ProtomuxRPC = require('protomux-rpc')
const ProtomuxRPCRouter = require('protomux-rpc-router')
const BlindPeerRouter = require('blind-peer-router')
const { ADMIN_CHANNEL_ID } = require('blind-peer-encodings')
const BlindPushGateway = require('blind-push-gateway')
const BlindPeer = require('../..')

const DEBUG = false
let clientCounter = 0 // For clean teardown order

const clientOpts = { batchIdleWait: 250, batchMaxWait: 1000 }

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

async function setupCoreHolder(t, bootstrap, { active } = {}) {
  const { swarm, store } = await setupPeer(t, bootstrap, { active })

  const core = store.get({ name: 'core' })
  await core.append('Block 0')
  await core.append('Block 1')
  swarm.join(core.discoveryKey)

  return { swarm, store, core }
}

function createClient(t, dht, store, opts) {
  const client = new Client(dht, store, opts)
  t.teardown(async () => await client.close())
  return client
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

  const bee = new Autobee(store.namespace('autobee'), key, {
    isTrusted: (key) => true,
    mostRecentTrusted: () => ({ key: bee.local.key, length: bee.local.length }),
    apply
  })
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
    retryRecordLookupTimeout,
    perReferrerRateLimitParams
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
    retryRecordLookupTimeout,
    perReferrerRateLimitParams
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
  const client = createClient(t, swarm.dht, store, {
    ...clientOpts,
    wakeup: base.wakeupProtocol,
    keys: [blindPeer.publicKey]
  })

  return { client, base, store, swarm, wakeup: base.wakeupProtocol }
}

function sleep(delay = 1000) {
  return new Promise((resolve) => setTimeout(resolve, delay))
}

module.exports = {
  DEBUG,
  clientOpts,
  setupCoreHolder,
  loadAutobase,
  loadAutobee,
  setupBlindPeer,
  initBlindPeer,
  setupBlindPeers,
  getBlindPeerCoreLength,
  setupAdminClient,
  setupPushGateway,
  getTestnet,
  setupRouter,
  setupPeer,
  setupMuxer,
  setupAutobaseHolder,
  setupAutobeeHolder,
  getWakeupPeer,
  sleep,
  createClient
}
