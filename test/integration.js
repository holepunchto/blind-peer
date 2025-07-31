const test = require('brittle')
const { spawn } = require('child_process')
const HypCrypto = require('hypercore-crypto')
const IdEnc = require('hypercore-id-encoding')
const pino = require('pino')
const NewlineDecoder = require('newline-decoder')
const setupTestnet = require('hyperdht/testnet')
const tmpDir = require('test-tmp')
const rrp = require('resolve-reject-promise')
const path = require('path')
const b4a = require('b4a')
const { once } = require('events')
const Hyperswarm = require('hyperswarm')
const Corestore = require('corestore')
const BlindPeerClient = require('blind-peering')

const DEBUG = true

test('integration test', async t => {
  const tMain = t.test('Main flow')
  tMain.plan(1)
  const testnet = await setupTestnet()
  t.teardown(async () => {
    console.log('destroying testnet')
    await testnet.destroy()
  }, { order: 9999999 })
  const { bootstrap } = testnet
  const txtBootstrap = `${bootstrap[0].host}:${bootstrap[0].port}`

  const logger = pino({
    transport: {
      target: 'pino-pretty', options: { colorize: true }
    }
  })

  const autodiscoverySeed = HypCrypto.randomBytes(32)
  const allowedKeyPair = HypCrypto.keyPair(autodiscoverySeed)
  const allowedPubKey = IdEnc.normalize(allowedKeyPair.publicKey)
  const { rpcKey } = await setupAutobaseDiscovery(t, allowedPubKey, txtBootstrap, logger.child({ service: 'autodiscovery' }))
  const { blindPeerKey } = await setupBlindPeer(t, autodiscoverySeed, rpcKey, txtBootstrap, logger.child({ service: 'blind peer' }))

  const { coreKey, coreLength } = await setupCoreHolder(t, bootstrap, blindPeerKey)

  // await downloadCoreFromBlindPeer(t, bootstrap, coreKey, coreLength, blindPeerKey)

  tMain.pass('completed test')
})

async function setupBlindPeer (t, autodiscoverySeed, autodiscoveryRpcKey, bootstrap, logger) {
  const subT = t.test('blind peer')
  subT.plan(2)

  autodiscoverySeed = b4a.toString(autodiscoverySeed, 'hex')
  const storage = await tmpDir()
  const exProc = spawn(process.execPath, [path.join(path.dirname(__dirname), 'bin.js'), '--bootstrap', bootstrap, '--storage', storage, '--autodiscovery-seed', autodiscoverySeed, '--autodiscovery-rpc-key', autodiscoveryRpcKey])

  process.on('exit', () => { exProc.kill('SIGKILL') })

  t.teardown(async () => {
    exProc.kill('SIGTERM')
    await once(exProc, 'exit')
  }, { order: 100 })

  const { resolve, reject, promise } = rrp()

  exProc.stderr.on('data', d => {
    logger.error(d.toString())
    reject('Blind peer unexpected error')
  })
  const stdoutDec = new NewlineDecoder('utf-8')
  exProc.stdout.on('data', async d => {
    for (const line of stdoutDec.push(d)) {
      if (DEBUG) logger.info(parseLine(line))

      if (line.includes('Listening at ')) {
        subT.pass('Blind peer started up')
        resolve(line.split('Listening at')[1].split('"}')[0].trim())
      } else if (line.includes('Successfully requested to be added to the autodiscovery service')) {
        subT.pass('Added to autodiscovery')
      }
    }
  })

  const blindPeerKey = await promise
  await flush()

  return { blindPeerKey }
}

async function setupAutobaseDiscovery (t, allowedPubKey, bootstrap, logger) {
  const subT = t.test('autodiscovery')
  subT.plan(1)

  const storage = await tmpDir()
  const autodiscBinLoc = path.join(
    path.dirname(__dirname), 'node_modules', 'autobase-discovery', 'bin.js'
  )
  const exProc = spawn('node', [autodiscBinLoc, 'run', allowedPubKey, '--bootstrap', bootstrap, '--storage', storage])

  // To avoid zombie processes in case there's an error
  process.on('exit', () => {
    // TODO: unset this handler on clean run
    exProc.kill('SIGKILL')
  })

  t.teardown(async () => {
    exProc.kill('SIGTERM')
    await once(exProc, 'exit')
  }, { order: 100 })

  const { resolve, reject, promise } = rrp()
  exProc.stderr.on('data', d => {
    logger.error(d.toString())
    reject('unexpected autodiscovery error')
  })
  const stdoutDec = new NewlineDecoder('utf-8')
  exProc.stdout.on('data', async d => {
    for (const line of stdoutDec.push(d)) {
      if (DEBUG)logger.info(parseLine(line))

      if (line.includes('RPC server public key:')) {
        subT.pass('autobase discovery started up')
        resolve(line.split('RPC server public key:')[1].split('"}')[0].trim())
      }
    }
  })

  const rpcKey = await promise
  await flush()
  return { rpcKey }
}

async function setupCoreHolder (t, bootstrap, blindPeerKey, { nrBlocks = 1000, name = 'core' } = {}) {
  const subT = t.test('core holder')
  subT.plan(1)
  blindPeerKey = IdEnc.decode(blindPeerKey)

  const { store, swarm } = await setupPeer(t, bootstrap)

  const core = store.get({ name })
  await core.ready()

  const batch = []
  for (let i = 0; i < nrBlocks; i++) {
    batch.push(`Block ${i}`)
  }
  await core.append(batch)

  let nrUploaded = 0
  core.on('upload', () => { // Works because it's a single peer
    if (++nrUploaded === nrBlocks) subT.pass('Uploaded hypercore to blind peer')
  })

  const coreLength = core.length
  const client = new BlindPeerClient(swarm, store, { mirrors: [blindPeerKey] })
  await client.addCore(core)

  await subT
  await client.close()
  await swarm.destroy()
  await store.close()

  return { coreKey: core.key, coreLength }
}

async function downloadCoreFromBlindPeer (t, bootstrap, coreKey, coreLength, blindPeerKey) {
  coreKey = IdEnc.decode(coreKey)
  blindPeerKey = IdEnc.decode(coreKey)
  const { swarm, store } = await setupPeer(t, bootstrap)

  const core = store.get({ key: coreKey })
  console.log('joining peer', blindPeerKey)
  swarm.joinPeer(blindPeerKey)

  console.log('downloading', coreLength)
  await core.download({ start: 0, end: coreLength }).done()

  t.is(core.length > 0, true, 'sanity check')
  t.is(core.length, coreLength, 'Can download core from blind peer')

  await swarm.destroy()
  await store.close()
  console.log('downloaded')
}

async function setupPeer (t, bootstrap) {
  const swarm = new Hyperswarm({ bootstrap })
  const store = new Corestore(await t.tmp())

  swarm.on('connection', (conn) => {
    console.log('opened connection')
    // store.replicate(conn)
  })

  return { swarm, store }
}

function parseLine (line) {
  let msg = line
  try {
    msg = JSON.parse(line)?.msg
  } catch {} // for lines not logged through pino
  return msg
}

async function flush () {
  await new Promise(resolve => setTimeout(resolve, 500))
}
