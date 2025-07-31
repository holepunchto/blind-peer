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
  await setupBlindPeer(t, autodiscoverySeed, rpcKey, txtBootstrap, logger.child({ service: 'blind peer' }))

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
  await new Promise(resolve => setTimeout(resolve, 500)) // to flush announce
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
  await new Promise(resolve => setTimeout(resolve, 500)) // to flush announce
  return { rpcKey }
}

function parseLine (line) {
  let msg = line
  try {
    msg = JSON.parse(line)?.msg
  } catch {} // for lines not logged through pino
  return msg
}
