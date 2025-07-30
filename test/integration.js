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

async function main () {
  const testnet = await setupTestnet()
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
  const { rpcKey } = await setupAutobaseDiscovery(allowedPubKey, txtBootstrap, logger.child({ service: 'autodiscovery' }))
  await setupBlindPeer(autodiscoverySeed, rpcKey, txtBootstrap, logger.child({ service: 'blind peer' }))
}

async function setupBlindPeer (autodiscoverySeed, autodiscoveryRpcKey, bootstrap, logger) {
  autodiscoverySeed = b4a.toString(autodiscoverySeed, 'hex')
  const storage = await tmpDir()
  const exProc = spawn(process.execPath, [path.join(path.dirname(__dirname), 'bin.js'), '--bootstrap', bootstrap, '--storage', storage, '--autodiscovery-seed', autodiscoverySeed, '--autodiscovery-rpc-key', autodiscoveryRpcKey])

  process.on('exit', () => { exProc.kill('SIGKILL') })

  exProc.stderr.on('data', d => {
    logger.error(d.toString())
  })
  const stdoutDec = new NewlineDecoder('utf-8')
  exProc.stdout.on('data', async d => {
    for (const line of stdoutDec.push(d)) {
      logger.info(parseLine(line))
    }
  })
}

async function setupAutobaseDiscovery (allowedPubKey, bootstrap, logger) {
  const storage = await tmpDir()
  const exProc = spawn('npx', ['autodiscovery', 'run', allowedPubKey, '--bootstrap', bootstrap, '--storage', storage])

  // To avoid zombie processes in case there's an error
  process.on('exit', () => {
    // TODO: unset this handler on clean run
    exProc.kill('SIGKILL')
  })

  const { resolve, reject, promise } = rrp()
  exProc.stderr.on('data', d => {
    logger.error(d.toString())
    reject('unexpected autodiscovery error')
  })
  const stdoutDec = new NewlineDecoder('utf-8')
  exProc.stdout.on('data', async d => {
    for (const line of stdoutDec.push(d)) {
      logger.info(parseLine(line))
      if (line.includes('RPC server public key:')) {
        resolve(line.split('RPC server public key:')[1].split('"}')[0].trim())
      }
    }
  })

  const rpcKey = await promise
  await new Promise(resolve => setTimeout(resolve, 1000)) // to flush announce
  console.log('RPCPRPC', rpcKey)
  return { rpcKey }
}

function parseLine (line) {
  let msg = line
  try {
    msg = JSON.parse(line)?.msg
  } catch {} // for lines not logged through pino
  return msg
}

main()
