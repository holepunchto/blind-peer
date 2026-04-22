const { spawn } = require('child_process')
const path = require('path')

const Hyperswarm = require('hyperswarm')
const setupTestnet = require('hyperdht/testnet')
const IdEnc = require('hypercore-id-encoding')
const tmpDir = require('test-tmp')

const BlindPeer = require('..')

const BRIDGE_HOST = '10.200.1.1'
const NETNS_BY_IP = {
  '10.200.1.2': 'test-net-1',
  '10.200.1.3': 'test-net-2',
  '10.200.1.4': 'test-net-3',
  '10.200.1.5': 'test-net-4'
}
const NETNS_IPS = Object.keys(NETNS_BY_IP)

async function setupBlindPeer(t, bootstrap, options = {}) {
  const storage = await tmpDir(t)

  const swarm = new Hyperswarm({ bootstrap })
  const blindPeer = new BlindPeer(storage, {
    swarm,
    ...options
  })

  t.teardown(async () => {
    await blindPeer.close()
    await swarm.destroy()
  })

  await blindPeer.listen()

  return { blindPeer, storage }
}

async function setUpNetwork(t, size = 10, opts = {}) {
  const testnet = await setupTestnet(size, { ...opts, host: BRIDGE_HOST })
  t.teardown(
    async () => {
      await testnet.destroy()
    },
    { order: 5000 }
  )
  return testnet
}

async function execFileOnNetns(netns, file, args = [], opts = {}) {
  return await new Promise((resolve, reject) => {
    const cp = spawn('ip', ['netns', 'exec', netns, process.execPath, file, ...args], {
      ...opts,
      stdio: ['ignore', 'pipe', 'pipe']
    })

    let stdout = ''
    let stderr = ''

    cp.stdout.on('data', (data) => {
      stdout += data.toString()
    })

    cp.stderr.on('data', (data) => {
      stderr += data.toString()
    })

    cp.on('error', reject)

    cp.on('close', (code) => {
      if (code === 0) {
        resolve(stdout)
        return
      }

      try {
        reject(JSON.parse(stderr))
      } catch {
        reject(new Error(`Command failed with code ${code}: ${stderr.trim()}`))
      }
    })
  })
}

async function addCoresWithIp(ip, bootstrap, blindPeerKey, requests = 1, opts = {}) {
  const netns = NETNS_BY_IP[ip]
  return await execFileOnNetns(
    netns,
    path.join(__dirname, 'make-add-cores.js'),
    [JSON.stringify(bootstrap), IdEnc.encode(blindPeerKey), requests.toString()],
    opts
  )
}

module.exports = {
  NETNS_IPS,
  setupBlindPeer,
  setUpNetwork,
  execFileOnNetns,
  addCoresWithIp
}
