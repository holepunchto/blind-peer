const { spawn } = require('child_process')
const path = require('path')

const test = require('brittle')
const Hyperswarm = require('hyperswarm')
const setupTestnet = require('hyperdht/testnet')
const promClient = require('bare-prom-client')
const tmpDir = require('test-tmp')
const { once } = require('events')
const IdEnc = require('hypercore-id-encoding')

const BlindPeer = require('..')

const BRIDGE_HOST = '10.200.1.1'
const NAMESPACES = ['test-net-1', 'test-net-2', 'test-net-3', 'test-net-4']

test('Prometheus top-k metrics reflect add-cores traffic by remote IP across namespaces', async (t) => {
  const { bootstrap } = await setUpNetwork(t, 10, { host: BRIDGE_HOST })
  const topK = { bucketCount: 6, bucketTime: 500, k: 3 }
  const { blindPeer } = await setupBlindPeer(t, bootstrap, { topK })
  await blindPeer.swarm.flush()
  blindPeer.registerMetrics(promClient)
  t.teardown(() => {
    promClient.register.clear()
  })

  // we create 4 peers, with the 1st one sending 1 request, 2nd one sending 2 requests...
  const nrPeers = NAMESPACES.length
  // with that the total requests are 1+2+3+4 or (4*5)/2
  const totalRequests = (nrPeers * (nrPeers + 1)) / 2
  // with k=3 and requests [1,2,3,4], top-3 sum is 2+3+4 or totalRequests - 1
  const top3Requests = totalRequests - 1

  // wait for top-k to rotate before scheduling addCores,
  // to prevent requests from being split across rotate cycles
  await once(blindPeer.topKByIp, 'rotated')

  const allPromises = []
  for (let i = 0; i < nrPeers; i++) {
    allPromises.push(
      execFileOnNetns(NAMESPACES[i], path.join(__dirname, 'make-add-cores.js'), [
        JSON.stringify(bootstrap),
        IdEnc.encode(blindPeer.publicKey),
        (i + 1).toString()
      ])
    )
  }

  // wait for all add cores to finish and the top-k to rotate
  allPromises.push(once(blindPeer.topKByIp, 'rotated'))

  await Promise.all(allPromises)

  const metrics = await promClient.register.metrics()
  const getMetricValue = (name) => {
    return parseInt(metrics.split(`\n${name} `)[1].split('\n')[0])
  }

  t.is(getMetricValue('blind_peer_add_cores_rx'), totalRequests, 'tracked add-cores requests')
  t.is(blindPeer.topKByIp.spikeThreshold, null, 'remote IP top-k does not emit spike alerts')
  t.alike(
    blindPeer.topKByIp.topK,
    [
      { key: '10.200.1.5', count: 4 },
      { key: '10.200.1.4', count: 3 },
      { key: '10.200.1.3', count: 2 }
    ],
    'cached IP top-k entries are the highest three namespace request volumes'
  )
  t.is(blindPeer.topKByIp.topKSum(), top3Requests, 'top-k IP sum reflects the top 3 namespaces')
  t.is(
    getMetricValue('blind_peer_add_cores_top5_by_remote_ip'),
    top3Requests,
    'Prometheus IP top-k stat reflects the top 3 namespaces'
  )
})

async function setupBlindPeer(t, bootstrap, { topK } = {}) {
  const storage = await tmpDir(t)

  const swarm = new Hyperswarm({ bootstrap })
  const blindPeer = new BlindPeer(storage, {
    swarm,
    topK
  })

  t.teardown(async () => {
    await blindPeer.close()
    await swarm.destroy()
  })

  await blindPeer.listen()

  return { blindPeer, storage }
}

async function setUpNetwork(t, size = 10, opts = {}) {
  const testnet = await setupTestnet(size, opts)
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
