const path = require('path')

const test = require('brittle')
const Hyperswarm = require('hyperswarm')
const promClient = require('bare-prom-client')
const tmpDir = require('test-tmp')
const { once } = require('events')
const IdEnc = require('hypercore-id-encoding')

const BlindPeer = require('..')
const { execFileOnNetns, setUpNetwork } = require('./helper')

const BRIDGE_HOST = '10.200.1.1'
const NAMESPACES = ['test-net-1', 'test-net-2', 'test-net-3', 'test-net-4']
const REQUEST_COUNTS = [1, 2, 3, 4]

test('Prometheus top-k metrics reflect add-cores traffic by remote IP across namespaces', async (t) => {
  const { bootstrap } = await setUpNetwork(t, 10, { host: BRIDGE_HOST })
  const topK = { bucketCount: 6, bucketTime: 500, k: 3 }
  const { blindPeer } = await setupBlindPeer(t, bootstrap, { topK })
  await blindPeer.swarm.flush()
  blindPeer.registerMetrics(promClient)
  t.teardown(() => {
    promClient.register.clear()
  })

  const totalRequests = REQUEST_COUNTS.reduce((sum, count) => sum + count, 0)
  const top3Requests = REQUEST_COUNTS.slice(1).reduce((sum, count) => sum + count, 0)

  await once(blindPeer.topKByIp, 'rotated')

  const addCoreRequests = NAMESPACES.map((netns, i) => {
    return execFileOnNetns(netns, path.join(__dirname, 'make-add-cores.js'), [
      JSON.stringify(bootstrap),
      IdEnc.encode(blindPeer.publicKey),
      REQUEST_COUNTS[i].toString(),
      `netns-top-k-core-${i}`
    ])
  })

  await Promise.all(addCoreRequests)
  await once(blindPeer.topKByIp, 'rotated')

  const metrics = await promClient.register.metrics()
  const getMetricValue = (name) => {
    return parseInt(metrics.split(`\n${name} `)[1].split('\n')[0])
  }

  t.is(getMetricValue('blind_peer_add_cores_rx'), totalRequests, 'tracked add-cores requests')
  t.is(blindPeer.topKByIp.spikeThreshold, null, 'remote IP top-k does not emit spike alerts')
  t.alike(
    blindPeer.topKByIp.topK.map(({ count }) => count),
    [4, 3, 2],
    'cached IP top-k counts are the highest three namespace request volumes'
  )
  t.is(blindPeer.topKByIp.topKSum(), top3Requests, 'top-k IP sum reflects the top 3 namespaces')
  t.is(
    getMetricValue('blind_peer_add_cores_top5_by_remote_ip'),
    top3Requests,
    'Prometheus IP top-k stat reflects the top 3 namespaces'
  )
})

async function setupBlindPeer(t, bootstrap, { storage, topK } = {}) {
  if (!storage) storage = await tmpDir(t)

  const swarm = new Hyperswarm({ bootstrap })
  const blindPeer = new BlindPeer(storage, {
    swarm,
    wakeupGcTickTime: 100,
    topK
  })

  t.teardown(async () => {
    await blindPeer.close()
    await swarm.destroy()
  })

  await blindPeer.listen()

  return { blindPeer, storage }
}
