const { spawn, spawnSync } = require('child_process')
const path = require('path')

const test = require('brittle')
const Corestore = require('corestore')
const Hyperswarm = require('hyperswarm')
const setupTestnet = require('hyperdht/testnet')
const IpBanList = require('ip-ban-list')
const tmpDir = require('test-tmp')
const IdEnc = require('hypercore-id-encoding')

const BlindPeer = require('..')

const BRIDGE_HOST = '10.200.1.1'
const NAMESPACES = ['test-net-1', 'test-net-2', 'test-net-3', 'test-net-4']
const netnsTest = supportsNetns() ? test : test.skip

netnsTest('multiple ban IP lists block any matching namespace IP and allow the rest', async (t) => {
  const { bootstrap } = await setUpNetwork(t, 10, { host: BRIDGE_HOST })
  const requestCounts = [1, 1, 3, 1]
  const expectedAllowedByHost = new Map([
    ['10.200.1.2', 1],
    ['10.200.1.4', 3]
  ])
  const expectedBannedByHost = new Map([
    ['10.200.1.3', 1],
    ['10.200.1.5', 1]
  ])

  const { keys: banIpListKeys } = await seedBanLists(t, bootstrap, [['10.200.1.3'], ['10.200.1.5']])

  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    banIpListKeys,
    banTimeout: 50
  })
  await blindPeer.swarm.flush()
  t.is(blindPeer.banIpLists.length, 2, 'opened both ban lists')

  await waitForBanListReplication(blindPeer, [
    { listIndex: 0, host: '10.200.1.3' },
    { listIndex: 1, host: '10.200.1.5' }
  ])

  const allowedByHost = new Map()
  const bannedByHost = new Map()

  blindPeer.on('add-cores-done', (stream) => {
    incrementCount(allowedByHost, stream.rawStream.remoteHost)
  })
  blindPeer.on('connection-banned', (stream) => {
    incrementCount(bannedByHost, stream.rawStream.remoteHost)
  })

  const workerResults = await Promise.allSettled(
    NAMESPACES.map((namespace, index) =>
      execFileOnNetns(namespace, path.join(__dirname, 'make-add-cores.js'), [
        JSON.stringify(bootstrap),
        IdEnc.encode(blindPeer.publicKey),
        requestCounts[index].toString()
      ])
    )
  )

  await waitFor(() => {
    return (
      sumCounts(allowedByHost) === sumCounts(expectedAllowedByHost) &&
      sumCounts(bannedByHost) === sumCounts(expectedBannedByHost)
    )
  })

  t.alike(
    countsToArray(allowedByHost),
    countsToArray(expectedAllowedByHost),
    'only non-banned namespace IPs completed add-cores requests'
  )
  t.alike(
    countsToArray(bannedByHost),
    countsToArray(expectedBannedByHost),
    'each ban list blocked its matching namespace IP'
  )
  t.is(blindPeer.stats.addCoresRx, 4, 'only non-banned add-cores requests were processed')
  t.alike(
    blindPeer.topKByIp.topK,
    [
      { key: '10.200.1.4', count: 3 },
      { key: '10.200.1.2', count: 1 }
    ],
    'remote IP accounting excludes banned namespace IPs'
  )

  t.is(workerResults[0].status, 'fulfilled', 'test-net-1 request succeeded')
  t.is(workerResults[1].status, 'rejected', 'test-net-2 request was blocked')
  t.is(getWorkerErrorMessage(workerResults[1]), 'Timed out', 'test-net-2 timed out from ban list 1')
  t.is(workerResults[2].status, 'fulfilled', 'test-net-3 request succeeded')
  t.is(workerResults[3].status, 'rejected', 'test-net-4 request was blocked')
  t.is(getWorkerErrorMessage(workerResults[3]), 'Timed out', 'test-net-4 timed out from ban list 2')
})

async function setupBlindPeer(t, bootstrap, { banIpListKeys, banTimeout } = {}) {
  const storage = await tmpDir(t)

  const swarm = new Hyperswarm({ bootstrap })
  const blindPeer = new BlindPeer(storage, {
    swarm,
    banIpListKeys,
    banTimeout
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

async function seedBanLists(t, bootstrap, bannedHostsByList) {
  const banListStore = new Corestore(await tmpDir(t))
  t.teardown(() => banListStore.close())

  const banIpLists = []
  for (const bannedHosts of bannedHostsByList) {
    const banIpList = new IpBanList(banListStore)
    banIpLists.push(banIpList)
    t.teardown(() => banIpList.close())

    await banIpList.ready()
    for (const host of bannedHosts) {
      await banIpList.ban(host)
    }
  }

  const banListSwarm = new Hyperswarm({ bootstrap })
  t.teardown(() => banListSwarm.destroy())

  banListSwarm.on('connection', (conn) => banListStore.replicate(conn))
  for (const banIpList of banIpLists) {
    banListSwarm.join(banIpList.discoveryKey)
  }
  await banListSwarm.flush()

  return {
    keys: banIpLists.map((banIpList) => banIpList.key)
  }
}

async function waitForBanListReplication(blindPeer, expectedBans) {
  await waitFor(async () => {
    const checks = await Promise.allSettled(
      expectedBans.map(({ listIndex, host }) => blindPeer.banIpLists[listIndex].isBanned(host))
    )
    return checks.every((check) => check.status === 'fulfilled' && check.value)
  })
}

async function waitFor(fn, { timeout = 10_000, interval = 50 } = {}) {
  const start = Date.now()

  while (true) {
    if (await fn()) return
    if (Date.now() - start >= timeout) {
      throw new Error('Timed out waiting for test condition')
    }
    await new Promise((resolve) => setTimeout(resolve, interval))
  }
}

function incrementCount(map, key) {
  map.set(key, (map.get(key) || 0) + 1)
}

function sumCounts(map) {
  let sum = 0
  for (const count of map.values()) sum += count
  return sum
}

function countsToArray(map) {
  return [...map.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([host, count]) => ({ host, count }))
}

function getWorkerErrorMessage(result) {
  return result.status === 'rejected' ? result.reason.message : null
}

function supportsNetns() {
  if (process.platform !== 'linux') return false

  const result = spawnSync('ip', ['netns', 'list'], { stdio: 'ignore' })
  return result.error === undefined && result.status === 0
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
