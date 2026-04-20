const test = require('brittle')
const Corestore = require('corestore')
const Hyperswarm = require('hyperswarm')
const IpBanList = require('ip-ban-list')
const { once } = require('events')
const tmpDir = require('test-tmp')

const { NETNS_IPS, addCoresWithIp, setUpNetwork, setupBlindPeer } = require('./helper')

test('banned IPs are rejected while non-banned IPs complete add-cores', async (t) => {
  t.plan(4)

  const { bootstrap } = await setUpNetwork(t)
  const expectedAllowedIps = new Set(['10.200.1.2', '10.200.1.4'])
  const expectedBannedIps = new Set(['10.200.1.3', '10.200.1.5'])

  const ipBanList1 = await setupBanList(t, bootstrap)
  const ipBanList2 = await setupBanList(t, bootstrap)

  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    ipBanListKeys: [ipBanList1.key, ipBanList2.key]
  })
  await blindPeer.ready()

  await ipBanList1.ban('10.200.1.3')
  await ipBanList2.ban('10.200.1.5')
  // wait for all ban list to be updated
  await Promise.all(blindPeer.ipBanLists.map((banList) => once(banList.bee, 'update')))

  blindPeer.on('add-cores-done', (stream) => {
    const ip = stream.rawStream.remoteHost
    t.is(expectedAllowedIps.delete(ip), true, `${ip} was allowed and completed add-cores`)
  })
  blindPeer.on('connection-banned', (stream) => {
    const ip = stream.rawStream.remoteHost
    t.is(expectedBannedIps.delete(ip), true, `${ip} was banned`)
  })

  await Promise.all(NETNS_IPS.map((ip) => addCoresWithIp(ip, bootstrap, blindPeer.publicKey)))
})

test('ban-ip-list simple flow', async (t) => {
  const { bootstrap } = await setUpNetwork(t)
  const ipBanList = await setupBanList(t, bootstrap)
  const { blindPeer } = await setupBlindPeer(t, bootstrap, {
    ipBanListKeys: [ipBanList.key]
  })
  await blindPeer.ready()

  const ip = '10.200.1.2'

  const initiallyAllowedEventPromise = once(blindPeer, 'add-cores-done')
  await addCoresWithIp(ip, bootstrap, blindPeer.publicKey)
  const [initiallyAllowedStream] = await initiallyAllowedEventPromise
  t.is(initiallyAllowedStream.rawStream.remoteHost, ip, 'ip is allowed before ban list has entries')

  await ipBanList.ban(ip)
  await once(blindPeer.ipBanLists[0].bee, 'update')
  const bannedEventPromise = once(blindPeer, 'connection-banned')
  await addCoresWithIp(ip, bootstrap, blindPeer.publicKey)
  const [bannedStream] = await bannedEventPromise
  t.is(bannedStream.rawStream.remoteHost, ip, 'ip is blocked while banned')

  await ipBanList.unban(ip)
  await once(blindPeer.ipBanLists[0].bee, 'update')
  const allowedEventPromise = once(blindPeer, 'add-cores-done')
  await addCoresWithIp(ip, bootstrap, blindPeer.publicKey)
  const [allowedStream] = await allowedEventPromise
  t.is(allowedStream.rawStream.remoteHost, ip, 'ip is allowed after unban')
})

async function setupBanList(t, bootstrap) {
  const banListStore = new Corestore(await tmpDir(t))
  t.teardown(() => banListStore.close())

  const ipBanList = new IpBanList(banListStore)
  t.teardown(() => ipBanList.close())
  await ipBanList.ready()

  const banListSwarm = new Hyperswarm({ bootstrap })
  t.teardown(() => banListSwarm.destroy())
  banListSwarm.on('connection', (conn) => banListStore.replicate(conn))
  banListSwarm.join(ipBanList.discoveryKey)
  await banListSwarm.flush()

  return ipBanList
}
