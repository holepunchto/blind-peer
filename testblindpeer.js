const BlindPeer = require('.')
const Hyperswarm = require('hyperswarm')
const tmp = require('test-tmp')

module.exports = async function createTestBlindPeer(
  t,
  bootstrap,
  { storage, maxBytes, enableGc, trustedPubKeys, debug, order } = {}
) {
  if (!storage) storage = await tmp(t)

  const swarm = new Hyperswarm({ bootstrap })
  const peer = new BlindPeer(storage, {
    swarm,
    maxBytes,
    enableGc,
    trustedPubKeys
  })

  t.teardown(
    async () => {
      await peer.close()
      await swarm.destroy()
    },
    { order }
  )

  await peer.listen()
  if (debug) {
    peer.swarm.on('connection', () => {
      console.log('Blind peer connection opened')
    })
  }

  return { blindPeer: peer, storage }
}
