const fs = require('fs/promises')
const os = require('os')
const path = require('path')

const Corestore = require('corestore')
const goodbye = require('graceful-goodbye')
const Hyperswarm = require('hyperswarm')
const BlindPeerMuxer = require('blind-peer-muxer')
const IdEnc = require('hypercore-id-encoding')

async function main() {
  const bootstrap = JSON.parse(process.argv[2])
  const blindPeerPublicKey = IdEnc.decode(process.argv[3])
  const requestCount = Number(process.argv[4])

  const storage = await fs.mkdtemp(path.join(os.tmpdir(), 'blind-peer-netns-'))
  goodbye(() => fs.rm(storage, { recursive: true, force: true }), 0)

  const swarm = new Hyperswarm({ bootstrap })
  goodbye(() => swarm.destroy(), -1)
  const store = new Corestore(storage)
  goodbye(() => store.close(), -1)

  try {
    const core = store.get({ name: 'test' })
    await core.ready()

    const stream = swarm.dht.connect(blindPeerPublicKey)
    store.replicate(stream)
    goodbye(() => stream.destroy(), -2)

    const muxer = new BlindPeerMuxer(stream)
    goodbye(() => muxer.close(), -3)
    await muxer.channel.fullyOpened()

    for (let i = 0; i < requestCount; i++) {
      await muxer.addCores({
        referrer: core.key,
        priority: 0,
        announce: false,
        cores: [{ key: core.key, length: core.length }]
      })
    }

    // addCores is fire-and-forget, so keep the worker alive briefly before teardown.
    await new Promise((resolve) => setTimeout(resolve, 200))

    process.exit(0)
  } catch (error) {
    console.error(
      JSON.stringify({
        message: error.message,
        code: error.code,
        cause: error.cause ? { message: error.cause.message, code: error.cause.code } : undefined
      })
    )

    process.exit(1)
  }
}

main()
