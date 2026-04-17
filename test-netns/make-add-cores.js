const fs = require('fs/promises')
const os = require('os')
const path = require('path')

const Corestore = require('corestore')
const Hyperswarm = require('hyperswarm')
const BlindPeerMuxer = require('blind-peer-muxer')
const IdEnc = require('hypercore-id-encoding')

async function main() {
  const bootstrap = JSON.parse(process.argv[2])
  const blindPeerPublicKey = IdEnc.decode(process.argv[3])
  const requestCount = Number(process.argv[4])
  const coreName = process.argv[5]

  const storage = await fs.mkdtemp(path.join(os.tmpdir(), 'blind-peer-netns-'))
  const swarm = new Hyperswarm({ bootstrap })
  const store = new Corestore(storage)

  let stream = null
  let muxer = null

  try {
    const core = store.get({ name: coreName })
    await core.ready()

    stream = swarm.dht.connect(blindPeerPublicKey)
    store.replicate(stream)

    muxer = new BlindPeerMuxer(stream)
    await muxer.channel.fullyOpened()

    for (let i = 0; i < requestCount; i++) {
      await muxer.addCores({
        referrer: core.key,
        priority: 0,
        announce: false,
        cores: [{ key: core.key, length: core.length }]
      })
    }
  } catch (error) {
    console.error(
      JSON.stringify({
        message: error.message,
        code: error.code,
        cause: error.cause ? { message: error.cause.message, code: error.cause.code } : undefined
      })
    )
    process.exitCode = 1
  } finally {
    if (muxer) muxer.close()
    if (stream) stream.destroy()
    await store.close().catch(noop)
    await swarm.destroy().catch(noop)
    await fs.rm(storage, { recursive: true, force: true }).catch(noop)
  }
}

function noop() {}

main()
