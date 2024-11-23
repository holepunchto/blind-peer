import BlindPeerClient from '../client.js'
import Hyperswarm from 'hyperswarm'

const publicKey = Buffer.from(process.argv[2], 'hex')
const autobase = Buffer.from(process.argv[3], 'hex')
const message = process.argv[4]

const s = new Hyperswarm()

s.on('connection', async c => {
  if (!c.remotePublicKey.equals(publicKey)) return

  const peer = new BlindPeerClient(c)
  const reply = await peer.post({ autobase, message })

  console.log(reply)

  s.destroy()
})

s.joinPeer(publicKey)
