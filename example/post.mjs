import BlindPeerClient from 'blind-peer-client'
import Hyperswarm from 'hyperswarm'
import IdEnc from 'hypercore-id-encoding'

const publicKey = IdEnc.decode(process.argv[2])
const autobase = IdEnc.decode(process.argv[3])
const rawMessage = process.argv[4]
const message = Buffer.from(
  JSON.stringify({ mailbox: true, message: rawMessage })
)

const s = new Hyperswarm()

s.on('connection', async c => {
  if (!c.remotePublicKey.equals(publicKey)) return

  const client = new BlindPeerClient(s)
  const reply = await client.postToMailbox(publicKey, { id: autobase, message })

  console.log(reply)

  s.destroy()
})

s.joinPeer(publicKey)
