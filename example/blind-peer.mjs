import BlindPeer from '../index.js'

const m = new BlindPeer('/tmp/blind-peer')
await m.listen()

console.log(m.publicKey.toString('hex'))
