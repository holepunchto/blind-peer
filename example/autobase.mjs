import BlindPeerClient from '@holepunchto/blind-peer-client'
import Autobase from 'autobase'
import c from 'compact-encoding'
import Corestore from 'corestore'
import Hyperswarm from 'hyperswarm'
import debounce from 'debounceify'
import IdEnc from 'hypercore-id-encoding'
import HypCrypto from 'hypercore-crypto'
import b4a from 'b4a'

const base = new Autobase(new Corestore('/tmp/my-corestore'), {
  encryptionKey: Buffer.alloc(30).fill('secret'),
  open (store) {
    return store.get('view', { valueEncoding: c.json })
  },
  async apply (nodes, view, base) {
    for (const node of nodes) {
      const jsonValue = JSON.parse(node.value.toString())
      if (jsonValue.add) await base.addWriter(Buffer.from(jsonValue.key, 'hex'), { indexer: false })
      view.append(jsonValue)
    }
  }
})

await base.ready()
console.log('Autobase:', base.key.toString('hex'))

base.view.on('append', debounce(async function () {
  base.ack() // ack for good messure
  console.log('someone appended to the autobase!')
  for (let i = 0; i < base.view.length; i++) {
    console.log(i, await base.view.get(i))
  }
}))

// TODO: record in autobase
const publicKey = IdEnc.decode(process.argv[2])

const s = new Hyperswarm(publicKey, { keyPair: await base.store.createKeyPair('tmp') })
const client = new BlindPeerClient(s)

s.on('connection', async c => {
  base.store.replicate(c)
  c.on('error', (e) => { console.debug(e) })
})

const id = HypCrypto.randomBytes(32)
const info = await client.addMailbox(publicKey, { id, autobase: base.key })
if (info.open === false) {
  const message = b4a.from(
    JSON.stringify({ add: true, key: info.writer.toString('hex') })
  )
  await base.append(message)
  await base.update()

  const core = base.store.get({ key: info.writer, active: false })
  await core.setEncryptionKey(base.encryptionKey)
  const req = { id, autobase: base.key, blockEncryptionKey: core.encryption.blockKey }
  await core.close()

  await client.addMailbox(publicKey, req)
}

s.joinPeer(publicKey)
