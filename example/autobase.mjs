import BlindPeerClient from '../client.js'
import Autobase from 'autobase'
import c from 'compact-encoding'
import Corestore from 'corestore'
import Hyperswarm from 'hyperswarm'
import debounce from 'debounceify'

const base = new Autobase(new Corestore('/tmp/my-corestore'), {
  encryptionKey: Buffer.alloc(30).fill('secret'),
  valueEncoding: c.json,
  open (store) {
    return store.get('view', { valueEncoding: c.json })
  },
  async apply (nodes, view, base) {
    for (const node of nodes) {
      if (node.value.add) await base.addWriter(Buffer.from(node.value.key, 'hex'), { indexer: false })
      view.append(node.value)
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
const publicKey = Buffer.from(process.argv[2], 'hex')

const s = new Hyperswarm({ keyPair: await base.store.createKeyPair('tmp') })

s.on('connection', async c => {
  base.store.replicate(c)

  if (!c.remotePublicKey.equals(publicKey)) return

  const peer = new BlindPeerClient(c)

  const info = await peer.addMailbox({ autobase: base.key })

  if (info.open === false) {
    await base.append({ add: true, key: info.writer.toString('hex') })
    await base.update()

    const core = base.store.get({ key: info.writer, active: false })
    await core.setEncryptionKey(base.encryptionKey)
    const req = { autobase: base.key, blockEncryptionKey: core.encryption.blockKey }
    await core.close()

    await peer.addMailbox(req)

    console.log('opened mailbox...')
  }
})

s.joinPeer(publicKey)
