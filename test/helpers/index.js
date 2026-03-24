const b4a = require('b4a')
const Autobee = require('autobee')

async function loadAutobee(store, key = null, { isIndexer = false } = {}) {
  const apply = async (nodes, view, host) => {
    for (const node of nodes) {
      const data = JSON.parse(b4a.toString(node.value))

      if (data.addWriter) {
        host.addWriter(data.addWriter, { isIndexer })
      }

      const w = view.write()
      w.tryPut(b4a.from('value'), node.value)
      await w.flush()
    }
  }

  const auto = new Autobee(store.namespace('auto'), key, {
    apply
  })
  await auto.ready()

  return { auto }
}

module.exports = { loadAutobee }
