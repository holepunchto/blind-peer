const ReadyResource = require('ready-resource')
const IdEnc = require('hypercore-id-encoding')
const { ADMIN_CHANNEL_ID, AdminQueryTopKEncoding } = require('blind-peer-encodings')

/**
 * Exposes admin RPC methods over an existing blind-peer swarm.
 */
class BlindPeerAdminRpc extends ReadyResource {
  /**
   * @param {import('hyperswarm')} swarm
   * @param {import('protomux-rpc-router')} router
   * @param {Buffer[]} adminKeys
   * @param {{
   *   ip: import('./top-k.js'),
   *   referrer: import('./top-k.js'),
   *   key: import('./top-k.js')
   * }} topKs
   */
  constructor(swarm, router, adminKeys, topKs) {
    super()

    this.swarm = swarm
    this.router = router
    this.topKs = topKs
    this.adminKeys = new Set(adminKeys.map((key) => IdEnc.normalize(key)))
    this.router.use({
      onrequest: (ctx, next) => {
        if (!this.adminKeys.has(IdEnc.normalize(ctx.connection.remotePublicKey))) {
          throw new Error('Only admin peers can query top-k')
        }

        return next()
      }
    })
    this.router.method('query-top-k', AdminQueryTopKEncoding, this._onquerytopk.bind(this))
    this._onconnection = (connection) => {
      this.router.handleConnection(connection, ADMIN_CHANNEL_ID)
    }
  }

  async _open() {
    await this.router.ready()
    this.swarm.on('connection', this._onconnection)
  }

  async _close() {
    this.swarm.removeListener('connection', this._onconnection)
    await this.router.close()
  }

  _onquerytopk() {
    return {
      ip: this.topKs.ip.topK,
      referrer: this.topKs.referrer.topK,
      key: this.topKs.key.topK
    }
  }
}

module.exports = BlindPeerAdminRpc
