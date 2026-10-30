const { Version } = require('bare-semver')

const OTHER = 'other'
const LABEL_NAMES = ['client_name', 'client_version']

module.exports = class StatsTracker {
  constructor({ metricsOpts = null } = {}) {
    this._stats = {
      bytesGcd: 0,
      coresAdded: 0,
      activations: 0,
      activatedReplications: 0,
      wakeups: 0,
      addCoresRx: 0,
      notificationsRx: 0,
      notificationsSent: 0,
      notificationErrors: 0,
      muxerPaired: 0,
      muxerErrors: 0,
      coreTrackersCreated: 0,
      coreTrackersDestroyed: 0,
      coreResetDownload: 0,
      referrerRateLimited: 0,
      gc: {
        prio0Gcd: 0,
        prio1Gcd: 0,
        prio2Gcd: 0,
        firstTimeCoresGcd: 0,
        coresGcd: 0
      }
    }

    this._labeled = metricsOpts !== null
    this._allowedClientNames = new Set(metricsOpts?.allowedClientNames || [])
    this._maxUniqueClientVersions = metricsOpts?.maxUniqueClientVersions ?? 5
    this._seenVersions = new Map()
    this._labeledStats = {
      addCoresRx: new Map(),
      activations: new Map(),
      notificationsRx: new Map(),
      notificationErrors: new Map()
    }
  }

  incCoresAdded() {
    this._stats.coresAdded++
  }

  incActivatedReplications() {
    this._stats.activatedReplications++
  }

  incWakeups() {
    this._stats.wakeups++
  }

  incNotificationsSent() {
    this._stats.notificationsSent++
  }

  incMuxerPaired() {
    this._stats.muxerPaired++
  }

  incMuxerErrors() {
    this._stats.muxerErrors++
  }

  incCoreTrackersCreated() {
    this._stats.coreTrackersCreated++
  }

  incCoreTrackersDestroyed() {
    this._stats.coreTrackersDestroyed++
  }

  incCoreResetDownload() {
    this._stats.coreResetDownload++
  }

  incReferrerRateLimited() {
    this._stats.referrerRateLimited++
  }

  incAddCoresRx(stream) {
    this._incLabeled('addCoresRx', stream)
  }

  incActivations(stream) {
    this._incLabeled('activations', stream)
  }

  incNotificationsRx(stream) {
    this._incLabeled('notificationsRx', stream)
  }

  incNotificationErrors(stream) {
    this._incLabeled('notificationErrors', stream)
  }

  addGcStats({ bytesCleared, coresGcd, newCoresGcd, prio0Gcd, prio1Gcd, prio2Gcd }) {
    this._stats.bytesGcd += bytesCleared
    this._stats.gc.coresGcd += coresGcd
    this._stats.gc.firstTimeCoresGcd += newCoresGcd
    this._stats.gc.prio0Gcd += prio0Gcd
    this._stats.gc.prio1Gcd += prio1Gcd
    this._stats.gc.prio2Gcd += prio2Gcd
  }

  registerMetrics(promClient) {
    const self = this

    new promClient.Gauge({
      name: 'blind_peer_cores_added',
      help: 'The total amount of add-core RPC requests that have been processed',
      collect() {
        this.set(self._stats.coresAdded)
      }
    })
    new promClient.Gauge({
      name: 'blind_peer_bytes_gcd',
      help: 'The total amount of bytes garbage collected since the process started',
      collect() {
        this.set(self._stats.bytesGcd)
      }
    })
    new promClient.Gauge({
      name: 'blind_peer_gc_prio_0',
      help: 'Number of times a core of priority 0 got garbage collected',
      collect() {
        this.set(self._stats.gc.prio0Gcd)
      }
    })
    new promClient.Gauge({
      name: 'blind_peer_gc_prio_1',
      help: 'Number of times a core of priority 1 got garbage collected',
      collect() {
        this.set(self._stats.gc.prio1Gcd)
      }
    })
    new promClient.Gauge({
      name: 'blind_peer_gc_prio_2',
      help: 'Number of times a core of priority 2 got garbage collected',
      collect() {
        this.set(self._stats.gc.prio2Gcd)
      }
    })
    new promClient.Gauge({
      name: 'blind_peer_gc_cores_total',
      help: 'Number of times a core got garbage collected',
      collect() {
        this.set(self._stats.gc.coresGcd)
      }
    })
    new promClient.Gauge({
      name: 'blind_peer_gc_cores_first_time_total',
      help: 'Number of cores that got garbage collected for the first time',
      collect() {
        this.set(self._stats.gc.firstTimeCoresGcd)
      }
    })
    new promClient.Gauge({
      name: 'blind_peer_replication_sessions_opened',
      help: 'The total amount of hypercore replication sessions ever opened',
      collect() {
        this.set(self._stats.activatedReplications)
      }
    })
    new promClient.Gauge({
      name: 'blind_peer_wakeups',
      help: 'The total amount of hypercore wakeups since the process started',
      collect() {
        this.set(self._stats.wakeups)
      }
    })
    new promClient.Gauge({
      name: 'blind_peer_muxer_errors',
      help: 'The amount of errors on the protomux muxer',
      collect() {
        this.set(self._stats.muxerErrors)
      }
    })
    new promClient.Gauge({
      name: 'blind_peer_muxer_paired',
      help: 'The amount of blind-peer-muxer sessions paired',
      collect() {
        this.set(self._stats.muxerPaired)
      }
    })
    new promClient.Gauge({
      name: 'blind_peer_referrer_rate_limited',
      help: 'The number of requests rejected by the per-referrer rate limit',
      collect() {
        this.set(self._stats.referrerRateLimited)
      }
    })
    new promClient.Gauge({
      name: 'blind_peer_push_notifications_sent',
      help: 'Number of push notification sent to a push gateway',
      collect() {
        this.set(self._stats.notificationsSent)
      }
    })
    new promClient.Gauge({
      name: 'blind_peer_core_trackers_created',
      help: 'How many core trackers were created',
      collect() {
        this.set(self._stats.coreTrackersCreated)
      }
    })
    new promClient.Gauge({
      name: 'blind_peer_core_trackers_destroyed',
      help: 'How many core trackers were destroyed',
      collect() {
        this.set(self._stats.coreTrackersDestroyed)
      }
    })
    new promClient.Gauge({
      name: 'blind_peer_core_reset_download',
      help: 'How many core trigger reset download',
      collect() {
        this.set(self._stats.coreResetDownload)
      }
    })

    const labelNames = this._labeled ? LABEL_NAMES : []
    const setLabels = (gauge, clients) => {
      for (const [name, versions] of clients) {
        for (const [version, count] of versions) {
          gauge.set({ client_name: name, client_version: version }, count)
        }
      }
    }

    new promClient.Gauge({
      name: 'blind_peer_add_cores_rx',
      help: 'The amount of add-cores requests received',
      labelNames,
      collect() {
        if (!self._labeled) return this.set(self._stats.addCoresRx)
        setLabels(this, self._labeledStats.addCoresRx)
      }
    })
    new promClient.Gauge({
      name: 'blind_peer_core_activations',
      help: 'The total amount of hypercore activations since the process started',
      labelNames,
      collect() {
        if (!self._labeled) return this.set(self._stats.activations)
        setLabels(this, self._labeledStats.activations)
      }
    })
    new promClient.Gauge({
      name: 'blind_peer_push_notifications_rx',
      help: 'Number of push notification requests received',
      labelNames,
      collect() {
        if (!self._labeled) return this.set(self._stats.notificationsRx)
        setLabels(this, self._labeledStats.notificationsRx)
      }
    })
    new promClient.Gauge({
      name: 'blind_peer_push_notifications_errors',
      help: 'Number of errors when forwarding a push notification to a push gateway',
      labelNames,
      collect() {
        if (!self._labeled) return this.set(self._stats.notificationErrors)
        setLabels(this, self._labeledStats.notificationErrors)
      }
    })
  }

  _incLabeled(statName, stream) {
    this._stats[statName]++
    if (!this._labeled) return

    const { name, version } = this._resolveClient(stream)
    const clients = this._labeledStats[statName]
    let versions = clients.get(name)
    if (!versions) {
      versions = new Map()
      clients.set(name, versions)
    }
    versions.set(version, (versions.get(version) || 0) + 1)
  }

  _resolveClient(stream) {
    const handshake = stream.userData?.getLastChannel({ protocol: 'blind-peer' })?.handshake
    const name = handshake?.clientName
    if (!name || !this._allowedClientNames.has(name)) return { name: OTHER, version: OTHER }

    return { name, version: this._resolveVersion(name, handshake.clientVersion) }
  }

  _resolveVersion(name, version) {
    if (typeof version !== 'string') return OTHER

    let normalized
    try {
      const { major, minor } = Version.parse(version)
      normalized = `${major}.${minor}`
    } catch {
      return OTHER
    }

    let versions = this._seenVersions.get(name)
    if (!versions) {
      versions = new Set()
      this._seenVersions.set(name, versions)
    }

    if (versions.has(normalized)) return normalized
    if (versions.size >= this._maxUniqueClientVersions) return OTHER

    versions.add(normalized)
    return normalized
  }
}
