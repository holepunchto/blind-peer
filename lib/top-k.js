const ReadyResource = require('ready-resource')

/**
 * Tracks the most frequent keys within a rolling time window.
 */
class TopKWindow extends ReadyResource {
  /**
   * @param {number} [bucketCount=6]
   * @param {number} [bucketTime=10_000]
   * @param {number} [k=5]
   * @param {number | null} [spikeThreshold=null]
   * @param {number} [spikeAlertStep=10]
   */
  constructor(
    bucketCount = 6,
    bucketTime = 10_000,
    k = 5,
    spikeThreshold = null,
    spikeAlertStep = 10
  ) {
    super()

    this.bucketCount = bucketCount
    this.bucketTime = bucketTime
    this.k = k
    this.spikeThreshold = spikeThreshold
    this.spikeAlertStep = spikeAlertStep

    /** @type {Map<string, number>[]} */
    this._buckets = Array.from({ length: bucketCount }, () => new Map())
    this._index = 0
    this.topK = []
    this._timer = null
  }

  /**
   * @param {string} key
   */
  hit(key) {
    const bucket = this._buckets[this._index]
    bucket.set(key, (bucket.get(key) || 0) + 1)

    let count = 0
    for (const bucket of this._buckets) {
      count += bucket.get(key) || 0
    }

    if (
      this.spikeAlertStep !== null &&
      count >= this.spikeThreshold &&
      count % this.spikeAlertStep === 0
    ) {
      this.emit('spike', key, count)
    }
  }

  /**
   * @returns {number}
   */
  topKSum() {
    let sum = 0
    for (const { count } of this.topK) {
      sum += count
    }
    return sum
  }

  async _open() {
    this._timer = setInterval(this._rotate.bind(this), this.bucketTime)
  }

  async _close() {
    if (this._timer !== null) {
      clearInterval(this._timer)
      this._timer = null
    }

    this.topK = []
    for (const bucket of this._buckets) {
      bucket.clear()
    }
  }

  _rotate() {
    // compute the top-k and cache
    const totals = new Map()
    for (const bucket of this._buckets) {
      for (const [key, count] of bucket) {
        totals.set(key, (totals.get(key) || 0) + count)
      }
    }

    this.topK = [...totals.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, this.k)
      .map(([key, count]) => ({ key, count }))

    // rotate current bucket and clear the old bucket
    this._index = (this._index + 1) % this.bucketCount
    this._buckets[this._index].clear()
  }
}

module.exports = TopKWindow
