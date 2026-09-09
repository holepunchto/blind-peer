const BucketRateLimiter = require('bucket-rate-limit')

class PerKeyRateLimit {
  constructor(capacity, intervalMs) {
    this.capacity = capacity
    this.intervalMs = intervalMs
    this.destroyed = false

    this.limiters = new Map()
    this.refillMs = capacity * intervalMs
    this._gcTimer = setInterval(this._gc.bind(this), this.refillMs)
    this._gcTimer.unref()
  }

  tryAcquire(key) {
    let entry = this.limiters.get(key)
    if (!entry) {
      entry = {
        limiter: new BucketRateLimiter(this.capacity, this.intervalMs),
        lastUsed: 0
      }
      this.limiters.set(key, entry)
    }

    entry.lastUsed = Date.now()
    return entry.limiter.tryAcquire()
  }

  _gc() {
    const cutoff = Date.now() - this.refillMs

    for (const [key, entry] of this.limiters) {
      if (entry.lastUsed <= cutoff) {
        entry.limiter.destroy()
        this.limiters.delete(key)
      }
    }
  }

  destroy() {
    if (this.destroyed) return
    this.destroyed = true

    clearInterval(this._gcTimer)
    for (const entry of this.limiters.values()) entry.limiter.destroy()
    this.limiters.clear()
  }
}

module.exports = PerKeyRateLimit
