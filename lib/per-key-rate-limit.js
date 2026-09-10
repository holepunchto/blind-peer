class PerKeyRateLimit {
  constructor(capacity, intervalMs) {
    this.capacity = capacity
    this.intervalMs = intervalMs
    this.destroyed = false

    this.tokens = new Map()
    this._refillTimer = setInterval(this._refill.bind(this), intervalMs)
    this._refillTimer.unref()
  }

  tryAcquire(key) {
    if (this.destroyed) return false

    const tokens = this.tokens.get(key) ?? this.capacity
    if (tokens === 0) return false

    this.tokens.set(key, tokens - 1)
    return true
  }

  _refill() {
    for (const [key, tokens] of this.tokens) {
      if (tokens + 1 >= this.capacity) {
        this.tokens.delete(key)
      } else {
        this.tokens.set(key, tokens + 1)
      }
    }
  }

  destroy() {
    if (this.destroyed) return
    this.destroyed = true

    clearInterval(this._refillTimer)
    this._refillTimer = null
    this.tokens.clear()
  }
}

module.exports = PerKeyRateLimit
