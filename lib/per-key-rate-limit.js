const ReadyResource = require('ready-resource')

module.exports = class PerKeyRateLimit extends ReadyResource {
  constructor(capacity, intervalMs) {
    super()
    this.capacity = capacity
    this.intervalMs = intervalMs

    this.tokens = new Map()
    this._refillTimer = null
  }

  _open() {
    this._refillTimer = setInterval(this._refill.bind(this), this.intervalMs)
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
        // tokens is full, delete the key, this help to avoid memory leak
        this.tokens.delete(key)
      } else {
        this.tokens.set(key, tokens + 1)
      }
    }
  }

  _close() {
    if (this._refillTimer) {
      clearInterval(this._refillTimer)
      this._refillTimer = null
    }

    this.tokens.clear()
  }
}
