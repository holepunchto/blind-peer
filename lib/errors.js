module.exports = class BlindPeerError extends Error {
  constructor(msg, code, fn = BlindPeerError, { cause } = {}) {
    super(`${code}: ${msg}`, { cause })
    this.code = code

    if (Error.captureStackTrace) Error.captureStackTrace(this, fn)
  }

  get name() {
    return 'BlindPeerError'
  }

  static UNKNOWN_CORE(msg = 'unknown core') {
    return new BlindPeerError(msg, 'UNKNOWN_CORE', BlindPeerError.UNKNOWN_CORE)
  }

  static RATE_LIMITED(msg = 'rate limited') {
    return new BlindPeerError(msg, 'RATE_LIMITED', BlindPeerError.RATE_LIMITED)
  }
}
