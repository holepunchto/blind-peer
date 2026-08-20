module.exports = class BlindPeerError extends Error {
  constructor(msg, code, fn = BlindPeerError, { cause, data = null } = {}) {
    super(`${code}: ${msg}`, { cause })
    this.code = code
    this.data = data

    if (Error.captureStackTrace) Error.captureStackTrace(this, fn)
  }

  get name() {
    return 'BlindPeerError'
  }

  static UNKNOWN_CORE(msg = 'unknown core') {
    return new BlindPeerError(msg, 'UNKNOWN_CORE', BlindPeerError.UNKNOWN_CORE)
  }

  static CREATE_NOTIFICATION_ERROR(msg = 'failed to create notification', opts = {}) {
    return new BlindPeerError(
      msg,
      'CREATE_NOTIFICATION_ERROR',
      BlindPeerError.CREATE_NOTIFICATION_ERROR,
      opts
    )
  }
}
