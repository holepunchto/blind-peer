class BlindPeerError extends Error {
  constructor (msg, code, fn = BlindPeerError) {
    super(`${code}: ${msg}`)
    this.code = code

    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, fn)
    }
  }

  get name () {
    return 'BlindPeerError'
  }

  static MAILBOX_NOT_FOUND () {
    return new BlindPeerError('Mailbox not found', 'MAILBOX_NOT_FOUND', BlindPeerError.MAILBOX_NOT_FOUND)
  }
}

module.exports = BlindPeerError
