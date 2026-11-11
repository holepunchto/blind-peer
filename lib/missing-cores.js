// Cores that peers asked for before we had them, bounded per stream (oldest dropped first)
module.exports = class MissingCores {
  constructor(maxPerStream) {
    this.maxPerStream = maxPerStream
    this.streams = new Map() // discovery key (hex) -> streams that asked for it
    this.ids = new Map() // stream -> discovery keys (hex) it asked for, oldest first
  }

  add(stream, id) {
    let ids = this.ids.get(stream)
    if (ids === undefined) {
      ids = new Set()
      this.ids.set(stream, ids)
    }
    if (ids.has(id)) return

    if (ids.size >= this.maxPerStream) {
      const oldest = ids.values().next().value
      ids.delete(oldest)
      this._remove(oldest, stream)
    }

    ids.add(id)

    let streams = this.streams.get(id)
    if (streams === undefined) {
      streams = new Set()
      this.streams.set(id, streams)
    }
    streams.add(stream)
  }

  // returns the streams that asked for it (or null), and forgets them
  take(id) {
    const streams = this.streams.get(id)
    if (streams === undefined) return null
    this.streams.delete(id)

    for (const stream of streams) {
      const ids = this.ids.get(stream)
      ids.delete(id)
      if (ids.size === 0) this.ids.delete(stream)
    }

    return streams
  }

  delete(stream) {
    const ids = this.ids.get(stream)
    if (ids === undefined) return
    this.ids.delete(stream)

    for (const id of ids) this._remove(id, stream)
  }

  _remove(id, stream) {
    const streams = this.streams.get(id)
    streams.delete(stream)
    if (streams.size === 0) this.streams.delete(id)
  }
}
