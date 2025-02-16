exports.mapByReferrer = (record) => {
  if (!record.referrer) return []
  return [{ referrer: record.referrer, updated: record.updated, core: record.key }]
}

exports.mapByAnnounce = (record) => {
  if (!record.announce) return []
  return [{ core: record.key }]
}
