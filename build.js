const path = require('path')

const Hyperschema = require('hyperschema')
const HyperDB = require('hyperdb/builder')

const SCHEMA_DIR = path.join(__dirname, './spec/hyperschema')
const DB_DIR = path.join(__dirname, './spec/hyperdb')

const schema = Hyperschema.from(SCHEMA_DIR, { versioned: false })

const peerSchema = schema.namespace('blind-peer')

peerSchema.register({
  name: 'auth',
  fields: [
    {
      name: 'swarming',
      type: 'fixed32',
      required: true
    },
    {
      name: 'encryption',
      type: 'fixed32',
      required: true
    }
  ]
})

peerSchema.register({
  name: 'digest',
  fields: [
    {
      name: 'referrers',
      type: 'uint',
      required: true
    },
    {
      name: 'cores',
      type: 'uint',
      required: true
    },
    {
      name: 'bytesAllocated',
      type: 'uint',
      required: true
    },
    {
      name: 'flushed',
      type: 'bool'
    }
  ]
})

peerSchema.register({
  name: 'mailbox',
  fields: [
    {
      name: 'version',
      type: 'uint',
      required: true
    },
    {
      name: 'seed',
      type: 'fixed32',
      required: true
    },
    {
      name: 'referrer',
      type: 'fixed32',
      required: false
    },
    {
      name: 'blockEncryptionKey',
      type: 'fixed32',
      required: false
    }
  ]
})

peerSchema.register({
  name: 'add-core-request',
  flagsPosition: 0,
  fields: [
    {
      name: 'key',
      type: 'fixed32',
      required: true
    },
    {
      name: 'referrer',
      type: 'fixed32',
      required: false
    },
    {
      name: 'deprecatedAutobase',
      type: 'fixed32',
      required: false
    },
    {
      name: 'deprecatedAutobaseBlockKey',
      type: 'fixed32',
      required: false
    },
    {
      name: 'priority',
      type: 'uint',
      required: false
    },
    {
      name: 'announce',
      type: 'bool',
      required: false
    }
  ]
})

peerSchema.register({
  name: 'post-to-mailbox-request',
  fields: [
    {
      name: 'mailbox',
      type: 'buffer',
      required: true
    },
    {
      name: 'message',
      type: 'buffer',
      required: true
    }
  ]
})

peerSchema.register({
  name: 'wakeup-entry',
  compact: true,
  fields: [
    {
      name: 'key',
      type: 'fixed32',
      required: true
    },
    {
      name: 'length',
      type: 'uint',
      required: true
    }
  ]
})

peerSchema.register({
  name: 'wakeup-reply',
  fields: [
    {
      name: 'version',
      type: 'uint',
      required: true
    },
    {
      name: 'type',
      type: 'uint',
      required: true
    },
    {
      name: 'writers',
      type: '@blind-peer/wakeup-entry',
      array: true,
      required: true
    }
  ]
})

peerSchema.register({
  name: 'core',
  fields: [
    {
      name: 'key',
      type: 'fixed32',
      required: true
    },
    {
      name: 'length',
      type: 'uint',
      required: true
    },
    {
      name: 'bytesAllocated',
      type: 'uint',
      required: true
    },
    {
      name: 'updated',
      type: 'uint',
      required: true
    },
    {
      name: 'active',
      type: 'uint',
      required: true
    },
    {
      name: 'priority',
      type: 'uint',
      required: true
    },
    {
      name: 'announce',
      type: 'bool',
      required: false
    },
    {
      name: 'referrer',
      type: 'fixed32',
      required: false
    },
    {
      name: 'blocksCleared',
      type: 'uint',
      required: false
    },
    {
      name: 'bytesCleared',
      type: 'uint',
      required: false
    }

  ]
})

Hyperschema.toDisk(schema)

const db = HyperDB.from(SCHEMA_DIR, DB_DIR)
const peerDb = db.namespace('blind-peer')

peerDb.require(path.join(__dirname, './lib/db-actions.js'))

peerDb.collections.register({
  name: 'auth',
  schema: '@blind-peer/auth',
  key: []
})

peerDb.collections.register({
  name: 'digest',
  schema: '@blind-peer/digest',
  key: []
})

peerDb.collections.register({
  name: 'cores',
  schema: '@blind-peer/core',
  key: ['key']
})

peerDb.indexes.register({
  name: 'cores-by-referrer',
  collection: '@blind-peer/cores',
  key: {
    type: {
      fields: [
        {
          name: 'referrer',
          type: 'fixed32'
        },
        {
          name: 'updated',
          type: 'uint'
        },
        {
          name: 'core',
          type: 'fixed32'
        }
      ]
    },
    map: 'mapByReferrer'
  },
  unique: true
})

peerDb.indexes.register({
  name: 'cores-by-announce',
  collection: '@blind-peer/cores',
  key: {
    type: {
      fields: [
        {
          name: 'core',
          type: 'fixed32'
        }
      ]
    },
    map: 'mapByAnnounce'
  },
  unique: true
})

peerDb.indexes.register({
  name: 'cores-by-activity',
  collection: '@blind-peer/cores',
  key: ['priority', 'active'],
  unique: false
})

peerDb.indexes.register({
  name: 'cores-by-bytes-allocated',
  collection: '@blind-peer/cores',
  key: ['bytesAllocated'],
  unique: false
})

HyperDB.toDisk(db)
