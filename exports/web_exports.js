'use strict'

const MemoryAdapter = require('../lib/adapters/memory')
const IndexedDBAdapter = require('../lib/adapters/indexeddb')
const WebStorageAdapter = require('../lib/adapters/web_storage')
const Store = require('../lib/store')

module.exports = {
  MemoryAdapter,
  IndexedDBAdapter,
  WebStorageAdapter,
  Store
}
