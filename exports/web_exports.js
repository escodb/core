'use strict'

const MemoryAdapter = require('../lib/adapters/memory')
const WebStorageAdapter = require('../lib/adapters/web_storage')
const Store = require('../lib/store')

module.exports = {
  MemoryAdapter,
  WebStorageAdapter,
  Store
}
