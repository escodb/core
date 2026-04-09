'use strict'

const MemoryAdapter = require('../../lib/adapters/memory')
const IndexedDBAdapter = require('../../lib/adapters/indexeddb')
const WebStorageAdapter = require('../../lib/adapters/web_storage')

module.exports = {
  memory: {
    createAdapter () {
      return MemoryAdapter.create()
    },

    cleanup () {}
  },

  indexeddb: {
    createAdapter () {
      let n = Math.floor(Math.random() * 1e12)
      let name = `escodb-${Date.now()}-${n}`
      return IndexedDBAdapter.create({ name })
    },

    async cleanup () {
      let dbs = await indexedDB.databases()

      for (let { name } of dbs) {
        indexedDB.deleteDatabase(name)
      }
    }
  },

  web_storage: {
    createAdapter () {
      return WebStorageAdapter.create({ storage: localStorage })
    },

    cleanup () {
      localStorage.clear()
    }
  }
}
