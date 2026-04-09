'use strict'

const MemoryAdapter = require('../../lib/adapters/memory')
const WebStorageAdapter = require('../../lib/adapters/web_storage')

module.exports = {
  memory: {
    createAdapter () {
      return MemoryAdapter.create()
    },

    cleanup () {}
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
