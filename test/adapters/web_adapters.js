'use strict'

const MemoryAdapter = require('../../lib/adapters/memory')
const WebStorageAdapter = require('../../lib/adapters/web_storage')

module.exports = {
  memory: {
    createAdapter () {
      return new MemoryAdapter()
    },

    cleanup () {}
  },

  web_storage: {
    createAdapter () {
      return new WebStorageAdapter({ storage: localStorage })
    },

    cleanup () {
      localStorage.clear()
    }
  }
}
