'use strict'

const MemoryAdapter = require('../../lib/adapters/memory')

module.exports = {
  memory: {
    createAdapter () {
      return new MemoryAdapter()
    },

    cleanup () {}
  }
}
