'use strict'

const adapters = require('.')

function testWithAdapters (name, tests) {
  for (let [type, impl] of Object.entries(adapters)) {
    describe(`${name} (adapter: ${type})`, () => {
      tests({
        createAdapter: () => impl.createAdapter(name),
        cleanup: () => impl.cleanup(name)
      })
    })
  }
}

module.exports = {
  testWithAdapters
}
