'use strict'

function testWithAdapters (name, tests) {
  describe(`${name} (Memory)`, () => {
    const MemoryAdapter = require('../../lib/adapters/memory')

    tests({
      createAdapter () {
        return new MemoryAdapter()
      },

      cleanup () {}
    })
  })

  describe(`${name} (File)`, () => {
    const fs = require('fs').promises
    const path = require('path')
    const FileAdapter = require('../../lib/adapters/file')

    const STORE_PATH = path.resolve(__dirname, '..', '..', 'tmp', `test-${name}`)

    tests({
      createAdapter () {
        return new FileAdapter({ path: STORE_PATH, fsync: false })
      },

      async cleanup () {
        let fn = fs.rm ? 'rm' : 'rmdir'
        await fs[fn](STORE_PATH, { recursive: true }).catch(e => e)
      }
    })
  })
}

module.exports = {
  testWithAdapters
}
