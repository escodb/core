'use strict'

const fs = require('fs').promises
const path = require('path')

const FileAdapter = require('../../lib/adapters/file')
const testAdapterBehaviour = require('./behaviour')

const STORE_PATH = path.resolve(__dirname, '..', '..', 'tmp', 'file-adapter')

describe('FileAdapter', () => {
  testAdapterBehaviour({
    createAdapter () {
      return new FileAdapter({ path: STORE_PATH, fsync: false })
    },

    async cleanup () {
      let fn = fs.rm ? 'rm' : 'rmdir'
      await fs[fn](STORE_PATH, { recursive: true }).catch(e => e)
    }
  })
})
