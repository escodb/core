'use strict'

const fs = require('fs').promises
const { resolve } = require('path')

const web_adapters = require('./web_adapters')
const FileAdapter = require('../../lib/adapters/file')

const TMP_PATH = resolve(__dirname, '..', '..', 'tmp')

module.exports = {
  memory: web_adapters.memory,

  file: {
    createAdapter (name) {
      let storePath = resolve(TMP_PATH, `test-${name}`)
      return new FileAdapter({ path: storePath, fsync: false })
    },

    async cleanup (name) {
      let storePath = resolve(TMP_PATH, `test-${name}`)
      let fn = fs.rm ? 'rm' : 'rmdir'
      await fs[fn](storePath, { recursive: true }).catch(e => e)
    }
  }
}
