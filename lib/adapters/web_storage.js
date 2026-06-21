'use strict'

const { ConflictError } = require('../errors')
const Options = require('../options')

const DEFAULT_PREFIX = 'escodb'

const StorageOptions = new Options({
  prefix: { required: false },
  storage: { required: false }
})

class WebStorageAdapter {
  static async create (options) {
    return new WebStorageAdapter(options)
  }

  constructor (options = {}) {
    options = StorageOptions.parse(options)
    this._prefix = options.prefix || DEFAULT_PREFIX
    this._storage = options.storage || localStorage
  }

  async read (id) {
    let key = `${this._prefix}:${id}`
    let rev = this._storage.getItem(`${key}:rev`)
    let value = this._storage.getItem(`${key}:value`)

    if (typeof rev === 'string') {
      rev = parseInt(rev, 10)
      return { value, rev }
    } else {
      return null
    }
  }

  async write (id, value, rev = null) {
    let key = `${this._prefix}:${id}`

    let expected = this._storage.getItem(`${key}:rev`)
    expected = (typeof expected === 'string') ? parseInt(expected, 10) : null

    if (rev !== expected) throw new ConflictError()

    rev = (rev || 0) + 1
    this._storage.setItem(`${key}:rev`, rev)
    this._storage.setItem(`${key}:value`, value)

    return { rev }
  }
}

module.exports = WebStorageAdapter
