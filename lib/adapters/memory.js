'use strict'

const { ConflictError } = require('../errors')

class MemoryAdapter {
  constructor () {
    this._shards = new Map()
  }

  async read (id) {
    let record = this._shards.get(id)

    if (record) {
      return { value: record.value, rev: record.rev }
    } else {
      return null
    }
  }

  async write (id, value, rev = null) {
    let record = this._shards.get(id)
    let expect = record ? record.rev : null

    if (rev !== expect) throw new ConflictError()

    rev = (rev || 0) + 1
    this._shards.set(id, { value, rev })

    return { rev }
  }
}

module.exports = MemoryAdapter
