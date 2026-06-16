'use strict'

const { Buffer } = require('@escodb/buffer')
const binaries = require('./binaries')

class Encoder {
  _reset () {
    this._patterns = []
    this._values = []
  }

  _push (type, value) {
    this._patterns.push(type)
    this._values.push(value)
  }

  encode (context) {
    this._reset()

    let entries = toEntries(context)
    this._push('u64', 2 * entries.length)

    for (let [key, val] of entries) {
      this._push('u64', key.length)
      this._push('bytes', key)

      this._push('u64', val.length)
      this._push('bytes', val)
    }

    return binaries.dump(this._patterns, this._values)
  }
}

function toEntries (context) {
  let entries = (context instanceof Map) ? [...context] : Object.entries(context)

  return entries
      .map(([key, val]) => [toBuffer(key), toBuffer(val)])
      .sort(([a], [b]) => Buffer.compare(a, b))
}

function toBuffer (value) {
  if (Buffer.isBuffer(value)) return value

  if (typeof value === 'string') {
    return Buffer.from(value, 'utf8')
  }
  if (typeof value === 'number') {
    return binaries.dump(['u64'], [value])
  }

  throw new Error('unsupported context value type')
}

function encode (context) {
  return new Encoder().encode(context)
}

module.exports = { encode }
