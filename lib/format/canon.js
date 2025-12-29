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
      let bkey = toBuffer(key)
      this._push('u64', bkey.length)
      this._push('bytes', bkey)

      let bval = toBuffer(val)
      this._push('u64', bval.length)
      this._push('bytes', bval)
    }

    return binaries.dump(this._patterns, this._values)
  }
}

function toEntries (context) {
  let entries = (context instanceof Map) ? [...context] : Object.entries(context)

  return entries.sort(([a], [b]) => {
    return (a < b) ? -1 : (a > b) ? 1 : 0
  })
}

function toBuffer (value) {
  if (value instanceof Buffer) return value

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
