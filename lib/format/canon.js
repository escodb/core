'use strict'

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

    let keys = Object.keys(context).sort()
    this._push('u64', 2 * keys.length)

    for (let key of keys) {
      let bkey = toBuffer(key)
      this._push('u64', bkey.length)
      this._push('bytes', bkey)

      let bval = toBuffer(context[key])
      this._push('u64', bval.length)
      this._push('bytes', bval)
    }

    return binaries.dump(this._patterns, this._values)
  }
}

function toBuffer (value) {
  if (value instanceof Buffer) return value

  if (typeof value === 'string') {
    return Buffer.from(value, 'utf8')
  }
  if (typeof value === 'number') {
    return Buffer.from(String(value), 'utf8')
  }

  throw new Error('unsupported context value type')
}

function encode (context) {
  return new Encoder().encode(context)
}

module.exports = { encode }
