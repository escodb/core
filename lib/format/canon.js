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
      let bkey = Buffer.from(key, 'utf8')
      this._push('u64', bkey.length)
      this._push('bytes', bkey)

      let bval = Buffer.from(stringify(context[key]), 'utf8')
      this._push('u64', bval.length)
      this._push('bytes', bval)
    }

    return binaries.dump(this._patterns, this._values)
  }
}

function stringify (value) {
  if (typeof value === 'string') return value
  if (typeof value === 'number') return String(value)

  throw new Error('unsupported context value type')
}

function encode (context) {
  return new Encoder().encode(context)
}

module.exports = { encode }
