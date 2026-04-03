'use strict'

const { ContextError } = require('../errors')
const canon = require('../format/canon')

class Context {
  static create (prefix = null, values = {}) {
    return new Context(prefix, new Map()).add(values)
  }

  constructor (prefix, values) {
    this._prefix = prefix
    this._values = values
  }

  prefix (prefix = null) {
    return new Context(prefix, this._values)
  }

  add (values) {
    let copy = new Map(this._values)

    for (let [key, value] of Object.entries(values)) {
      if (this._prefix) key = this._prefix + '.' + key

      if (copy.has(key)) {
        throw new ContextError(`context key conflict: '${key}'`)
      } else {
        copy.set(key, value)
      }
    }

    return new Context(this._prefix, copy)
  }

  toObject () {
    return Object.fromEntries(this._values)
  }

  toBuffer () {
    return canon.encode(this._values)
  }
}

module.exports = Context
