'use strict'

const binaries = require('./format/binaries')
const { CounterError } = require('./errors')

class Counters {
  static parse (state, ids) {
    let counters = new Counters()
    let values = binaries.loadArray('u64', state)

    for (let [i, id] of ids.entries()) {
      counters.init(id, values[i])
    }

    return counters
  }

  constructor () {
    this._ids = []
    this._inits = new Map()
    this._values = new Map()
  }

  serialize () {
    let values = this._ids.map((id) => this._values.get(id))
    return binaries.dumpArray('u64', values)
  }

  init (id, ctr = 0n) {
    if (this._values.has(id)) {
      throw new CounterError(`counter "${id}" is already initialised`)
    } else {
      this._ids.push(id)
      this._inits.set(id, BigInt(ctr))
      this._values.set(id, BigInt(ctr))
    }
  }

  incr (id, value = 1n) {
    if (this._values.has(id)) {
      let ctr = this._values.get(id)
      this._values.set(id, ctr + BigInt(value))
    } else {
      throw new CounterError(`cannot increment unknown counter "${id}"`)
    }
  }

  get (id) {
    return this._values.get(id) || 0n
  }

  merge (other) {
    for (let id of other._ids) {
      if (!this._values.has(id)) continue

      let init = other._inits.get(id)
      if (init === 0n) continue

      let value = other._values.get(id)
      let diff = value - init

      let ctr = this._values.get(id)
      this._values.set(id, ctr + diff)
    }
  }

  commit () {
    for (let [id, value] of this._values) {
      this._inits.set(id, value)
    }
  }
}

module.exports = Counters
