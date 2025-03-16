'use strict'

const binaries = require('./format/binaries')

class Counters {
  static async parse (state, ids, verifier) {
    state = await verifier.parse(state)

    let counters = new Counters(verifier)

    let pattern = new Array(ids.length).fill('u64')
    let values = binaries.load(pattern, state)

    for (let [i, id] of ids.entries()) {
      counters.init(id, values[i])
    }

    return counters
  }

  constructor (verifier, ids = []) {
    this._verifier = verifier
    this._ids = ids
    this._inits = new Map()
    this._values = new Map()
  }

  async serialize () {
    let values = this._ids.map((id) => this._values.get(id))
    let pattern = new Array(values.length).fill('u64')
    let buf = binaries.dump(pattern, values)

    return this._verifier.sign(buf)
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

class CounterError extends Error {
  constructor (message) {
    super(message)
    this.code = 'ERR_COUNTER'
    this.name = 'CounterError'
  }
}

module.exports = Counters
