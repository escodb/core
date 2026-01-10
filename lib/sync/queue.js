'use strict'

const { withResolvers } = require('./promise')

class Queue {
  constructor ({ limit = 0 } = {}) {
    this._items = []
    this._limit = limit
    this._pending = 0
    this._reset()
    this._resolve()
  }

  _reset () {
    let { promise, resolve } = withResolvers()
    this._promise = promise
    this._resolve = resolve
  }

  onEmpty () {
    return this._promise
  }

  push (f) {
    let fn = async () => f()
    let { promise, resolve, reject } = withResolvers()

    this._items.push({ fn, resolve, reject })
    this._drain()

    return promise
  }

  _drain (extra = 0) {
    while (this._items.length > 0) {
      if (this._limit > 0 && this._pending >= this._limit + extra) break
      this._exec(this._items.shift())
    }
  }

  async _exec ({ fn, resolve, reject }) {
    if (this._pending === 0) this._reset()
    this._pending += 1

    try {
      resolve(await fn())
    } catch (error) {
      reject(error)
    } finally {
      this._drain(1)
      this._pending -= 1
      if (this._pending === 0) this._resolve()
    }
  }
}

module.exports = Queue
