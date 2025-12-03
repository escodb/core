'use strict'

const Queue = require('./queue')

class Mutex {
  constructor () {
    this._queue = new Queue({ limit: 1 })
  }

  lock (fn) {
    return this._queue.push(fn)
  }
}

module.exports = Mutex
