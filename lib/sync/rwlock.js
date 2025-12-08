'use strict'

const Queue = require('./queue')

class RWLock {
  constructor () {
    this._inbox = new Queue({ limit: 1 })
    this._read  = new Queue({ limit: 0 })
    this._write = new Queue({ limit: 1 })
  }

  read (fn) {
    return this._exec(fn, this._read, this._write)
  }

  write (fn) {
    return this._exec(fn, this._write, this._read)
  }

  async _exec (fn, runner, blocker) {
    let { promise } = await this._inbox.push(async () => {
      await blocker.onEmpty()
      return { promise: runner.push(fn) }
    })
    return promise
  }
}

module.exports = RWLock
