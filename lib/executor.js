'use strict'

const Schedule = require('./schedule')

class Executor {
  constructor (cache) {
    this._cache = cache
    this._schedule = new Schedule()
  }

  add (shard, deps, fn) {
    let { promise, resolve, reject } = deferred()
    let id = this._schedule.add(shard, deps, { fn, resolve, reject })

    return { id, promise }
  }

  async poll () {
    while (true) {
      let group = this._schedule.nextGroup()
      if (!group) return

      this._request(group)
    }
  }

  async _request (group) {
    group.started()

    await this._loadShards()
    let shard = await this._cache.read(group.getShard())

    for (let op of group.values()) {
      op.result = await op.fn(shard)
    }
    await this._writeShard(group)

    this.poll()
  }

  _loadShards () {
    let shards = this._schedule.shards()
    let reads = [...shards].map((shard) => this._cache.read(shard))

    return Promise.all(reads)
  }

  async _writeShard (group) {
    try {
      await this._cache.write(group.getShard())
      this._groupCompleted(group)
    } catch (error) {
      this._groupFailed(group, error)
    }
  }

  _groupCompleted (group) {
    for (let op of group.values()) {
      op.resolve(op.result)
    }
    group.completed()
  }

  _groupFailed (group, error) {
    for (let { reject } of group.failed()) {
      reject(error)
    }
  }
}

function deferred () {
  let resolve, reject

  let promise = new Promise((res, rej) => {
    resolve = res
    reject = rej
  })

  return { promise, resolve, reject }
}

module.exports = Executor
