'use strict'

const Schedule = require('./schedule')
const Queue = require('./sync/queue')
const { withResolvers } = require('./sync/promise')

class Executor {
  constructor (cache) {
    this._cache = cache
    this._schedule = new Schedule()
    this._queue = new Queue()
  }

  add (shard, deps, fn) {
    let { promise, resolve, reject } = withResolvers()
    let depIds = deps.map((dep) => dep.id)
    let id = this._schedule.add(shard, depIds, { fn, resolve, reject })

    return { id, promise }
  }

  poll () {
    while (true) {
      let group = this._schedule.nextGroup()
      if (!group) break

      this._queue.push(() => this._request(group))
    }
  }

  onIdle () {
    return this._queue.onEmpty()
  }

  async _request (group) {
    group.started()

    await this._loadShards()
    let shard = await this._cache.read(group.getShard())

    for (let op of group.values()) {
      try {
        op.result = await op.fn(shard)
      } catch (error) {
        this._groupFailed(group, error)
        return this.poll()
      }
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

module.exports = Executor
