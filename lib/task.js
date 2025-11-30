'use strict'

const Cache = require('./cache')
const Executor = require('./executor')
const Path = require('./path')

const RETRY_LIMIT = 5
const RETRY_DELAY_INCREMENT = 10

const RETRY_ERROR_CODES = [
  'ERR_CONFLICT',
  'ERR_SCHEDULE'
]

class Task {
  constructor (adapter, router, env) {
    this._cache = new Cache(adapter, env)
    this._executor = new Executor(this._cache)
    this._router = router
    this._marked = new Set()
  }

  async list (pathStr) {
    let path = this._parsePath(pathStr, 'isDir')
    let shard = await this._loadShard(path)
    return shard.list(path.full())
  }

  async get (pathStr) {
    let path = this._parsePath(pathStr, 'isDoc')
    let shard = await this._loadShard(path)
    return shard.get(path.full())
  }

  async * find (pathStr, root = null) {
    let path = this._parsePath(pathStr, 'isDir')
    root = root || path

    let dir = await this.list(path)
    if (dir === null) return

    let items = dir.map((name) => path.join(name))
    let subdirs = items.filter((item) => item.isDir())

    await Promise.all(subdirs.map((dir) => this._loadShard(dir)))

    for (let item of items) {
      if (item.isDir()) {
        for await (let doc of this.find(item, root)) {
          yield doc
        }
      } else if (item.isDoc()) {
        yield item.relative(root)
      }
    }
  }

  async _loadShard (path) {
    let key = await this._getShardId(path.full())
    return this._cache.read(key)
  }

  async update (pathStr, fn) {
    let path = this._parsePath(pathStr, 'isDoc')
    let pathKey = await this._getShardId(path.full())
    let dirKeys = await this._getDirKeys(path)

    let results = await this._retryOnConflict(() => {
      let links = path.links().map(([dir, name]) => {
        let key = dirKeys.get(dir)
        return this._executor.add(key, [], (shard) => shard.link(dir, name))
      })

      let put = this._executor.add(pathKey, links, (shard) => shard.put(path.full(), fn))

      return [...links, put]
    })

    return results.pop()
  }

  async remove (pathStr) {
    let path = this._parsePath(pathStr, 'isDoc')
    await this._doRemove(path)
  }

  async prune (pathStr) {
    let path = this._parsePath(pathStr, 'isDir')
    await this._doRemove(path)
  }

  async _doRemove (path) {
    await this._retryOnConflict(async () => {
      let dirStates = await this._getDirStates(path)

      let dir = Path.parse(this._lastEmptyParent(path, dirStates))
      return [await this._rmtree(dir)]
    })
  }

  _lastEmptyParent (path, dirStates) {
    let last = path.full()
    this._marked.add(last)

    for (let [dir, name] of path.links().reverse()) {
      let items = dirStates.get(dir) || []
      items = items.filter((item) => !this._marked.has(dir + item))

      if (items.length > 0) return last

      last = dir
      this._marked.add(last)
    }

    return '/'
  }

  async _rmtree (path) {
    let deps = null
    let key = await this._getShardId(path.full())

    if (path.isDoc()) {
      deps = [this._executor.add(key, [], (shard) => shard.rm(path.full()))]
    } else {
      let items = await this.list(path) || []
      deps = await Promise.all(items.map((item) => this._rmtree(path.join(item))))
    }

    if (path.full() === '/') return { promise: Promise.all(deps.map((d) => d.promise)) }

    let [[dir, name]] = path.links().reverse()
    let dirKey = await this._getShardId(dir)

    let unlink = this._executor.add(dirKey, deps, async (shard) => {
      await shard.unlink(dir, name)
    })

    unlink.promise.finally(() => this._marked.delete(dir + name))

    return unlink
  }

  async _getDirInfo (path, fn) {
    let infos = path.dirs().map(async (dir) => [dir, await fn(dir)])
    return new Map(await Promise.all(infos))
  }

  _getDirKeys (path) {
    return this._getDirInfo(path, (dir) => this._getShardId(dir))
  }

  _getDirStates (path) {
    return this._getDirInfo(path, (dir) => this.list(dir))
  }

  _parsePath (pathStr, type) {
    let path = Path.parse(pathStr)

    if (!path.isValid() || !path[type]()) {
      throw new Path.PathError(`'${pathStr}' is not a valid path`)
    }
    return path
  }

  _getShardId (pathStr) {
    return this._router.getShardId(pathStr)
  }

  async _retryOnConflict (planner, n = 1) {
    try {
      let ops = await planner()
      queueMicrotask(() => this._executor.poll())
      return await Promise.all(ops.map((op) => op.promise))

    } catch (error) {
      if (RETRY_ERROR_CODES.includes(error.code)) {
        if (n % RETRY_LIMIT === 0) {
          await sleep(Math.random() * RETRY_DELAY_INCREMENT * n / RETRY_LIMIT)
        }
        return this._retryOnConflict(planner, n + 1)
      } else {
        throw error
      }
    }
  }
}

function sleep (ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

module.exports = Task
