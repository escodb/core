'use strict'

const Cache = require('./cache')
const Executor = require('./executor')
const { Path, PathError } = require('./path')

const RETRY_LIMIT = 5
const RETRY_DELAY_INCREMENT = 10

const RETRY_ERROR_CODES = [
  'ERR_CONFLICT',
  'ERR_SCHEDULE'
]

class Task {
  constructor (adapter, router, cipher, verifier) {
    this._cache = new Cache(adapter, cipher, verifier)
    this._executor = new Executor(this._cache)
    this._router = router
    this._unlinks = new Map()
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

    return this._retryOnConflict(() => {
      let links = path.links().map(([dir, name]) => {
        let key = dirKeys.get(dir)
        return this._executor.add(key, [], (shard) => shard.link(dir, name))
      })

      let linkIds = links.map((link) => link.id)
      let put = this._executor.add(pathKey, linkIds, (shard) => shard.put(path.full(), fn))

      return [...links, put]
    })
  }

  async remove (pathStr) {
    let path = this._parsePath(pathStr, 'isDoc')
    let pathKey = await this._getShardId(path.full())
    let dirKeys = await this._getDirKeys(path)

    return this._retryOnConflict(async () => {
      let dirStates = await this._getDirStates(path, dirKeys)

      let rm = this._executor.add(pathKey, [], (shard) => shard.rm(path.full()))
      let ops = [rm]

      for (let [dir, name] of path.links().reverse()) {
        let lastOp = ops[ops.length - 1]
        let item = Path.parse(dir).join(name)

        let unlink = this._planUnlink(dirKeys, dirStates, lastOp, item)
        if (!unlink) break

        ops.push(unlink)
        this._storeUnlink(item, unlink)
      }

      return ops
    })
  }

  _planUnlink (dirKeys, dirStates, lastOp, item) {
    let dir = item.dirname()
    let name = item.basename()
    let key = dirKeys.get(dir)

    if (item.isDoc()) {
      return this._executor.add(key, [lastOp.id], (shard) => shard.unlink(dir, name))
    }

    let children = dirStates.get(item.full()) || []
    let unlinks = children.map((name) => this._unlinks.get(item.join(name).full()))

    if (unlinks.some((u) => !u)) return null

    let unlinkIds = [lastOp, ...unlinks].map((op) => op.id)

    return this._executor.add(key, unlinkIds, async (shard) => {
      let list = await this._listRouted(item.full(), dirKeys)
      if (list === null) await shard.unlink(dir, name)
    })
  }

  async _getDirInfo (path, fn) {
    let infos = path.dirs().map(async (dir) => [dir, await fn(dir)])
    return new Map(await Promise.all(infos))
  }

  _getDirKeys (path) {
    return this._getDirInfo(path, (dir) => this._getShardId(dir))
  }

  _getDirStates (path, dirKeys) {
    return this._getDirInfo(path, (dir) => this._listRouted(dir, dirKeys))
  }

  async _listRouted (dir, dirKeys) {
    let key = dirKeys.get(dir)
    let shard = await this._cache.read(key)
    return shard.list(dir, { shared: true })
  }

  _storeUnlink (item, unlink) {
    this._unlinks.set(item.full(), unlink)

    unlink.promise.finally(() => {
      let stored = this._unlinks.get(item.full())

      if (stored.id === unlink.id) {
        this._unlinks.delete(item.full())
      }
    })
  }

  async prune (pathStr) {
    let path = this._parsePath(pathStr, 'isDir')
    let docs = []

    for await (let doc of this.find(path)) {
      docs.push(path.join(doc))
    }

    let rms = docs.map((doc) => this.remove(doc))
    return Promise.all(rms)
  }

  _parsePath (pathStr, type) {
    let path = Path.parse(pathStr)

    if (!path.isValid() || !path[type]()) {
      throw new PathError(`'${pathStr}' is not a valid path`)
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
      await Promise.all(ops.map((op) => op.promise))

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
