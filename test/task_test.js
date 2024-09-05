'use strict'

const Cache = require('../lib/cache')
const FileAdapter = require('../lib/adapters/file')
const MemoryAdapter = require('../lib/adapters/memory')
const Router = require('../lib/router')
const Task = require('../lib/task')

const { assert } = require('chai')
const fs = require('fs').promises
const path = require('path')

function testTaskBehaviour (config) {
  let router, store, task, checker

  function newTask () {
    return new Task(store, router)
  }

  async function find (path) {
    let docs = []
    for await (let doc of checker.find(path)) {
      docs.push(doc)
    }
    return docs
  }

  beforeEach(async () => {
    router = new Router({ level: 2, key: await Router.generateKey() })
    store = config.createAdapter()
    task = newTask()
    checker = newTask()
  })

  afterEach(async () => {
    if (config.cleanup) await config.cleanup()
  })

  it('throws an error for getting an invalid path', async () => {
    let error = await task.get('x').catch(e => e)
    assert.equal(error.code, 'ERR_INVALID_PATH')
  })

  it('throws an error for getting a non-doc path', async () => {
    let error = await task.get('/x/').catch(e => e)
    assert.equal(error.code, 'ERR_INVALID_PATH')
  })

  it('throws an error for listing an invalid path', async () => {
    let error = await task.list('x').catch(e => e)
    assert.equal(error.code, 'ERR_INVALID_PATH')
  })

  it('throws an error for listing a non-dir path', async () => {
    let error = await task.list('/x').catch(e => e)
    assert.equal(error.code, 'ERR_INVALID_PATH')
  })

  it('returns null for a missing document', async () => {
    let doc = await task.get('/doc')
    assert.isNull(doc)
  })

  it('returns null for an empty directory', async () => {
    let dir = await task.list('/')
    assert.isNull(dir)
  })

  describe('update()', () => {
    it('throws an error for a non-doc path', async () => {
      let error = await task.update('/path/', () => {}).catch(e => e)
      assert.equal(error.code, 'ERR_INVALID_PATH')
    })

    it('exposes an error when writing a shard', async () => {
      store.write = () => Promise.reject(new Error('oh no'))

      let error = await task.update('/doc', () => ({ a: 1 })).catch(e => e)
      assert.equal(error.message, 'oh no')

      assert.isNull(await checker.list('/'))
      assert.isNull(await checker.get('/doc'))
    })

    it('creates a document', async () => {
      await task.update('/doc', () => ({ a: 1 }))

      assert.deepEqual(await checker.list('/'), ['doc'])
      assert.deepEqual(await checker.get('/doc'), { a: 1 })
    })

    it('creates a document in a nested directory', async () => {
      await task.update('/path/to/doc', () => ({ a: 1 }))

      assert.deepEqual(await checker.list('/'), ['path/'])
      assert.deepEqual(await checker.list('/path/'), ['to/'])
      assert.deepEqual(await checker.list('/path/to/'), ['doc'])
      assert.deepEqual(await checker.get('/path/to/doc'), { a: 1 })
    })

    it('creates two documents with common ancestors', async () => {
      await Promise.all([
        task.update('/path/to/doc', () => ({ a: 1 })),
        task.update('/path/of/val', () => ({ b: 2 }))
      ])

      assert.deepEqual(await checker.list('/'), ['path/'])
      assert.deepEqual(await checker.list('/path/'), ['of/', 'to/'])
      assert.deepEqual(await checker.list('/path/to/'), ['doc'])

      assert.deepEqual(await checker.get('/path/to/doc'), { a: 1 })
      assert.deepEqual(await checker.get('/path/of/val'), { b: 2 })
    })

    it('updates a document', async () => {
      await task.update('/doc', () => ({ a: 1 }))
      await task.update('/doc', (doc) => ({ ...doc, b: 2 }))

      let doc = await checker.get('/doc')
      assert.deepEqual(doc, { a: 1, b: 2 })
    })

    it('applies concurrent updates from different tasks', async () => {
      await task.update('/doc', () => ({ a: 1 }))

      await Promise.all([
        newTask().update('/doc', (doc) => ({ ...doc, b: 2 })),
        newTask().update('/doc', (doc) => ({ ...doc, c: 3 }))
      ])

      let doc = await checker.get('/doc')
      assert.deepEqual(doc, { a: 1, b: 2, c: 3 })
    })
  })

  describe('find()', () => {
    beforeEach(async () => {
      await Promise.all([
        task.update('/a', () => ({ a: 1 })),
        task.update('/path/b', () => ({ b: 2 })),
        task.update('/path/c', () => ({ c: 3 })),
        task.update('/path/to/nested/d', () => ({ d: 4 }))
      ])
    })

    it('returns the paths of all the docs', async () => {
      assert.deepEqual(await find('/'), [
        '/a',
        '/path/b',
        '/path/c',
        '/path/to/nested/d'
      ])
    })

    it('returns the docs inside a specific directory', async () => {
      assert.deepEqual(await find('/path/'), [
        '/path/b',
        '/path/c',
        '/path/to/nested/d'
      ])

      assert.deepEqual(await find('/path/to/'), [
        '/path/to/nested/d'
      ])
    })

    it('returns an empty list for a non-existent directory', async () => {
      assert.deepEqual(await find('/none/'), [])
    })

    it('throws an error for a non-dir path', async () => {
      let error = await find('/path').catch(e => e)
      assert.equal(error.code, 'ERR_INVALID_PATH')
    })
  })

  describe('remove()', () => {
    beforeEach(async () => {
      let writer = newTask()

      await Promise.all([
        writer.update('/path/to/x', () => ({ a: 1 })),
        writer.update('/path/to/y', () => ({ b: 2 })),
        writer.update('/path/nested/to/z', () => ({ c: 3 }))
      ])
    })

    it('throws an error for a non-doc path', async () => {
      let error = await task.remove('/path/').catch(e => e)
      assert.equal(error.code, 'ERR_INVALID_PATH')
    })

    it('removes a document', async () => {
      await task.remove('/path/to/x')

      let doc = await checker.get('/path/to/x')
      assert.isNull(doc)
    })

    it('removes a document from its parent directory', async () => {
      await task.remove('/path/to/x')

      let dir = await checker.list('/path/to/')
      assert.deepEqual(dir, ['y'])
    })

    it('leaves non-empty parent directories in place', async () => {
      await task.remove('/path/to/x')

      let dir = await checker.list('/path/')
      assert.deepEqual(dir, ['nested/', 'to/'])
    })

    it('removes empty parent directories', async () => {
      await task.remove('/path/nested/to/z')

      assert.isNull(await checker.list('/path/nested/to/'))
      assert.isNull(await checker.list('/path/nested/'))
      assert.deepEqual(await checker.list('/path/'), ['to/'])
      assert.deepEqual(await checker.list('/'), ['path/'])
    })

    it('does not remove a directory if a non-existent item is removed', async () => {
      await task.remove('/path/to/nested/a')

      assert.deepEqual(await checker.list('/path/nested/to/'), ['z'])
      assert.deepEqual(await checker.list('/path/nested/'), ['to/'])
    })

    it('removes a parent directory if two clients independently remove its items', async () => {
      await Promise.all([
        newTask().remove('/path/to/x'),
        newTask().remove('/path/to/y')
      ])

      assert.isNull(await checker.list('/path/to/'))
      assert.deepEqual(await checker.list('/path/'), ['nested/'])
      assert.deepEqual(await checker.list('/'), ['path/'])
    })

    it('removes a parent directory if one client removes its items', async () => {
      await Promise.all([
        task.remove('/path/to/x'),
        task.remove('/path/to/y')
      ])

      assert.isNull(await checker.list('/path/to/'))
      assert.deepEqual(await checker.list('/path/'), ['nested/'])
      assert.deepEqual(await checker.list('/'), ['path/'])
    })

    it('lets several tasks fully empty the storage', async () => {
      await Promise.all([
        newTask().remove('/path/to/x'),
        newTask().remove('/path/to/y'),
        newTask().remove('/path/nested/to/z')
      ])

      for (let dir of ['/', '/path/', '/path/nested/', '/path/to/']) {
        assert.isNull(await checker.list(dir))
      }
    })

    it('lets a single task fully empty the storage', async () => {
      await Promise.all([
        task.remove('/path/to/x'),
        task.remove('/path/to/y'),
        task.remove('/path/nested/to/z')
      ])

      for (let dir of ['/', '/path/', '/path/nested/', '/path/to/']) {
        assert.isNull(await checker.list(dir))
      }
    })

    it('only removes directories that are genuinely empty', async () => {
      await Promise.all([
        task.remove('/path/to/x'),
        task.update('/path/to/a', () => ({ a: 1 })),
        task.remove('/path/to/y'),
        task.remove('/path/nested/to/z')
      ])

      assert.deepEqual(await checker.list('/'), ['path/'])
      assert.deepEqual(await checker.list('/path/'), ['to/'])
      assert.deepEqual(await checker.list('/path/to/'), ['a'])

      assert.isNull(await checker.list('/path/nested/'))
    })

    it('links a new doc while a non-existent doc is removed', async () => {
      await Promise.all([
        task.remove('/new/a'),
        task.update('/new/b', () => ({ b: 2 }))
      ])

      assert.deepEqual(await checker.list('/'), ['new/', 'path/'])
      assert.deepEqual(await checker.list('/new/'), ['b'])
    })
  })

  describe('remove() after partial failure', () => {
    beforeEach(async () => {
      let cache = new Cache(store)

      let id = await router.getShardId('/')
      let shard = await cache.read(id)
      shard.link('/', 'path/')
      await cache.write(id)

      id = await router.getShardId('/path/')
      shard = await cache.read(id)
      shard.link('/path/', 'a/')
      shard.link('/path/', 'b/')
      await cache.write(id)
    })

    it('completes cleaning up the tree', async () => {
      await Promise.all([
        newTask().remove('/path/a/1'),
        newTask().remove('/path/b/2')
      ])

      assert.isNull(await checker.list('/'))
      assert.isNull(await checker.list('/path/'))
      assert.isNull(await checker.list('/path/a/'))
      assert.isNull(await checker.list('/path/b/'))
    })
  })

  describe('prune()', () => {
    beforeEach(async () => {
      await Promise.all([
        task.update('/a', () => ({ a: 1 })),
        task.update('/path/b', () => ({ b: 2 })),
        task.update('/path/c', () => ({ c: 3 })),
        task.update('/path/to/nested/d', () => ({ d: 4 }))
      ])
    })

    it('removes all docs', async () => {
      await task.prune('/')
      assert.deepEqual(await find('/'), [])
    })

    it('removes the docs from a directory', async () => {
      await task.prune('/path/')
      assert.deepEqual(await find('/'), ['/a'])
    })

    it('removes the docs from a nested directory', async () => {
      await task.prune('/path/to/')
      assert.deepEqual(await find('/'), ['/a', '/path/b', '/path/c'])
    })

    it('throws an error for a non-dir path', async () => {
      let error = await task.prune('/path').catch(e => e)
      assert.equal(error.code, 'ERR_INVALID_PATH')
    })
  })
}

describe('Task (Memory)', () => {
  testTaskBehaviour({
    createAdapter () {
      return new MemoryAdapter()
    }
  })
})

const STORE_PATH = path.resolve(__dirname, '..', 'tmp', 'task-file')

describe('Task (File)', () => {
  testTaskBehaviour({
    createAdapter () {
      return new FileAdapter(STORE_PATH)
    },

    async cleanup () {
      let fn = fs.rm ? 'rm' : 'rmdir'
      await fs[fn](STORE_PATH, { recursive: true }).catch(e => e)
    }
  })
})
