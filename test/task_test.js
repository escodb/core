'use strict'

const AesGcmCipher = require('../lib/ciphers/aes_gcm')
const Cache = require('../lib/cache')
const Router = require('../lib/router')
const Task = require('../lib/task')
const Verifier = require('../lib/verifier')

const { assert } = require('chai')
const { testWithAdapters } = require('./adapters/utils')
const { assertOneOf, generate } = require('./utils')

testWithAdapters('Task', (impl) => {
  let router, adapter, env, executors, writer, checker

  function newTask () {
    let task = new Task(adapter, router, env)
    executors.push(task._executor)
    return task
  }

  async function find (path) {
    let docs = []
    for await (let doc of checker.find(path)) {
      docs.push(doc)
    }
    return docs
  }

  beforeEach(async () => {
    let cipher = await generate(AesGcmCipher)
    let verifier = await generate(Verifier)
    env = { cipher, verifier }

    router = await generate(Router, { n: 4 })
    adapter = await impl.createAdapter()

    executors = []
    writer = newTask()
    checker = newTask()
  })

  afterEach(async () => {
    await Promise.all(executors.map((ex) => ex.onIdle()))
    await impl.cleanup()
  })

  it('throws an error for getting an invalid path', async () => {
    let error = await checker.get('x').catch(e => e)
    assert.equal(error.code, 'ERR_INVALID_PATH')
  })

  it('throws an error for getting a non-doc path', async () => {
    let error = await checker.get('/x/').catch(e => e)
    assert.equal(error.code, 'ERR_INVALID_PATH')
  })

  it('throws an error for listing an invalid path', async () => {
    let error = await checker.list('x').catch(e => e)
    assert.equal(error.code, 'ERR_INVALID_PATH')
  })

  it('throws an error for listing a non-dir path', async () => {
    let error = await checker.list('/x').catch(e => e)
    assert.equal(error.code, 'ERR_INVALID_PATH')
  })

  it('returns null for a missing document', async () => {
    let doc = await checker.get('/doc')
    assert.isNull(doc)
  })

  it('returns null for an empty directory', async () => {
    let dir = await checker.list('/')
    assert.isNull(dir)
  })

  describe('update()', () => {
    it('throws an error for a non-doc path', async () => {
      let error = await writer.update('/path/', () => {}).catch(e => e)
      assert.equal(error.code, 'ERR_INVALID_PATH')
    })

    it('exposes an error when writing a shard', async () => {
      adapter.write = () => Promise.reject(new Error('oh no'))

      let error = await writer.update('/doc', () => ({ a: 1 })).catch(e => e)
      assert.equal(error.message, 'oh no')

      assert.isNull(await checker.list('/'))
      assert.isNull(await checker.get('/doc'))
    })

    it('creates a document', async () => {
      await writer.update('/doc', () => ({ a: 1 }))

      assert.deepEqual(await checker.list('/'), ['doc'])
      assert.deepEqual(await checker.get('/doc'), { a: 1 })
    })

    it('creates a document in a nested directory', async () => {
      await writer.update('/path/to/doc', () => ({ a: 1 }))

      assert.deepEqual(await checker.list('/'), ['path/'])
      assert.deepEqual(await checker.list('/path/'), ['to/'])
      assert.deepEqual(await checker.list('/path/to/'), ['doc'])
      assert.deepEqual(await checker.get('/path/to/doc'), { a: 1 })
    })

    it('creates two documents with common ancestors', async () => {
      await Promise.all([
        writer.update('/path/to/doc', () => ({ a: 1 })),
        writer.update('/path/of/val', () => ({ b: 2 }))
      ])

      assert.deepEqual(await checker.list('/'), ['path/'])
      assert.deepEqual(await checker.list('/path/'), ['of/', 'to/'])
      assert.deepEqual(await checker.list('/path/to/'), ['doc'])

      assert.deepEqual(await checker.get('/path/to/doc'), { a: 1 })
      assert.deepEqual(await checker.get('/path/of/val'), { b: 2 })
    })

    it('exposes an error from the update() fn', async () => {
      let result = writer.update('/doc', () => { throw new Error('oh no') })
      let error = await result.catch(e => e)
      assert.equal(error.message, 'oh no')
    })

    it('does not corrupt shards if the update() fn throws', async () => {
      let updates = []

      for (let i = 0; i < 100; i++) {
        let update = writer.update(`/doc-${i}`, () => { throw new Error('oh no') })
        updates.push(update)
      }

      await Promise.all(updates).catch(e => e)
      await writer.update('/sentinel', () => ({ a: 1 }))

      let doc = await checker.get('/sentinel')
      assert.deepEqual(doc, { a: 1 })

      let list = await checker.list('/')
      assert.include(list, 'sentinel')
    })

    it('updates a document', async () => {
      await writer.update('/doc', () => ({ a: 1 }))
      await writer.update('/doc', (doc) => ({ ...doc, b: 2 }))

      let doc = await checker.get('/doc')
      assert.deepEqual(doc, { a: 1, b: 2 })
    })

    it('returns the updated state of the document', async () => {
      await writer.update('/doc', () => ({ a: 1 }))
      let doc = await writer.update('/doc', (doc) => ({ ...doc, b: 2 }))

      assert.deepEqual(doc, { a: 1, b: 2 })
    })

    it('yields a different copy of the doc to each updater', async () => {
      let doc_b, doc_c

      await writer.update('/doc', () => ({ a: 1 }))

      let results = await Promise.all([
        writer.update('/doc', (doc) => {
          doc_b = doc
          doc.b = 2
          return doc
        }),
        writer.update('/doc', (doc) => {
          doc_c = doc
          doc.c = 3
          return doc
        }),
      ])

      assert(doc_b !== doc_c)

      assert.deepEqual(results, [{ a: 1, b: 2 }, { a: 1, b: 2, c: 3 }])

      let doc = await checker.get('/doc')
      assert.deepEqual(doc, { a: 1, b: 2, c: 3 })
    })
  })

  describe('find()', () => {
    beforeEach(async () => {
      await Promise.all([
        writer.update('/a', () => ({ a: 1 })),
        writer.update('/path/b', () => ({ b: 2 })),
        writer.update('/path/c', () => ({ c: 3 })),
        writer.update('/path/to/nested/d', () => ({ d: 4 }))
      ])
    })

    it('returns the paths of all the docs', async () => {
      assert.deepEqual(await find('/'), [
        'a',
        'path/b',
        'path/c',
        'path/to/nested/d'
      ])
    })

    it('returns the docs inside a specific directory', async () => {
      assert.deepEqual(await find('/path/'), [
        'b',
        'c',
        'to/nested/d'
      ])

      assert.deepEqual(await find('/path/to/'), [
        'nested/d'
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
      await Promise.all([
        writer.update('/path/to/x', () => ({ a: 1 })),
        writer.update('/path/to/y', () => ({ b: 2 })),
        writer.update('/path/nested/to/z', () => ({ c: 3 }))
      ])
    })

    it('throws an error for a non-doc path', async () => {
      let error = await writer.remove('/path/').catch(e => e)
      assert.equal(error.code, 'ERR_INVALID_PATH')
    })

    it('removes a document', async () => {
      await writer.remove('/path/to/x')

      let doc = await checker.get('/path/to/x')
      assert.isNull(doc)
    })

    it('removes a document from its parent directory', async () => {
      await writer.remove('/path/to/x')

      let dir = await checker.list('/path/to/')
      assert.deepEqual(dir, ['y'])
    })

    it('leaves non-empty parent directories in place', async () => {
      await writer.remove('/path/to/x')

      let dir = await checker.list('/path/')
      assert.deepEqual(dir, ['nested/', 'to/'])
    })

    it('removes empty parent directories', async () => {
      await writer.remove('/path/nested/to/z')

      assert.isNull(await checker.list('/path/nested/to/'))
      assert.isNull(await checker.list('/path/nested/'))
      assert.deepEqual(await checker.list('/path/'), ['to/'])
      assert.deepEqual(await checker.list('/'), ['path/'])
    })

    it('does not remove a directory if a non-existent item is removed', async () => {
      await writer.remove('/path/to/nested/a')

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
        writer.remove('/path/to/x'),
        writer.remove('/path/to/y')
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
        writer.remove('/path/to/x'),
        writer.remove('/path/to/y'),
        writer.remove('/path/nested/to/z')
      ])

      for (let dir of ['/', '/path/', '/path/nested/', '/path/to/']) {
        assert.isNull(await checker.list(dir))
      }
    })

    it('only removes directories that are genuinely empty', async () => {
      await Promise.all([
        writer.remove('/path/to/x'),
        writer.update('/path/to/a', () => ({ a: 1 })),
        writer.remove('/path/to/y'),
        writer.remove('/path/nested/to/z')
      ])

      assert.deepEqual(await checker.list('/'), ['path/'])
      assert.deepEqual(await checker.list('/path/'), ['to/'])
      assert.deepEqual(await checker.list('/path/to/'), ['a'])

      assert.isNull(await checker.list('/path/nested/'))
    })

    it('links a new doc while a non-existent doc is removed', async () => {
      await Promise.all([
        writer.remove('/new/a'),
        writer.update('/new/b', () => ({ b: 2 }))
      ])

      assert.deepEqual(await checker.list('/'), ['new/', 'path/'])
      assert.deepEqual(await checker.list('/new/'), ['b'])
    })
  })

  describe('remove() after partial failure', () => {
    beforeEach(async () => {
      let cache = new Cache(adapter, env)

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
        writer.update('/a', () => ({ a: 1 })),
        writer.update('/path/b', () => ({ b: 2 })),
        writer.update('/path/c', () => ({ c: 3 })),
        writer.update('/path/to/nested/d', () => ({ d: 4 }))
      ])
    })

    it('removes all docs', async () => {
      await writer.prune('/')
      assert.deepEqual(await find('/'), [])
    })

    it('removes the docs from a directory', async () => {
      await writer.prune('/path/')
      assert.deepEqual(await find('/'), ['a'])
    })

    it('removes the docs from a nested directory', async () => {
      await writer.prune('/path/to/')
      assert.deepEqual(await find('/'), ['a', 'path/b', 'path/c'])
    })

    it('removes a parent directory if left empty', async () => {
      await writer.prune('/path/to/nested/')
      assert.deepEqual(await find('/'), ['a', 'path/b', 'path/c'])
      assert.deepEqual(await checker.list('/path/'), ['b', 'c'])
    })

    it('throws an error for a non-dir path', async () => {
      let error = await writer.prune('/path').catch(e => e)
      assert.equal(error.code, 'ERR_INVALID_PATH')
    })
  })

  describe('concurrent operations', () => {
    describe('update()', () => {
      it('applies concurrent updates from the same task', async () => {
        await Promise.all([
          writer.update('/doc', (doc) => ({ ...doc, a: 1 })),
          writer.update('/doc', (doc) => ({ ...doc, b: 2 })),
          writer.update('/doc', (doc) => ({ ...doc, c: 3 }))
        ])

        let doc = await checker.get('/doc')
        assert.deepEqual(doc, { a: 1, b: 2, c: 3 })
      })

      it('applies concurrent updates from different tasks', async () => {
        await Promise.all([
          newTask().update('/doc', (doc) => ({ ...doc, a: 1 })),
          newTask().update('/doc', (doc) => ({ ...doc, b: 2 })),
          newTask().update('/doc', (doc) => ({ ...doc, c: 3 }))
        ])

        let doc = await checker.get('/doc')
        assert.deepEqual(doc, { a: 1, b: 2, c: 3 })
      })

      it('applies each change exactly once', async () => {
        await writer.update('/doc', () => ({ n: [] }))
        let counter = 0

        function update (k) {
          return newTask().update('/doc', (doc) => {
            counter += 1
            doc.n.push(k)
            return doc
          })
        }

        await Promise.all([update(1), update(2), update(4)])
        assert.isAbove(counter, 3)

        let doc = await checker.get('/doc')
        assert.deepEqual(doc.n.sort(), [1, 2, 4])
      })
    })

    describe('update() and remove()', () => {
      beforeEach(async () => {
        await Promise.all([
          writer.update('/path/to/x', () => ({ a: 1 })),
          writer.update('/path/to/y', () => ({ b: 2 })),
          writer.update('/path/nested/to/z', () => ({ c: 3 }))
        ])
      })

      it('serializes concurrent requests', async () => {
        await newTask().update('/path/nested/to/z', () => ({ ok: true }))

        await Promise.all([
          newTask().update('/path/nested/to/z', (doc) => ({ ...doc, z: 0 })),
          newTask().remove('/path/nested/to/z')
        ])

        let doc = await checker.get('/path/nested/to/z')

        await assertOneOf({
          'doc is updated': async () => {
            assert.deepEqual(doc, { z: 0 })
            assert.deepEqual(await checker.list('/path/nested/to/'), ['z'])
            assert.deepEqual(await checker.list('/path/nested/'), ['to/'])
            assert.deepEqual(await checker.list('/path/'), ['nested/', 'to/'])
          },
          'doc is removed': async () => {
            assert.isNull(doc)
            assert.isNull(await checker.list('/path/nested/to/'))
            assert.isNull(await checker.list('/path/nested/'))
            assert.deepEqual(await checker.list('/path/'), ['to/'])
          }
        })
      })

      it('allows a new document being created in the same directory', async () => {
        await newTask().prune('/path/nested/')
        await newTask().update('/path/nested/to/z', () => ({ z: 0 }))

        await Promise.all([
          newTask().remove('/path/nested/to/z'),
          newTask().update('/path/nested/to/y', () => ({ y: 0 }))
        ])

        let doc = await checker.get('/path/nested/to/y')

        assert.deepEqual(doc, { y: 0 })
        assert.deepEqual(await checker.list('/path/nested/to/'), ['y'])
        assert.deepEqual(await checker.list('/path/nested/'), ['to/'])
        assert.deepEqual(await checker.list('/path/'), ['nested/', 'to/'])
      })
    })

    describe('update() and prune()', () => {
      beforeEach(async () => {
        let ns = [1, 2, 3, 4]

        let updates = ns.flatMap((x) => {
          return ns.flatMap((y) => {
            return ns.map((z) => writer.update(`/a${x}/b${y}/c${z}`, () => ({ x, y, z })))
          })
        })
        await Promise.all(updates)
      })

      it('handles a new doc being created in the pruned directory', async () => {
        await Promise.all([
          newTask().prune('/'),
          newTask().update('/a2/b3/new', () => ({ ok: true }))
        ])

        let docs = await find('/')

        await assertOneOf({
          'directory is empty': () => {
            assert.deepEqual(docs, [])
          },
          'directory contains only the new doc': () => {
            assert.deepEqual(docs, ['a2/b3/new'])
          }
        })
      })
    })
  })
})
