'use strict'

const AesGcmCipher = require('../lib/ciphers/aes_gcm')
const Cache = require('../lib/cache')
const Executor = require('../lib/executor')
const Shard = require('../lib/shard')
const Verifier = require('../lib/verifier')

const { assert } = require('chai')
const { testWithAdapters } = require('./adapters/utils')

testWithAdapters('Executor', (impl) => {
  let store, cipher, verifier, executor, cache

  beforeEach(async () => {
    store = impl.createAdapter()
    cipher = await AesGcmCipher.generate()
    verifier = await Verifier.generate()
    executor = new Executor(new Cache(store, cipher, verifier))
    cache = new Cache(store, cipher, verifier)
  })

  afterEach(impl.cleanup)

  it('executes a single change to a shard', async () => {
    let link = executor.add('A', [], (s) => s.link('/', 'doc.txt'))
    executor.poll()
    await link.promise

    let dir = await cache.read('A').then((s) => s.list('/'))
    assert.deepEqual(dir, ['doc.txt'])
  })

  it('executes two dependent changes to the same shard', async () => {
    let x = executor.add('A', [], (s) => s.link('/x/', 'doc1'))
    let y = executor.add('A', [x.id], (s) => s.link('/y/', 'doc2'))

    executor.poll()
    await Promise.all([x.promise, y.promise])

    let dir = await cache.read('A').then((s) => s.list('/x/'))
    assert.deepEqual(dir, ['doc1'])

    dir = await cache.read('A').then((s) => s.list('/y/'))
    assert.deepEqual(dir, ['doc2'])
  })

  it('executes two dependent changes to different shards', async () => {
    let x = executor.add('A', [], (s) => s.link('/x/', 'doc1'))
    let y = executor.add('B', [x.id], (s) => s.link('/y/', 'doc2'))

    executor.poll()
    await Promise.all([x.promise, y.promise])

    let dir = await cache.read('A').then((s) => s.list('/x/'))
    assert.deepEqual(dir, ['doc1'])

    dir = await cache.read('B').then((s) => s.list('/y/'))
    assert.deepEqual(dir, ['doc2'])
  })

  it('executes two separate requests for the same shard', async () => {
    let x = executor.add('A', [], (s) => s.link('/x/', 'doc1'))
    executor.poll()

    let y = executor.add('A', [], (s) => s.link('/y/', 'doc2'))
    executor.poll()

    await Promise.all([x.promise, y.promise])

    let dir = await cache.read('A').then((s) => s.list('/x/'))
    assert.deepEqual(dir, ['doc1'])

    dir = await cache.read('A').then((s) => s.list('/y/'))
    assert.deepEqual(dir, ['doc2'])
  })

  it('exposes an error when writing the shard', async () => {
    store.write = () => Promise.reject(new Error('oh no'))

    let x = executor.add('A', [], (s) => s.link('/x/', 'doc1'))
    let y = executor.add('A', [], (s) => s.link('/y/', 'doc2'))

    executor.poll()

    let error = await x.promise.catch(e => e)
    assert.equal(error.message, 'oh no')

    error = await y.promise.catch(e => e)
    assert.equal(error.message, 'oh no')
  })

  it('continues execution after a write failure', async () => {
    store.write = () => Promise.reject(new Error('oh no'))

    let x = executor.add('A', [], (s) => s.link('/x/', 'doc1'))
    executor.poll()

    let y = executor.add('A', [], (s) => s.link('/y/', 'doc2'))
    executor.poll()

    let error = await y.promise.catch(e => e)
    assert.equal(error.message, 'oh no')
  })

  describe('execution order', () => {
    let ops

    beforeEach(() => ops = [])

    function log (name, fn) {
      return async (...args) => {
        ops.push(`start ${name}`)
        await fn(...args)
        ops.push(`end ${name}`)
      }
    }

    it('runs independent changes for different shards concurrently', async () => {
      let x = executor.add('A', [], log('x', (s) => s.link('/x/', 'doc1')))
      let y = executor.add('B', [], log('y', (s) => s.link('/y/', 'doc2')))

      executor.poll()
      await Promise.all([x.promise, y.promise])

      assert.deepEqual(ops, [
        'start x',
        'start y',
        'end x',
        'end y'
      ])
    })

    it('runs independent changes for the same shard sequentially', async () => {
      let x = executor.add('A', [], log('x', (s) => s.link('/x/', 'doc1')))
      let y = executor.add('A', [], log('y', (s) => s.link('/y/', 'doc2')))

      executor.poll()
      await Promise.all([x.promise, y.promise])

      assert.deepEqual(ops, [
        'start x',
        'end x',
        'start y',
        'end y'
      ])
    })

    it('runs dependent changes for different shards sequentially', async () => {
      let x = executor.add('A', [], log('x', (s) => s.link('/x/', 'doc1')))
      let y = executor.add('B', [x.id], log('y', (s) => s.link('/y/', 'doc2')))

      executor.poll()
      await Promise.all([x.promise, y.promise])

      assert.deepEqual(ops, [
        'start x',
        'end x',
        'start y',
        'end y'
      ])
    })
  })

  describe('with items in different shards', () => {
    beforeEach(async () => {
      let shard = await Shard.parse(null, cipher, verifier)
      await shard.link('/', 'doc')
      await store.write('A', await shard.serialize())

      shard = await Shard.parse(null, cipher, verifier)
      await shard.put('/doc', () => ({ x: 1 }))
      await store.write('B', await shard.serialize())
    })

    function doUpdate () {
      let exec = new Executor(new Cache(store, cipher, verifier))

      let link = exec.add('A', [], (s) => s.link('/', 'doc'))
      let put = exec.add('B', [link.id], (s) => s.put('/doc', (doc) => ({ ...doc, y: 2 })))

      exec.poll()
      return Promise.all([link.promise, put.promise])
    }

    function doRemove () {
      let exec = new Executor(new Cache(store, cipher, verifier))

      let rm = exec.add('B', [], (s) => s.rm('/doc'))
      let unlink = exec.add('A', [rm.id], (s) => s.unlink('/', 'doc'))

      exec.poll()
      return Promise.all([rm.promise, unlink.promise])
    }

    it('triggers a conflict between two clients', async () => {
      let alice = new Executor(new Cache(store, cipher, verifier))
      let op1 = alice.add('A', [], (s) => s.link('/', 'x'))
      alice.poll()

      let bob = new Executor(new Cache(store, cipher, verifier))
      let op2 = bob.add('A', [], (s) => s.link('/', 'y'))
      bob.poll()

      let [e1, e2] = await Promise.all([
        op1.promise.catch(e => e),
        op2.promise.catch(e => e)
      ])

      let [none, error] = e1 ? [e2, e1] : [e1, e2]

      assert.isUndefined(none)
      assert.equal(error.code, 'ERR_CONFLICT')

      let dir = await cache.read('A').then((s) => s.list('/'))
      let item = (e1 === error) ? 'y' : 'x'
      assert.deepEqual(dir, ['doc', item])
    })

    it('loads all shards before writing to prevent race conditions', async () => {
      let error = await Promise.all([doUpdate(), doRemove()]).catch(e => e)
      assert.equal(error.code, 'ERR_CONFLICT')

      let dir = await cache.read('A').then((s) => s.list('/'))
      let doc = await cache.read('B').then((s) => s.get('/doc'))

      assert.isTrue(doc === null || dir.includes('doc'))
    })
  })
})
