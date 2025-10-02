'use strict'

const AesGcmCipher = require('../lib/ciphers/aes_gcm')
const Cache = require('../lib/cache')
const Shard = require('../lib/shard')
const Verifier = require('../lib/verifier')

const { assert } = require('chai')
const { testWithAdapters } = require('./adapters/utils')
const { generate } = require('./utils')

testWithAdapters('Cache', (impl) => {
  let adapter, env, cache

  beforeEach(async () => {
    let cipher = await generate(AesGcmCipher)
    let verifier = await generate(Verifier)
    env = { cipher, verifier }

    adapter = impl.createAdapter()
    cache = new Cache(adapter, env)
  })

  afterEach(impl.cleanup)

  async function readFromStore (id) {
    let { value } = await adapter.read(id)
    return Shard.parse(value, { id, ...env })
  }

  describe('with no stored shards', () => {
    it('returns an new empty shard', async () => {
      let shard = await cache.read('x')
      assert.instanceOf(shard, Shard)
      assert.equal(await shard.size(), 0)
    })

    it('writes a new shard to the adapter', async () => {
      let shard = await cache.read('x')
      await shard.link('/', 'doc.txt')
      await cache.write('x')

      let copy = await readFromStore('x')
      assert.deepEqual(await copy.list('/'), ['doc.txt'])
    })
  })

  describe('with a shard stored', () => {
    beforeEach(async () => {
      let shard = await Shard.parse(null, { id: 'x', ...env })

      await shard.link('/', 'path/')
      await shard.link('/path/', 'doc.txt')
      await shard.put('/path/doc.txt', () => ({ p: 1 }))

      await adapter.write('x', await shard.serialize())
    })

    it('returns an existing shard', async () => {
      let shard = await cache.read('x')

      assert.deepEqual(await shard.list('/'), ['path/'])
      assert.deepEqual(await shard.get('/path/doc.txt'), { p: 1 })
    })

    it('does not cache anything on a read error', async () => {
      let n = 0
      adapter.read = () => Promise.reject(new Error(`oh no: ${++ n}`))

      let error = await cache.read('x').catch(e => e)
      assert.equal(error.message, 'oh no: 1')

      error = await cache.read('x').catch(e => e)
      assert.equal(error.message, 'oh no: 2')
    })

    it('writes an updated shard to the adapter', async () => {
      let shard = await cache.read('x')
      await shard.put('/path/doc.txt', (doc) => ({ ...doc, q: 2 }))
      await cache.write('x')

      let copy = await readFromStore('x')
      assert.deepEqual(await copy.get('/path/doc.txt'), { p: 1, q: 2 })
    })

    it('returns the updated shard after writing', async () => {
      let shard = await cache.read('x')
      await shard.put('/path/doc.txt', (doc) => ({ ...doc, q: 2 }))
      await cache.write('x')

      shard = await cache.read('x')
      assert.deepEqual(await shard.get('/path/doc.txt'), { p: 1, q: 2 })
    })

    it('updates the same shard more than once', async () => {
      let shard = await cache.read('x')

      await shard.put('/path/doc.txt', (doc) => ({ ...doc, q: 2 }))
      await cache.write('x')

      await shard.put('/path/doc.txt', (doc) => ({ ...doc, r: 3 }))
      await cache.write('x')

      let copy = await readFromStore('x')
      assert.deepEqual(await copy.get('/path/doc.txt'), { p: 1, q: 2, r: 3 })
    })

    it('allows sequential updates from two clients', async () => {
      let other = new Cache(adapter, env)

      let copy = await other.read('x')
      await copy.put('/path/doc.txt', (doc) => ({ ...doc, q: 2 }))
      await other.write('x')

      let shard = await cache.read('x')
      await shard.put('/path/doc.txt', (doc) => ({ ...doc, r: 3 }))
      await cache.write('x')

      let check = await readFromStore('x')
      assert.deepEqual(await check.get('/path/doc.txt'), { p: 1, q: 2, r: 3 })
    })

    describe('when another client performs a concurrent update', () => {
      let other

      beforeEach(async () => {
        await cache.read('x')

        other = new Cache(adapter, env)
        let copy = await other.read('x')
        await copy.put('/path/doc.txt', (doc) => ({ ...doc, q: 2 }))
        await other.write('x')
      })

      it('returns its existing copy of the shard', async () => {
        let shard = await cache.read('x')
        assert.deepEqual(await shard.get('/path/doc.txt'), { p: 1 })
      })

      it('throws a conflict error', async () => {
        let error = await cache.write('x').catch(e => e)
        assert.equal(error.code, 'ERR_CONFLICT')
      })

      it('reloads the shard on detecting a conflict', async () => {
        await cache.write('x').catch(e => e)

        let shard = await cache.read('x')
        assert.deepEqual(await shard.get('/path/doc.txt'), { p: 1, q: 2 })
      })

      it('updates successfully after a conflict', async () => {
        await cache.write('x').catch(e => e)

        let shard = await cache.read('x')
        await shard.put('/path/doc.txt', (doc) => ({ ...doc, r: 3 }))
        await cache.write('x')

        let copy = await readFromStore('x')
        assert.deepEqual(await copy.get('/path/doc.txt'), { p: 1, q: 2, r: 3 })
      })

      it('does not reload a shard on a non-conflict error', async () => {
        adapter.write = () => Promise.reject(new Error('oh no'))

        let error = await cache.write('x').catch(e => e)
        assert.equal(error.message, 'oh no')

        let shard = await cache.read('x')
        assert.deepEqual(await shard.get('/path/doc.txt'), { p: 1 })
      })

      it('merges the shard key counter state on reload', async () => {
        let shard = await cache.read('x')
        let counters = shard.getCounters()

        assert.equal(counters.get('1.msg'), 4n)

        await shard.link('/', 'path/')
        await shard.link('/path/', 'doc.txt')
        await shard.put('/path/doc.txt', (doc) => ({ ...doc, r: 3 }))

        await cache.write('x').catch(e => e)

        assert.equal(counters.get('1.msg'), 7n)

        shard = await cache.read('x')
        counters = shard.getCounters()

        assert.equal(counters.get('1.msg'), 8n)
      })

      it('does not double-count encryptions already committed', async () => {
        let copy = await other.read('x')
        let counters = copy.getCounters()

        assert.equal(counters.get('1.msg'), 5n)

        cache = new Cache(adapter, env)
        let shard = await cache.read('x')
        await shard.put('/path/doc.txt', (doc) => ({ ...doc, r: 3 }))
        await cache.write('x')

        await copy.put('/path/doc.txt', (doc) => ({ ...doc, s: 4 }))
        await other.write('x').catch(e => e)

        copy = await other.read('x')
        counters = copy.getCounters()

        assert.equal(counters.get('1.msg'), 7n)
      })
    })
  })
})
