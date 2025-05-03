'use strict'

const Store = require('../lib/store')

const { assert } = require('chai')
const { testWithAdapters } = require('./adapters/utils')

testWithAdapters('Store', (impl) => {
  let adapter, store, checker
  let password = 'the password'
  let createKey = { password, iterations: 10 }

  let openOpts = { key: { password } }
  let createOpts = { password: { iterations: 10 } }

  beforeEach(async () => {
    adapter = impl.createAdapter()
  })

  afterEach(impl.cleanup)

  describe('with no existing data store', () => {
    it('fails to open the store', async () => {
      let error = await new Store(adapter, openOpts).open().catch(e => e)
      assert.equal(error.code, 'ERR_MISSING')
    })

    it('fails to create a store with no password', async () => {
      let error = await new Store(adapter).create().catch(e => e)
      assert.equal(error.code, 'ERR_CONFIG')

      error = await new Store(adapter, { key: {} }).create().catch(e => e)
      assert.equal(error.code, 'ERR_CONFIG')
    })

    it('creates the store and lets items by written to it', async () => {
      let store = await new Store(adapter, openOpts).create(createOpts)
      await store.update('/doc', () => ({ x: 42 }))

      let checker = await new Store(adapter, openOpts).open()
      let doc = await checker.get('/doc')
      assert.deepEqual(doc, { x: 42 })
    })
  })

  describe('with an existing data store', () => {
    beforeEach(async () => {
      store = await new Store(adapter, openOpts).create(createOpts)
      checker = await new Store(adapter, openOpts).open()
    })

    it('does not allow the store to be re-created', async () => {
      let error = await new Store(adapter, openOpts).create(createOpts).catch(e => e)
      assert.equal(error.code, 'ERR_EXISTS')
    })

    it('updates several items', async () => {
      await Promise.all([
        store.update('/a', () => ({ a: 1 })),
        store.update('/path/b', () => ({ b: 2 })),
        store.update('/path/to/c', () => ({ c: 3 }))
      ])

      let docs = []

      for await (let doc of checker.find('/')) {
        docs.push(doc)
      }
      assert.deepEqual(docs, ['a', 'path/b', 'path/to/c'])
    })

    it('updates the same doc multiple times', async () => {
      await Promise.all([
        store.update('/doc', (doc) => ({ ...doc, a: 1 })),
        store.update('/doc', (doc) => ({ ...doc, b: 2 })),
        store.update('/doc', (doc) => ({ ...doc, c: 3 }))
      ])

      let doc = await checker.get('/doc')
      assert.deepEqual(doc, { a: 1, b: 2, c: 3 })
    })

    it('fails to open with the incorrect password', async () => {
      let error = await new Store(adapter, { key: { password: 'wrong' } }).open().catch(e => e)
      assert.equal(error.code, 'ERR_ACCESS')
    })

    it('fails to open with no password', async () => {
      let error = await new Store(adapter).open().catch(e => e)
      assert.equal(error.code, 'ERR_CONFIG')
    })
  })

  describe('openOrCreate()', () => {
    let a, b

    function newStore () {
      return new Store(adapter, openOpts).openOrCreate(createOpts)
    }

    beforeEach(async () => {
      let clients = await Promise.all([newStore(), newStore()])
      a = clients[0]
      b = clients[1]
    })

    it('lets either client create the store', async () => {
      await a.update('/doc', () => ({ a: 1 }))
      await b.update('/doc', (doc) => ({ ...doc, b: 2 }))
      await a.update('/doc', (doc) => ({ ...doc, c: 3 }))

      let aDoc = await a.get('/doc')
      assert.deepEqual(aDoc, { a: 1, b: 2, c: 3 })

      let bDoc = await b.get('/doc')
      assert.deepEqual(bDoc, { a: 1, b: 2, c: 3 })
    })
  })
})
