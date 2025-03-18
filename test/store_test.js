'use strict'

const Store = require('../lib/store')

const { assert } = require('chai')
const { testWithAdapters } = require('./adapters/utils')

testWithAdapters('Store', (impl) => {
  let adapter, store, checker
  let password = 'the password'
  let createKey = { password, iterations: 10 }

  beforeEach(async () => {
    adapter = impl.createAdapter()
  })

  afterEach(impl.cleanup)

  describe('with no existing data store', () => {
    it('fails to open the store', async () => {
      let error = await Store.open(adapter, { key: { password } }).catch(e => e)
      assert.equal(error.code, 'ERR_MISSING')
    })

    it('fails to create a store with no password', async () => {
      let error = await Store.create(adapter).catch(e => e)
      assert.equal(error.code, 'ERR_CONFIG')

      error = await Store.create(adapter, { key: {} }).catch(e => e)
      assert.equal(error.code, 'ERR_CONFIG')
    })

    it('opens the store and lets items by written to it', async () => {
      let store = await Store.create(adapter, { key: createKey })
      await store.update('/doc', () => ({ x: 42 }))

      let checker = await Store.open(adapter, { key: { password } })
      let doc = await checker.get('/doc')
      assert.deepEqual(doc, { x: 42 })
    })
  })

  describe('with an existing data store', () => {
    beforeEach(async () => {
      store = await Store.create(adapter, { key: createKey })
      checker = await Store.open(adapter, { key: { password } })
    })

    it('does not allow the store to be re-created', async () => {
      let error = await Store.create(adapter, { key: createKey }).catch(e => e)
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
      let error = await Store.open(adapter, { key: { password: 'wrong' } }).catch(e => e)
      assert.equal(error.code, 'ERR_ACCESS')
    })

    it('fails to open with no password', async () => {
      let error = await Store.open(adapter).catch(e => e)
      assert.equal(error.code, 'ERR_CONFIG')
    })
  })

  describe('cryptography', () => {
    const AesGcmCipher = require('../lib/ciphers/aes_gcm')
    const binaries = require('../lib/format/binaries')
    const { pbkdf2 } = require('../lib/crypto')

    beforeEach(async () => {
      store = await Store.create(adapter, { key: createKey, shards: { n: 1 } })
      await store.update('/doc', () => ({ secret: 'value' }))
    })

    it('uses a chain of keys to encrypt items', async () => {
      let { value: config } = await adapter.read('config')
      let { value: shard } = await adapter.read('shard-0000-ffff')

      let [header, index, ...items] = shard.split('\n')
      config = JSON.parse(config)
      header = JSON.parse(header)

      // The user key is derived from the password using PBKDF2
      let salt = Buffer.from(config.password.salt, 'base64')
      let userKey = await pbkdf2.digest(password, salt, config.password.iterations, 256)

      // The root key is encrypted using the user key
      let rootKey = Buffer.from(config.cipher.key, 'base64')
      let ctx = { shard: 'config', scope: 'keys.cipher' }
      rootKey = await new AesGcmCipher({ key: userKey }).decrypt(rootKey, ctx)

      // The shard key is stored in the shard header along with its seq
      let keyBuf = Buffer.from(header.cipher.keys[0], 'base64')
      let [keySeq, shardKey] = binaries.load(['u32', 'bytes'], keyBuf)

      // The shard key is encrypted using the root key, and bound to its shard
      // and key seq
      ctx = { shard: 'shard-0000-ffff', scope: 'keys', key: keySeq }
      shardKey = await new AesGcmCipher({ key: rootKey }).decrypt(shardKey, ctx)
      shardKey = binaries.load(['u16', 'bytes'], shardKey)[1]

      // The item is stored with the seq of the shard key
      let itemBuf = Buffer.from(items[items.length - 1], 'base64')
      let [itemSeq, item] = binaries.load(['u32', 'bytes'], itemBuf)

      // The item is encrypted using the shard key, and bound to its shard, key
      // seq, and item path
      ctx = { shard: 'shard-0000-ffff', scope: 'items', key: itemSeq, path: '/doc' }
      item = await new AesGcmCipher({ key: shardKey }).decrypt(item, ctx)

      assert.equal(itemSeq, keySeq)
      assert.equal(item, '{"secret":"value"}')
    })
  })
})
