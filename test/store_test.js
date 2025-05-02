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
    const canon = require('../lib/format/canon')
    const { hmacSha256, pbkdf2 } = require('../lib/crypto')

    beforeEach(async () => {
      store = await Store.create(adapter, { key: createKey, shards: { n: 1 } })
      await store.update('/doc', () => ({ secret: 'value' }))
    })

    function b64 (str) {
      assert.typeOf(str, 'string')
      return Buffer.from(str, 'base64')
    }

    function decrypt (key, data, context) {
      return new AesGcmCipher({ key }).decrypt(data, context)
    }

    it('uses a chain of keys to encrypt items', async () => {
      let { value: config } = await adapter.read('config')
      config = JSON.parse(config)

      let { value: shard } = await adapter.read('shard-0000-ffff')
      let [header, index, ...items] = shard.split('\n')
      header = JSON.parse(header)

      // The user key is derived from the password using PBKDF2
      let salt = b64(config.password.salt)
      let userKey = await pbkdf2.digest(password, salt, config.password.iterations, 256)
      assert.equal(userKey.length, 32)

      // The root key is encrypted using the user key
      let rootKey = b64(config.cipher.key)
      let ctx = { shard: 'config', scope: 'keys.cipher' }
      rootKey = await decrypt(userKey, rootKey, ctx)
      assert.equal(rootKey.length, 32)

      // The auth key is encrypted using the user key
      let authKey = b64(config.auth.key)
      ctx = { shard: 'config', scope: 'keys.auth' }
      authKey = await decrypt(userKey, authKey, ctx)
      assert.equal(authKey.length, 64)

      // The key seqs and counters are authenticated using the auth key and are
      // bound to the shard
      let keys = header.cipher.keys.map((key) => {
        let [seq, cell] = binaries.load(['u32', 'bytes'], b64(key))
        return { seq, cell }
      })
      let seqs = binaries.dumpArray('u32', keys.map((k) => k.seq))
      let state = b64(header.cipher.state)
      ctx = { shard: 'shard-0000-ffff', scope: 'keys', keys: seqs, state }
      let mac = b64(header.cipher.mac)
      let verified = await hmacSha256.verify(authKey, canon.encode(ctx), mac)

      assert.equal(seqs.length, 4)
      assert.equal(state.length, 16)
      assert.equal(mac.length, 32)
      assert.equal(verified, true)

      // The shard key is encrypted using the root key, and bound to its shard
      // and key seq
      let { seq: keySeq, cell: shardKey } = keys[0]
      ctx = { shard: 'shard-0000-ffff', scope: 'keys', key: keySeq }
      shardKey = await decrypt(rootKey, shardKey, ctx)
      shardKey = binaries.load(['u16', 'bytes'], shardKey)[1]
      assert.equal(shardKey.length, 32)

      // The item is stored with the seq of the shard key
      let itemBuf = b64(items[items.length - 1])
      let [itemSeq, item] = binaries.load(['u32', 'bytes'], itemBuf)

      // The item is encrypted using the shard key, and bound to its shard, key
      // seq, and item path
      ctx = { shard: 'shard-0000-ffff', scope: 'items', key: itemSeq, path: '/doc' }
      item = await decrypt(shardKey, item, ctx)

      assert.equal(itemSeq, keySeq)
      assert.equal(item, '{"secret":"value"}')
    })
  })
})
