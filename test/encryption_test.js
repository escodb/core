'use strict'

const { Buffer } = require('@escodb/buffer')

const AesGcmCipher = require('../lib/ciphers/aes_gcm')
const { hmacSha256, pbkdf2 } = require('../lib/crypto')

const binaries = require('../lib/format/binaries')
const canon = require('../lib/format/canon')

const MemoryAdapter = require('../lib/adapters/memory')
const Store = require('../lib/store')

const { assert } = require('chai')

describe('encryption', () => {
  let store, adapter
  let password = 'swansong'
  let createOpts = { password: { iterations: 10 }, shards: { n: 1 } }

  beforeEach(async () => {
    adapter = new MemoryAdapter()
    store = await new Store(adapter, { key: { password } }).create(createOpts)
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
    let ctx = { file: 'config', scope: 'keys.cipher' }
    rootKey = await decrypt(userKey, rootKey, ctx)
    assert.equal(rootKey.length, 32)

    // The auth key is encrypted using the user key
    let authKey = b64(config.auth.key)
    ctx = { file: 'config', scope: 'keys.auth' }
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
    ctx = { file: 'shard-0000-ffff', scope: 'keys', keys: seqs, state }
    let mac = b64(header.cipher.mac)
    let verified = await hmacSha256.verify(authKey, canon.encode(ctx), mac)

    assert.equal(seqs.length, 4)
    assert.equal(state.length, 16)
    assert.equal(mac.length, 32)
    assert.equal(verified, true)

    // The shard key is encrypted using the root key, and bound to its shard
    // and key seq
    let { seq: keySeq, cell: shardKey } = keys[0]
    ctx = { file: 'shard-0000-ffff', scope: 'keys', key: keySeq }
    shardKey = await decrypt(rootKey, shardKey, ctx)
    shardKey = binaries.load(['u16', 'bytes'], shardKey)[1]
    assert.equal(shardKey.length, 32)

    // The item is stored with the seq of the shard key
    let itemBuf = b64(items[items.length - 1])
    let [itemSeq, item] = binaries.load(['u32', 'bytes'], itemBuf)

    // The item is encrypted using the shard key, and bound to its shard, key
    // seq, and item path
    ctx = { file: 'shard-0000-ffff', scope: 'items', key: itemSeq, path: '/doc' }
    item = await decrypt(shardKey, item, ctx)

    assert.equal(itemSeq, keySeq)
    assert.equal(item, '{"secret":"value"}')
  })
})
