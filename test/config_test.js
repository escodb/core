'use strict'

const { Config } = require('../lib/config')

const { assert } = require('chai')
const { testWithAdapters } = require('./adapters/utils')

testWithAdapters('Config', (impl) => {
  let adapter, config
  let password = 'hello'
  let openOpts = { key: { password } }
  let createOpts = { password: { iterations: 10 } }

  beforeEach(() => {
    adapter = impl.createAdapter()
  })

  afterEach(impl.cleanup)

  it('writes initial config to the storage', async () => {
    await Config.create(adapter, openOpts, createOpts)

    let { value } = await adapter.read('config')
    let config = JSON.parse(value)

    assert.equal(config.version, 1)

    assert.match(config.password.salt, /^[a-z0-9/+]+=*$/i)
    assert.typeOf(config.password.iterations, 'number')

    assert.match(config.cipher.key, /^[a-z0-9/+]+=*$/i)

    assert.match(config.auth.key, /^[a-z0-9/+]+=*$/i)

    assert.match(config.shards.key, /^[a-z0-9/+]+=*$/i)
    assert.equal(config.shards.n, 4)
  })

  it('sets the password iterations', async () => {
    await Config.create(adapter, openOpts, { password: { iterations: 50 } })

    let { value } = await adapter.read('config')
    let config = JSON.parse(value)

    assert.equal(config.password.iterations, 50)
  })

  it('sets the number of shards', async () => {
    await Config.create(adapter, openOpts, { ...createOpts, shards: { n: 3 } })

    let { value } = await adapter.read('config')
    let config = JSON.parse(value)

    assert.equal(config.shards.n, 3)
  })

  it('sets the number of shards to zero', async () => {
    let params = { ...createOpts, shards: { n: 0 } }
    let error = await Config.create(adapter, openOpts, params).catch(e => e)

    assert.equal(error.code, 'ERR_CONFIG')
  })

  it('makes concurrently created clients agree on the config', async () => {
    let configs = []

    for (let i = 0; i < 10; i++) {
      configs.push(Config.openOrCreate(adapter, openOpts, createOpts))
    }
    configs = await Promise.all(configs)

    let keys = new Set(configs.map((c) => c._data.cipher.key))
    assert.equal(keys.size, 1)
  })
})
