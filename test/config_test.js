'use strict'

const { Config } = require('../lib/config')

const { assert } = require('chai')
const { testWithAdapters } = require('./adapters/utils')

testWithAdapters('Config', (impl) => {
  let adapter, config
  let password = 'hello'
  let createKey = { password, iterations: 10 }

  beforeEach(() => {
    adapter = impl.createAdapter()
  })

  afterEach(impl.cleanup)

  it('writes initial config to the storage', async () => {
    await Config.create(adapter, { key: createKey })

    let { value } = await adapter.read('config')
    let config = JSON.parse(value)

    assert.equal(config.version, 1)

    assert.match(config.password.salt, /^[a-z0-9/+]+=*$/i)
    assert.typeOf(config.password.iterations, 'number')

    assert.match(config.cipher.key, /^[a-z0-9/+]+=*$/i)

    assert.match(config.auth.key, /^[a-z0-9/+]+=*$/i)

    assert.match(config.shards.key, /^[a-z0-9/+]+=*$/i)
    assert.equal(config.shards.n, 2)
  })

  it('sets the key iterations', async () => {
    await Config.create(adapter, { key: { password, iterations: 50 } })

    let { value } = await adapter.read('config')
    let config = JSON.parse(value)

    assert.equal(config.password.iterations, 50)
  })

  it('sets the number of shards', async () => {
    await Config.create(adapter, { key: createKey, shards: { n: 3 } })

    let { value } = await adapter.read('config')
    let config = JSON.parse(value)

    assert.equal(config.shards.n, 3)
  })

  it('sets the number of shards to zero', async () => {
    let params = { key: createKey, shards: { n: 0 } }
    let error = await Config.create(adapter, params).catch(e => e)

    assert.equal(error.code, 'ERR_CONFIG')
  })

  async function open (adapter, options) {
    try {
      let openOpts = { key: { password: options.key.password } }
      return await Config.open(adapter, openOpts)
    } catch (error) {
      if (error.code !== 'ERR_MISSING') throw error

      try {
        return await Config.create(adapter, options)
      } catch (error) {
        if (error.code === 'ERR_EXISTS') {
          return open(adapter, options)
        } else {
          throw error
        }
      }
    }
  }

  it('makes concurrently created clients agree on the config', async () => {
    let configs = []

    for (let i = 0; i < 10; i++) {
      configs.push(open(adapter, { key: createKey }))
    }
    configs = await Promise.all(configs)

    let keys = new Set(configs.map((c) => c._data.cipher.key))
    assert.equal(keys.size, 1)
  })
})
