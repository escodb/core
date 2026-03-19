'use strict'

const Builder = require('./builder')
const Options = require('../options')
const { OpenOptions, CreateOptions } = require('./options')
const { ExistingStore, MissingStore } = require('../errors')

const AesGcmCipher = require('../ciphers/aes_gcm')
const Context = require('../ciphers/context')
const PasswordCipher = require('../ciphers/password')

const Router = require('../router')
const Verifier = require('../verifier')

const VERSION = 1
const SHARD_ID = 'config'

const CTX = Context.create('config', { file: SHARD_ID })

const BUILDER_SECTIONS = {
  cipher: AesGcmCipher,
  verify: Verifier,
  shards: Router
}

class Config {
  static async create (adapter, openOpts = {}, createOpts = {}) {
    openOpts = OpenOptions.parse(openOpts)
    createOpts = CreateOptions.parse(createOpts)

    let options = Options.merge(openOpts, createOpts)
    let data = await buildInitialConfig(options)
    let json = JSON.stringify(data, true, 2)

    try {
      await adapter.write(SHARD_ID, json, null)
      return new Config(data, openOpts)
    } catch (error) {
      if (error.code === 'ERR_CONFLICT') {
        throw new ExistingStore('store already exists; use Store.open() to access it')
      } else {
        throw error
      }
    }
  }

  static async open (adapter, openOpts = {}) {
    openOpts = OpenOptions.parse(openOpts)
    let response = await adapter.read(SHARD_ID)

    if (response) {
      let data = JSON.parse(response.value)
      return new Config(data, openOpts)
    } else {
      throw new MissingStore('store does not exist; use Store.create() to initialise it')
    }
  }

  static async openOrCreate (adapter, openOpts, createOpts) {
    try {
      return await this.open(adapter, openOpts)
    } catch (error) {
      if (error.code !== 'ERR_MISSING') throw error

      try {
        return await this.create(adapter, openOpts, createOpts)
      } catch (error) {
        if (error.code === 'ERR_EXISTS') {
          return this.openOrCreate(adapter, openOpts, createOpts)
        } else {
          throw error
        }
      }
    }
  }

  constructor (data, options) {
    this._data = data

    let params = this._data.password
    let { password } = options.key
    this._pwKey = PasswordCipher.create({ ...params, password })
  }

  buildCipher () {
    return this._build('cipher')
  }

  buildVerifier () {
    return this._build('verify')
  }

  buildRouter () {
    return this._build('shards')
  }

  async _build (name) {
    let cipher = await this._pwKey
    let builder = new Builder(cipher, CTX, BUILDER_SECTIONS)
    return builder.load(name, this._data)
  }
}

async function buildInitialConfig (options) {
  let { key: { password }, password: { iterations } } = options
  let pwKey = await PasswordCipher.generate({ password, iterations })
  let builder = new Builder(pwKey, CTX, BUILDER_SECTIONS)

  return {
    version: VERSION,
    password: pwKey.toConfig(),

    cipher: await builder.generate('cipher', options),
    verify: await builder.generate('verify', options),
    shards: await builder.generate('shards', options)
  }
}

module.exports = Config
