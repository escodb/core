'use strict'

const { Cell, NullCodec } = require('./cell')
const Context = require('./ciphers/context')
const Options = require('./options')
const { AccessDenied, ExistingStore, MissingStore } = require('./errors')

const AesGcmCipher = require('./ciphers/aes_gcm')
const PasswordCipher = require('./ciphers/password')
const Router = require('./router')
const Verifier = require('./verifier')

const VERSION = 1
const SHARD_ID = 'config'
const KEY_FORMAT = 'base64'
const DEFAULT_SHARDS = 4
const PBKDF2_ITERATIONS = 600000

const CTX = Context.create('config', { file: SHARD_ID })

const OpenOptions = new Options({
  key: {
    password: {
      required: true,
      valid: (val) => typeof val === 'string' && val.length > 0,
      msg: 'must be a non-empty string'
    }
  }
})

function isPositiveInt (val) {
  return typeof val === 'number' && val > 0 && val === Math.round(val)
}

const CreateOptions = new Options({
  password: {
    iterations: {
      required: false,
      default: PBKDF2_ITERATIONS,
      valid: isPositiveInt,
      msg: 'must be a positive integer'
    }
  },
  shards: {
    n: {
      required: false,
      default: DEFAULT_SHARDS,
      valid: isPositiveInt,
      msg: 'must be a positive integer'
    }
  }
})

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

  async buildCipher () {
    let key = await this._pwKey
    return new Builder('cipher', key).load(this._data)
  }

  async buildVerifier () {
    let key = await this._pwKey
    return new Builder('verify', key).load(this._data)
  }

  async buildRouter () {
    let key = await this._pwKey
    return new Builder('shards', key).load(this._data)
  }
}

const BUILDER_CLASSES = {
  cipher: AesGcmCipher,
  verify: Verifier,
  shards: Router
}

class Builder {
  constructor (name, cipher) {
    this._name = name
    this._cipher = cipher
  }

  async generate (options) {
    let name = this._name
    let key = await BUILDER_CLASSES[name].generateKey()
    let params = options[name]

    let context = this._getContext(params)
    let cell = new Cell(this._cipher, NullCodec, { context, format: KEY_FORMAT })

    cell.set(key)
    return { key: await cell.serialize(), ...params }
  }

  async load (config) {
    let name = this._name
    let Class = BUILDER_CLASSES[name]
    let { key: data, ...params } = config[name]

    let context = this._getContext(params)
    let cell = new Cell(this._cipher, NullCodec, { context, data, format: KEY_FORMAT })

    try {
      let key = await cell.get()
      return new Class({ key, ...params })
    } catch (cause) {
      throw new AccessDenied('could not unlock the store; make sure the password is correct', { cause })
    }
  }

  _getContext (params = {}) {
    let name = this._name
    return CTX.add({ scope: 'keys.' + name }).prefix(name).add(params)
  }
}

async function buildInitialConfig (options) {
  let { key: { password }, password: { iterations } } = options
  let pwKey = await PasswordCipher.generate({ password, iterations })

  let cipherKey = new Builder('cipher', pwKey)
  let verifyKey = new Builder('verify', pwKey)
  let routerKey = new Builder('shards', pwKey)

  return {
    version: VERSION,
    password: pwKey.toConfig(),

    cipher: await cipherKey.generate(options),
    verify: await verifyKey.generate(options),
    shards: await routerKey.generate(options)
  }
}

module.exports = {
  Config,
  OpenOptions,
  CreateOptions
}
