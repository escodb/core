'use strict'

const { Cell, NullCodec } = require('./cell')
const Options = require('./options')
const Router = require('./router')
const Verifier = require('./verifier')

const AesGcmCipher = require('./ciphers/aes_gcm')
const PasswordCipher = require('./ciphers/password')
const { pbkdf2 } = require('./crypto')

const VERSION = 1
const SHARD_ID = 'config'
const KEY_FORMAT = 'base64'
const DEFAULT_SHARDS = 2
const PBKDF2_ITERATIONS = 600000

const SCOPE_KEY_CIPHER = 'keys.cipher'
const SCOPE_KEY_AUTH   = 'keys.auth'
const SCOPE_KEY_ROUTER = 'keys.router'

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
    },
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

    let password = options.key.password
    this._pwKey = this._deriveKey(password)
  }

  _deriveKey (password) {
    let { salt, iterations } = this._data.password
    salt = Buffer.from(salt, KEY_FORMAT)
    return PasswordCipher.create({ password, salt, iterations })
  }

  async _decrypt (scope, key) {
    let pwKey = await this._pwKey
    let context = { file: SHARD_ID, scope }
    let cell = new Cell(pwKey, NullCodec, { context, format: KEY_FORMAT, data: key })
    try {
      return await cell.get()
    } catch (error) {
      throw new AccessDenied('could not unlock the store; make sure the password is correct')
    }
  }

  async buildCipher () {
    let { key, ...rest } = this._data.cipher
    key = await this._decrypt(SCOPE_KEY_CIPHER, key)
    return new AesGcmCipher({ key, ...rest })
  }

  async buildVerifier () {
    let { key, ...rest } = this._data.auth
    key = await this._decrypt(SCOPE_KEY_AUTH, key)
    return new Verifier({ key, ...rest })
  }

  async buildRouter () {
    let { key, ...rest } = this._data.shards
    key = await this._decrypt(SCOPE_KEY_ROUTER, key)
    return new Router({ key, ...rest })
  }
}

async function buildInitialConfig (options) {
  let { key: { password }, password: { iterations } } = options
  let salt = await pbkdf2.generateSalt()
  let pwKey = await PasswordCipher.create({ password, salt, iterations })
  let ctx = { file: SHARD_ID }

  let newcell = (scope, value) => {
    let context = { ...ctx, scope }
    let cell = new Cell(pwKey, NullCodec, { context, format: KEY_FORMAT })
    return cell.set(value)
  }

  let cipherKey = newcell(SCOPE_KEY_CIPHER, AesGcmCipher.generateKey())
  let authKey = newcell(SCOPE_KEY_AUTH, Verifier.generateKey())
  let routerKey = newcell(SCOPE_KEY_ROUTER, Router.generateKey())

  return {
    version: VERSION,

    password: {
      salt: salt.toString(KEY_FORMAT),
      iterations
    },

    cipher: {
      key: await cipherKey.serialize()
    },

    auth: {
      key: await authKey.serialize()
    },

    shards: {
      key: await routerKey.serialize(),
      n: options.shards.n
    }
  }
}

class MissingStore extends Error {
  constructor (message) {
    super(message)
    this.code = 'ERR_MISSING'
    this.name = 'MissingStore'
  }
}

class ExistingStore extends Error {
  constructor (message) {
    super(message)
    this.code = 'ERR_EXISTS'
    this.name = 'ExistingStore'
  }
}

class AccessDenied extends Error {
  constructor (message) {
    super(message)
    this.code = 'ERR_ACCESS'
    this.name = 'AccessDenied'
  }
}

module.exports = {
  Config,
  OpenOptions,
  CreateOptions
}
