'use strict'

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

const CreateOptions = OpenOptions.extend({
  key: {
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
  static async create (adapter, options = {}) {
    options = CreateOptions.parse(options)
    let data = await buildInitialConfig(options)
    let json = JSON.stringify(data, true, 2)

    try {
      await adapter.write(SHARD_ID, json, null)
      return new Config(data, options)
    } catch (error) {
      if (error.code === 'ERR_CONFLICT') {
        throw new ExistingStore('store already exists; use Store.open() to access it')
      } else {
        throw error
      }
    }
  }

  static async open (adapter, options = {}) {
    options = OpenOptions.parse(options)
    let response = await adapter.read(SHARD_ID)

    if (response) {
      let data = JSON.parse(response.value)
      return new Config(data, options)
    } else {
      throw new MissingStore('store does not exist; use Store.create() to initialise it')
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

  async _decrypt (key) {
    let pwKey = await this._pwKey
    try {
      return await pwKey.decrypt(Buffer.from(key, KEY_FORMAT))
    } catch (error) {
      throw new AccessDenied('could not unlock the store; make sure the password is correct')
    }
  }

  async buildCipher () {
    let { key, ...rest } = this._data.cipher
    key = await this._decrypt(key)
    return new AesGcmCipher({ key, ...rest })
  }

  async buildVerifier () {
    let { key, ...rest } = this._data.auth
    key = await this._decrypt(key)
    return new Verifier({ key, ...rest })
  }

  async buildRouter () {
    let { key, ...rest } = this._data.shards
    key = await this._decrypt(key)
    return new Router({ key, ...rest })
  }
}

async function buildInitialConfig (options) {
  let { password, iterations } = options.key
  let salt = await pbkdf2.generateSalt()
  let pwKey = await PasswordCipher.create({ password, salt, iterations })

  let cipherKey = await AesGcmCipher.generateKey()
  let authKey = await Verifier.generateKey()
  let routerKey = await Router.generateKey()

  let encrypt = async (key) => {
    key = await pwKey.encrypt(key)
    return key.toString(KEY_FORMAT)
  }

  return {
    version: VERSION,

    password: {
      salt: salt.toString(KEY_FORMAT),
      iterations
    },

    cipher: {
      key: await encrypt(cipherKey)
    },

    auth: {
      key: await encrypt(authKey)
    },

    shards: {
      key: await encrypt(routerKey),
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
