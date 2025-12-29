'use strict'

const { Buffer } = require('@escodb/buffer')

const { pbkdf2 } = require('../crypto')
const AesGcmCipher = require('./aes_gcm')
const Context = require('./context')

const { AES_KEY_SIZE } = require('../crypto/constants')

const CTX_PREFIX  = 'passwd'
const SALT_FORMAT = 'base64'

class PasswordCipher {
  static async generate (params) {
    let salt = await pbkdf2.generateSalt()
    return PasswordCipher.create({ ...params, salt })
  }

  static async create (params) {
    let { password, salt, iterations } = params
    salt = Buffer.from(salt, SALT_FORMAT)

    let key = await pbkdf2.digest(password, salt, iterations, AES_KEY_SIZE)
    return new PasswordCipher({ ...params, salt, key })
  }

  constructor (params) {
    let { key, password, ...rest } = params
    this._aes = new AesGcmCipher({ key })
    this._params = rest
  }

  toConfig () {
    let { salt, iterations } = this._params

    return {
      salt: salt.toString(SALT_FORMAT),
      iterations
    }
  }

  encrypt (data, context) {
    let ctx = this._addContext(context)
    return this._aes.encrypt(data, ctx)
  }

  decrypt (data, context) {
    let ctx = this._addContext(context)
    return this._aes.decrypt(data, ctx)
  }

  _addContext (context = Context.create()) {
    return context.prefix(CTX_PREFIX).add(this._params)
  }
}

module.exports = PasswordCipher
