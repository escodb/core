'use strict'

const { aes256gcm } = require('../crypto')
const { AES_GCM_IV_SIZE } = require('../crypto/constants')
const canon = require('../format/canon')
const { DecryptError } = require('../errors')

class AesGcmCipher {
  static async generate () {
    let key = await aes256gcm.generateKey()
    return new AesGcmCipher({ key })
  }

  static async generateKey () {
    return aes256gcm.generateKey()
  }

  constructor (config) {
    this._config = config
  }

  getKey () {
    return this._config.key
  }

  async encrypt (data, context = {}) {
    let { key } = this._config

    let iv = await aes256gcm.generateIv()
    let aad = canon.encode(context)
    let enc = await aes256gcm.encrypt(key, iv, data, aad)

    return Buffer.concat([iv, enc])
  }

  async decrypt (data, context = {}) {
    let { key } = this._config

    let a = AES_GCM_IV_SIZE / 8
    let iv = data.subarray(0, a)
    let enc = data.subarray(a, data.length)
    let aad = canon.encode(context)

    try {
      return await aes256gcm.decrypt(key, iv, enc, aad)
    } catch (error) {
      throw new DecryptError('decryption failure', { cause: error })
    }
  }
}

module.exports = AesGcmCipher
