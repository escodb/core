'use strict'

const { aes256gcm } = require('../crypto')
const { AES_GCM_IV_SIZE } = require('../crypto/constants')

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

  async encrypt (data) {
    let { key } = this._config

    let iv = await aes256gcm.generateIv()
    let enc = await aes256gcm.encrypt(key, iv, data)

    return Buffer.concat([iv, enc])
  }

  async decrypt (data) {
    let { key } = this._config

    let a = AES_GCM_IV_SIZE / 8
    let iv = data.subarray(0, a)
    let enc = data.subarray(a, data.length)

    return aes256gcm.decrypt(key, iv, enc)
  }
}

module.exports = AesGcmCipher
