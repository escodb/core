'use strict'

const { hmacSha256 } = require('./crypto')

const OUTPUT_FORMAT = 'base64'
const SEPARATOR = '.'

class Verifier {
  static async generate () {
    let key = await hmacSha256.generateKey()
    return new Verifier({ key })
  }

  static generateKey () {
    return hmacSha256.generateKey()
  }

  constructor (config) {
    this._config = config
  }

  async sign (data) {
    let signature = await hmacSha256.sign(this._config.key, data)

    let parts = [
      data.toString(OUTPUT_FORMAT),
      signature.toString(OUTPUT_FORMAT)
    ]

    return parts.join(SEPARATOR)
  }

  async parse (signed) {
    let parts = signed.split(SEPARATOR)

    if (parts.length !== 2) {
      throw new AuthenticationFailure('malformed signed data payload')
    }

    let [data, signature] = parts.map((str) => Buffer.from(str, OUTPUT_FORMAT))

    if (await hmacSha256.verify(this._config.key, data, signature)) {
      return data
    } else {
      throw new AuthenticationFailure('invalid authentication signature')
    }
  }
}

class AuthenticationFailure extends Error {
  constructor (message) {
    super(message)
    this.code = 'ERR_AUTH_FAILED'
    this.name = 'AuthenticationFailure'
  }
}

module.exports = Verifier
