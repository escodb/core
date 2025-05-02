'use strict'

const { hmacSha256 } = require('./crypto')
const canon = require('./format/canon')

const OUTPUT_FORMAT = 'base64'

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

  async sign (context) {
    let message = canon.encode(context)
    let signature = await hmacSha256.sign(this._config.key, message)

    return signature.toString(OUTPUT_FORMAT)
  }

  async verify (context, signature) {
    let message = canon.encode(context)
    signature = Buffer.from(signature, OUTPUT_FORMAT)

    if (await hmacSha256.verify(this._config.key, message, signature)) {
      return null
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
