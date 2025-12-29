'use strict'

const { Buffer } = require('@escodb/buffer')

const { AuthenticationFailure } = require('./errors')
const { hmacSha256 } = require('./crypto')

const OUTPUT_FORMAT = 'base64'

class Verifier {
  static async generateKey () {
    return hmacSha256.generateKey()
  }

  constructor (config) {
    this._config = config
  }

  async sign (context) {
    let message = context.toBuffer()
    let signature = await hmacSha256.sign(this._config.key, message)

    return signature.toString(OUTPUT_FORMAT)
  }

  async verify (context, signature) {
    let message = context.toBuffer()
    signature = Buffer.from(signature, OUTPUT_FORMAT)

    if (await hmacSha256.verify(this._config.key, message, signature)) {
      return null
    } else {
      throw new AuthenticationFailure('invalid authentication signature')
    }
  }
}

module.exports = Verifier
