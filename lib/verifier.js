'use strict'

const { AuthenticationFailure } = require('./errors')
const { hmacSha256 } = require('./crypto')
const canon = require('./format/canon')

const OUTPUT_FORMAT = 'base64'

class Verifier {
  static async generateKey () {
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

module.exports = Verifier
