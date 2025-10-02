'use strict'

const AesGcmCipher = require('../../lib/ciphers/aes_gcm')

const testCipherBehaviour = require('./behaviour')
const { generate } = require('../utils')

describe('AesGcmCipher', () => {
  testCipherBehaviour({
    createCipher () {
      return generate(AesGcmCipher)
    }
  })
})
