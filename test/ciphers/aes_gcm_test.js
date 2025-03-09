'use strict'

const AesGcmCipher = require('../../lib/ciphers/aes_gcm')

const testCipherBehaviour = require('./behaviour')

describe('AesGcmCipher', () => {
  testCipherBehaviour({
    createCipher () {
      return AesGcmCipher.generate()
    }
  })
})
