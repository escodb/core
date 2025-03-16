'use strict'

const crypto = require('../../lib/crypto')
const PasswordCipher = require('../../lib/ciphers/password')

const testCipherBehaviour = require('./behaviour')

describe('PasswordCipher', () => {
  testCipherBehaviour({
    async createCipher () {
      return PasswordCipher.create({
        password: 'hello',
        salt: await crypto.pbkdf2.generateSalt(),
        iterations: 100
      })
    }
  })
})
