'use strict'

const { Buffer } = require('@escodb/buffer')

const crypto = require('../../lib/crypto')
const Context = require('../../lib/ciphers/context')
const PasswordCipher = require('../../lib/ciphers/password')

const { assert } = require('chai')
const testCipherBehaviour = require('./behaviour')

describe('PasswordCipher', () => {
  let params = { password: 'hello', iterations: 100 }

  testCipherBehaviour({
    async createCipher () {
      return PasswordCipher.generate(params)
    }
  })

  describe('parameter binding', () => {
    let cipher, salt

    beforeEach(async () => {
      salt = await crypto.pbkdf2.generateSalt()
      cipher = await PasswordCipher.create({ ...params, salt })
    })

    it('exports values for the config file', () => {
      assert.deepEqual(cipher.toConfig(), {
        salt: salt.toString('base64'),
        iterations: 100
      })
    })

    it('will only decrypt with matching config parameters', async () => {
      let msg = Buffer.from('toy ornithopter', 'utf8')
      let enc = await cipher.encrypt(msg)

      let correct = await PasswordCipher.create({ ...params, salt })
      let dec = await correct.decrypt(enc)
      assert.equal(dec.toString('utf8'), 'toy ornithopter')

      let modified = await PasswordCipher.create({ ...params, salt, extra: 5 })
      let error = await modified.decrypt(enc).catch(e => e)
      assert.equal(error.code, 'ERR_DECRYPT')
    })
  })
})
