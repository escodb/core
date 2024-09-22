'use strict'

const crypto = require('crypto')
const { promisify } = require('util')

const {
  HMAC_KEY_SIZE,
  AES_KEY_SIZE,
  AES_GCM_IV_SIZE,
  GCM_TAG_SIZE,
  PBKDF2_SALT_SIZE
} = require('./constants')

const generateKey = crypto.generateKey ? promisify(crypto.generateKey) : null

module.exports = {
  randomBytes: crypto.randomBytes,

  sha256: {
    async digest (data) {
      let hash = crypto.createHash('sha256')
      hash.update(data)
      return hash.digest()
    }
  },

  hmacSha256: {
    async generateKey () {
      if (!generateKey) return crypto.randomBytes(HMAC_KEY_SIZE / 8)

      let key = await generateKey('hmac', { length: HMAC_KEY_SIZE })
      return key.export({ format: 'buffer' })
    },

    async digest (key, data) {
      let hmac = crypto.createHmac('sha256', key)
      hmac.update(data)
      return hmac.digest()
    }
  },

  aes256gcm: {
    async generateKey () {
      if (!generateKey) return crypto.randomBytes(AES_KEY_SIZE / 8)

      let key = await generateKey('aes', { length: AES_KEY_SIZE })
      return key.export({ format: 'buffer' })
    },

    async generateIv () {
      return crypto.randomBytes(AES_GCM_IV_SIZE / 8)
    },

    async encrypt (key, iv, data) {
      let options = { authTagLength: GCM_TAG_SIZE / 8 }
      let cipher = crypto.createCipheriv('aes-256-gcm', key, iv, options)

      let chunks = [
        cipher.update(data),
        cipher.final(),
        cipher.getAuthTag()
      ]

      return Buffer.concat(chunks)
    },

    async decrypt (key, iv, data) {
      let boundary = data.length - GCM_TAG_SIZE / 8
      let ciphertext = data.slice(0, boundary)
      let authTag = data.slice(boundary, data.length)

      let cipher = crypto.createDecipheriv('aes-256-gcm', key, iv)
      cipher.setAuthTag(authTag)

      let chunks = [
        cipher.update(ciphertext),
        cipher.final()
      ]

      return Buffer.concat(chunks)
    }
  },

  pbkdf2: {
    async generateSalt () {
      return crypto.randomBytes(PBKDF2_SALT_SIZE / 8)
    },

    async digest (password, salt, iterations, size) {
      let pw = password.normalize('NFKD')
      let fn = promisify(crypto.pbkdf2)
      return fn(pw, salt, iterations, size / 8, 'sha256')
    }
  }
}
