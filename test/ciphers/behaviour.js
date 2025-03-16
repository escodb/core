'use strict'

const { assert } = require('chai')

function testCipherBehaviour (impl) {
  let cipher, message

  beforeEach(async () => {
    cipher = await impl.createCipher()

    // 48 bytes, i.e. 3x 16-byte blocks
    message = Buffer.from('the quick brown fox jumps over the slow lazy dog', 'utf8')
  })

  it('encrypts a message', async () => {
    let enc = await cipher.encrypt(message)

    assert.instanceOf(enc, Buffer)

    // 12-byte IV, 48-byte ciphertext, 16-byte auth tag
    // possible extra 4-byte header for key ID
    assert(enc.length === 76 || enc.length === 80)
  })

  it('returns a different ciphertext each time', async () => {
    let enc1 = await cipher.encrypt(message)
    let enc2 = await cipher.encrypt(message)

    assert.notEqual(enc1.toString('base64'), enc2.toString('base64'))
  })

  it('decrypts an encrypted message', async () => {
    let enc = await cipher.encrypt(message)
    let dec = await cipher.decrypt(enc)

    assert.equal(dec.toString('utf8'), message)
  })
}

module.exports = testCipherBehaviour
