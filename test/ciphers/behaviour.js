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

  it('encrypts a message with binding context', async () => {
    let aad = Buffer.from('binding context', 'utf8')
    let enc = await cipher.encrypt(message, { n: 42 })

    assert.instanceOf(enc, Buffer)
    assert(enc.length === 76 || enc.length === 80)
  })

  it('decrypts a message with binding context', async () => {
    let aad = Buffer.from('binding context', 'utf8')
    let enc = await cipher.encrypt(message, { n: 42 })
    let dec = await cipher.decrypt(enc, { n: 42 })

    assert.equal(dec.toString('utf8'), message)
  })

  it('fails to decrypt a message with no binding context', async () => {
    let aad = Buffer.from('binding context', 'utf8')
    let enc = await cipher.encrypt(message, { n: 42 })

    let error = await cipher.decrypt(enc).catch(e => e)
    assert.equal(error.code, 'ERR_DECRYPT')
  })

  it('fails to decrypt a message with incorrect binding context', async () => {
    let aad = Buffer.from('binding context', 'utf8')
    let enc = await cipher.encrypt(message, { n: 42 })

    let error = await cipher.decrypt(enc, { n: 43 }).catch(e => e)
    assert.equal(error.code, 'ERR_DECRYPT')
  })
}

module.exports = testCipherBehaviour
