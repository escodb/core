'use strict'

const AesGcmCipher = require('../../lib/ciphers/aes_gcm')
const KeySequenceCipher = require('../../lib/ciphers/key_sequence')
const crypto = require('../../lib/crypto')
const Verifier = require('../../lib/verifier')

const testCipherBehaviour = require('./behaviour')
const { assert } = require('chai')

const LIMIT = 10

describe('KeySequenceCipher', () => {
  testCipherBehaviour({
    async createCipher () {
      let root = await AesGcmCipher.generate()
      let verifier = await Verifier.generate()
      return new KeySequenceCipher(root, verifier)
    }
  })

  describe('key rotation', () => {
    let root, verifier, cipher

    beforeEach(async () => {
      root = await AesGcmCipher.generate()
      verifier = await Verifier.generate()
      cipher = new KeySequenceCipher(root, verifier, { limit: LIMIT })
    })

    it('encrypts up to the limit with a single key', async () => {
      for (let i = 0; i < LIMIT; i++) {
        await cipher.encrypt(Buffer.from('a message', 'utf8'))
      }
      assert.equal(cipher.size(), 1)
    })

    it('creates a new key each time the limit is reached', async () => {
      let message = Buffer.from('a message', 'utf8')

      for (let i = 0; i < 3 * LIMIT + 1; i++) {
        await cipher.encrypt(message)
      }
      assert.equal(cipher.size(), 4)
    })

    it('creates new keys correctly during concurrent encryptions', async () => {
      let message = Buffer.from('a message', 'utf8')
      let ops = []

      for (let i = 0; i < 3 * LIMIT + 1; i++) {
        ops.push(cipher.encrypt(message))
      }

      await Promise.all(ops)
      assert.equal(cipher.size(), 4)
    })

    it('decrypts ciphertexts made with any previous key', async () => {
      let messages = []

      for (let i = 0; i < 3 * LIMIT + 1; i++) {
        messages.push(crypto.randomBytes(16))
      }

      let encs = await Promise.all(messages.map((msg) => cipher.encrypt(msg)))
      let decs = await Promise.all(encs.map((enc) => cipher.decrypt(enc)))

      assert(messages.length > 0)
      assert.equal(messages.length, decs.length)

      for (let i = 0; i < messages.length; i++) {
        assert.equal(messages[i].toString('base64'), decs[i].toString('base64'))
      }
    })

    it('rejects ciphertexts with bad sequence numbers', async () => {
      let enc = await cipher.encrypt(Buffer.from('hi', 'utf8'))
      enc.writeUInt32BE(42, 0)
      let error = await cipher.decrypt(enc).catch(e => e)
      assert.equal(error.code, 'ERR_MISSING_KEY')
    })

    it('can serialize and restore the key sequence state', async () => {
      let message = Buffer.from('the message', 'utf8')
      let encs = []

      let n = 3
      let a = n * LIMIT - 3
      let b = n * LIMIT

      for (let i = 0; i < a; i++) {
        encs.push(await cipher.encrypt(message))
      }
      assert.equal(cipher.size(), n)

      let state = await cipher.serialize()
      let copy = await KeySequenceCipher.parse(state, root, verifier, { limit: LIMIT })

      for (let i = a; i < b; i++) {
        encs.push(await copy.encrypt(message))
      }
      assert.equal(copy.size(), n)

      encs.push(await copy.encrypt(message))
      assert.equal(copy.size(), n + 1)

      for (let [i, enc] of encs.entries()) {
        if (i < b) {
          assert.equal(await cipher.decrypt(enc), 'the message')
        }
        assert.equal(await copy.decrypt(enc), 'the message')
      }
    })

    describe('two clients hitting the limit on the same key', () => {
      let message = Buffer.from('a message', 'utf8')
      let alice, bob

      async function clone (cipher) {
        let state = await cipher.serialize()
        return KeySequenceCipher.parse(state, root, verifier, { limit: LIMIT })
      }

      beforeEach(async () => {
        for (let i = 0; i < 3 * LIMIT - 2; i++) {
          await cipher.encrypt(message)
        }

        alice = await clone(cipher)
        bob = await clone(cipher)

        for (let i = 0; i < LIMIT / 2; i++) {
          await alice.encrypt(message)
          await bob.encrypt(message)
        }
      })

      it('merges the state of the last shared key', async () => {
        let counters = alice.getCounters()

        counters.commit()
        counters.merge(bob.getCounters())

        assert.equal(counters.get('3.msg'), LIMIT + 2)
      })

      it('does not merge the state of the newly added key', async () => {
        let counters = alice.getCounters()

        counters.commit()
        counters.merge(bob.getCounters())

        assert.equal(counters.get('4.msg'), LIMIT / 2 - 2)
      })
    })
  })
})
