'use strict'

const { Buffer } = require('@escodb/buffer')

const AesGcmCipher = require('../../lib/ciphers/aes_gcm')
const KeySequenceCipher = require('../../lib/ciphers/key_sequence')
const crypto = require('../../lib/crypto')
const Verifier = require('../../lib/verifier')
const binaries = require('../../lib/format/binaries')

const testCipherBehaviour = require('./behaviour')
const { generate } = require('../utils')
const { assert } = require('chai')

const LIMIT = 10

describe('KeySequenceCipher', () => {
  testCipherBehaviour({
    async createCipher () {
      let root = await generate(AesGcmCipher)
      let verifier = await generate(Verifier)
      return new KeySequenceCipher({}, root, verifier)
    }
  })

  describe('authentication', () => {
    let context = { a: 'foo', b: 'bar' }, root, verifier, cipher

    beforeEach(async () => {
      root = await generate(AesGcmCipher)
      verifier = await generate(Verifier)
      cipher = new KeySequenceCipher(context, root, verifier, { limit: LIMIT })

      for (let i = 0; i < 1.5 * LIMIT; i++) {
        await cipher.encrypt(Buffer.from('a message', 'utf8'))
      }
    })

    it('signs the key IDs and counter state', async () => {
      let { mac } = await cipher.serialize()

      let keys = Buffer.from([
        0, 0, 0, 1,
        0, 0, 0, 2
      ])

      let state = Buffer.from([
        0, 0, 0, 0, 0, 0, 0, 10,
        0, 0, 0, 0, 0, 0, 0, 20,
        0, 0, 0, 0, 0, 0, 0, 5,
        0, 0, 0, 0, 0, 0, 0, 10
      ])

      let signature = await verifier.sign({ keys, state, a: 'foo', b: 'bar' })

      assert.equal(mac, signature)
    })

    it('parses a state with a valid context', async () => {
      let state = await cipher.serialize()
      let parsed = await KeySequenceCipher.parse(state, context, root, verifier)
      assert.instanceOf(parsed, KeySequenceCipher)
    })

    it('rejects a state with a different context', async () => {
      let state = await cipher.serialize()
      let error = await KeySequenceCipher.parse(state, { diff: 'context' }, root, verifier).catch(e => e)
      assert.equal(error.code, 'ERR_AUTH_FAILED')
    })

    it('rejects a state with an altered key ID', async () => {
      let state = await cipher.serialize()
      let key = Buffer.from(state.keys[0], 'base64')
      let [seq, cell] = binaries.load(['u32', 'bytes'], key)

      seq += 30

      key = binaries.dump(['u32', 'bytes'], [seq, cell])
      state.keys[0] = key.toString('base64')

      let error = await KeySequenceCipher.parse(state, context, root, verifier).catch(e => e)
      assert.equal(error.code, 'ERR_AUTH_FAILED')
    })

    it('rejects a state with swapped keys', async () => {
      let state = await cipher.serialize()

      let [a, b, ...rest] = state.keys
      state.keys = [b, a, ...rest]

      let error = await KeySequenceCipher.parse(state, context, root, verifier).catch(e => e)
      assert.equal(error.code, 'ERR_AUTH_FAILED')
    })

    async function editCounters (cipher, fn) {
      let state = await cipher.serialize()

      let counters = Buffer.from(state.state, 'base64')
      counters = binaries.loadArray('u64', counters)

      counters = await fn(counters)

      counters = binaries.dumpArray('u64', counters)
      counters = counters.toString('base64')

      return { ...state, state: counters }
    }

    it('rejects a state with an altered counter', async () => {
      let state = await editCounters(cipher, (counters) => {
        counters[0] += 1n
        return counters
      })

      let error = await KeySequenceCipher.parse(state, context, root, verifier).catch(e => e)
      assert.equal(error.code, 'ERR_AUTH_FAILED')
    })

    it('rejects a state with swapped counters', async () => {
      let state = await editCounters(cipher, (counters) => {
        let [a, b, ...rest] = counters
        return [b, a, ...rest]
      })

      let error = await KeySequenceCipher.parse(state, context, root, verifier).catch(e => e)
      assert.equal(error.code, 'ERR_AUTH_FAILED')
    })
  })

  describe('key rotation', () => {
    let context = {}, root, verifier, cipher

    beforeEach(async () => {
      root = await generate(AesGcmCipher)
      verifier = await generate(Verifier)
      cipher = new KeySequenceCipher(context, root, verifier, { limit: LIMIT })
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
      let copy = await KeySequenceCipher.parse(state, context, root, verifier, { limit: LIMIT })

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
        return KeySequenceCipher.parse(state, context, root, verifier, { limit: LIMIT })
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
