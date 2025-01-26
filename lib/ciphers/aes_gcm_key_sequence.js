'use strict'

const AesGcmSingleKeyCipher = require('./aes_gcm_single_key')
const binaries = require('../binaries')
const { Cell } = require('../cell')
const Counters = require('../counters')
const Mutex = require('../sync/mutex')

const { AES_BLOCK_SIZE } = require('../crypto/constants')

const LIMIT_MESSAGES = 2 ** 31
const LIMIT_BLOCKS = 2 ** 47

const KEY_FORMAT = 'base64'

const ALGO = {
  AES_256_GCM: 1
}

const KeyCodec = {
  encode ({ algo, key }) {
    return binaries.dump(['u16', 'bytes'], [algo, key])
  },

  decode (buf) {
    let [algo, key] = binaries.load(['u16', 'bytes'], buf)
    return { algo, key }
  }
}

function aesGcmBlocks (buffer) {
  return 1 + Math.ceil(8 * buffer.length / AES_BLOCK_SIZE)
}

class AesGcmKeySequenceCipher {
  static async parse ({ keys, state }, cipher, verifier, config = {}) {
    keys = keys.map((str) => {
      let buf = Buffer.from(str, KEY_FORMAT)
      let [seq, key] = binaries.load(['u32', 'bytes'], buf)
      let cell = new Cell(cipher, KeyCodec, null, key)

      return { seq, cell }
    })

    let ids = keys.flatMap((key) => [`${key.seq}.msg`, `${key.seq}.blk`])
    let counters = await Counters.parse(state, ids, verifier)

    return new AesGcmKeySequenceCipher(cipher, verifier, config, keys, counters)
  }

  constructor (cipher, verifier, config = {}, keys = null, counters = null) {
    this._cipher = cipher
    this._limit = config.limit || LIMIT_MESSAGES
    this._keys = keys || []
    this._counters = counters || new Counters(verifier)
    this._mutex = new Mutex()

    this._bySeq = new Map()

    for (let [i, { seq }] of this._keys.entries()) {
      this._bySeq.set(seq, i)
    }
  }

  getCounters () {
    return this._counters
  }

  size () {
    return this._keys.length
  }

  async serialize () {
    return {
      keys: await this._getKeys(),
      state: await this._counters.serialize()
    }
  }

  _getKeys () {
    let keys = this._keys.map(async ({ seq, cell }) => {
      let key = await cell.serialize()
      let buf = binaries.dump(['u32', 'bytes'], [seq, key])
      return buf.toString(KEY_FORMAT)
    })

    return Promise.all(keys)
  }

  async encrypt (data) {
    return this._mutex.lock(async () => {
      let blocks = aesGcmBlocks(data)
      let { seq, cell } = await this._getLatestKey(blocks)
      let { key } = await cell.get()

      this._counters.incr(`${seq}.msg`, 1)
      this._counters.incr(`${seq}.blk`, blocks)

      let cipher = new AesGcmSingleKeyCipher({ key })
      let enc = await cipher.encrypt(data)

      return binaries.dump(['u32', 'bytes'], [seq, enc])
    })
  }

  async decrypt (data) {
    return this._mutex.lock(async () => {
      let [seq, enc] = binaries.load(['u32', 'bytes'], data)
      let { algo, key } = await this._getKeyBySeq(seq)

      if (algo !== ALGO.AES_256_GCM) {
        throw new KeyParseError(`unrecognised algorithm ID: #${algo}`)
      }

      let cipher = new AesGcmSingleKeyCipher({ key })
      return cipher.decrypt(enc)
    })
  }

  async _getLatestKey (blocks) {
    let len = this._keys.length

    if (len > 0) {
      let last = this._keys[len - 1]
      let seq = last.seq

      let msg = this._counters.get(`${seq}.msg`) + 1n
      let blk = this._counters.get(`${seq}.blk`) + BigInt(blocks)

      if (msg <= this._limit && blk <= LIMIT_BLOCKS) {
        return last
      }
    }

    let cell = new Cell(this._cipher, KeyCodec, null).set({
      algo: ALGO.AES_256_GCM,
      key: await AesGcmSingleKeyCipher.generateKey()
    })

    let seq = (len === 0) ? 1 : this._keys[len - 1].seq + 1

    this._keys.push({ seq, cell })
    this._bySeq.set(seq, len)

    this._counters.init(`${seq}.msg`, 0)
    this._counters.init(`${seq}.blk`, 0)

    return this._keys[len]
  }

  async _getKeyBySeq (seq) {
    if (!this._bySeq.has(seq)) {
      throw new MissingKeyError(`no key found with sequence number #${seq}`)
    }

    let idx = this._bySeq.get(seq)
    let key = this._keys[idx]

    return key.cell.get()
  }
}

class KeyParseError extends Error {
  constructor (message) {
    super(message)
    this.code = 'ERR_PARSE_KEY'
    this.name = 'KeyParseError'
  }
}

class MissingKeyError extends Error {
  constructor (message) {
    super(message)
    this.code = 'ERR_MISSING_KEY'
    this.name = 'MissingKeyError'
  }
}

module.exports = AesGcmKeySequenceCipher
