'use strict'

const AesGcmCipher = require('./aes_gcm')
const binaries = require('../format/binaries')
const { Cell } = require('../cell')
const Counters = require('../counters')

const { AES_BLOCK_SIZE } = require('../crypto/constants')

const LIMIT_MESSAGES = 2 ** 31
const LIMIT_BLOCKS = 2 ** 47

const KEY_FORMAT = 'base64'
const SEQ_TYPE = 'u32'

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

function aesGcmBlocks (bytes) {
  return 1 + Math.ceil(8 * bytes / AES_BLOCK_SIZE)
}

class KeySequenceCipher {
  static async parse ({ keys, state }, context, cipher, verifier, config = {}) {
    keys = keys.map((str) => {
      let buf = Buffer.from(str, KEY_FORMAT)
      let [seq, key] = binaries.load([SEQ_TYPE, 'bytes'], buf)
      let cell = new Cell(cipher, KeyCodec, { context: { ...context, key: seq }, data: key })

      return { seq, cell }
    })

    let ids = keys.flatMap((key) => [`${key.seq}.msg`, `${key.seq}.blk`])
    let counters = await Counters.parse(state, ids, verifier)

    return new KeySequenceCipher(context, cipher, verifier, config, keys, counters)
  }

  constructor (context, cipher, verifier, config = {}, keys = null, counters = null) {
    this._context = context
    this._cipher = cipher
    this._limit = config.limit || LIMIT_MESSAGES
    this._keys = keys || []
    this._counters = counters || new Counters(verifier)

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
      let buf = binaries.dump([SEQ_TYPE, 'bytes'], [seq, key])
      return buf.toString(KEY_FORMAT)
    })

    return Promise.all(keys)
  }

  async encrypt (data, context = {}) {
    let { seq, cell } = await this._getLatestKey(data.length)
    let { key } = await cell.get()

    let cipher = new AesGcmCipher({ key })
    let enc = await cipher.encrypt(data, { ...context, key: seq })

    return binaries.dump([SEQ_TYPE, 'bytes'], [seq, enc])
  }

  async decrypt (data, context = {}) {
    let [seq, enc] = binaries.load([SEQ_TYPE, 'bytes'], data)
    let { algo, key } = await this._getKeyBySeq(seq)

    if (algo !== ALGO.AES_256_GCM) {
      throw new KeyParseError(`unrecognised algorithm ID: #${algo}`)
    }

    let cipher = new AesGcmCipher({ key })
    return cipher.decrypt(enc, { ...context, key: seq })
  }

  _getLatestKey (nbytes) {
    let blocks = aesGcmBlocks(nbytes)
    let len = this._keys.length
    let key = null

    if (len > 0) {
      let last = this._keys[len - 1]

      let msg = this._counters.get(`${last.seq}.msg`)
      let blk = this._counters.get(`${last.seq}.blk`)

      if (msg <= this._limit - 1 && blk <= LIMIT_BLOCKS - blocks) {
        key = last
      }
    }

    if (key === null) {
      key = this._generateNewKey(len)
    }

    this._counters.incr(`${key.seq}.msg`, 1)
    this._counters.incr(`${key.seq}.blk`, blocks)

    return key
  }

  _generateNewKey (len) {
    let newKey = AesGcmCipher.generateKey()
    let seq = (len === 0) ? 1 : this._keys[len - 1].seq + 1

    let context = { ...this._context, key: seq }
    let cell = new Cell(this._cipher, KeyCodec, { context })
    cell.set(newKey.then((key) => ({ algo: ALGO.AES_256_GCM, key })))

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

module.exports = KeySequenceCipher
