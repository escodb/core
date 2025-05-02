'use strict'

const AesGcmCipher = require('./aes_gcm')
const binaries = require('../format/binaries')
const { Cell } = require('../cell')
const Counters = require('../counters')
const Mutex = require('../sync/mutex')

const { AES_BLOCK_SIZE } = require('../crypto/constants')

const KEY_FORMAT = 'base64'
const SEQ_TYPE = 'u32'

const ALGORITHMS = {
  AES_256_GCM: {
    id: 1,
    limits: { msg: 2 ** 31, blk: 2 ** 47 },

    countBlocks (bytes) {
      return 1 + Math.ceil(8 * bytes / AES_BLOCK_SIZE)
    }
  }
}

function getAlgo (id) {
  return Object.values(ALGORITHMS).find((algo) => algo.id === id)
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

class KeySequenceCipher {
  static async parse ({ keys, state, mac }, context, cipher, verifier, config = {}) {
    keys = keys.map((str) => {
      let buf = Buffer.from(str, KEY_FORMAT)
      let [seq, key] = binaries.load([SEQ_TYPE, 'bytes'], buf)
      let ctx = { ...context, key: seq }
      let cell = new Cell(cipher, KeyCodec, { context: ctx, data: key })

      return { seq, cell }
    })

    let seqs = binaries.dumpArray(SEQ_TYPE, keys.map((key) => key.seq))
    state = Buffer.from(state, KEY_FORMAT)
    await verifier.verify({ ...context, keys: seqs, state }, mac)

    let ids = keys.flatMap((key) => [`${key.seq}.msg`, `${key.seq}.blk`])
    let counters = await Counters.parse(state, ids)

    return new KeySequenceCipher(context, cipher, verifier, config, keys, counters)
  }

  constructor (context, cipher, verifier, config = {}, keys = null, counters = null) {
    this._context = context
    this._cipher = cipher
    this._verifier = verifier
    this._limit = config.limit || null
    this._keys = keys || []
    this._counters = counters || new Counters()
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
    let keys = await this._getKeys()

    let seqs = binaries.dumpArray(SEQ_TYPE, this._keys.map((key) => key.seq))
    let state = this._counters.serialize()
    let mac = await this._verifier.sign({ ...this._context, keys: seqs, state })

    return { keys, state: state.toString(KEY_FORMAT), mac }
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
    let { seq, cell } = await this._mutex.lock(() => this._getLatestKey(data.length))
    let { key } = await cell.get()

    let cipher = new AesGcmCipher({ key })
    let enc = await cipher.encrypt(data, { ...context, key: seq })

    return binaries.dump([SEQ_TYPE, 'bytes'], [seq, enc])
  }

  async decrypt (data, context = {}) {
    let [seq, enc] = binaries.load([SEQ_TYPE, 'bytes'], data)
    let { algo, key } = await this._getKeyBySeq(seq)

    if (algo !== ALGORITHMS.AES_256_GCM.id) {
      throw new KeyParseError(`unrecognised algorithm ID: #${algo}`)
    }

    let cipher = new AesGcmCipher({ key })
    return cipher.decrypt(enc, { ...context, key: seq })
  }

  async _getLatestKey (nbytes) {
    let len = this._keys.length
    let key = null
    let blocks = null

    if (len > 0) {
      let last = this._keys[len - 1]
      let { algo } = await last.cell.get()
      let algoCfg = getAlgo(algo)
      blocks = algoCfg.countBlocks(nbytes)

      let msg = this._counters.get(`${last.seq}.msg`)
      let blk = this._counters.get(`${last.seq}.blk`)

      let msgLimit = this._limit || algoCfg.limits.msg
      let blkLimit = algoCfg.limits.blk

      if (msg <= msgLimit - 1 && blk <= blkLimit - blocks) {
        key = last
      }
    }

    if (key === null) {
      key = this._generateNewKey(len)
      blocks = await this._countBlocks(key, nbytes)
    }

    this._counters.incr(`${key.seq}.msg`, 1)
    this._counters.incr(`${key.seq}.blk`, blocks)

    return key
  }

  async _countBlocks (key, nbytes) {
    let { algo } = await key.cell.get()
    let algoCfg = getAlgo(algo)
    return algoCfg.countBlocks(nbytes)
  }

  _generateNewKey (len) {
    let newKey = AesGcmCipher.generateKey()
    let seq = (len === 0) ? 1 : this._keys[len - 1].seq + 1

    let context = { ...this._context, key: seq }
    let cell = new Cell(this._cipher, KeyCodec, { context })
    let algo = ALGORITHMS.AES_256_GCM.id
    cell.set(newKey.then((key) => ({ algo, key })))

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
