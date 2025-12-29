'use strict'

const { Buffer } = require('@escodb/buffer')
const { Corruption } = require('./errors')

class Cell {
  constructor (cipher, codec, { context, format, data } = {}) {
    this._cipher = cipher
    this._codec = codec
    this._context = context || {}
    this._format = format || null
    this._encrypted = data || null
    this._decrypted = null
    this._modified = false
  }

  async serialize () {
    if (this._modified) {
      let value = await this._decrypted
      if (value === null) return this._fail()

      let data = this._codec.encode(value)
      let enc = await this._cipher.encrypt(data, this._context)
      if (this._format) enc = enc.toString(this._format)

      this._encrypted = enc
      this._modified = false
    }

    if (!this._encrypted) return this._fail()

    return this._encrypted
  }

  _fail () {
    let ctx = JSON.stringify(this._context)
    throw new Corruption(`attempted to serialize an empty cell: ${ctx}`)
  }

  get () {
    this._decrypted = this._decrypted || this._decrypt()
    return this._decrypted
  }

  async _decrypt () {
    if (this._encrypted === null) return null

    let enc = this._encrypted
    if (this._format) enc = Buffer.from(enc, this._format)

    let data = await this._cipher.decrypt(enc, this._context)
    return this._codec.decode(data)
  }

  set (value) {
    this._decrypted = value
    this._modified = true
    return this
  }

  async update (fn) {
    let value = await this.get()
    value = await fn(value)
    this.set(value)
    return value
  }
}

const JsonCodec = {
  encode (value) {
    let json = JSON.stringify(value)
    return Buffer.from(json, 'utf8')
  },

  decode (buffer) {
    let string = buffer.toString('utf8')
    return JSON.parse(string)
  }
}

const NullCodec = {
  encode (value) {
    return value
  },

  decode (buffer) {
    return buffer
  }
}

module.exports = {
  Cell,
  JsonCodec,
  NullCodec
}
