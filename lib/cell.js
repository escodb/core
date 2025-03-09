'use strict'

class Cell {
  constructor (cipher, codec, format, data) {
    this._cipher = cipher
    this._codec = codec
    this._format = format || null
    this._encrypted = data || null
    this._decrypted = null
    this._modified = false
  }

  async serialize () {
    if (this._modified) {
      let data = this._codec.encode(await this._decrypted)

      let enc = await this._cipher.encrypt(data)
      if (this._format) enc = enc.toString(this._format)

      this._encrypted = enc
      this._modified = false
    }

    return this._encrypted
  }

  get () {
    this._decrypted = this._decrypted || this._decrypt()
    return this._decrypted
  }

  async _decrypt () {
    if (this._encrypted === null) return null

    let enc = this._encrypted
    if (this._format) enc = Buffer.from(enc, this._format)

    let data = await this._cipher.decrypt(enc)
    return this._codec.decode(data)
  }

  set (value) {
    this._decrypted = value
    this._modified = true
    return this
  }

  async update (fn) {
    let value = await this.get()
    this.set(await fn(value))
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
