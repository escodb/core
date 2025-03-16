'use strict'

const KeySequenceCipher = require('./ciphers/key_sequence')
const { Cell, JsonCodec } = require('./cell')
const { randomBytes } = require('./crypto')
const RWLock = require('./sync/rwlock')

const HEADER = { version: 1 }
const ITEM_FORMAT = 'base64'
const TAG_SIZE = 64

class Shard {
  static async parse (string, cipher, verifier) {
    let keyseq = null
    let cell = (value) => new Cell(keyseq, JsonCodec, { format: ITEM_FORMAT, data: value })

    if (!string) {
      keyseq = new KeySequenceCipher(cipher, verifier)
      let index = cell().set([])
      return new Shard(keyseq, index, [])
    }

    let [header, index, ...items] = string.split('\n')

    header = JSON.parse(header)
    keyseq = await KeySequenceCipher.parse(header.cipher, cipher, verifier)

    index = cell(index)
    items = items.map((item) => cell(item))

    return new Shard(keyseq, index, items)
  }

  constructor (cipher, index, items) {
    this._cipher = cipher
    this._index = index
    this._items = items
    this._rwlock = new RWLock()
  }

  async serialize () {
    return this._rwlock.read(async () => {
      let items = [this._index, ...this._items]
      items = await Promise.all(items.map((item) => item.serialize()))

      let tag = randomBytes(TAG_SIZE / 8).toString('base64')
      let cipher = await this._cipher.serialize()
      let header = JSON.stringify({ ...HEADER, tag, cipher })

      return [header, ...items].join('\n')
    })
  }

  getCounters () {
    return this._cipher.getCounters()
  }

  async size () {
    return this._rwlock.read(async () => {
      let index = await this._index.get()
      return index.length
    })
  }

  async list (path, options = {}) {
    return this._rwlock.read(() => this._read(path, options))
  }

  async link (path, name) {
    return this._rwlock.write(async () => {
      let item = await this._getOr(path, [])

      await item.update((dir) => {
        let idx = binarySearch(dir, name)

        if (idx < 0) {
          idx = Math.abs(idx) - 1
          dir.splice(idx, 0, name)
        }
        return dir
      })
    })
  }

  async unlink (path, name) {
    return this._rwlock.write(async () => {
      let idx = await this._indexOf(path)
      if (idx < 0) return

      await this._items[idx].update(async (dir) => {
        let ofs = binarySearch(dir, name)

        if (ofs >= 0) {
          dir.splice(ofs, 1)
        }
        if (dir.length === 0) {
          await this._removeAt(idx)
        }
        return dir
      })
    })
  }

  async get (path) {
    return this._rwlock.read(() => this._read(path))
  }

  async put (path, fn) {
    return this._rwlock.write(async () => {
      let item = await this._getOr(path, null)
      await item.update(fn)
    })
  }

  async rm (path) {
    return this._rwlock.write(async () => {
      let idx = await this._indexOf(path)
      if (idx >= 0) await this._removeAt(idx)
    })
  }

  async _read (path, options = {}) {
    let idx = await this._indexOf(path)
    if (idx < 0) return null

    let value = await this._items[idx].get()
    if (!options.shared) value = clone(value)

    return value
  }

  async _getOr (path, init) {
    let idx = await this._indexOf(path)

    if (idx < 0) {
      idx = Math.abs(idx) - 1

      await this._index.update((index) => {
        index.splice(idx, 0, path)
        return index
      })

      let item = new Cell(this._cipher, JsonCodec, { format: ITEM_FORMAT }).set(init)
      this._items.splice(idx, 0, item)
    }

    return this._items[idx]
  }

  async _removeAt (idx) {
    await this._index.update((index) => {
      index.splice(idx, 1)
      return index
    })

    this._items.splice(idx, 1)
  }

  async _indexOf (path) {
    return binarySearch(await this._index.get(), path)
  }
}

function binarySearch (array, target) {
  let low = 0
  let high = array.length - 1

  while (low <= high) {
    let mid = Math.floor((low + high) / 2)
    let value = array[mid]

    if (value < target) {
      low = mid + 1
    } else if (value > target) {
      high = mid - 1
    } else {
      return mid
    }
  }

  return -1 - low
}

function clone (value) {
  if (value === null) return null

  if (Array.isArray(value)) {
    return value.map((item) => clone(item))
  }

  if (typeof value === 'object') {
    let copy = {}
    for (let key in value) {
      copy[key] = clone(value[key])
    }
    return copy
  }

  return value
}

module.exports = Shard
