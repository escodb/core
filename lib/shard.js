'use strict'

const KeySequenceCipher = require('./ciphers/key_sequence')
const { Cell, JsonCodec } = require('./cell')
const { Corruption } = require('./errors')
const { randomBytes } = require('./crypto')
const RWLock = require('./sync/rwlock')

const HEADER = { version: 1 }
const ITEM_FORMAT = 'base64'
const TAG_SIZE = 64

const SCOPE_KEYS  = 'keys'
const SCOPE_INDEX = 'index'
const SCOPE_ITEMS = 'items'

function newcell (keyseq, context, data) {
  return new Cell(keyseq, JsonCodec, { context, data, format: ITEM_FORMAT })
}

class Shard {
  static async parse (id, string, cipher, verifier) {
    let keyseq = null

    let ctx = { file: id }
    let keysCtx = { ...ctx, scope: SCOPE_KEYS }
    let indexCtx = { ...ctx, scope: SCOPE_INDEX }

    if (!string) {
      keyseq = new KeySequenceCipher(keysCtx, cipher, verifier)
      let index = newcell(keyseq, indexCtx).set([])
      return new Shard(id, keyseq, index, [])
    }

    let [header, index, ...items] = string.split('\n')
    items = items.filter((item) => !/^ *$/.test(item))

    header = JSON.parse(header)
    keyseq = await KeySequenceCipher.parse(header.cipher, keysCtx, cipher, verifier)

    index = newcell(keyseq, indexCtx, index)
    let paths = await index.get()

    if (items.length !== paths.length) {
      throw new Corruption('file contains unequal numbers of paths and items')
    }

    items = items.map((item, i) => {
      let context = { ...ctx, scope: SCOPE_ITEMS, path: paths[i] }
      return newcell(keyseq, context, item)
    })

    return new Shard(id, keyseq, index, items)
  }

  constructor (id, cipher, index, items) {
    this._id = id
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
    return this._read(path, options)
  }

  async link (path, name) {
    await this._update(path, { create: true }, (dir) => {
      if (!dir) return [name]

      let ofs = binarySearch(dir, name)

      if (ofs < 0) {
        ofs = Math.abs(ofs) - 1
        dir.splice(ofs, 0, name)
      }
      return dir
    })
  }

  async unlink (path, name) {
    await this._update(path, {}, (dir) => {
      let ofs = binarySearch(dir, name)

      if (ofs >= 0) {
        dir.splice(ofs, 1)
      }

      return (dir.length === 0) ? null : dir
    })
  }

  async get (path) {
    return this._read(path)
  }

  async put (path, fn) {
    return this._update(path, { create: true }, (doc) => fn(clone(doc)))
  }

  async rm (path) {
    await this._update(path, {}, null)
  }

  async _read (path, options = {}) {
    return this._rwlock.read(async () => {
      let idx = await this._indexOf(path)
      if (idx < 0) return null

      let value = await this._items[idx].get()
      if (!options.shared) value = clone(value)

      return value
    })
  }

  async _update (path, options, fn) {
    return this._rwlock.write(async () => {
      let idx = await this._indexOf(path)
      let item = null

      if (idx < 0) {
        if (!options.create) return null

        idx = Math.abs(idx) - 1
        item = await this._insertAt(idx, path)
      } else {
        item = this._items[idx]
      }

      let value = fn ? await item.update(fn) : null
      if (value === null) await this._removeAt(idx)

      return value
    })
  }

  async _insertAt (idx, path) {
    await this._index.update((index) => {
      index.splice(idx, 0, path)
      return index
    })

    let context = { file: this._id, scope: SCOPE_ITEMS, path }
    let item = newcell(this._cipher, context, null)
    this._items.splice(idx, 0, item)

    return item
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
