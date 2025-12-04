'use strict'

const AesGcmCipher = require('../lib/ciphers/aes_gcm')
const Shard = require('../lib/shard')
const Verifier = require('../lib/verifier')

const { assert } = require('chai')
const { generate } = require('./utils')

describe('Shard', () => {
  let env, shard

  beforeEach(async () => {
    let cipher = await generate(AesGcmCipher)
    let verifier = await generate(Verifier)
    env = { cipher, verifier }

    shard = await Shard.parse(null, { id: 'shard-id', ...env })
  })

  it('returns null for a non-existent directory', async () => {
    assert.isNull(await shard.list('/'))
  })

  it('adds an item to a directory', async () => {
    await shard.link('/', 'doc.txt')
    assert.deepEqual(await shard.list('/'), ['doc.txt'])
  })

  it('keeps directory items in order', async () => {
    for (let item of ['z', 'd', 'a', 'b', 'c']) {
      await shard.link('/', item)
    }
    let dir = await shard.list('/')
    assert.deepEqual(dir, ['a', 'b', 'c', 'd', 'z'])
  })

  it('returns a copy of the directory contents', async () => {
    await shard.link('/', 'doc.txt')

    let dir = await shard.list('/')
    dir.push('extra')

    assert.deepEqual(await shard.list('/'), ['doc.txt'])
  })

  it('can return a shared reference to the directory contents', async () => {
    await shard.link('/', 'doc.txt')

    let dir = await shard.list('/', { shared: true })
    dir.push('extra')

    assert.deepEqual(await shard.list('/'), ['doc.txt', 'extra'])
  })

  it('removes an item from a directory', async () => {
    await shard.link('/', 'a')
    await shard.link('/', 'b')
    await shard.unlink('/', 'a')

    assert.deepEqual(await shard.list('/'), ['b'])
  })

  it('removes all the items from a directory', async () => {
    await shard.link('/', 'a')
    await shard.unlink('/', 'a')

    assert.isNull(await shard.list('/'))
  })

  it('removes an item in a shared list', async () => {
    await shard.link('/', 'a')
    await shard.link('/', 'b')
    let list = await shard.list('/', { shared: true })
    await shard.unlink('/', 'a')

    assert.deepEqual(list, ['b'])
  })

  it('removes the final item in a shared list', async () => {
    await shard.link('/', 'a')
    let list = await shard.list('/', { shared: true })
    await shard.unlink('/', 'a')

    assert.deepEqual(list, [])
  })

  it('returns null for a non-existent document', async () => {
    assert.isNull(await shard.get('/doc.txt'))
  })

  it('creates a new document', async () => {
    await shard.put('/doc.txt', () => ({ x: 1 }))
    assert.deepEqual(await shard.get('/doc.txt'), { x: 1 })
  })

  it('updates an existing document', async () => {
    await shard.put('/doc.txt', () => ({ x: 1 }))
    await shard.put('/doc.txt', (doc) => ({ ...doc, y: 2 }))

    let doc = await shard.get('/doc.txt')
    assert.deepEqual(doc, { x: 1, y: 2 })
  })

  it('returns a copy of the document', async () => {
    await shard.put('/doc.txt', () => ({ a: 1 }))

    let doc = await shard.get('/doc.txt')
    doc.extra = true

    assert.deepEqual(await shard.get('/doc.txt'), { a: 1 })
  })

  it('removes a document', async () => {
    await shard.put('/doc.txt', () => ({ x: 1 }))
    await shard.rm('/doc.txt')

    assert.isNull(await shard.get('/doc.txt'))
  })

  describe('serialization', () => {
    let serial

    beforeEach(async () => {
      await shard.link('/', 'doc.txt')
      await shard.put('/doc.txt', () => ({ a: 1 }))

      await shard.link('/', 'other.txt')
      await shard.put('/other.txt', () => ({ b: 2 }))

      serial = await shard.serialize()
    })

    it('can be de/serialized', async () => {
      let copy = await Shard.parse(serial, { id: 'shard-id', ...env })

      assert.deepEqual(await copy.list('/'), ['doc.txt', 'other.txt'])
      assert.deepEqual(await copy.get('/doc.txt'), { a: 1 })
      assert.deepEqual(await copy.get('/other.txt'), { b: 2 })
    })

    it('binds items to their corresponding paths', async () => {
      let rows = serial.split('\n')
      assert.equal(rows.length, 5)

      let [a, b] = [rows[3], rows[4]]
      rows[3] = b
      rows[4] = a
      serial = rows.join('\n')

      let copy = await Shard.parse(serial, { id: 'shard-id', ...env })
      let error = await copy.get('/doc.txt').catch(e => e)
      assert.equal(error.code, 'ERR_DECRYPT')
    })

    it('binds all shard content to the shard ID', async () => {
      let error = await Shard.parse(serial, { id: 'wrong-id', ...env }).catch(e => e)
      assert.equal(error.code, 'ERR_AUTH_FAILED')
    })

    it('binds keys and items to the key ID', async () => {
      let [header, ...items] = serial.split('\n')

      function incrementKeyId (item) {
        let buf = Buffer.from(item, 'base64')
        buf.writeUInt32BE(2, 0)
        return buf.toString('base64')
      }

      header = JSON.parse(header)
      header.cipher.keys = header.cipher.keys.map(incrementKeyId)
      items = items.map(incrementKeyId)

      serial = [JSON.stringify(header), ...items].join('\n')

      let error = await Shard.parse(serial, { id: 'shard-id', ...env }).catch(e => e)
      assert.equal(error.code, 'ERR_AUTH_FAILED')
    })

    it('is not corrupted if a client fails while updating', async () => {
      let put = shard.put('/nope', () => { throw new Error('oh no') })

      let error = await put.catch(e => e)
      assert.equal(error.message, 'oh no')

      serial = await shard.serialize()
      let copy = await Shard.parse(serial, { id: 'shard-id', ...env })

      assert.deepEqual(await copy.list('/'), ['doc.txt', 'other.txt'])
    })
  })

  describe('concurrent operations', () => {
    async function delayed (n, fn) {
      while (n--) await null
      return fn()
    }

    for (let i = 1; i <= 5; i++) {
      describe(`delay: ${i}`, () => {
        it('lists a directory during an insert', async () => {
          await shard.link('/', 'path/')
          await shard.link('/path/', 'b')
          await shard.put('/path/b', () => ({ b: 2 }))

          let [_, list] = await Promise.all([
            shard.put('/a', () => ({ a: 1 })),
            delayed(i, () => shard.list('/path/'))
          ])

          assert.deepEqual(list, ['b'])
        })

        it('lists a directory during a removal', async () => {
          await shard.link('/', 'path/')
          await shard.put('/a', () => ({ a: 1 }))
          await shard.link('/path/', 'b')
          await shard.put('/path/b', () => ({ b: 2 }))

          let [_, list] = await Promise.all([
            shard.rm('/a'),
            delayed(i, () => shard.list('/path/'))
          ])

          assert.deepEqual(list, ['b'])
        })

        it('lists a directory during a directory creation', async () => {
          await shard.link('/', 'path/')
          await shard.link('/path/', 'b')
          await shard.put('/path/b', () => ({ b: 2 }))

          let [_, list] = await Promise.all([
            shard.link('/a/', 'x'),
            delayed(i, () => shard.list('/path/'))
          ])

          assert.deepEqual(list, ['b'])
        })

        it('lists a directory during a directory removal', async () => {
          await shard.link('/', 'path/')
          await shard.link('/a/', 'x')
          await shard.link('/path/', 'b')
          await shard.put('/path/b', () => ({ b: 2 }))

          let [_, list] = await Promise.all([
            shard.unlink('/a/', 'x'),
            delayed(i, () => shard.list('/path/'))
          ])

          assert.deepEqual(list, ['b'])
        })
      })
    }
  })
})
