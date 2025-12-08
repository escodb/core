'use strict'

const { Cell, JsonCodec } = require('../lib/cell')
const AesGcmCipher = require('../lib/ciphers/aes_gcm')

const { assert } = require('chai')
const { generate } = require('./utils')

describe('Cell', () => {
  let cipher, cell

  beforeEach(async () => {
    cipher = await generate(AesGcmCipher)
    cell = new Cell(cipher, JsonCodec)
  })

  it('returns nothing when empty', async () => {
    let value = await cell.get()
    assert.isNull(value)
  })

  it('returns a value that has been placed inside it', async () => {
    cell.set({ hello: 'world' })
    let value = await cell.get()
    assert.deepEqual(value, { hello: 'world' })
  })

  it('returns an encrypted value', async () => {
    cell.set({ secret: 'machine' })
    let buf = await cell.serialize()
    assert.instanceOf(buf, Buffer)
  })

  it('returns the same ciphertext if the value is unchanged', async () => {
    cell.set({ secret: 'machine' })
    let buf1 = await cell.serialize()
    let buf2 = await cell.serialize()
    assert.equal(buf1, buf2)
  })

  it('returns a different ciphertext if the value is re-set', async () => {
    cell.set({ secret: 'machine' })
    let buf1 = await cell.serialize()

    cell.set({ secret: 'machine' })
    let buf2 = await cell.serialize()

    assert.notEqual(buf1, buf2)
  })

  it('returns a different ciphertext for each cell', async () => {
    cell.set({ secret: 'machine' })
    let buf1 = await cell.serialize()

    let cell2 = new Cell(cipher, JsonCodec)
    cell2.set({ secret: 'machine' })
    let buf2 = await cell2.serialize()

    assert.notEqual(buf1, buf2)
  })

  it('fails to serialize if the cell is empty', async () => {
    let error = await cell.serialize().catch(e => e)
    assert.equal(error.code, 'ERR_CORRUPT')
  })

  it('fails to serialize if set to a null value', async () => {
    cell.set(null)
    let error = await cell.serialize().catch(e => e)
    assert.equal(error.code, 'ERR_CORRUPT')
  })

  it('decrypts the value it is constructed with', async () => {
    cell.set({ ok: 'cool' })
    let encrypted = await cell.serialize()

    let cell2 = new Cell(cipher, JsonCodec, { data: encrypted })

    let value = await cell2.get()
    assert.deepEqual(value, { ok: 'cool' })
  })

  it('returns the ciphertext it was constructed with if unchanged', async () => {
    cell.set({ hidden: 'track' })
    let buf1 = await cell.serialize()

    let copy = new Cell(cipher, JsonCodec, { data: buf1 })
    let buf2 = await copy.serialize()

    assert.equal(buf1, buf2)
  })

  it('returns a new ciphertext if the initial value is changed', async () => {
    cell.set({ hidden: 'track' })
    let buf1 = await cell.serialize()

    let copy = new Cell(cipher, JsonCodec, { data: buf1 })
    copy.set({ different: 'data' })
    let buf2 = await copy.serialize()

    assert.notEqual(buf1, buf2)
  })

  // The following is important because Shard.list() sometimes wants to return
  // a shared reference to a directory list. So, we need the cell to cache the
  // parsed object, instead of returning a JSON string that needs to be
  // re-parsed on every use. This is why the cell needs a "codec", to tell it
  // how to further process the decrypted buffer into an object.
  //
  // Even in cases where we want to clone the decrypted object, it is cheaper
  // to do that using a dedicated cloning function than by re-parsing a JSON
  // string.

  it('returns the same object reference every time', async () => {
    cell.set({ hello: 'world' })
    let val1 = await cell.get()
    let val2 = await cell.get()
    assert(val1 === val2)
  })

  it('returns the same decrypted object every time', async () => {
    cell.set({ ok: 'cool' })
    let encrypted = await cell.serialize()

    let cell2 = new Cell(cipher, JsonCodec, { data: encrypted })

    let val1 = await cell2.get()
    let val2 = await cell2.get()

    assert(val1 === val2)
  })

  describe('context binding', () => {
    beforeEach(() => {
      cell = new Cell(cipher, JsonCodec, { context: { n: 42 } })
    })

    it('returns nothing when empty', async () => {
      let value = await cell.get()
      assert.isNull(value)
    })

    it('returns a value that has been placed inside it', async () => {
      cell.set({ hello: 'world' })
      let value = await cell.get()
      assert.deepEqual(value, { hello: 'world' })
    })

    it('returns an encrypted value', async () => {
      cell.set({ secret: 'machine' })
      let buf = await cell.serialize()
      assert.instanceOf(buf, Buffer)
    })

    it('decrypts a value with the right context', async () => {
      cell.set({ secret: 'machine' })
      let buf = await cell.serialize()

      cell = new Cell(cipher, JsonCodec, { context: { n: 42 }, data: buf })

      let value = await cell.get()
      assert.deepEqual(value, { secret: 'machine' })
    })

    it('fails to decrypt without the right context', async () => {
      cell.set({ secret: 'machine' })
      let buf = await cell.serialize()

      cell = new Cell(cipher, JsonCodec, { context: { n: 43 }, data: buf })

      let error = await cell.get().catch(e => e)
      assert.equal(error.code, 'ERR_DECRYPT')
    })

    it('fails to decrypt with additional context', async () => {
      cell.set({ secret: 'machine' })
      let buf = await cell.serialize()

      cell = new Cell(cipher, JsonCodec, { context: { n: 42, extra: 1 }, data: buf })

      let error = await cell.get().catch(e => e)
      assert.equal(error.code, 'ERR_DECRYPT')
    })

    it('fails to decrypt without any context', async () => {
      cell.set({ secret: 'machine' })
      let buf = await cell.serialize()

      cell = new Cell(cipher, JsonCodec, { data: buf })

      let error = await cell.get().catch(e => e)
      assert.equal(error.code, 'ERR_DECRYPT')
    })

    it('ignores the order of context fields', async () => {
      cell = new Cell(cipher, JsonCodec, { context: { a: 1, b: 2 } })
      cell.set({ outer: 'wilds' })
      let buf = await cell.serialize()

      let cell1 = new Cell(cipher, JsonCodec, { context: { a: 1, b: 2 }, data: buf })
      let val1 = await cell1.get()
      assert.deepEqual(val1, { outer: 'wilds' })

      let cell2 = new Cell(cipher, JsonCodec, { context: { b: 2, a: 1 }, data: buf })
      let val2 = await cell2.get()
      assert.deepEqual(val2, { outer: 'wilds' })
    })
  })

  describe('output format', () => {
    beforeEach(() => {
      cell = new Cell(cipher, JsonCodec, { format: 'hex' })
    })

    it('serialises to the requested format', async () => {
      cell.set({ some: 'value' })
      let buf = await cell.serialize()
      assert.typeOf(buf, 'string')
      assert.match(buf, /^[0-9a-f]+$/i)
    })

    it('decodes from the given output format', async () => {
      cell.set({ some: 'value' })

      let buf = await cell.serialize()
      let copy = new Cell(cipher, JsonCodec, { format: 'hex', data: buf })

      let value = await copy.get()
      assert.deepEqual(value, { some: 'value' })
    })
  })
})
