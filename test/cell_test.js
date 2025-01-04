'use strict'

const { Cell, JsonCodec } = require('../lib/cell')
const AesGcmSingleKeyCipher = require('../lib/ciphers/aes_gcm_single_key')

const { assert } = require('chai')

describe('Cell', () => {
  let cipher, cell

  beforeEach(async () => {
    cipher = await AesGcmSingleKeyCipher.generate()
    cell = new Cell(cipher, JsonCodec, null)
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

    let cell2 = new Cell(cipher, JsonCodec, null)
    cell2.set({ secret: 'machine' })
    let buf2 = await cell2.serialize()

    assert.notEqual(buf1, buf2)
  })

  it('decrypts the value it is constructed with', async () => {
    cell.set({ ok: 'cool' })
    let encrypted = await cell.serialize()

    let cell2 = new Cell(cipher, JsonCodec, null, encrypted)

    let value = await cell2.get()
    assert.deepEqual(value, { ok: 'cool' })
  })

  it('returns the ciphertext it was constructed with if unchanged', async () => {
    cell.set({ hidden: 'track' })
    let buf1 = await cell.serialize()

    let copy = new Cell(cipher, JsonCodec, null, buf1)
    let buf2 = await copy.serialize()

    assert.equal(buf1, buf2)
  })

  it('returns a new ciphertext if the initial value is changed', async () => {
    cell.set({ hidden: 'track' })
    let buf1 = await cell.serialize()

    let copy = new Cell(cipher, JsonCodec, null, buf1)
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

    let cell2 = new Cell(cipher, JsonCodec, null, encrypted)

    let val1 = await cell2.get()
    let val2 = await cell2.get()

    assert(val1 === val2)
  })

  describe('output format', () => {
    beforeEach(() => {
      cell = new Cell(cipher, JsonCodec, 'hex')
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
      let copy = new Cell(cipher, JsonCodec, 'hex', buf)

      let value = await copy.get()
      assert.deepEqual(value, { some: 'value' })
    })
  })
})
