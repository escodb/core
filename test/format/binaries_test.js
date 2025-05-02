'use strict'

const { assert } = require('chai')
const binaries = require('../../lib/format/binaries')

describe('binaries', () => {
  function assertEncode (pattern, values, bytes) {
    let buffer = binaries.dump(pattern, values)
    assert.deepEqual(buffer, Buffer.from(bytes, 'hex'))
    let parsed = binaries.load(pattern, buffer)
    assert.deepEqual(parsed, values)
  }

  function assertEncodeArray (type, values, bytes) {
    let buffer = binaries.dumpArray(type, values)
    assert.deepEqual(buffer, Buffer.from(bytes, 'hex'))
    let parsed = binaries.loadArray(type, buffer)
    assert.deepEqual(parsed, values)
  }

  it('encodes a byte', () => {
    assertEncode(['u8'], [127], '7f')
  })

  it('encodes a u16', () => {
    assertEncode(['u16'], [37950], '943e')
  })

  it('encodes a u32', () => {
    assertEncode(['u32'], [4027821715], 'f013ae93')
  })

  it('encodes a u64', () => {
    assertEncode(['u64'], [13764230404643270000n], 'bf045ea08364ad70')
  })

  it('encodes a buffer', () => {
    assertEncode(['bytes'], [Buffer.from('deadbeef', 'hex')], 'deadbeef')
  })

  it('encodes two values', () => {
    assertEncode(['u32', 'u16'], [1497233282, 2588], '593df7820a1c')
  })

  it('encodes three values', () => {
    assertEncode(['u8', 'u32', 'u16'], [32, 1794881426, 43733], '206afbb792aad5')
  })

  it('encodes two numbers and a buffer', () => {
    assertEncode(
      ['u32', 'u16', 'bytes'],
      [3033088928, 22898, Buffer.from('beefcafe', 'hex')],
      'b4c943a05972beefcafe'
    )
  })

  it('encodes an array of bytes', () => {
    assertEncodeArray(
      'u8',
      [177, 9, 95, 165],
      'b1095fa5'
    )
  })

  it('encodes an array of u16', () => {
    assertEncodeArray(
      'u16',
      [20969, 1603, 14443, 11647],
      '51e90643386b2d7f'
    )
  })

  it('encodes an array of u32', () => {
    assertEncodeArray(
      'u32',
      [197488786, 4279211303, 1113928800, 3175803103],
      '0bc57092ff0f952742653460bd4ae8df'
    )
  })

  it('encodes an array of u64', () => {
    assertEncodeArray(
      'u64',
      [9821241840207857318n, 15662873413103057859n, 18095375158096578588n, 1657841687468294905n],
      '884c0f6b22cb06a6d95db4623deafbc3fb1fafd537c6481c1701d63b2b61c2f9'
    )
  })
})
