'use strict'

const { assert } = require('chai')
const binaries = require('../lib/binaries')

describe('binaries', () => {
  function assertEncode (pattern, values, bytes) {
    let buffer = binaries.dump(pattern, values)
    assert.deepEqual(buffer, Buffer.from(bytes, 'hex'))
    let parsed = binaries.load(pattern, buffer)
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
})
