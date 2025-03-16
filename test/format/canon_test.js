'use strict'

const { assert } = require('chai')
const canon = require('../../lib/format/canon')

describe('canon', () => {
  it('encodes a single value', () => {
    let buf = canon.encode({ hello: 'world' })
    assert.equal(
      buf.toString('hex'),
      '0000000000000002000000000000000568656c6c6f0000000000000005776f726c64')
  })

  it('encodes numbers as decimal strings', () => {
    let buf = canon.encode({ n: 42 })
    assert.equal(
      buf.toString('hex'),
      '000000000000000200000000000000016e00000000000000023432')
  })

  it('encodes multiple key-value pairs', () => {
    let buf = canon.encode({ hello: 'world', n: 42 })
    assert.equal(
      buf.toString('hex'),
      '0000000000000004000000000000000568656c6c6f0000000000000005776f726c6400000000000000016e00000000000000023432')
  })

  it('sorts multi-valued contexts by key', () => {
    let buf = canon.encode({ n: 42, hello: 'world' })
    assert.equal(
      buf.toString('hex'),
      '0000000000000004000000000000000568656c6c6f0000000000000005776f726c6400000000000000016e00000000000000023432')
  })
})
