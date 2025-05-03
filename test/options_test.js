'use strict'

const Options = require('../lib/options')
const { OpenOptions, CreateOptions } = require('../lib/config')
const { assert } = require('chai')

describe('merge()', () => {
  it('merges two flat objects', () => {
    let merged = Options.merge(
      { a: 1, b: 2 },
      { c: 3 }
    )
    assert.deepEqual(merged, {
      a: 1, b: 2, c: 3
    })
  })

  it('merges nested objects', () => {
    let merged = Options.merge(
      { x: { a: 1, b: 2 } },
      { x: { c: 3 } }
    )
    assert.deepEqual(merged, {
      x: { a: 1, b: 2, c: 3 }
    })
  })

  it('merges a deep structure inside a nested field', () => {
    let merged = Options.merge(
      { x: { a: 1, b: 2 } },
      { x: { c: { d: { e: 3 } } } }
    )
    assert.deepEqual(merged, {
      x: { a: 1, b: 2, c: { d: { e: 3 } } }
    })
  })

  it('is commutative', () => {
    let merged = Options.merge(
      { x: { c: { d: { e: 3 } } } },
      { x: { a: 1, b: 2 } }
    )
    assert.deepEqual(merged, {
      x: { a: 1, b: 2, c: { d: { e: 3 } } }
    })
  })

  it('does not overwrite values with objects', () => {
    let merged = Options.merge(
      { x: { a: { b: 0 } } },
      { x: { a: { b: { c: 1 }, d: 2 } } }
    )
    assert.deepEqual(merged, {
      x: { a: { b: 0, d: 2 } }
    })
  })
})

describe('OpenOptions', () => {
  it('parses a valid set of options', () => {
    let options = OpenOptions.parse({ key: { password: 'hello' } })

    assert.deepEqual(options, {
      key: { password: 'hello' }
    })
  })

  it('fails if password is the wrong type', () => {
    assert.throws(() => OpenOptions.parse({ key: { password: 42 } }))
  })

  it('fails if password is missing', () => {
    assert.throws(() => OpenOptions.parse({ key: {} }))
  })

  it('fails if password parent section is missing', () => {
    assert.throws(() => OpenOptions.parse({}))
  })

  it('fails if unrecognised options are given', () => {
    assert.throws(() => OpenOptions.parse({ key: { password: 'a', nope: 1 } }))
  })
})

describe('CreateOptions', () => {
  it('sets default options', () => {
    let options = CreateOptions.parse({ key: { password: 'hi' } })

    assert.equal(options.key.password, 'hi')
    assert.equal(options.password.iterations, 600000)
    assert.equal(options.shards.n, 2)
  })

  it('sets optional parameters', () => {
    let options = CreateOptions.parse({
      key: { password: 'hi' },
      password: { iterations: 4200 },
      shards: { n: 5 }
    })

    assert.equal(options.password.iterations, 4200)
    assert.equal(options.shards.n, 5)
  })

  it('fails if password.iterations is negative', () => {
    assert.throws(() => CreateOptions.parse({ key: { password: 'a' }, password: { iterations: -1 } }))
  })

  it('fails if shards.n is negative', () => {
    assert.throws(() => CreateOptions.parse({ key: { password: 'a' }, shards: { n: -1 } }))
  })
})
