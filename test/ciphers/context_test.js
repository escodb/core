'use strict'

const Context = require('../../lib/ciphers/context')
const { assert } = require('chai')

describe('Context', () => {
  it('adds keys with no prefix', () => {
    let ctx = Context.create(null, { a: 1 })
    assert.deepEqual(ctx.toObject(), { a: 1 })
  })

  it('adds keys with a prefix', () => {
    let ctx = Context.create('foo', { a: 1, b: 2 })
    assert.deepEqual(ctx.toObject(), { 'foo.a': 1, 'foo.b': 2 })
  })

  it('allows the prefix to be changed', () => {
    let ctx = Context.create('foo')
        .add({ a: 1, b: 2 })
        .prefix('bar')
        .add({ c: 3, d: 4 })

    assert.deepEqual(ctx.toObject(), {
      'foo.a': 1,
      'foo.b': 2,
      'bar.c': 3,
      'bar.d': 4
    })
  })

  it('creates fresh objects without mutation', () => {
    let foo = Context.create('foo', { a: 1, b: 2 })
    let bar = foo.prefix('bar').add({ c: 3, d: 4 })

    assert.deepEqual(foo.toObject(), {
      'foo.a': 1,
      'foo.b': 2
    })
    assert.deepEqual(foo.add({ x: 0 }).toObject(), {
      'foo.a': 1,
      'foo.b': 2,
      'foo.x': 0
    })
    assert.deepEqual(bar.toObject(), {
      'foo.a': 1,
      'foo.b': 2,
      'bar.c': 3,
      'bar.d': 4
    })
  })

  it('forbids adding a key that already exists', () => {
    let ctx = Context.create('foo', { a: 1 })
    assert.throws(() => ctx.add({ a: 1 }))
  })

  it('allows matching keys to be added with different prefixes', () => {
    let ctx = Context.create('foo', { a: 1 })
        .prefix('bar').add({ a: 2 })

    assert.deepEqual(ctx.toObject(), { 'foo.a': 1, 'bar.a': 2 })
  })

  it('serializes to canonical encoding', () => {
    let ctx = Context.create('foo', { a: 1, b: 2 })

    assert.equal(
      ctx.toBuffer().toString('base64'),
      'AAAAAAAAAAQAAAAAAAAABWZvby5hAAAAAAAAAAgAAAAAAAAAAQAAAAAAAAAFZm9vLmIAAAAAAAAACAAAAAAAAAAC')
  })
})
