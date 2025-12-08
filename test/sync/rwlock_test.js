'use strict'

const RWLock = require('../../lib/sync/rwlock')
const { assert } = require('chai')
const { logger } = require('./utils')

describe('RWLock', () => {
  let rwlock, logs

  beforeEach(() => {
    rwlock = new RWLock()
    logs = []
  })

  it('allows concurrent access for reads', async () => {
    await Promise.all([
      rwlock.read(logger(logs, 'a', 'b')),
      rwlock.read(logger(logs, 'c', 'd'))
    ])

    assert.deepEqual(logs, ['a', 'c', 'b', 'd'])
  })

  it('forbids concurrent access for writes', async () => {
    await Promise.all([
      rwlock.write(logger(logs, 'a', 'b')),
      rwlock.write(logger(logs, 'c', 'd'))
    ])

    assert.deepEqual(logs, ['a', 'b', 'c', 'd'])
  })

  it('forbids concurrent access for reads and writes', async () => {
    await Promise.all([
      rwlock.read(logger(logs, 'a', 'b')),
      rwlock.write(logger(logs, 'c', 'd'))
    ])

    assert.deepEqual(logs, ['a', 'b', 'c', 'd'])
  })

  it('allows multiple concurrent reads that delay write access', async () => {
    await Promise.all([
      rwlock.read(logger(logs, 'a', 'b')),
      rwlock.read(logger(logs, 'c', 'd')),
      rwlock.write(logger(logs, 'e', 'f'))
    ])

    assert.deepEqual(logs, ['a', 'c', 'b', 'd', 'e', 'f'])
  })

  it('delays multiple reads behind a write', async () => {
    await Promise.all([
      rwlock.write(logger(logs, 'a', 'b')),
      rwlock.read(logger(logs, 'c', 'd')),
      rwlock.read(logger(logs, 'e', 'f'))
    ])

    assert.deepEqual(logs, ['a', 'b', 'c', 'e', 'd', 'f'])
  })

  it('delays a read behind multiple writes', async () => {
    await Promise.all([
      rwlock.write(logger(logs, 'a', 'b')),
      rwlock.write(logger(logs, 'c', 'd')),
      rwlock.read(logger(logs, 'e', 'f'))
    ])

    assert.deepEqual(logs, ['a', 'b', 'c', 'd', 'e', 'f'])
  })

  it('forces sequential execution around a write', async () => {
    await Promise.all([
      rwlock.read(logger(logs, 'a', 'b')),
      rwlock.write(logger(logs, 'c', 'd')),
      rwlock.read(logger(logs, 'e', 'f'))
    ])

    assert.deepEqual(logs, ['a', 'b', 'c', 'd', 'e', 'f'])
  })

  it('returns the result of each function', async () => {
    let results = await Promise.all([
      rwlock.write(() => 'a'),
      rwlock.read(() => 'b'),
      rwlock.read(() => 'c')
    ])

    assert.deepEqual(results, ['a', 'b', 'c'])
  })

  it('returns errors thrown by functions', async () => {
    let results = await Promise.all([
      rwlock.write(() => 'a'),
      rwlock.read(() => { throw new Error('oh no') }).catch(e => e.message),
      rwlock.read(() => 'c')
    ])

    assert.deepEqual(results, ['a', 'oh no', 'c'])
  })
})
