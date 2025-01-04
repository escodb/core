'use strict'

const Mutex = require('../../lib/sync/mutex')
const { assert } = require('chai')

class Counter {
  constructor () {
    this._value = 0
  }

  get () {
    return this._value
  }

  async inc () {
    let value = this._value
    await null
    this._value = value + 1
  }
}

describe('Mutex', () => {
  let counter, mutex

  beforeEach(() => {
    counter = new Counter()
    mutex = new Mutex()
  })

  it('is inconsistent without a mutex', async () => {
    await Promise.all([
      counter.inc(),
      counter.inc(),
      counter.inc()
    ])

    assert.equal(counter.get(), 1)
  })

  it('is consistent using a mutex', async () => {
    await Promise.all([
      mutex.lock(() => counter.inc()),
      mutex.lock(() => counter.inc()),
      mutex.lock(() => counter.inc())
    ])

    assert.equal(counter.get(), 3)
  })

  it('forces functions to execute sequentially', async () => {
    let logs = []

    await Promise.all([
      mutex.lock(async () => {
        logs.push('a')
        await null
        logs.push('b')
      }),
      mutex.lock(async () => {
        logs.push('c')
        await null
        logs.push('d')
      })
    ])

    assert.deepEqual(logs, ['a', 'b', 'c', 'd'])
  })

  it('returns the result of each function', async () => {
    let results = await Promise.all([
      mutex.lock(async () => {
        await counter.inc()
        return counter.get()
      }),
      mutex.lock(async () => {
        await counter.inc()
        return counter.get()
      })
    ])

    assert.deepEqual(results, [1, 2])
  })

  it('returns an error thrown by a function', async () => {
    let error = await mutex.lock(() => {
      throw new Error('oh no')
    }).catch(e => e)

    assert.equal(error.message, 'oh no')
  })

  it('executes other functions after an error is thrown', async () => {
    let results = await Promise.all([
      mutex.lock(() => counter.inc()),

      mutex.lock(() => {
        throw new Error('oh no')
      }).catch(e => e),

      mutex.lock(() => counter.inc())
    ])

    assert.equal(counter.get(), 2)
    assert.equal(results[1].message, 'oh no')
  })
})
