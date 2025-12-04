'use strict'

const Queue = require('../../lib/sync/queue')
const { assert } = require('chai')
const { logger } = require('./utils')

describe('Queue', () => {
  let queue, logs

  beforeEach(() => {
    logs = []
  })

  describe('with no limit', () => {
    beforeEach(() => {
      queue = new Queue()
    })

    it('executes a single function', async () => {
      let result = await queue.push(() => 42)
      assert.equal(result, 42)
    })

    it('returns an error thrown by the function', async () => {
      let error = await queue.push(() => {
        throw new Error('oh no')
      }).catch(e => e)

      assert.equal(error.message, 'oh no')
    })

    it('executes functions concurrently', async () => {
      await Promise.all([
        queue.push(logger(logs, 'a', 'b')),
        queue.push(logger(logs, 'c', 'd')),
        queue.push(logger(logs, 'e', 'f')),
        queue.push(logger(logs, 'g', 'h'))
      ])

      assert.deepEqual(logs, ['a', 'c', 'e', 'g', 'b', 'd', 'f', 'h'])
    })
  })

  describe('with a limit of 1', () => {
    beforeEach(() => {
      queue = new Queue({ limit: 1 })
    })

    it('executes functions sequentially', async () => {
      await Promise.all([
        queue.push(logger(logs, 'a', 'b')),
        queue.push(logger(logs, 'c', 'd')),
        queue.push(logger(logs, 'e', 'f')),
        queue.push(logger(logs, 'g', 'h'))
      ])

      assert.deepEqual(logs, ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'])
    })
  })

  describe('with a limit greater than 1', () => {
    beforeEach(() => {
      queue = new Queue({ limit: 3 })
    })

    it('executes functions concurrently up to the limit', async () => {
      await Promise.all([
        queue.push(logger(logs, 'a', 'b')),
        queue.push(logger(logs, 'c', 'd')),
        queue.push(logger(logs, 'e', 'f')),
        queue.push(logger(logs, 'g', 'h'))
      ])

      assert.deepEqual(logs, ['a', 'c', 'e', 'b', 'd', 'f', 'g', 'h'])
    })

    it('resets the capacity when the queue drains', async () => {
      await Promise.all([
        queue.push(logger(logs, 'w', 'x')),
        queue.push(logger(logs, 'y', 'z'))
      ])

      await Promise.all([
        queue.push(logger(logs, 'a', 'b')),
        queue.push(logger(logs, 'c', 'd')),
        queue.push(logger(logs, 'e', 'f')),
        queue.push(logger(logs, 'g', 'h'))
      ])

      assert.deepEqual(logs, [
        'w', 'y', 'x', 'z',
        'a', 'c', 'e', 'b', 'd', 'f', 'g', 'h'
      ])
    })
  })

  describe('onEmpty()', () => {
    beforeEach(() => {
      queue = new Queue({ limit: 3 })
    })

    it('resolves when the queue is empty', async () => {
      await queue.onEmpty()
    })

    it('resolves after a single task is processed', async () => {
      queue.push(logger(logs, 'a', 'b'))
      await queue.onEmpty().then(() => logs.push('z'))

      assert.deepEqual(logs, ['a', 'b', 'z'])
    })

    it('resolves after a multiple tasks', async () => {
      queue.push(logger(logs, 'a', 'b'))
      queue.push(logger(logs, 'c', 'd'))
      queue.push(logger(logs, 'e', 'f'))

      await queue.onEmpty().then(() => logs.push('z'))

      assert.deepEqual(logs, ['a', 'c', 'e', 'b', 'd', 'f', 'z'])
    })

    it('waits until all tasks are handled', async () => {
      queue.push(logger(logs, 'a', 'b'))
      queue.push(logger(logs, 'c', 'd'))
      queue.push(logger(logs, 'e', 'f'))

      let empty = queue.onEmpty().then(() => logs.push('z'))

      queue.push(logger(logs, 'g', 'h'))
      queue.push(logger(logs, 'i', 'j'))

      await empty

      assert.deepEqual(logs, [
        'a', 'c', 'e', 'b', 'd', 'f',
        'g', 'i', 'h', 'j',
        'z'
      ])
    })

    it('resolves when all current tasks are awaited', async () => {
      let tasks = Promise.all([
        queue.push(logger(logs, 'a', 'b')),
        queue.push(logger(logs, 'c', 'd')),
        queue.push(logger(logs, 'e', 'f')),
        queue.push(logger(logs, 'g', 'h'))
      ])

      queue.onEmpty().then(() => logs.push('z'))
      await tasks

      queue.push(logger(logs, 'i', 'j'))
      await queue.push(logger(logs, 'k', 'l'))

      assert.deepEqual(logs, [
        'a', 'c', 'e', 'b', 'd', 'f', 'g', 'h',
        'z',
        'i', 'k', 'j', 'l'
      ])
    })

    it('resolves correctly with a limit of 1', async () => {
      let queue = new Queue({ limit: 1 })

      queue.push(logger(logs, 'a', 'b'))
      let empty = queue.onEmpty().then(() => logs.push('z'))
      queue.push(logger(logs, 'c', 'd'))

      await empty

      assert.deepEqual(logs, ['a', 'b', 'c', 'd', 'z'])
    })
  })
})
