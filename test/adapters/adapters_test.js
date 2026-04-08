'use strict'

const { assert } = require('chai')
const { testWithAdapters } = require('./utils')

testWithAdapters('Adapter', (impl) => {
  let adapter

  beforeEach(async () => {
    adapter = await impl.createAdapter()
  })

  afterEach(impl.cleanup)

  it('returns null for an unknown key', async () => {
    let result = await adapter.read('x')
    assert.isNull(result)
  })

  it('allows creation with no rev', async () => {
    await adapter.write('x', 'hello')

    let result = await adapter.read('x')
    assert.strictEqual(result.value, 'hello')
  })

  it('allows creation with null rev', async () => {
    await adapter.write('x', 'hello', null)

    let result = await adapter.read('x')
    assert.strictEqual(result.value, 'hello')
  })

  it('does not allow creation with a bad rev', async () => {
    let error = await adapter.write('x', 'hello', 64).catch(e => e)
    assert.strictEqual(error.code, 'ERR_CONFLICT')
  })

  it('allows updating using the rev from write()', async () => {
    let { rev } = await adapter.write('x', 'hello')
    await adapter.write('x', 'world', rev)

    let result = await adapter.read('x')
    assert.strictEqual(result.value, 'world')
    assert.notEqual(result.rev, rev)
  })

  it('allows updating using the rev from read()', async () => {
    await adapter.write('x', 'hello')

    let { rev } = await adapter.read('x')
    await adapter.write('x', 'world', rev)

    let result = await adapter.read('x')
    assert.strictEqual(result.value, 'world')
    assert.notEqual(result.rev, rev)
  })

  it('does not allow updating with no rev', async () => {
    await adapter.write('x', 'hello')

    let error = await adapter.write('x', 'world').catch(e => e)
    assert.strictEqual(error.code, 'ERR_CONFLICT')
  })

  it('does not allow updating with a bad rev', async () => {
    await adapter.write('x', 'hello')

    let error = await adapter.write('x', 'world', 64).catch(e => e)
    assert.strictEqual(error.code, 'ERR_CONFLICT')
  })

  it('only allows one concurrent write to succeed', async () => {
    await adapter.write('x', 'hello')
    let { rev } = await adapter.read('x')

    let [w1, w2] = await Promise.all([
      adapter.write('x', 'world', rev).catch(e => e),
      adapter.write('x', 'other', rev).catch(e => e)
    ])

    let [passed, failed] = w1.rev ? [w1, w2] : [w2, w1]

    assert.isDefined(passed.rev)
    assert.strictEqual(failed.code, 'ERR_CONFLICT')

    let result = await adapter.read('x')
    assert.strictEqual(result.value, w1.rev ? 'world' : 'other')
  })
})
