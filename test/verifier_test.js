'use strict'

const Context = require('../lib/ciphers/context')
const Verifier = require('../lib/verifier')
const { randomBytes } = require('../lib/crypto')

const { assert } = require('chai')
const { generate } = require('./utils')

describe('Verifier', () => {
  let verifier

  beforeEach(async () => {
    verifier = await generate(Verifier)
  })

  it('signs a payload', async () => {
    let payload = Buffer.from('trusted data', 'utf8')
    let ctx = Context.create(null, { payload })
    let signed = await verifier.sign(ctx)

    assert.typeOf(signed, 'string')
    assert.match(signed, /^[a-z0-9/+]+=*$/i)
  })

  it('verifies a signed payload', async () => {
    let payload = Buffer.from('trusted data', 'utf8')
    let ctx = Context.create(null, { payload })
    let signed = await verifier.sign(ctx)

    await verifier.verify(ctx, signed)
  })

  it('rejects a payload with a bad signature', async () => {
    let payload = Buffer.from('trusted data', 'utf8')
    let ctx = Context.create(null, { payload })
    let signature = randomBytes(32)

    let error = await verifier.verify(ctx, signature).catch(e => e)
    assert.equal(error.code, 'ERR_AUTH_FAILED')
  })

  it('signs a payload with a context', async () => {
    let payload = Buffer.from('trusted data', 'utf8')
    let ctx = Context.create(null, { payload, foo: 'bar' })
    let signed = await verifier.sign(ctx)

    assert.typeOf(signed, 'string')
    assert.match(signed, /^[a-z0-9/+]+=*$/i)
  })

  it('verifies a signed payload with context', async () => {
    let payload = Buffer.from('trusted data', 'utf8')
    let ctx = Context.create(null, { payload, foo: 'bar' })
    let signed = await verifier.sign(ctx)

    await verifier.verify(ctx, signed)
  })

  it('rejects a payload with the wrong context', async () => {
    let payload = Buffer.from('trusted data', 'utf8')
    let ctx = Context.create(null, { payload, foo: 'bar' })
    let signed = await verifier.sign(ctx)

    let error = await verifier.verify(ctx.add({ bar: 'foo' }), signed).catch(e => e)
    assert.equal(error.code, 'ERR_AUTH_FAILED')
  })
})
