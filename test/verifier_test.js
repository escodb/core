'use strict'

const { Buffer } = require('@escodb/buffer')

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
    let signed = await verifier.sign({ payload })

    assert.typeOf(signed, 'string')
    assert.match(signed, /^[a-z0-9/+]+=*$/i)
  })

  it('verifies a signed payload', async () => {
    let payload = Buffer.from('trusted data', 'utf8')
    let signed = await verifier.sign({ payload })

    await verifier.verify({ payload }, signed)
  })

  it('rejects a payload with a bad signature', async () => {
    let payload = Buffer.from('trusted data', 'utf8')
    let signature = randomBytes(32)

    let error = await verifier.verify({ payload }, signature).catch(e => e)
    assert.equal(error.code, 'ERR_AUTH_FAILED')
  })

  it('signs a payload with a context', async () => {
    let payload = Buffer.from('trusted data', 'utf8')
    let signed = await verifier.sign({ payload, foo: 'bar' })

    assert.typeOf(signed, 'string')
    assert.match(signed, /^[a-z0-9/+]+=*$/i)
  })

  it('verifies a signed payload with context', async () => {
    let payload = Buffer.from('trusted data', 'utf8')
    let signed = await verifier.sign({ payload, foo: 'bar' })

    await verifier.verify({ payload, foo: 'bar' }, signed)
  })

  it('rejects a payload the wrong context', async () => {
    let payload = Buffer.from('trusted data', 'utf8')
    let signed = await verifier.sign({ payload, foo: 'bar' })

    let error = await verifier.verify({ payload, bar: 'foo' }, signed).catch(e => e)
    assert.equal(error.code, 'ERR_AUTH_FAILED')
  })
})
