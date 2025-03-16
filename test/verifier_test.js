'use strict'

const Verifier = require('../lib/verifier')
const { randomBytes } = require('../lib/crypto')

const { assert } = require('chai')

describe('Verifier', () => {
  let verifier

  beforeEach(async () => {
    verifier = await Verifier.generate()
  })

  it('signs a payload', async () => {
    let payload = Buffer.from('trusted data', 'utf8')
    let signed = await verifier.sign(payload)

    assert.typeOf(signed, 'string')
    assert.match(signed, /^[a-z0-9/+]+=*\.[a-z0-9/+]+=*$/i)
  })

  it('parses a signed payload', async () => {
    let payload = Buffer.from('trusted data', 'utf8')
    let signed = await verifier.sign(payload)
    let parsed = await verifier.parse(signed)

    assert.equal(parsed.toString('utf8'), 'trusted data')
  })

  it('rejects a payload with a bad signature', async () => {
    let payload = Buffer.from('trusted data', 'utf8')
    let signature = randomBytes(32)
    let invalid = [payload, signature].map((buf) => buf.toString('base64')).join('.')

    let error = await verifier.parse(invalid).catch(e => e)
    assert.equal(error.code, 'ERR_AUTH_FAILED')
  })
})
