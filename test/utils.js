'use strict'

async function assertOneOf (outcomes) {
  let passing = []
  let failing = []

  for (let [name, fn] of Object.entries(outcomes)) {
    try {
      await fn()
      passing.push(name)
    } catch (error) {
      failing.push(name)
    }
  }

  if (passing.length === 0) {
    throw new Error('none of the expected outcomes came to pass')
  }
  if (passing.length > 1) {
    throw new Error(`more than one outcome came to pass: ${passing.join(', ')}`)
  }
}

async function generate (Class, config = {}) {
  let key = await Class.generateKey()
  return new Class({ ...config, key })
}

module.exports = {
  assertOneOf,
  generate
}
