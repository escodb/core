'use strict'

const Options = require('../options')

const PBKDF2_ITERATIONS = 600000
const DEFAULT_SHARDS = 4

function isPositiveInt (val) {
  return typeof val === 'number' && val > 0 && val === Math.round(val)
}

const OpenOptions = new Options({
  key: {
    password: {
      required: true,
      valid: (val) => typeof val === 'string' && val.length > 0,
      msg: 'must be a non-empty string'
    }
  }
})

const CreateOptions = new Options({
  password: {
    iterations: {
      required: false,
      default: PBKDF2_ITERATIONS,
      valid: isPositiveInt,
      msg: 'must be a positive integer'
    }
  },
  shards: {
    n: {
      required: false,
      default: DEFAULT_SHARDS,
      valid: isPositiveInt,
      msg: 'must be a positive integer'
    }
  }
})

module.exports = {
  OpenOptions,
  CreateOptions
}
