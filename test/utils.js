'use strict'

async function generate (Class, config = {}) {
  let key = await Class.generateKey()
  return new Class({ ...config, key })
}

module.exports = {
  generate
}
