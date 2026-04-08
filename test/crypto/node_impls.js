'use strict'

const NodeCrypto = require('../../lib/crypto/node_crypto')

const impls = {
  node: NodeCrypto
}

const version = process.version.match(/\d+/g).map((n) => parseInt(n, 10))

if (version[0] >= 20) {
  const web_impls = require('./web_impls')
  Object.assign(impls, web_impls)
}

module.exports = impls
