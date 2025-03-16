'use strict'

class NullCipher {
  static async create () {
    return new NullCipher()
  }

  async encrypt (data) {
    return data
  }

  async decrypt (data) {
    return data
  }
}

module.exports = NullCipher
