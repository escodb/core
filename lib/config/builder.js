'use strict'

const { Cell, NullCodec } = require('../cell')
const { AccessDenied } = require('../errors')

const KEY_FORMAT = 'base64'

class Builder {
  constructor (cipher, context, sections) {
    this._cipher = cipher
    this._context = context
    this._sections = sections
  }

  async generate (name, options) {
    let key = await this._sections[name].generateKey()
    let params = options[name]

    let context = this._getContext(name, params)
    let cell = new Cell(this._cipher, NullCodec, { context, format: KEY_FORMAT })

    cell.set(key)
    return { key: await cell.serialize(), ...params }
  }

  async load (name, config) {
    let Class = this._sections[name]
    let { key: data, ...params } = config[name]

    let context = this._getContext(name, params)
    let cell = new Cell(this._cipher, NullCodec, { context, data, format: KEY_FORMAT })

    try {
      let key = await cell.get()
      return new Class({ key, ...params })
    } catch (cause) {
      throw new AccessDenied('could not unlock the store; make sure the password is correct', { cause })
    }
  }

  _getContext (name, params = {}) {
    let scope = 'keys.' + name
    return this._context.add({ scope }).prefix(name).add(params)
  }
}

module.exports = Builder
