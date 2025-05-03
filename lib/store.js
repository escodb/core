'use strict'

const { Config } = require('./config')
const Task = require('./task')

class Store {
  constructor (adapter, openOpts = {}) {
    this._adapter = adapter
    this._openOpts = openOpts
  }

  async create (createOpts = {}) {
    let config = await Config.create(this._adapter, this._openOpts, createOpts)
    return this._newHandle(config)
  }

  async open () {
    let config = await Config.open(this._adapter, this._openOpts)
    return this._newHandle(config)
  }

  async openOrCreate (createOpts = {}) {
    let config = await Config.openOrCreate(this._adapter, this._openOpts, createOpts)
    return this._newHandle(config)
  }

  async _newHandle (config) {
    let cipher = await config.buildCipher()
    let verifier = await config.buildVerifier()
    let router = await config.buildRouter()

    return new StoreHandle(this._adapter, router, cipher, verifier)
  }
}

class StoreHandle {
  constructor (adapter, router, cipher, verifier) {
    this._adapter = adapter
    this._router = router
    this._cipher = cipher
    this._verifier = verifier
  }

  task () {
    return new Task(this._adapter, this._router, this._cipher, this._verifier)
  }
}

const TASK_METHODS = ['get', 'list', 'find', 'update', 'remove', 'prune']

for (let method of TASK_METHODS) {
  StoreHandle.prototype[method] = function (...args) {
    let task = this.task()
    return task[method](...args)
  }
}

module.exports = Store
