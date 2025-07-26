'use strict'

const { Config } = require('./config')
const Mutex = require('./sync/mutex')
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
    this._mutex = new Mutex()
  }

  task () {
    let env = { cipher: this._cipher, verifier: this._verifier }
    return new Task(this._adapter, this._router, env)
  }
}

const READ_METHODS = ['get', 'list', 'find']
const WRITE_METHODS = ['update', 'remove', 'prune']

for (let method of READ_METHODS) {
  StoreHandle.prototype[method] = function (...args) {
    let task = this.task()
    return task[method](...args)
  }
}

for (let method of WRITE_METHODS) {
  StoreHandle.prototype[method] = function (...args) {
    return this._mutex.lock(() => {
      let task = this.task()
      return task[method](...args)
    })
  }
}

module.exports = Store
