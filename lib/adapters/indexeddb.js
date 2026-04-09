'use strict'

const { ConflictError } = require('../errors')
const Options = require('../options')

const IDB_VERSION = 1
const OBJECT_STORE = 'shards'

const IndexedDBOptions = new Options({
  name: { required: true }
})

class IndexedDBAdapter {
  static async create (options = {}) {
    options = IndexedDBOptions.parse(options)

    return new Promise((resolve, reject) => {
      let open = indexedDB.open(options.name, IDB_VERSION)

      open.onupgradeneeded = () => {
        let db = open.result
        db.createObjectStore(OBJECT_STORE, { keyPath: 'id' })
      }

      open.onsuccess = () => resolve(new IndexedDBAdapter(open.result))
      open.onerror = () => reject(open.error)
    })
  }

  constructor (db) {
    this._db = db
  }

  async read (id) {
    return new Promise((resolve, reject) => {
      let txn = this._db.transaction([OBJECT_STORE], 'readonly')
      txn.onerror = () => reject(txn.error)

      let store = txn.objectStore(OBJECT_STORE)
      let get = store.get(id)

      get.onsuccess = () => {
        if (get.result) {
          let { value, rev } = get.result
          resolve({ value, rev })
        } else {
          resolve(null)
        }
      }
    })
  }

  async write (id, value, rev = null) {
    return new Promise((resolve, reject) => {
      let txn = this._db.transaction([OBJECT_STORE], 'readwrite')
      txn.onerror = () => reject(txn.error)

      let store = txn.objectStore(OBJECT_STORE)
      let get = store.get(id)

      get.onsuccess = () => {
        let expected = get.result ? get.result.rev : null
        if (rev !== expected) return reject(new ConflictError())

        rev = (rev || 0) + 1
        let put = store.put({ id, value, rev })
        put.onsuccess = () => resolve({ rev })
      }
    })
  }
}

module.exports = IndexedDBAdapter
