'use strict'

const { sha256 } = require('../crypto')
const fs = require('fs').promises
const path = require('path')

const ConflictError = require('./conflict_error')
const Options = require('../options')

const { O_CREAT, O_EXCL, O_RDWR } = require('fs').constants

const HASH_FORMAT = 'base64'

const FileOptions = new Options({
  fsync: {
    required: false,
    default: true,
    valid: (val) => typeof val === 'boolean',
    msg: 'must be true or false'
  }
})

async function hash (value) {
  value = Buffer.from(value, 'utf8')
  let hash = await sha256.digest(value)
  return hash.toString(HASH_FORMAT)
}

class FileAdapter {
  constructor (dir, options = {}) {
    this._dir = dir
    this._options = FileOptions.parse(options)
  }

  async read (id) {
    let filepath = path.resolve(this._dir, id)

    try {
      let value = await fs.readFile(filepath, 'utf8')
      let rev = await hash(value)

      return { value, rev }

    } catch (error) {
      if (error.code === 'ENOENT') {
        return null
      } else {
        throw error
      }
    }
  }

  async write (id, value, rev = null) {
    let filepath = path.resolve(this._dir, id)
    let lockpath = filepath + '.lock'

    let file = await this._openFile(lockpath)

    let record = await this.read(id)
    let expect = record ? record.rev : null

    if (rev !== expect) {
      await file.close()
      await fs.unlink(lockpath)
      throw new ConflictError()
    }

    await file.write(value, 0, 'utf8')

    if (this._options.fsync) {
      await file.sync()
    }
    await file.close()
    await fs.rename(lockpath, filepath)

    rev = await hash(value)
    return { rev }
  }

  async _openFile (lockpath) {
    try {
      return await fs.open(lockpath, O_RDWR | O_CREAT | O_EXCL)

    } catch (error) {
      if (error.code === 'ENOENT') {
        await fs.mkdir(path.dirname(lockpath), { recursive: true })
        return this._openFile(lockpath)
      }
      if (error.code === 'EEXIST') {
        throw new ConflictError()
      } else {
        throw error
      }
    }
  }
}

module.exports = FileAdapter
