'use strict'

const { PathError } = require('./errors')

const SEP = '/'
const MULTI_SEP = /\/+/g

function parse (path) {
  let parts = path.split(SEP)
  let links = []

  for (let i = 0; i < parts.length - 1; i++) {
    parts[i] += SEP
  }
  if (parts[parts.length - 1] === '') {
    parts.pop()
  }
  for (let i = 1; i < parts.length; i++) {
    let link = [parts.slice(0, i).join(''), parts[i]]
    links.push(link)
  }
  return links
}

function resolve (dir, tail) {
  return (dir + SEP + tail).replace(MULTI_SEP, SEP)
}

class Path {
  static parse (pathStr) {
    if (pathStr instanceof Path) {
      return pathStr
    } else {
      return new Path(pathStr)
    }
  }

  constructor (path, parts = null) {
    this._path = path
    this._parts = parts || parse(path)
  }

  isValid () {
    return this._path.startsWith(SEP)
  }

  isDir () {
    return this._path.endsWith(SEP)
  }

  isDoc () {
    return !this._path.endsWith(SEP)
  }

  full () {
    return this._path
  }

  dirname () {
    return this._lastSegment(0)
  }

  basename () {
    return this._lastSegment(1)
  }

  _lastSegment (n) {
    if (this._parts.length === 0) {
      return null
    } else {
      let last = this._parts[this._parts.length - 1]
      return last[n]
    }
  }

  dirs () {
    return this._parts.map(([dir]) => dir)
  }

  links () {
    return this._parts.slice()
  }

  join (tail) {
    if (!this.isDir()) {
      throw new PathError(`cannot join() a non-directory path: '${this._path}'`)
    }

    let joined = (this._path + SEP + tail).replace(MULTI_SEP, SEP)
    return Path.parse(joined)
  }

  relative (other) {
    let ofs = 0

    while (true) {
      if (ofs >= this._parts.length) break
      if (ofs >= other._parts.length) break
      if (this._parts[ofs][1] !== other._parts[ofs][1]) break
      ofs += 1
    }

    let len = Math.max(other._parts.length - ofs - 1, 0)
    let up = new Array(len).fill('..' + SEP)

    let down = this._parts.slice(ofs).map(([_, name]) => name)

    if (up.length + down.length === 0) {
      down = ['.' + SEP]
    }

    return [...up, ...down].join('')
  }
}

module.exports = {
  Path,
  PathError
}
