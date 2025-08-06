'use strict'

const { PathError } = require('./errors')

const SEP = '/'
const VALID_SEGMENT = /^[^/\0]+\/?$/
const NORM_MODE = 'NFC'

function parse (pathStr) {
  if (pathStr instanceof Path) {
    return pathStr
  } else {
    let path = pathStr.normalize(NORM_MODE)
    let parts = parseParts(path)
    return new Path(path, parts)
  }
}

function parseParts (path) {
  let parts = parseSegments(path)
  let links = []

  for (let i = 1; i < parts.length; i++) {
    let link = [parts.slice(0, i).join(''), parts[i]]
    links.push(link)
  }
  return links
}

function parseSegments (path) {
  return path.match(/\/|[^/]+\/?/g)
}

class Path {
  constructor (path, parts) {
    this._path = path
    this._parts = parts
  }

  isValid () {
    if (!this._path.startsWith(SEP)) return false
    return this._parts.every(([_, part]) => VALID_SEGMENT.test(part))
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

    let parts = parseSegments(tail.normalize(NORM_MODE))
    return parts.reduce((path, name) => path._join(name), this)
  }

  _join (name) {
    if (!VALID_SEGMENT.test(name)) {
      throw new PathError(`cannot join() to an invalid path segment: '${name}'`)
    }

    let parts = [...this._parts, [this._path, name]]
    return new Path(this._path + name, parts)
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
  parse,
  PathError
}
