'use strict'

const Path = require('../lib/path')
const { assert } = require('chai')

describe('Path', () => {
  describe('isValid()', () => {
    it('returns true if the path begins with a slash', () => {
      let path = Path.parse('/foo')
      assert(path.isValid())
    })

    it('returns false if the path does not begin with a slash', () => {
      let path = Path.parse('foo')
      assert(!path.isValid())
    })

    it('returns true if the path contains allowed characters', () => {
      let path = Path.parse('/a-b_c.0')
      assert(path.isValid())
    })

    it('returns false if the path contains disallowed characters', () => {
      let path = Path.parse('/a:b')
      assert(!path.isValid())
    })

    it('returns false if the path contains empty segments', () => {
      let path = Path.parse('/a//b')
      assert(!path.isValid())
    })

    it('returns false if the path ends with an empty segment', () => {
      let path = Path.parse('/a/b//')
      assert(!path.isValid())
    })
  })

  describe('isDir()', () => {
    it('returns true if the path is a directory', () => {
      let path = Path.parse('/foo/')
      assert(path.isDir())
    })

    it('returns false if the path is not a directory', () => {
      let path = Path.parse('/foo')
      assert(!path.isDir())
    })
  })

  describe('isDoc()', () => {
    it('returns true if the path is a document', () => {
      let path = Path.parse('/foo')
      assert(path.isDoc())
    })

    it('returns false if the path is not a document', () => {
      let path = Path.parse('/foo/')
      assert(!path.isDoc())
    })
  })

  describe('full()', () => {
    it('returns the full path for a document', () => {
      let path = Path.parse('/path/to/x.json')
      assert.equal(path.full(), '/path/to/x.json')
    })

    it('returns the full path for a directory', () => {
      let path = Path.parse('/path/to/')
      assert.equal(path.full(), '/path/to/')
    })
  })

  describe('dirname()', () => {
    it('returns the parent directory for a document', () => {
      let path = Path.parse('/path/to/x.json')
      assert.equal(path.dirname(), '/path/to/')
    })

    it('returns the parent directory for a top-level document', () => {
      let path = Path.parse('/x.json')
      assert.equal(path.dirname(), '/')
    })

    it('returns the parent directory of a directory', () => {
      let path = Path.parse('/path/to/')
      assert.equal(path.dirname(), '/path/')
    })

    it('returns the parent directory of the root directory', () => {
      let path = Path.parse('/')
      assert.isNull(path.dirname())
    })
  })

  describe('basename()', () => {
    it('returns the base name for a document', () => {
      let path = Path.parse('/path/to/x.json')
      assert.equal(path.basename(), 'x.json')
    })

    it('returns the base name for a top-level document', () => {
      let path = Path.parse('/x.json')
      assert.equal(path.basename(), 'x.json')
    })

    it('returns the base name of a directory', () => {
      let path = Path.parse('/path/to/')
      assert.equal(path.basename(), 'to/')
    })

    it('returns the base name of the root directory', () => {
      let path = Path.parse('/')
      assert.isNull(path.basename())
    })
  })

  describe('dirs()', () => {
    it('returns the parent directories for a document', () => {
      let path = Path.parse('/path/to/x.json')
      assert.deepEqual(path.dirs(), ['/', '/path/', '/path/to/'])
    })

    it('returns the parent directories for a directory', () => {
      let path = Path.parse('/path/to/')
      assert.deepEqual(path.dirs(), ['/', '/path/'])
    })
  })

  describe('links()', () => {
    it('returns the required links for a document', () => {
      let path = Path.parse('/path/to/x.json')
      assert.deepEqual(path.links(), [['/', 'path/'], ['/path/', 'to/'], ['/path/to/', 'x.json']])
    })

    it('returns the required links for a directory', () => {
      let path = Path.parse('/path/to/')
      assert.deepEqual(path.links(), [['/', 'path/'], ['/path/', 'to/']])
    })
  })

  describe('join()', () => {
    it('joins a directory with a document name', () => {
      let path = Path.parse('/path/to/')
      let joined = path.join('x')
      assert.equal(joined.full(), '/path/to/x')
      assert.deepEqual(joined.links(), [['/', 'path/'], ['/path/', 'to/'], ['/path/to/', 'x']])
    })

    it('joins a directory with a directory name', () => {
      let path = Path.parse('/path/to/')
      let joined = path.join('x/')
      assert.equal(joined.full(), '/path/to/x/')
      assert.deepEqual(joined.links(), [['/', 'path/'], ['/path/', 'to/'], ['/path/to/', 'x/']])
    })

    it('joins a nested document name', () => {
      let path = Path.parse('/path/to/')
      let joined = path.join('nested/x')
      assert.equal(joined.full(), '/path/to/nested/x')
      assert.deepEqual(joined.links(), [['/', 'path/'], ['/path/', 'to/'], ['/path/to/', 'nested/'], ['/path/to/nested/', 'x']])
    })

    it('does not affect the original path', () => {
      let path = Path.parse('/path/to/')
      let joined = path.join('x')
      assert.equal(path.full(), '/path/to/')
      assert.deepEqual(path.links(), [['/', 'path/'], ['/path/', 'to/']])
    })

    it('throws an error trying to join a document path', () => {
      let path = Path.parse('/path/to')
      assert.throws(() => path.join('x'))
    })

    it('throws an error trying to join to an empty segment', () => {
      let path = Path.parse('/path/to/')
      assert.throws(() => path.join(''))
    })

    it('throws an error trying to join to an invalid segment', () => {
      let path = Path.parse('/path/to/')
      assert.throws(() => path.join('a//b'))
    })
  })

  describe('relative()', () => {
    it('returns the relative path of a child document', () => {
      let dir = Path.parse('/path/to/')
      let doc = Path.parse('/path/to/x.json')
      assert.equal(doc.relative(dir), 'x.json')
    })

    it('returns the relative path of a sibling document', () => {
      let x = Path.parse('/path/to/x.json')
      let y = Path.parse('/path/to/y.json')
      assert.equal(y.relative(x), 'y.json')
    })

    it('returns the relative path of a grandchild document', () => {
      let dir = Path.parse('/path/to/')
      let doc = Path.parse('/path/to/nested/x.json')
      assert.equal(doc.relative(dir), 'nested/x.json')
    })

    it('returns the relative path of a child directory', () => {
      let a = Path.parse('/path/to/')
      let b = Path.parse('/path/to/x/')
      assert.equal(b.relative(a), 'x/')
    })

    it('returns the relative path of a parent directory', () => {
      let dir = Path.parse('/path/to/')
      let doc = Path.parse('/path/to/x.json')
      assert.equal(dir.relative(doc), './')
    })

    it('returns the relative path of a grandparent directory', () => {
      let dir = Path.parse('/path/')
      let doc = Path.parse('/path/to/x.json')
      assert.equal(dir.relative(doc), '../')
    })

    it('returns the relative path of a cousin document', () => {
      let a = Path.parse('/path/a/1.json')
      let b = Path.parse('/path/to/b/2.json')

      assert.equal(a.relative(b), '../../a/1.json')
      assert.equal(b.relative(a), '../to/b/2.json')
    })
  })
})
