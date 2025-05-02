'use strict'

const Counters = require('../lib/counters')
const { assert } = require('chai')

describe('Counters', () => {
  let counters

  beforeEach(() => {
    counters = new Counters()
  })

  it('returns zero for an unknown ID', () => {
    let ctr = counters.get('x')
    assert.equal(ctr, 0n)
  })

  it('refuses to increment an unknown ID', () => {
    assert.throws(() => counters.incr('x'))
  })

  it('increments an initialised ID', () => {
    counters.init('x')
    counters.incr('x')
    assert.equal(counters.get('x'), 1n)
  })

  it('refuses to re-initialise an existing ID', () => {
    counters.init('x')
    assert.throws(() => counters.init('x'))
  })

  it('allows an ID to be initialised with a non-zero number', () => {
    counters.init('x', 2)
    for (let i = 0; i < 3; i++) counters.incr('x')
    assert.equal(counters.get('x'), 5n)
  })

  describe('with some non-zero counters stored', () => {
    beforeEach(() => {
      counters.init('x')
      for (let i = 0; i < 3; i++) counters.incr('x')

      counters.init('y')
      for (let i = 0; i < 5; i++) counters.incr('y')
    })

    it('stores the number of times incr() is called for each ID', () => {
      assert.equal(counters.get('y'), 5n)
      assert.equal(counters.get('x'), 3n)
    })

    it('can be de/serialised', () => {
      let state = counters.serialize()
      let copy = Counters.parse(state, ['x', 'y'])

      assert.equal(copy.get('y'), 5n)
      assert.equal(copy.get('x'), 3n)
    })

    describe('with two copies of the same starting state', () => {
      let alice, bob

      beforeEach(() => {
        let state = counters.serialize()
        let ids = ['x', 'y']

        alice = Counters.parse(state, ids)
        bob = Counters.parse(state, ids)
      })

      it('can merge uncommitted updates', () => {
        for (let i = 0; i < 7; i++) alice.incr('x')
        for (let i = 0; i < 11; i++) bob.incr('y')

        assert.equal(alice.get('y'), 5n)
        alice.merge(bob)
        assert.equal(alice.get('x'), 10n)
        assert.equal(alice.get('y'), 16n)
      })

      it('does not merge diffs that are already committed', () => {
        for (let i = 0; i < 11; i++) bob.incr('y')
        bob.commit()

        assert.equal(alice.get('y'), 5n)
        alice.merge(bob)
        assert.equal(alice.get('y'), 5n)
      })

      it('does not merge counts for newly initialised IDs', () => {
        alice.init('z', 0)
        for (let i = 0; i < 2; i++) alice.incr('z')

        bob.init('z', 0)
        for (let i = 0; i < 3; i++) bob.incr('z')

        alice.merge(bob)
        assert.equal(alice.get('z'), 2n)

        bob.merge(alice)
        assert.equal(bob.get('z'), 3n)
      })
    })
  })
})
