'use strict'

const Schedule = require('../lib/schedule')
const { assert } = require('chai')

function assertGraph (schedule, spec) {
  let { _groups: { _nodes: groups } } = schedule

  let groupIds = Object.keys(spec)
  let mapping = new Map()

  assert.equal(groups.size, groupIds.length,
    `schedule expected to contain ${groupIds.length} groups but contained ${groups.size}`)

  for (let [id, [shard, ops, deps = []]] of Object.entries(spec)) {
    let group = findGroup(groups, shard, ops)

    if (!group) {
      assert.fail(`no group found matching shard and operations for '${id}'`)
    }

    if (mapping.has(group.id)) {
      assert.fail(`duplicate group definitions: '${mapping.get(group.id)}' and '${id}'`)
    }

    mapping.set(group.id, id)
    let mappedDeps = [...group.parents].map((dep) => mapping.get(dep))
    assert.sameMembers(mappedDeps, deps)
  }
}

function findGroup (groups, shard, ops) {
  return [...groups.values()].find((group) => {
    if (group.shard !== shard || group.ops.size !== ops.length) {
      return false
    }
    if (ops.every((op) => group.ops.has(op))) {
      return true
    }
    return false
  })
}

function assertShardList (schedule, shard, ...expected) {
  let { groups } = schedule._shards.get(shard)

  assert.equal(groups.length, expected.length,
    `shard '${shard}' expected to have ${expected.length} groups but had ${groups.length}`)

  for (let [idx, groupId] of groups.entries()) {
    let [group] = schedule._groups.get([groupId])
    assert.sameMembers([...group.ops], expected[idx])
  }
}

describe('Schedule', () => {
  let schedule

  describe('basic planning', () => {
    beforeEach(() => {
      schedule = new Schedule()
    })

    //      |   +----+
    //    A |   | w1 |
    //      |   +----+
    //
    it('places a single operation', () => {
      let w1 = schedule.add('A', [])

      assertGraph(schedule, {
        g1: ['A', [w1]]
      })
    })

    //      |   +------------+
    //    A |   | w1      w2 |
    //      |   +------------+
    //
    it('places two independent operations for the same shard', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('A', [])

      assertGraph(schedule, {
        g1: ['A', [w1, w2]]
      })
    })

    //      |   +------------+
    //    A |   | w1 ---- w2 |
    //      |   +------------+
    //
    it('places two dependent operations for the same shard', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('A', [w1])

      assertGraph(schedule, {
        g1: ['A', [w1, w2]]
      })
    })

    it('throws an error if an unknown dependency is given', () => {
      let error

      try {
        let w1 = schedule.add('A', [])
        let w2 = schedule.add('A', [w1 + 'nope'])
      } catch (e) {
        error = e
      }

      assert.equal(error.code, 'ERR_SCHEDULE')
    })

    //      |   +----------------------------+
    //    A |   | w1 ---- w2 ---- w3 ---- w4 |
    //      |   +----------------------------+
    //
    it('groups a chain of operations on the same shard', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('A', [w1])
      let w3 = schedule.add('A', [w2])
      let w4 = schedule.add('A', [w3])

      assertGraph(schedule, {
        g1: ['A', [w1, w2, w3, w4]]
      })
    })

    //      |   +----+
    //    A |   | w1 |
    //      |   +----+
    //      |
    //      |   +----+
    //    B |   | w2 |
    //      |   +----+
    //
    it('places two independent operations for different shards', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2]]
      })
    })

    //      |   +----+
    //    A |   | w1 |
    //      |   +---\+
    //      |        \
    //      |        +\---+
    //    B |        | w2 |
    //      |        +----+
    //
    it('places two dependent operations for different shards', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2], ['g1']]
      })
    })

    //      |   +------------+
    //    A |   | w1 ---- w3 |
    //      |   +---\--------+
    //      |        \
    //      |        +\---+
    //    B |        | w2 |
    //      |        +----+
    //
    it('places two directly dependent operations in the same group', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('A', [w1])

      assertGraph(schedule, {
        g1: ['A', [w1, w3]],
        g2: ['B', [w2], ['g1']]
      })
    })

    //      |   +----+    +----+
    //    A |   | w1 |    | w3 |
    //      |   +---\+    +/---+
    //      |        \    /
    //      |        +\--/+
    //    B |        | w2 |
    //      |        +----+
    //
    it('places two indirectly dependent operations in different groups', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('A', [w2])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['A', [w3], ['g2']]
      })
    })

    //      |   +----+    +----+
    //    A |   | w1 ------ w3 |
    //      |   +---\+    +/---+
    //      |        \    /
    //      |        +\--/+
    //    B |        | w2 |
    //      |        +----+
    //
    it('places an op in its own group if any of its deps are indirect', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('A', [w1, w2])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['A', [w3], ['g1', 'g2']]
      })
    })

    //      |   +----+            +----+
    //    A |   | w1 |            | w4 |
    //      |   +---\+            +/---+
    //      |        \            /
    //      |        +\----------/+
    //    B |        | w2 ---- w3 |
    //      |        +------------+
    //
    it('tracks an indirect dependency through multiple hops', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('B', [w2])
      let w4 = schedule.add('A', [w3])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2, w3], ['g1']],
        g3: ['A', [w4], ['g2']]
      })
    })

    //      |   +----+            +----+
    //    A |   | w1 |            | w4 |
    //      |   +---\+            +/---+
    //      |        \            /
    //      |        +\----------/+
    //    B |        | w2      w3 |
    //      |        +------------+
    //
    it('tracks an indirect dependency via operations in the same group', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('B', [])
      let w4 = schedule.add('A', [w3])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2, w3], ['g1']],
        g3: ['A', [w4], ['g2']]
      })
    })

    //      |   +----+             +----+
    //    A |   | w1 |             | w4 |
    //      |   +---\+             +/---+
    //      |        \             /
    //      |        +\---+       /
    //    B |        | w2 |      /
    //      |        +---\+     /
    //      |             \    /
    //      |             +\--/+
    //    C |             | w3 |
    //      |             +----+
    //
    it('tracks an indirect dependency via a chain of groups', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('C', [w2])
      let w4 = schedule.add('A', [w3])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['C', [w3], ['g2']],
        g4: ['A', [w4], ['g3']]
      })
    })

    //      |   +------------+    +----+
    //    A |   | w1      w4 |    | w3 |
    //      |   +---\--------+    +/---+
    //      |        \            /
    //      |        +\---+      /
    //    B |        | w2 ------'
    //      |        +----+
    //
    it('places an indepdendent operation in the earliest group', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('A', [w2])
      let w4 = schedule.add('A', [])

      assertGraph(schedule, {
        g1: ['A', [w1, w4]],
        g2: ['B', [w2], ['g1']],
        g3: ['A', [w3], ['g2']]
      })
    })

    //      |   +----+    +------------+
    //    A |   | w1 |    | w3 ---- w4 |
    //      |   +---\+    +/-----------+
    //      |        \    /
    //      |        +\--/+
    //    B |        | w2 |
    //      |        +----+
    //
    it('places an operation no earlier than a direct dependency', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('A', [w2])
      let w4 = schedule.add('A', [w3])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['A', [w3, w4], ['g2']]
      })
    })

    //      |   +----+    +------------+
    //    A |   | w1 |    | w3      w4 |
    //      |   +---\+    +/-------/---+
    //      |        \    /       /
    //      |        +\--/+      /
    //    B |        | w2 ------'
    //      |        +----+
    //
    it('places an operation later than an indirect dependency', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('A', [w2])
      let w4 = schedule.add('A', [w2])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['A', [w3, w4], ['g2']]
      })
    })

    //      |        +----+
    //    A |        | w2 |
    //      |        +/--\+
    //      |        /    \
    //      |   +---/+    +\---+
    //    B |   | w1 |    | w3 |
    //      |   +----+    +---\+
    //      |                  \
    //      |          +--------\---+
    //    C |          | w4      w5 |
    //      |          +------------+
    //
    it('takes the group index from operations on the same shard', () => {
      let w1 = schedule.add('B', [])
      let w2 = schedule.add('A', [w1])
      let w3 = schedule.add('B', [w2])
      let w4 = schedule.add('C', [])
      let w5 = schedule.add('C', [w3])

      assertGraph(schedule, {
        g1: ['B', [w1]],
        g2: ['A', [w2], ['g1']],
        g3: ['B', [w3], ['g2']],
        g4: ['C', [w4, w5], ['g3']]
      })
    })

    //      |                +------------+
    //    A |          .------ w2      w7 ------.
    //      |         /      +--------/---+      \
    //      |        /               /            \
    //      |   +---/---------------/+    +--------\---+
    //    B |   | w1      w3      w6 |    | w5      w8 |
    //      |   +-----------\--------+    +/-------/---+
    //      |                \            /       /
    //      |                +\---+      /       /
    //    C |                | w4 ------'-------'
    //      |                +----+
    //
    it('places a dependent set of operations', () => {
      let w1 = schedule.add('B', [])
      let w2 = schedule.add('A', [w1])
      let w3 = schedule.add('B', [])
      let w4 = schedule.add('C', [w3])
      let w5 = schedule.add('B', [w4])
      let w6 = schedule.add('B', [])
      let w7 = schedule.add('A', [w6])
      let w8 = schedule.add('B', [w4, w7])

      assertGraph(schedule, {
        g1: ['B', [w1, w3, w6]],
        g2: ['A', [w2, w7], ['g1']],
        g3: ['C', [w4], ['g1']],
        g4: ['B', [w5, w8], ['g2', 'g3']]
      })
    })

    //      |   +----+                     +----+
    //    A |   | w3 |                     | w5 |
    //      |   +---\+                     +/---+
    //      |        \                     /
    //      |         \               +---/+
    //    B |          \              | w2 |
    //      |           \             +/---+
    //      |            \            /
    //      |            +\----------/+
    //    C |            | w4      w1 |
    //      |            +------------+
    //
    it('tracks indirect dependencies through group chains', () => {
      let w1 = schedule.add('C', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('A', [])
      let w4 = schedule.add('C', [w3])
      let w5 = schedule.add('A', [w2])

      assertGraph(schedule, {
        g1: ['A', [w3]],
        g2: ['C', [w4, w1], ['g1']],
        g3: ['B', [w2], ['g2']],
        g4: ['A', [w5], ['g3']]
      })
    })

    //      |   +----+
    //    A |   | w1 |
    //      |   +---\+
    //      |        \
    //      |        +\---+
    //    B |        | w2 ------.
    //      |        +---\+      \
    //      |             \       \
    //      |             +\-------\---+
    //    C |             | w3      w4 |
    //      |             +------------+
    //
    it('groups two operations with the same dependency', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('C', [w2])
      let w4 = schedule.add('C', [w2])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['C', [w3, w4], ['g2']]
      })
    })
  })

  describe('depth reduction', () => {
    beforeEach(() => {
      schedule = new Schedule({ depthLimit: 2 })
    })

    //      |   +----+
    //    A |   | w1 |
    //      |   +---\+
    //      |        \
    //      |        +\---+
    //    B |        | w2 |
    //      |        +---\+
    //      |             \
    //      |   +----+    +\---+
    //    C |   | w4 |    | w3 |
    //      |   +----+    +----+
    //
    it('places an independent op in a new group at the front of a shard list', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('C', [w2])
      let w4 = schedule.add('C', [])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['C', [w3], ['g2']],
        g4: ['C', [w4]]
      })

      assertShardList(schedule, 'C', [w4], [w3])
    })

    //      |   +----+    +----+    +----+
    //    A |   | w1 |    | w6 |    | w5 |
    //      |   +---\+    +/---+    +/---+
    //      |        \    /         /
    //      |        +\--/+    +---/+
    //    B |        | w2 |    | w4 |
    //      |        +---\+    +/---+
    //      |             \    /
    //      |             +\--/+
    //    C |             | w3 |
    //      |             +----+
    //
    it('places a dependent op in a new group in the middle of a shard list', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('C', [w2])
      let w4 = schedule.add('B', [w3])
      let w5 = schedule.add('A', [w4])
      let w6 = schedule.add('A', [w2])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['C', [w3], ['g2']],
        g4: ['B', [w4], ['g3']],
        g5: ['A', [w5], ['g4']],
        g6: ['A', [w6], ['g2']]
      })

      assertShardList(schedule, 'A', [w1], [w6], [w5])
      assertShardList(schedule, 'B', [w2], [w4])
    })

    //      |   +----+    +-------------+
    //    A |   | w1 |    | w5       w4 |
    //      |   +---\+    +/--------/---+
    //      |        \    /        /
    //      |        +\--/+       /
    //    B |        | w2 |      /
    //      |        +---\+     /
    //      |             \    /
    //      |             +\--/+
    //    C |             | w3 |
    //      |             +----+
    //
    it('does not create new groups if the depth saving is insufficient', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('C', [w2])
      let w4 = schedule.add('A', [w3])
      let w5 = schedule.add('A', [w2])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['C', [w3], ['g2']],
        g4: ['A', [w5, w4], ['g2', 'g3']],
      })

      assertShardList(schedule, 'A', [w1], [w5, w4])
    })

    //      |      +----+
    //    A |      | w1 |
    //      |      +---\+
    //      |           \
    //      |           +\---+
    //    B |           | w2 |
    //      |           +---\+
    //      |                \
    //      |        +--------\---+
    //    C |        | w5      w3 |
    //      |        +/----------\+
    //      |        /            \
    //      |   +---/+            +\---+
    //    D |   | w4 |            | w6 |
    //      |   +----+            +----+
    //
    it('places a depth-1 operation in a depth-2 group', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('C', [w2])
      let w4 = schedule.add('D', [])
      let w5 = schedule.add('C', [w4])
      let w6 = schedule.add('D', [w3])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['D', [w4]],
        g4: ['C', [w5, w3], ['g2', 'g3']],
        g5: ['D', [w6], ['g4']]
      })
    })

    //      |   +----+
    //    A |   | w1 |
    //      |   +---\+
    //      |        \
    //      |        +\---+
    //    B |        | w2 |
    //      |        +---\+
    //      |             \
    //      |             +\-----------+
    //    C |             | w3 ---- w4 |
    //      |             +------------+
    //
    it('places a dependent op no earlier than its direct dependency', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('C', [w2])
      let w4 = schedule.add('C', [w3])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['C', [w3, w4], ['g2']],
      })
    })

    //      |   +----+
    //    A |   | w1 |
    //      |   +---\+
    //      |        \
    //      |        +\---+
    //    B |        | w2 |
    //      |        +---\+
    //      |             \
    //      |   +----+    +\-----------+
    //    C |   | w4 |    | w3 ---- w5 |
    //      |   +----+    +------------+
    //
    it('places a dependent op in an index-shifted group', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('C', [w2])
      let w4 = schedule.add('C', [])
      let w5 = schedule.add('C', [w3])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['C', [w3, w5], ['g2']],
        g4: ['C', [w4]]
      })

      assertShardList(schedule, 'C', [w4], [w3, w5])
    })

    //      |   +----+
    //    A |   | w3 |
    //      |   +---\+
    //      |        \
    //      |        +\-----------+
    //    B |        | w4      w1 |
    //      |        +-----------\+
    //      |                     \
    //      |   +----+            +\---+
    //    C |   | w5 |            | w2 |
    //      |   +----+            +----+
    //
    it('adjusts the depth of downstream groups', () => {
      let w1 = schedule.add('B', [])
      let w2 = schedule.add('C', [w1])
      let w3 = schedule.add('A', [])
      let w4 = schedule.add('B', [w3])
      let w5 = schedule.add('C', [])

      assertGraph(schedule, {
        g1: ['A', [w3]],
        g2: ['B', [w4, w1], ['g1']],
        g3: ['C', [w2], ['g2']],
        g4: ['C', [w5]]
      })
    })

    //      |           +----+
    //    A |           | w1 |
    //      |           +---\+
    //      |                \
    //      |        +--------\---+
    //    B |        | w5      w2 |
    //      |        +/----------\+
    //      |        /            \
    //      |   +---/+            +\---+
    //    C |   | w4 |            | w3 |
    //      |   +----+            +----+
    //
    it('links two chains if it does not excessively increase the depth', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('C', [w2])
      let w4 = schedule.add('C', [])
      let w5 = schedule.add('B', [w4])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['C', [w4]],
        g3: ['B', [w5, w2], ['g1', 'g2']],
        g4: ['C', [w3], ['g3']],
      })

      assertShardList(schedule, 'C', [w4], [w3])
    })

    //      |        +------------+
    //    A |        | w2      w3 |
    //      |        +/----------\+
    //      |        /            \
    //      |   +---/+    +--------\---+
    //    B |   | w1 |    | w6      w4 |
    //      |   +----+    +/-----------+
    //      |             /
    //      |        +---/+
    //    C |        | w5 |
    //      |        +----+
    //
    it('places a dependent op avoiding increasing the graph depth', () => {
      let w1 = schedule.add('B', [])
      let w2 = schedule.add('A', [w1])
      let w3 = schedule.add('A', [])
      let w4 = schedule.add('B', [w3])
      let w5 = schedule.add('C', [])
      let w6 = schedule.add('B', [w5])

      assertGraph(schedule, {
        g1: ['B', [w1]],
        g2: ['A', [w2, w3], ['g1']],
        g3: ['C', [w5]],
        g4: ['B', [w6, w4], ['g2', 'g3']]
      })

      assertShardList(schedule, 'B', [w1], [w6, w4])
    })

    //      |        +------------+
    //    A |        | w2      w3 |
    //      |        +/----------\+
    //      |        /            \
    //      |   +---/--------+    +\---+
    //    B |   | w1 ---- w5 |    | w4 |
    //      |   +------------+    +----+
    //
    it('does not use direct dependencies to infer the op depth', () => {
      let w1 = schedule.add('B', [])
      let w2 = schedule.add('A', [w1])
      let w3 = schedule.add('A', [])
      let w4 = schedule.add('B', [w3])
      let w5 = schedule.add('B', [w1])

      assertGraph(schedule, {
        g1: ['B', [w1, w5]],
        g2: ['A', [w2, w3], ['g1']],
        g3: ['B', [w4], ['g2']]
      })

      assertShardList(schedule, 'B', [w1, w5], [w4])
    })

    //      |             +----+
    //    A |             | w3 |
    //      |             +/--\+
    //      |             /    \
    //      |        +---/+     \
    //    B |        | w2 |      \
    //      |        +/---+       \
    //      |        /             \
    //      |   +---/+             +\-----------+
    //    C |   | w1 |             | w4      w6 |
    //      |   +----+             +--------/---+
    //      |                              /
    //      |                         +---/+
    //    D |                         | w5 |
    //      |                         +----+
    //
    it('places an op in a group with the closest depth', () => {
      let w1 = schedule.add('C', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('A', [w2])
      let w4 = schedule.add('C', [w3])
      let w5 = schedule.add('D', [])
      let w6 = schedule.add('C', [w5])

      assertGraph(schedule, {
        g1: ['C', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['A', [w3], ['g2']],
        g4: ['D', [w5]],
        g5: ['C', [w4, w6], ['g3', 'g4']]
      })

      assertShardList(schedule, 'C', [w1], [w4, w6])
    })

    //      |        +------------+
    //    A |        | w2      w3 |
    //      |        +/----------\+
    //      |        /            \
    //      |   +---/+            +\-----------+
    //    B |   | w1 |            | w5      w8 |
    //      |   +---\+            +/-------/---+
    //      |        \            /       /
    //      |        +\----------/+      /
    //    C |        | w6      w4 |     /
    //      |        +------------+    /
    //      |                         /
    //      |                    +---/+
    //    D |                    | w7 |
    //      |                    +----+
    //
    it('tracks the depth of groups with multiple parents', () => {
      let w1 = schedule.add('B', [])
      let w2 = schedule.add('A', [w1])
      let w3 = schedule.add('A', [])
      let w4 = schedule.add('C', [])
      let w5 = schedule.add('B', [w3, w4])
      let w6 = schedule.add('C', [w1])
      let w7 = schedule.add('D', [])
      let w8 = schedule.add('B', [w7])

      assertGraph(schedule, {
        g1: ['B', [w1]],
        g2: ['A', [w2, w3], ['g1']],
        g3: ['C', [w6, w4], ['g1']],
        g4: ['D', [w7]],
        g5: ['B', [w5, w8], ['g2', 'g3', 'g4']]
      })

      assertShardList(schedule, 'B', [w1], [w5, w8])
    })

    //      |                 +----+
    //    A |                 | w9 --------------.
    //      |                 +/---+              \
    //      |                 /                    \
    //      |   +----+       /    +-----------------\----+    +----+
    //    B |   | w5 |      /     | w2       w4      w10 |    | w8 |
    //      |   +---\+     /      +/--------/--\---------+    +/---+
    //      |        \    /       /        /    \             /
    //      |        +\--/-------/+       /     +\---+       /
    //    C |        | w6      w1 |      /      | w7 -------'
    //      |        +-----------\+     /       +----+
    //      |                     \    /
    //      |                     +\--/+
    //    D |                     | w3 |
    //      |                     +----+
    //
    it('updates depth of descendants in topological order', () => {
      let w1 = schedule.add('C', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('D', [w1])
      let w4 = schedule.add('B', [w3])
      let w5 = schedule.add('B', [])
      let w6 = schedule.add('C', [w5])
      let w7 = schedule.add('C', [w4])
      let w8 = schedule.add('B', [w7])
      let w9 = schedule.add('A', [w6])
      let w10 = schedule.add('B', [w9])

      assertGraph(schedule, {
        g1: ['B', [w5]],
        g2: ['C', [w6, w1], ['g1']],
        g3: ['A', [w9], ['g2']],
        g4: ['D', [w3], ['g2']],
        g5: ['B', [w2, w4, w10], ['g2', 'g3', 'g4']],
        g6: ['C', [w7], ['g5']],
        g7: ['B', [w8], ['g6']]
      })

      assertShardList(schedule, 'B', [w5], [w2, w4, w10], [w8])
      assertShardList(schedule, 'C', [w6, w1], [w7])
    })

    //      |   +----+
    //    A |   | w1 |
    //      |   +---\+
    //      |        \
    //      |        +\---+
    //    B |        | w2 ------.
    //      |        +---\+      \
    //      |             \       \
    //      |   +----+    +\-------\---+
    //    C |   | w4 |    | w3      w5 |
    //      |   +----+    +------------+
    //
    it('groups two operations with the same dependency', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('C', [w2])
      let w4 = schedule.add('C', [])
      let w5 = schedule.add('C', [w2])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['C', [w3, w5], ['g2']],
        g4: ['C', [w4]]
      })

      assertShardList(schedule, 'C', [w4], [w3, w5])
    })

    //      |           +----+
    //    A |           | w1 |
    //      |           +---\+
    //      |                \
    //      |                +\---+
    //    B |                | w2 |
    //      |                +---\+
    //      |                     \
    //      |   +------------+    +\---+
    //    C |   | w5      w4 |    | w3 |
    //      |   +------------+    +----+
    //
    it('places an independent op into the earliest group', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('C', [w2])
      let w4 = schedule.add('C', [])
      let w5 = schedule.add('C', [])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['C', [w3], ['g2']],
        g4: ['C', [w4, w5]]
      })

      assertShardList(schedule, 'C', [w4, w5], [w3])
    })

    //      |   +----+
    //    A |   | w1 |
    //      |   +---\+
    //      |        \
    //      |        +\---+
    //    B |        | w2 |
    //      |        +---\+
    //      |             \
    //      |   +----+    +\-----------+
    //    C |   | w4 |    | w3      w6 |
    //      |   +----+    +--------/---+
    //      |                     /
    //      |                +---/+
    //    D |                | w5 |
    //      |                +----+
    //
    it('avoids an inverted dependency in a shallow graph', () => {
      let w1 = schedule.add('A', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('C', [w2])
      let w4 = schedule.add('C', [])
      let w5 = schedule.add('D', [])
      let w6 = schedule.add('C', [w5])

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['C', [w4]],
        g4: ['D', [w5]],
        g5: ['C', [w3, w6], ['g2', 'g4']]
      })
    })

    //      |                +----+
    //    A |                | w2 |
    //      |                +/--\+
    //      |                /    \
    //      |           +---/+    +\---+
    //    B |           | w1 |    | w3 |
    //      |           +---\+    +---\+
    //      |                \         \
    //      |        +--------\---+    +\---+
    //    C |        | w7      w5 |    | w4 |
    //      |        +/-----------+    +----+
    //      |        /
    //      |   +---/+
    //    D |   | w6 |
    //      |   +----+
    //
    it('sets up a potential inverted dependency in a deeper graph', () => {
      let w1 = schedule.add('B', [])
      let w2 = schedule.add('A', [w1])
      let w3 = schedule.add('B', [w2])
      let w4 = schedule.add('C', [w3])
      let w5 = schedule.add('C', [w1])
      let w6 = schedule.add('D', [])
      let w7 = schedule.add('C', [w6])

      assertGraph(schedule, {
        g1: ['B', [w1], []],
        g2: ['A', [w2], ['g1']],
        g3: ['B', [w3], ['g2']],
        g4: ['C', [w4], ['g3']],
        g5: ['D', [w6], []],
        g6: ['C', [w7, w5], ['g1', 'g5']]
      })

      assertShardList(schedule, 'C', [w7, w5], [w4])
    })

    //      |                +----+
    //    A |                | w2 |
    //      |                +/--\+
    //      |                /    \
    //      |           +---/+    +\---+
    //    B |           | w1 |    | w3 |
    //      |           +---\+    +---\+
    //      |                \         \
    //      |        +--------\---+    +\---+
    //    C |        | w7      w5 |    | w4 |
    //      |        +/-----------+    +---\+
    //      |        /                      \
    //      |   +---/+                      +\---+
    //    D |   | w6 |                      | w8 |
    //      |   +----+                      +----+
    //
    it('places a dependent op in a new group at the end of the shard list', () => {
      let w1 = schedule.add('B', [])
      let w2 = schedule.add('A', [w1])
      let w3 = schedule.add('B', [w2])
      let w4 = schedule.add('C', [w3])
      let w5 = schedule.add('C', [w1])
      let w6 = schedule.add('D', [])
      let w7 = schedule.add('C', [w6])
      let w8 = schedule.add('D', [w4])

      assertGraph(schedule, {
        g1: ['B', [w1], []],
        g2: ['A', [w2], ['g1']],
        g3: ['B', [w3], ['g2']],
        g4: ['C', [w4], ['g3']],
        g5: ['D', [w6], []],
        g6: ['C', [w7, w5], ['g1', 'g5']],
        g7: ['D', [w8], ['g4']]
      })

      assertShardList(schedule, 'C', [w7, w5], [w4])
      assertShardList(schedule, 'D', [w6], [w8])
    })

    //      |                +----+
    //    A |                | w2 |
    //      |                +/--\+
    //      |                /    \
    //      |           +---/+    +\---+
    //    B |           | w1 |    | w3 |
    //      |           +---\+    +---\+
    //      |                \         \
    //      |        +--------\---+    +\---+
    //    C |        | w7      w8 |    | w4 |
    //      |        +/-----------+    +---\+
    //      |        /                      \
    //      |   +---/+                      +\---+
    //    D |   | w6 |                      | w5 |
    //      |   +----+                      +----+
    //
    it('gives the same result for a second order of operations', () => {
      let w1 = schedule.add('B', [])
      let w2 = schedule.add('A', [w1])
      let w3 = schedule.add('B', [w2])
      let w4 = schedule.add('C', [w3])
      let w5 = schedule.add('D', [w4])
      let w6 = schedule.add('D', [])
      let w7 = schedule.add('C', [w6])
      let w8 = schedule.add('C', [w1])

      assertGraph(schedule, {
        g1: ['B', [w1], []],
        g2: ['A', [w2], ['g1']],
        g3: ['B', [w3], ['g2']],
        g4: ['C', [w4], ['g3']],
        g5: ['D', [w6], []],
        g6: ['C', [w7, w8], ['g1', 'g5']],
        g7: ['D', [w5], ['g4']]
      })

      assertShardList(schedule, 'C', [w7, w8], [w4])
      assertShardList(schedule, 'D', [w6], [w5])
    })

    //      |                +----+
    //    A |                | w2 |
    //      |                +/--\+
    //      |                /    \
    //      |           +---/+    +\---+
    //    B |           | w1 |    | w3 |
    //      |           +---\+    +---\+
    //      |                \         \
    //      |        +--------\---+    +\---+
    //    C |        | w8      w7 |    | w4 |
    //      |        +/-----------+    +---\+
    //      |        /                      \
    //      |   +---/+                      +\---+
    //    D |   | w6 |                      | w5 |
    //      |   +----+                      +----+
    //
    it('gives the same result for a third order of operations', () => {
      let w1 = schedule.add('B', [])
      let w2 = schedule.add('A', [w1])
      let w3 = schedule.add('B', [w2])
      let w4 = schedule.add('C', [w3])
      let w5 = schedule.add('D', [w4])
      let w6 = schedule.add('D', [])
      let w7 = schedule.add('C', [w1])
      let w8 = schedule.add('C', [w6])

      assertGraph(schedule, {
        g1: ['B', [w1], []],
        g2: ['A', [w2], ['g1']],
        g3: ['B', [w3], ['g2']],
        g4: ['C', [w4], ['g3']],
        g5: ['D', [w6], []],
        g6: ['C', [w8, w7], ['g1', 'g5']],
        g7: ['D', [w5], ['g4']]
      })

      assertShardList(schedule, 'C', [w8, w7], [w4])
      assertShardList(schedule, 'D', [w6], [w5])
    })

    //      |             +-------------+
    //    A |             | w3       w5 |
    //      |             +/--------/---+
    //      |             /        /
    //      |        +---/+       /
    //    B |        | w2 |      /
    //      |        +/--\+     /
    //      |        /    \    /
    //      |   +---/+    +\--/+
    //    C |   | w1 |    | w4 |
    //      |   +----+    +----+
    //
    it('merges operations with a common ancestor that are not mutually dependent', () => {
      let w1 = schedule.add('C', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('A', [w2])
      let w4 = schedule.add('C', [w2])
      let w5 = schedule.add('A', [w4])

      assertGraph(schedule, {
        g1: ['C', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['C', [w4], ['g2']],
        g4: ['A', [w3, w5], ['g2', 'g3']]
      })
    })

    //      |             +--------------+
    //    A |             | w3        w6 |
    //      |             +/---------/---+
    //      |             /         /
    //      |        +---/+    +---/+
    //    B |        | w2 |    | w5 |
    //      |        +/--\+    +/---+
    //      |        /    \    /
    //      |   +---/+    +\--/+
    //    C |   | w1 |    | w4 |
    //      |   +----+    +----+
    //
    it('merges an operation added at the end of a shard list', () => {
      let w1 = schedule.add('C', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('A', [w2])
      let w4 = schedule.add('C', [w2])
      let w5 = schedule.add('B', [w4])
      let w6 = schedule.add('A', [w5])

      assertGraph(schedule, {
        g1: ['C', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['C', [w4], ['g2']],
        g4: ['B', [w5], ['g3']],
        g5: ['A', [w3, w6], ['g2', 'g4']]
      })
    })

    //      |             +----+    +----+
    //    A |             | w6 |    | w5 |
    //      |             +/---+    +/---+
    //      |             /         /
    //      |        +---/+    +---/+
    //    B |        | w2 |    | w4 |
    //      |        +/--\+    +/---+
    //      |        /    \    /
    //      |   +---/+    +\--/+
    //    C |   | w1 |    | w3 |
    //      |   +----+    +----+
    //
    it('does not merge an operation added in the middle of a shard list', () => {
      let w1 = schedule.add('C', [])
      let w2 = schedule.add('B', [w1])
      let w3 = schedule.add('C', [w2])
      let w4 = schedule.add('B', [w3])
      let w5 = schedule.add('A', [w4])
      let w6 = schedule.add('A', [w2])

      assertGraph(schedule, {
        g1: ['C', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['C', [w3], ['g2']],
        g4: ['B', [w4], ['g3']],
        g5: ['A', [w5], ['g4']],
        g6: ['A', [w6], ['g2']]
      })
    })
  })

  describe('consumer interface', () => {
    let w1, w2, w3, w4, w5, w6

    //
    //      |                +------------+
    //    A |                | w4      w5 |
    //      |                +/----------\+
    //      |                /            \
    //      |   +-----------/+            +\---+
    //    B |   | w1      w3 |            | w6 |
    //      |   +---\--------+            +----+
    //      |        \
    //      |        +\---+
    //    C |        | w2 |
    //      |        +----+
    //
    beforeEach(() => {
      schedule = new Schedule()

      w1 = schedule.add('B', [], 'val 1')
      w2 = schedule.add('C', [w1], 'val 2')
      w3 = schedule.add('B', [], 'val 3')
      w4 = schedule.add('A', [w3], 'val 4')
      w5 = schedule.add('A', [], 'val 5')
      w6 = schedule.add('B', [w5], 'val 6')
    })

    it('schedules the operations as expected', () => {
      assertGraph(schedule, {
        g1: ['B', [w1, w3]],
        g2: ['C', [w2], ['g1']],
        g3: ['A', [w4, w5], ['g1']],
        g4: ['B', [w6], ['g3']]
      })

      assertShardList(schedule, 'B', [w1, w3], [w6])
    })

    it('returns the names of the shards in the graph', () => {
      assert.deepEqual([...schedule.shards()], ['B', 'C', 'A'])
    })

    it('returns the first available group', () => {
      let group = schedule.nextGroup()
      assert.deepEqual([...group.values()], ['val 1', 'val 3'])
    })

    it('returns the shard the group belongs to', () => {
      let group = schedule.nextGroup()
      assert.equal(group.getShard(), 'B')
    })

    it('returns null when no more groups are available', () => {
      schedule.nextGroup().started()
      assert.isNull(schedule.nextGroup())
    })

    it('does not place new ops in a group that has been started', () => {
      let group = schedule.nextGroup()
      group.started()

      let w7 = schedule.add('B', [])

      assertGraph(schedule, {
        g1: ['B', [w1, w3]],
        g2: ['C', [w2], ['g1']],
        g3: ['A', [w4, w5], ['g1']],
        g4: ['B', [w6, w7], ['g3']]
      })

      assertShardList(schedule, 'B', [w1, w3], [w6, w7])
    })

    //      |   +------------+
    //    A |   | w4      w5 |
    //      |   +-----------\+
    //      |                \
    //      |                +\---+
    //    B |                | w6 |
    //      |                +----+
    //      |
    //      |   +----+
    //    C |   | w2 |
    //      |   +----+
    //
    it('removes a group that completes successfully', () => {
      let group = schedule.nextGroup()
      group.started()
      group.completed()

      assertGraph(schedule, {
        g1: ['C', [w2]],
        g2: ['A', [w4, w5]],
        g3: ['B', [w6], ['g2']]
      })
    })

    it('allows a new group to be started when its dependency is finished', () => {
      let group = schedule.nextGroup()
      group.started()
      group.completed()

      group = schedule.nextGroup()
      assert.deepEqual([...group.values()], ['val 2'])
      group.started()

      group = schedule.nextGroup()
      assert.deepEqual([...group.values()], ['val 4', 'val 5'])
      group.started()

      group = schedule.nextGroup()
      assert.isNull(group)
    })

    it('does not recycle operation IDs', () => {
      let group = schedule.nextGroup()
      assert.deepEqual([...group.values()], ['val 1', 'val 3'])
      group.started()
      group.completed()

      let w7 = schedule.add('C', [])

      assert.isFalse([w1, w2, w3, w4, w5, w6].some((id) => id === w7))
    })

    //      |   +------------+
    //    A |   | w4      w5 |
    //      |   +-----------\+
    //      |                \
    //      |                +\---+
    //    B |                | w6 |
    //      |                +----+
    //      |
    //      |   +    +    +----+
    //    C |     w2      | w7 |
    //      |   +    +    +----+
    //
    it('creates a new group if no existing group is available', () => {
      let group = schedule.nextGroup()
      assert.deepEqual([...group.values()], ['val 1', 'val 3'])
      group.started()
      group.completed()

      group = schedule.nextGroup()
      assert.deepEqual([...group.values()], ['val 2'])
      group.started()

      let w7 = schedule.add('C', [])

      assertGraph(schedule, {
        g1: ['C', [w2]],
        g2: ['C', [w7]],
        g3: ['A', [w4, w5]],
        g4: ['B', [w6], ['g3']]
      })

      assertShardList(schedule, 'C', [w2], [w7])
    })

    //      |   +----+
    //    A |   | w5 |
    //      |   +----+
    //      |
    //      |
    //    B |
    //      |
    //      |
    //      |
    //    C |
    //      |
    //
    it('removes downstream operations for a group that fails', () => {
      let group = schedule.nextGroup()
      group.started()
      group.failed()

      assertGraph(schedule, {
        g1: ['A', [w5]]
      })
    })

    //      |                +------------+
    //    A |                | w4      w5 ----------.
    //      |                +/--\-------\+          \
    //      |                /    \       \           \
    //      |   +-----------/+    +\-------\---+       \
    //    B |   | w1      w3 |    | w7      w6 |        \
    //      |   +---\--------+    +-----------\+         \
    //      |        \                         \          \
    //      |        +\-------------------------\----------\---+
    //    C |        | w2                        w8         w9 |
    //      |        +-----------------------------------------+
    //
    it('removes all downstream operations for a failed group', () => {
      let w7 = schedule.add('B', [w4])
      let w8 = schedule.add('C', [w6])
      let w9 = schedule.add('C', [w5])

      assertGraph(schedule, {
        g1: ['B', [w1, w3]],
        g2: ['A', [w4, w5], ['g1']],
        g3: ['B', [w7, w6], ['g2']],
        g4: ['C', [w2, w8, w9], ['g1', 'g2', 'g3']]
      })

      let group = schedule.nextGroup()
      group.started()
      group.failed()

      assertGraph(schedule, {
        g1: ['A', [w5]],
        g2: ['C', [w9], ['g1']]
      })
    })

    it('removes downstream ops for a single failed op', () => {
      let group = schedule.nextGroup()

      group.started()
      group.opFailed(w1)

      assertGraph(schedule, {
        g1: ['B', [w1, w3]],
        g2: ['A', [w4, w5], ['g1']],
        g3: ['B', [w6], ['g2']]
      })

      group.completed()

      assertGraph(schedule, {
        g1: ['A', [w4, w5]],
        g2: ['B', [w6], ['g1']]
      })
    })

    it('removes all downstream ops for a single failed op', () => {
      let w7 = schedule.add('B', [w4])
      let group = schedule.nextGroup()

      group.started()
      group.opFailed(w3)
      group.completed()

      assertGraph(schedule, {
        g1: ['C', [w2]],
        g2: ['A', [w5]],
        g3: ['B', [w6], ['g2']]
      })
    })

    describe('when a shard has groups scheduled after a started group', () => {
      let schedule, group

      //      |          +----+    +----+
      //    A |          | w2 |    | w4 |
      //      |          +---\+    +/---+
      //      |               \    /
      //      |   +    +      +\--/+
      //    B |     w1        | w3 |
      //      |   +    +      +----+
      //
      beforeEach(() => {
        schedule = new Schedule()

        let w1 = schedule.add('B', [], 'val 1')

        group = schedule.nextGroup().started()

        let w2 = schedule.add('A', [], 'val 2')
        let w3 = schedule.add('B', [w2], 'val 3')
        let w4 = schedule.add('A', [w3], 'val 4')
      })

      it('removes all groups from a shard where a group fails', () => {
        assertGraph(schedule, {
          g1: ['B', [w1]],
          g2: ['A', [w2]],
          g3: ['B', [w3], ['g2']],
          g4: ['A', [w4], ['g3']]
        })

        assertShardList(schedule, 'A', [w2], [w4])
        assertShardList(schedule, 'B', [w1], [w3])

        group.failed()

        assertGraph(schedule, {
          g1: ['A', [w2]]
        })
      })

      it('returns a list of all the cancelled operations', () => {
        let ops = group.failed()
        assert.deepEqual(ops, ['val 1', 'val 3', 'val 4'])
      })
    })

    //      |   +    +      +----+
    //    A |     w1        | w2 |
    //      |   +    +      +----+
    //
    it('prevents concurrent processing of groups on the same shard', () => {
      let schedule = new Schedule()

      let w1 = schedule.add('A', [], 'val 1')
      let group = schedule.nextGroup().started()
      assert.deepEqual([...group.values()], ['val 1'])

      let w2 = schedule.add('A', [], 'val 2')
      assert.isNull(schedule.nextGroup())

      group.completed()

      group = schedule.nextGroup()
      assert.deepEqual([...group.values()], ['val 2'])
    })

    //      |   +------------+
    //    A |   | w1 ---- w2 |
    //      |   +---\--------+
    //      |        \
    //      |        +\---+
    //    B |        | w3 |
    //      |        +----+
    //
    it('handles events for a group with internal dependencies', () => {
      let schedule = new Schedule()

      let w1 = schedule.add('A', [], 'val 1')
      let w2 = schedule.add('A', [w1], 'val 2')
      let w3 = schedule.add('B', [w1], 'val 3')

      let group = schedule.nextGroup().started()
      assert.deepEqual([...group.values()], ['val 1', 'val 2'])

      group.completed()

      assertGraph(schedule, {
        g1: ['B', [w3]]
      })
    })

    //      |   +    +    +------------+
    //    A |     w1      | w3      w4 |
    //      |   +   \+    +/-----------+
    //      |        \    /
    //      |        +\--/+
    //    B |        | w2 |
    //      |        +----+
    //
    it('removes completed groups from the shard list', () => {
      let schedule = new Schedule()

      let w1 = schedule.add('A', [], 'val 1')
      let w2 = schedule.add('B', [w1], 'val 2')
      let w3 = schedule.add('A', [w2], 'val 3')

      let group = schedule.nextGroup().started()
      assert.deepEqual([...group.values()], ['val 1'])

      group.completed()
      let w4 = schedule.add('A', [], 'val 4')

      assertGraph(schedule, {
        g1: ['B', [w2]],
        g2: ['A', [w3, w4], ['g1']]
      })
    })

    //      |        +------------+
    //    A |        | w2      w6 -------.
    //      |        +/--\--------+       \
    //      |        /    \                \
    //      |   +---/+    +\-----------+    \
    //    B |   | w1 |    | w3      w5 |     \
    //      |   +----+    +--------/---+      \
    //      |                     /            \
    //      |                +---/--------------\---+
    //    C |                | w4                w7 |
    //      |                +----------------------+
    //
    it('removes group dependencies that result from removed operation dependencies', () => {
      let schedule = new Schedule()

      let w1 = schedule.add('B', [], 'val 1')
      let w2 = schedule.add('A', [w1])
      let w3 = schedule.add('B', [w2])
      let w4 = schedule.add('C', [])
      let w5 = schedule.add('B', [w4])
      let w6 = schedule.add('A', [])
      let w7 = schedule.add('C', [w6])

      assertGraph(schedule, {
        g1: ['B', [w1]],
        g2: ['A', [w2, w6], ['g1']],
        g3: ['C', [w4, w7], ['g2']],
        g4: ['B', [w3, w5], ['g2', 'g3']]
      })

      let group = schedule.nextGroup().started()
      assert.deepEqual([...group.values()], ['val 1'])
      group.failed()

      assertGraph(schedule, {
        g1: ['A', [w6]],
        g2: ['C', [w4, w7], ['g1']]
      })
    })

    //      |             +----+
    //    A |             | w3 |
    //      |             +/--\+
    //      |             /    \
    //      |        +---/+    +\---+
    //    B |        | w2 |    | w4 |
    //      |        +/---+    +---\+
    //      |        /              \
    //      |   +---/+    +----+    +\------------+                   +--------------+
    //    C |   | w1 |    | w8 |    | w5      w10 |     =>            | w8       w10 |
    //      |   +----+    +/---+    +--------/----+                   +/--------/----+
    //      |             /                 /                         /        /
    //      |        +---/+                /                     +---/+       /
    //    D |        | w7 |               /                      | w7 |      /
    //      |        +/--\+              /                       +/--\+     /
    //      |        /    \             /                        /    \    /
    //      |   +---/+    +\---+       /                    +   /+    +\--/---------+
    //    E |   | w6 |    | w9 -------'                       w6      | w9      w11 |
    //      |   +----+    +----+                            +    +    +-------------+
    //
    it('re-optimises the remaining operations after a failure', () => {
      let schedule = new Schedule()

      let w1 = schedule.add('C', [], 'val 1')
      let w2 = schedule.add('B', [w1], 'val 2')
      let w3 = schedule.add('A', [w2])
      let w4 = schedule.add('B', [w3])
      let w5 = schedule.add('C', [w4])
      let w6 = schedule.add('E', [], 'val 6')
      let w7 = schedule.add('D', [w6])
      let w8 = schedule.add('C', [w7])
      let w9 = schedule.add('E', [w7])
      let w10 = schedule.add('C', [w9])

      assertGraph(schedule, {
        g1: ['C', [w1]],
        g2: ['B', [w2], ['g1']],
        g3: ['A', [w3], ['g2']],
        g4: ['B', [w4], ['g3']],
        g5: ['E', [w6]],
        g6: ['D', [w7], ['g5']],
        g7: ['C', [w8], ['g6']],
        g8: ['E', [w9], ['g6']],
        g9: ['C', [w5, w10], ['g4', 'g8']]
      })

      assertShardList(schedule, 'C', [w1], [w8], [w5, w10])

      let group1 = schedule.nextGroup().started()
      assert.deepEqual([...group1.values()], ['val 1'])

      let group2 = schedule.nextGroup().started()
      assert.deepEqual([...group2.values()], ['val 6'])

      group1.completed()
      let group3 = schedule.nextGroup().started()
      assert.deepEqual([...group3.values()], ['val 2'])

      let w11 = schedule.add('E', [])
      group3.failed()

      assertGraph(schedule, {
        g1: ['E', [w6]],
        g2: ['D', [w7], ['g1']],
        g3: ['E', [w9, w11], ['g2']],
        g4: ['C', [w8, w10], ['g2', 'g3']]
      })

      assertShardList(schedule, 'C', [w8, w10])
    })

    describe('out-of-order group processing', () => {
      let schedule, w1, w2, w3, w4, w5, w6, w7

      //      |                +----+
      //    A |                | w2 |
      //      |                +/--\+
      //      |                /    \
      //      |           +---/+    +\---+
      //    B |           | w1 |    | w3 |
      //      |           +----+    +---\+
      //      |                          \
      //      |        +------------+    +\---+
      //    C |        | w7      w5 |    | w4 |
      //      |        +/-----------+    +----+
      //      |        /
      //      |   +---/+
      //    D |   | w6 |
      //      |   +----+
      //
      beforeEach(() => {
        schedule = new Schedule()

        w1 = schedule.add('B', [], 'val 1')
        w2 = schedule.add('A', [w1], 'val 2')
        w3 = schedule.add('B', [w2], 'val 3')
        w4 = schedule.add('C', [w3], 'val 4')

        w5 = schedule.add('C', [], 'val 5')
        w6 = schedule.add('D', [], 'val 6')
        w7 = schedule.add('C', [w6], 'val 7')

        assertGraph(schedule, {
          g1: ['B', [w1]],
          g2: ['A', [w2], ['g1']],
          g3: ['B', [w3], ['g2']],
          g4: ['C', [w4], ['g3']],
          g5: ['D', [w6]],
          g6: ['C', [w7, w5], ['g5']]
        })

        assertShardList(schedule, 'C', [w7, w5], [w4])
      })

      it('allows new ops to be placed in any non-started group', () => {
        for (let i = 1; i <= 3; i++) {
          schedule.nextGroup().started().completed()
        }

        assertGraph(schedule, {
          g4: ['C', [w4], []],
          g5: ['D', [w6]],
          g6: ['C', [w7, w5], ['g5']]
        })

        let group1 = schedule.nextGroup().started()
        assert.deepEqual([...group1.values()], ['val 4'])

        let group2 = schedule.nextGroup().started()
        assert.deepEqual([...group2.values()], ['val 6'])

        let w8 = schedule.add('C', [], 'val 8')

        assertGraph(schedule, {
          g4: ['C', [w4], []],
          g5: ['D', [w6]],
          g6: ['C', [w7, w5, w8], ['g5']]
        })

        assertShardList(schedule, 'C', [w7, w5, w8], [w4])
      })

      it('allows any dependency-free group in a shard to be processed next', () => {
        let group1 = schedule.nextGroup().started()
        assert.deepEqual([...group1.values()], ['val 1'])

        let group2 = schedule.nextGroup().started()
        assert.deepEqual([...group2.values()], ['val 6'])

        group1.completed()

        group1 = schedule.nextGroup().started()
        assert.deepEqual([...group1.values()], ['val 2'])

        group1.completed()

        group1 = schedule.nextGroup().started()
        assert.deepEqual([...group1.values()], ['val 3'])

        group1.completed()

        group1 = schedule.nextGroup().started()
        assert.deepEqual([...group1.values()], ['val 4'])

        group2.completed()

        group2 = schedule.nextGroup()
        assert.isNull(group2)

        group1.completed()

        group2 = schedule.nextGroup()
        assert.deepEqual([...group2.values()], ['val 5', 'val 7'])
      })
    })
  })

  describe('group handles', () => {
    let w1, w2, w3, w4

    //      |   +----+
    //    A |   | w1 |
    //      |   +---\+
    //      |        \
    //      |        +\-----------+
    //    B |        | w3      w4 |
    //      |        +/-----------+
    //      |        /
    //      |   +---/+
    //    C |   | w2 |
    //      |   +----+
    //
    beforeEach(() => {
      schedule = new Schedule()

      w1 = schedule.add('A', [], 'val 1')
      w2 = schedule.add('C', [], 'val 2')
      w3 = schedule.add('B', [w1, w2], 'val 3')
      w4 = schedule.add('B', [], 'val 4')

      assertGraph(schedule, {
        g1: ['A', [w1]],
        g2: ['C', [w2]],
        g3: ['B', [w3, w4], ['g1', 'g2']]
      })
    })

    it('handles concurrent failure of groups with overlapping descendants', () => {
      let group1 = schedule.nextGroup().started()
      let group2 = schedule.nextGroup().started()

      group1.failed()

      assertGraph(schedule, {
        g1: ['C', [w2]],
        g2: ['B', [w4]]
      })

      group2.failed()

      assertGraph(schedule, {
        g1: ['B', [w4]]
      })
    })

    it('preserves any started group handles when a group fails', () => {
      let group1 = schedule.nextGroup().started()
      let group2 = schedule.nextGroup().started()

      group1.failed()

      assert.deepEqual([...group2.values()], ['val 2'])
      group2.completed()

      assertGraph(schedule, {
        g1: ['B', [w4]]
      })

      let group3 = schedule.nextGroup()
      assert.deepEqual([...group3.values()], ['val 4'])
    })

    it('invalidates any unstarted group handles when a group fails', () => {
      let group1 = schedule.nextGroup().started()
      let group2 = schedule.nextGroup()

      group1.failed()

      assertGraph(schedule, {
        g1: ['C', [w2]],
        g2: ['B', [w4]]
      })

      assert.throws(() => group2.started())
    })

    it('preserves the shard request state when a group fails', () => {
      let group1 = schedule.nextGroup().started()
      let group2 = schedule.nextGroup().started()

      let w5 = schedule.add('C', [], 'val 5')

      group1.failed()

      assertGraph(schedule, {
        g1: ['B', [w4]],
        g2: ['C', [w2]],
        g3: ['C', [w5]]
      })

      let group = schedule.nextGroup().started()
      assert.deepEqual([...group.values()], ['val 4'])

      group = schedule.nextGroup()
      assert.isNull(group)

      group2.completed()

      group = schedule.nextGroup()
      assert.deepEqual([...group.values()], ['val 5'])
    })
  })
})
