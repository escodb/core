'use strict'

const { ScheduleError } = require('./errors')

const PREFIX_OP    = 'w'
const PREFIX_GROUP = 'G'

const EQ = 1
const GT = 2

const DEFAULT_DEPTH_LIMIT = 2

const AVAILABLE = 1
const STARTED   = 2
const COMPLETED = 3
const FAILED    = 4

class Schedule {
  constructor (options = {}) {
    this._depthLimit = options.depthLimit || DEFAULT_DEPTH_LIMIT

    this._counters = options.counters || {
      operation: new Counter(PREFIX_OP),
      group: new Counter(PREFIX_GROUP)
    }

    this._operations = new Graph(this._counters.operation)
    this._groups = new Graph(this._counters.group)
    this._shards = new Map()
  }

  add (shard, deps, value = null, id = null) {
    let op = this._createOp(shard, deps, value, id)

    let depGroups = this._operations.get(op.parents).map((dep) => dep.group)
    depGroups = this._groups.get(depGroups)

    let group = this._findGroup(op, depGroups) || this._createGroup(op.shard)
    this._placeOpInGroup(op, group, depGroups)

    return op.id
  }

  shards () {
    return this._shards.keys()
  }

  nextGroup () {
    for (let shard of this._shards.values()) {
      if (shard.state === STARTED) continue

      let groups = this._groups.get(shard.groups)

      for (let group of groups) {
        if (group.state === AVAILABLE && group.ancestors.size === 0) {
          return new GroupHandle(this, group.id)
        }
      }
    }
    return null
  }

  _createOp (shard, deps, value, id = null) {
    let node = { shard, group: null, value }
    return this._operations.add(node, deps, id)
  }

  _createGroup (shard, idx = null, id = null) {
    if (!this._shards.has(shard)) {
      this._shards.set(shard, { groups: [], state: AVAILABLE })
    }

    let node = { shard, ops: new Set(), depth: 0, state: AVAILABLE }
    let group = this._groups.add(node, [], id)

    let shardGroups = this._shards.get(shard).groups

    if (idx === null) {
      shardGroups.push(group.id)
    } else {
      shardGroups.splice(idx, 0, group.id)
    }

    return group
  }

  _removeOp (op) {
    if (!this._operations.delete(op.id)) return

    let [group] = this._groups.get([op.group])
    group.ops.delete(op.id)
  }

  _removeGroup (group) {
    if (!this._groups.delete(group.id)) return

    this._updateDepth(group)

    let { groups } = this._shards.get(group.shard)
    let idx = groups.indexOf(group.id)
    if (idx >= 0) groups.splice(idx, 1)
  }

  _findGroup (op, depGroups) {
    if (!this._shards.has(op.shard)) return null
    let groups = this._groups.get(this._shards.get(op.shard).groups)

    let { type, idx } = this._getLowerBound(op, groups, depGroups)

    let depth = depGroups
        .filter((depGroup) => depGroup.shard !== op.shard)
        .reduce((d, g) => Math.max(d, g.depth + 1), 0)

    idx = this._findClosestDepth(groups, depth, idx)

    let group = groups[idx]
    if (!group) return null

    if (type === GT && this._wellSeparated(depth, groups, idx)) {
      group = this._createGroup(op.shard, idx)
    }

    if (depth >= group.depth + this._depthLimit + 2) {
      group = this._createGroup(op.shard, idx + 1)
    }

    return group
  }

  _getLowerBound (op, groups, depGroups) {
    let groupAncestors = depGroups.flatMap((group) => [...group.ancestors])

    let depIds = new Set(depGroups.map((group) => group.id))
    let ancIds = new Set(groupAncestors)
    let idx = groups.length
    let bound = null

    while (!bound && idx--) {
      let { id } = groups[idx]

      if (depIds.has(id)) bound = { type: EQ, idx }
      if (ancIds.has(id)) bound = { type: GT, idx: idx + 1 }
    }

    bound = bound || { type: GT, idx: 0 }

    while (bound.idx < groups.length && groups[bound.idx].state !== AVAILABLE) {
      bound = { type: GT, idx: bound.idx + 1 }
    }

    return bound
  }

  _findClosestDepth (groups, depth, idx) {
    while (idx + 1 < groups.length) {
      if (depth <= groups[idx].depth) break

      let diffL = depth - groups[idx].depth
      let diffR = groups[idx + 1].depth - depth

      if (groups[idx].descendants.size === 0 && diffL < diffR) {
        break
      } else {
        idx += 1
      }
    }
    return idx
  }

  _wellSeparated (depth, groups, idx) {
    let limit = this._depthLimit

    if (idx > 0 && depth < groups[idx - 1].depth + limit) {
      return false
    }
    if (depth > groups[idx].depth - limit) {
      return false
    }
    return true
  }

  _placeOpInGroup (op, group, depGroups) {
    group.ops.add(op.id)
    op.group = group.id

    for (let depGroup of depGroups) {
      this._groups.addParent(group, depGroup)
    }

    this._updateDepth(group)
  }

  _updateDepth (group) {
    let descendants = this._groups.get([group.id, ...group.descendants])
    Graph.sortTopological(descendants)

    for (let desc of descendants) {
      let parents = this._groups.get(desc.parents)
      desc.depth = parents.reduce((d, g) => Math.max(d, g.depth + 1), 0)
    }
  }

  _handleGroupStarted (id) {
    let [group] = this._groups.get([id])

    let shard = this._shards.get(group.shard)
    shard.state = STARTED
  }

  _handleGroupCompleted (id) {
    let [group] = this._groups.get([id])

    let shard = this._shards.get(group.shard)
    shard.state = AVAILABLE

    let ops = this._operations.get(group.ops)

    for (let op of ops) {
      this._removeOp(op)
    }
    this._removeGroup(group)
  }

  _handleGroupFailed (id) {
    let [group] = this._groups.get([id])
    let shard = this._shards.get(group.shard)

    let shardGroups = this._groups.get(shard.groups)
    let shardOps = shardGroups.flatMap((group) => [...group.ops])

    let ops = this._operations.get(shardOps)
    return this._cancel(ops)
  }

  _handleOpFailed (groupId, opId) {
    let [group] = this._groups.get([groupId])

    if (group.state !== STARTED) {
      throw new Error(`cannot mark '${opId}' as failed as group '${groupId}' is not started`)
    }
    if (!group.ops.has(opId)) {
      throw new Error(`group '${groupId}' does not contain operation '${opId}'`)
    }

    let [op] = this._operations.get([opId])
    return this._cancel([op])
  }

  _cancel (ops) {
    let cancelled = new Set(ops.flatMap((op) => [op.id, ...op.descendants]))
    let values = this._operations.get(cancelled).map((op) => op.value)

    this._rebalance(cancelled)
    return values
  }

  _rebalance (cancelled) {
    let plan = new Schedule({ counters: this._counters, depthLimit: this._depthLimit })
    let started = [...this._groups.values()].filter((g) => g.state === STARTED)

    for (let group of started) {
      let newGroup = plan._createGroup(group.shard, null, group.id)
      newGroup.state = STARTED
      plan._shards.get(group.shard).state = STARTED

      let ops = this._operations.get(group.ops)

      for (let op of ops) {
        let newOp = plan._createOp(op.shard, [], op.value, op.id)
        plan._placeOpInGroup(newOp, newGroup, [])
      }
    }

    for (let op of this._operations.values()) {
      if (cancelled.has(op.id)) continue
      if (plan._operations.has(op.id)) continue

      plan.add(op.shard, op.parents, op.value, op.id)
    }

    this._operations = plan._operations
    this._groups = plan._groups
    this._shards = plan._shards
  }
}

class GroupHandle {
  constructor (schedule, group) {
    this._schedule = schedule
    this._group = group
  }

  getShard () {
    return this._getGroup().shard
  }

  * values () {
    for (let [_, value] of this.valuesWithIds()) {
      yield value
    }
  }

  * valuesWithIds () {
    let opIds = this._getGroup().ops
    let ops = this._schedule._operations.get(opIds)
    Graph.sortTopological(ops)

    for (let op of ops) {
      yield [op.id, op.value]
    }
  }

  started () {
    this._changeState(AVAILABLE, STARTED)
    this._schedule._handleGroupStarted(this._group)
    return this
  }

  completed () {
    this._changeState(STARTED, COMPLETED)
    this._schedule._handleGroupCompleted(this._group)
    return this
  }

  failed () {
    this._changeState(STARTED, FAILED)
    return this._schedule._handleGroupFailed(this._group)
  }

  opFailed (id) {
    return this._schedule._handleOpFailed(this._group, id)
  }

  _changeState (before, after) {
    let group = this._getGroup()

    if (group.state === before) {
      group.state = after
    } else {
      let msg = `group cannot be moved from state ${before} to state ${after}`
      throw new Error(msg)
    }
  }

  _getGroup () {
    let [group] = this._schedule._groups.get([this._group])

    if (group) {
      return group
    } else {
      throw new Error(`stale group handle: '${this._group}'`)
    }
  }
}

class Graph {
  static sortTopological (nodes) {
    nodes.sort((a, b) => {
      if (b.ancestors.has(a.id)) {
        return -1
      } else if (a.ancestors.has(b.id)) {
        return 1
      } else {
        return 0
      }
    })
  }

  constructor (counter) {
    this._counter = counter
    this._nodes = new Map()
  }

  has (id) {
    return this._nodes.has(id)
  }

  get (ids) {
    ids = Array.isArray(ids) ? ids : [...ids]
    return ids.map((id) => this._nodes.get(id)).filter((node) => !!node)
  }

  values () {
    return this._nodes.values()
  }

  add (value, deps = [], id = null) {
    for (let depId of deps) {
      if (!this._nodes.has(depId)) {
        throw new ScheduleError(`unrecognised operation ID: '${depId}'`)
      }
    }

    let node = {
      id: id || this._counter.next(),
      parents: new Set(),
      ancestors: new Set(),
      descendants: new Set(),
      ...value
    }

    this._nodes.set(node.id, node)

    for (let depId of deps) {
      let dep = this._nodes.get(depId)
      this._buildAncestors(node, dep)
    }

    return node
  }

  addParent (node, dep) {
    if (node.id === dep.id) return
    this._buildAncestors(node, dep)
  }

  delete (id) {
    if (!this._nodes.has(id)) return false

    let node = this._nodes.get(id)
    this._nodes.delete(node.id)
    this._removeAncestor(node)

    return true
  }

  _buildAncestors (node, dep) {
    let ancestors = this.get([dep.id, ...dep.ancestors])
    let descendants = this.get([node.id, ...node.descendants])

    node.parents.add(dep.id)

    for (let anc of ancestors) {
      for (let desc of descendants) {
        anc.descendants.add(desc.id)
        desc.ancestors.add(anc.id)
      }
    }
  }

  _removeAncestor (node) {
    let descendants = this.get(node.descendants)

    for (let desc of descendants) {
      desc.parents.delete(node.id)
      desc.ancestors.delete(node.id)
    }
  }
}

class Counter {
  constructor (prefix) {
    this._prefix = prefix
    this._value = 0
  }

  next () {
    this._value += 1
    return this._prefix + this._value
  }
}

module.exports = Schedule
