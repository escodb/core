'use strict'

const TYPES = {
  u8:  [1, 'readUInt8',       'writeUInt8'],
  u16: [2, 'readUInt16BE',    'writeUInt16BE'],
  u32: [4, 'readUInt32BE',    'writeUInt32BE'],
  u64: [8, 'readBigUInt64BE', 'writeBigUInt64BE']
}

const BYTES = 'bytes'

function dump (pattern, values) {
  let sizes = pattern.map((item, i) => {
    if (item in TYPES) return TYPES[item][0]
    if (item === BYTES) return values[i].length
  })

  let size = sizes.reduce((a, b) => a + b, 0)
  let buffer = Buffer.alloc(size)
  let offset = 0

  for (let [i, item] of pattern.entries()) {
    if (item in TYPES) {
      let [size, _, write] = TYPES[item]
      buffer[write](values[i], offset)
      offset += size
    } else if (item === BYTES) {
      values[i].copy(buffer, offset)
      offset += values[i].length
    }
  }

  return buffer
}

function load (pattern, buffer) {
  let values = []
  let offset = 0

  for (let item of pattern) {
    if (item in TYPES) {
      let [size, read] = TYPES[item]
      values.push(buffer[read](offset))
      offset += size
    } else if (item === BYTES) {
      values.push(buffer.subarray(offset, buffer.length))
      offset = buffer.length
    }
  }

  if (offset === buffer.length) {
    return values
  } else {
    throw new Error(`incomplete buffer parse: <${buffer.toString('base64')}> using [${pattern.join(', ')}]`)
  }
}

module.exports = { dump, load }
