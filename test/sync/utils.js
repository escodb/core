'use strict'

function logger (logs, m1, m2) {
  return async () => {
    logs.push(m1)
    for (let i = 0; i < 5; i++) await null
    logs.push(m2)
  }
}

module.exports = {
  logger
}
