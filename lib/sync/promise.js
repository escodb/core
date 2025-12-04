'use strict'

function withResolvers () {
  let resolve, reject

  let promise = new Promise((res, rej) => {
    resolve = res
    reject = rej
  })

  return { promise, resolve, reject }
}

module.exports = {
  withResolvers
}
