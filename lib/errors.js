'use strict'

class AccessDenied extends Error {
  code = 'ERR_ACCESS'
  name = 'AccessDenied'
}

class AuthenticationFailure extends Error {
  code = 'ERR_AUTH_FAILED'
  name = 'AuthenticationFailure'
}

class ConfigError extends Error {
  code = 'ERR_CONFIG'
  name = 'ConfigError'
}

class ConflictError extends Error {
  code = 'ERR_CONFLICT'
  name = 'ConflictError'
}

class Corruption extends Error {
  code = 'ERR_CORRUPT'
  name = 'Corruption'
}

class CounterError extends Error {
  code = 'ERR_COUNTER'
  name = 'CounterError'
}

class DecryptError extends Error {
  code = 'ERR_DECRYPT'
  name = 'DecryptError'
}

class ExistingStore extends Error {
  code = 'ERR_EXISTS'
  name = 'ExistingStore'
}

class KeyParseError extends Error {
  code = 'ERR_PARSE_KEY'
  name = 'KeyParseError'
}

class MissingKeyError extends Error {
  code = 'ERR_MISSING_KEY'
  name = 'MissingKeyError'
}

class MissingStore extends Error {
  code = 'ERR_MISSING'
  name = 'MissingStore'
}

class PathError extends Error {
  code = 'ERR_INVALID_PATH'
  name = 'PathError'
}

class ScheduleError extends Error {
  code = 'ERR_SCHEDULE'
  name = 'ScheduleError'
}

module.exports = {
  AccessDenied,
  AuthenticationFailure,
  ConfigError,
  ConflictError,
  Corruption,
  CounterError,
  DecryptError,
  ExistingStore,
  KeyParseError,
  MissingKeyError,
  MissingStore,
  PathError,
  ScheduleError
}
