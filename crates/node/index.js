'use strict'

const { AsyncLocalStorage } = require('node:async_hooks')
const { inspect } = require('node:util')
const native = require('./binding.js')

const { NativeDb, NativeIndex, CommitEvent } = native

const DONE = Object.freeze({ value: undefined, done: true })

function isThenable(value) {
  return value !== null && (typeof value === 'object' || typeof value === 'function') && typeof value.then === 'function'
}

function checkSql(sql) {
  if (typeof sql !== 'string') throw new TypeError(`sql must be a string, not ${typeof sql}`)
}

// Rows. Native calls pack every row's values into a buffer of tags and
// 8-byte slots, strings of text that each value's length slices, and an
// array of blobs. A constructor compiled once per column list builds the
// row objects.

const NULL = 0
const NUMBER = 1
const TEXT = 2
const BLOB = 3
const BIGINT = 4
/** Float64 slots before the packed values: cells, slots, texts and columns, then two the call fills. */
const HEADER = 6

class Views {
  constructor(buffer) {
    this.buffer = buffer
    this.f64 = new Float64Array(buffer)
    this.i64 = new BigInt64Array(buffer)
    this.u32 = new Uint32Array(buffer)
    this.u8 = new Uint8Array(buffer)
  }
}

// The buffer synchronous calls pack into, read before the next call. It
// grows when a result needs more room.
let scratch = new Views(new ArrayBuffer(64 * 1024))

/** The views of a call's result: its own buffer, or the scratch buffer. */
function viewsOf(buffer) {
  if (buffer === undefined) return scratch
  const views = new Views(buffer)
  if (buffer.byteLength > scratch.buffer.byteLength) scratch = views
  return views
}

function unpack(views, chunks, blobs) {
  const { f64, i64, u32, u8 } = views
  const cells = f64[0]
  const slots = f64[1]
  const texts = f64[2]
  const values = new Array(cells)
  let slot = HEADER
  let length = (HEADER + slots) * 2
  const tags = (HEADER + slots) * 8 + texts * 4
  // No text, one chunk as a string, or an array of chunks.
  let chunk = chunks === undefined ? '' : typeof chunks === 'string' ? chunks : chunks[0]
  let next = 1
  let at = 0
  let blob = 0
  for (let i = 0; i < cells; i++) {
    switch (u8[tags + i]) {
      case NULL:
        values[i] = null
        break
      case NUMBER:
        values[i] = f64[slot++]
        break
      case TEXT: {
        const units = u32[length++]
        // A text that does not fit the rest of its chunk starts the next.
        while (at + units > chunk.length) {
          chunk = chunks[next++]
          at = 0
        }
        values[i] = chunk.slice(at, at + units)
        at += units
        break
      }
      case BLOB:
        values[i] = blobs[blob++]
        break
      case BIGINT:
        values[i] = i64[slot++]
        break
    }
  }
  return values
}

const shapes = new Map()
const MAX_SHAPES = 1024

function compile(columns) {
  // Duplicate names keep the last value, as an object literal does. A
  // computed key keeps `__proto__` an ordinary property.
  const fields = columns.map((column, i) => `[${JSON.stringify(column)}]: v[i + ${i}]`)
  try {
    return new Function('v', 'i', `return { ${fields.join(', ')} }`)
  } catch {
    // Code generation is off (`--disallow-code-generation-from-strings`).
    return (v, i) => {
      const row = {}
      for (let j = 0; j < columns.length; j++) {
        Object.defineProperty(row, columns[j], { value: v[i + j], enumerable: true, writable: true, configurable: true })
      }
      return row
    }
  }
}

/** The row constructor for `width` columns whose names `key` joins with NUL. */
function shapeOf(key, width) {
  let shape = shapes.get(key)
  if (shape === undefined || shape.width !== width) {
    if (shapes.size >= MAX_SHAPES) shapes.clear()
    // One column's key is its name, which may be empty.
    const columns = width === 0 ? [] : key.split('\0')
    shape = { width, make: compile(columns) }
    shapes.set(key, shape)
  }
  return shape
}

/** Rows from a packed `[buffer, chunks, columns, blobs]`; `views` holds the unpacked buffer. */
function toRows(views, parts) {
  const values = unpack(views, parts[1], parts[3])
  const count = values.length
  if (count === 0) return []
  const { width, make } = shapeOf(parts[2], views.f64[3])
  const rows = new Array(count / width)
  for (let i = 0, r = 0; i < count; i += width) rows[r++] = make(values, i)
  return rows
}

function toResult(parts) {
  const views = viewsOf(parts[0])
  // Read the header before anything else can pack into the scratch buffer.
  const version = views.f64[4]
  const unchanged = views.f64[5] === 1
  return {
    rows: unchanged ? [] : toRows(views, parts),
    version,
    hash: parts[4],
    readSet: parts[5] === undefined ? undefined : new ReadSet(kToken, parts[5]),
    unchanged,
  }
}

// Read sets. Each wraps a native handle; only query results make them.

const kToken = Symbol('sqlive.readSet')
let handleOf

/** The tables, columns and rows a query result depends on, as of the snapshot version it read. */
class ReadSet {
  #handle

  static {
    handleOf = (readSet) => {
      if (!(readSet instanceof ReadSet)) throw new TypeError('expected a ReadSet from a query result')
      return readSet.#handle
    }
  }

  constructor(token, handle) {
    if (token !== kToken) throw new TypeError('read sets come from query results')
    this.#handle = handle
  }

  /** The snapshot version the query read. */
  get version() {
    return native.readSetVersion(this.#handle)
  }

  /** Whether every commit after the version intersects. */
  get everyCommit() {
    return native.readSetEveryCommit(this.#handle)
  }

  /** Each table the read set covers, with its columns and the rows it depends on as SQL. */
  get tables() {
    return native.readSetTables(this.#handle)
  }

  /** Whether `event` may have changed the query's result. */
  intersects(event) {
    return native.readSetIntersects(this.#handle, event)
  }

  /** Whether the result may depend on some row of `table`. */
  readsRowsOf(table) {
    return native.readSetReadsRowsOf(this.#handle, table)
  }

  toString() {
    return native.readSetDisplay(this.#handle)
  }

  toJSON() {
    return { version: this.version, everyCommit: this.everyCommit, tables: this.tables }
  }

  [inspect.custom]() {
    return `ReadSet { version: ${this.version}, ${this.everyCommit ? 'every commit' : `reads: ${JSON.stringify(this.toString())}`} }`
  }
}

/**
 * Read sets stored under keys you choose, such as subscription ids.
 * `affected(event)` returns the keys whose queries the commit may change.
 */
class ReadSetIndex {
  #native = new NativeIndex()

  get size() {
    return this.#native.size
  }

  /** Stores `readSet` under `key`, a number or a string, replacing the key's previous one. */
  insert(key, readSet) {
    this.#native.insert(key, handleOf(readSet))
    return this
  }

  /** Drops the key's read set; false if the key held none. */
  delete(key) {
    return this.#native.delete(key)
  }

  has(key) {
    return this.#native.has(key)
  }

  get(key) {
    const handle = this.#native.get(key)
    return handle === undefined ? undefined : new ReadSet(kToken, handle)
  }

  /** The keys whose queries `event` may change, in key order: numbers first, then strings. */
  affected(event) {
    return this.#native.affected(event)
  }
}

// Events and read sets stay native; these make them pleasant to inspect.

Object.defineProperty(CommitEvent.prototype, 'changes', {
  configurable: true,
  enumerable: false,
  get() {
    const changes = this.readChanges()
    Object.defineProperty(this, 'changes', { value: changes, enumerable: true })
    return changes
  },
})

Object.defineProperty(CommitEvent.prototype, 'toJSON', {
  configurable: true,
  writable: true,
  value: function toJSON() {
    return {
      version: this.version,
      schemaChanged: this.schemaChanged,
      changes: this.changes,
      wholeTables: this.wholeTables,
    }
  },
})

Object.defineProperty(CommitEvent.prototype, inspect.custom, {
  configurable: true,
  value: function (depth, options, inspectValue = inspect) {
    return `CommitEvent ${inspectValue(this.toJSON(), { ...options, depth: options.depth == null ? null : options.depth - 1 })}`
  },
})

// Change streams. A stream waits on the database's hub, which wakes it
// after each commit this process makes.

class Hub {
  constructor() {
    this.waiting = new Set()
    this.closed = false
    /** The newest version a write or read has reported. */
    this.seen = 0
  }

  notify() {
    if (this.waiting.size === 0) return
    for (const stream of [...this.waiting]) stream._wake()
  }

  /**
   * Wakes the streams after a read that found a version newer than any
   * reported, as when the database is ahead of the log and the log moves to
   * catch up, which lags every stream.
   */
  read(version) {
    if (version > this.seen) {
      this.seen = version
      this.notify()
    }
  }

  close() {
    this.closed = true
    this.notify()
  }
}

class ChangeStream {
  #native
  #hub
  #waiters = []
  #done = false

  constructor(stream, hub) {
    this.#native = stream
    this.#hub = hub
  }

  /** The version of the event this stream yields next. */
  get nextVersion() {
    return this.#native.nextVersion
  }

  /** The next event if one has committed, or `undefined`. */
  tryNext() {
    if (this.#done) return undefined
    let event
    try {
      event = this.#native.tryNext()
    } catch (error) {
      this.#done = true
      throw error
    }
    if (event == null) {
      if (this.#hub.closed) this.#done = true
      return undefined
    }
    return event
  }

  next() {
    if (this.#waiters.length === 0) {
      let event
      try {
        event = this.tryNext()
      } catch (error) {
        return Promise.reject(error)
      }
      if (event !== undefined) return Promise.resolve({ value: event, done: false })
      if (this.#done) return Promise.resolve(DONE)
    }
    return new Promise((resolve, reject) => {
      this.#waiters.push({ resolve, reject })
      this.#hub.waiting.add(this)
    })
  }

  return() {
    this.close()
    return Promise.resolve(DONE)
  }

  /** Ends the stream; pending `next()` calls resolve as done. */
  close() {
    this.#done = true
    for (const waiter of this.#waiters.splice(0)) waiter.resolve(DONE)
    this.#hub.waiting.delete(this)
  }

  [Symbol.asyncIterator]() {
    return this
  }

  _wake() {
    while (this.#waiters.length > 0) {
      let event
      try {
        event = this.tryNext()
      } catch (error) {
        this.#waiters.shift().reject(error)
        continue
      }
      if (event !== undefined) {
        this.#waiters.shift().resolve({ value: event, done: false })
      } else if (this.#done) {
        for (const waiter of this.#waiters.splice(0)) waiter.resolve(DONE)
      } else {
        return
      }
    }
    this.#hub.waiting.delete(this)
  }
}

// Writes. Each asynchronous write is a job: the writer thread opens its
// transaction and asks for its callback through `dispatch`, then reports
// how the commit went.

const writing = new AsyncLocalStorage()
const jobs = new Map()
let lastJob = 0

function nextJob() {
  do {
    lastJob = lastJob >= 0xfffffffe ? 1 : lastJob + 1
  } while (jobs.has(lastJob))
  return lastJob
}

function dispatch(kind, id, payload) {
  const job = jobs.get(id)
  if (job === undefined) return
  if (kind === 0) {
    job.begin(id)
    return
  }
  jobs.delete(id)
  job.settle(kind === 1, payload)
}

function nestedWrite() {
  const error = new Error(
    'write cannot run inside another write: the outer write holds the lock it would wait for. Use the tx the outer write passed to its callback.',
  )
  error.code = 'ERR_SQLIVE_WRITE_PENDING'
  return error
}

class WriteJob {
  constructor(db, hub, fn, resolve, reject) {
    this.db = db
    this.hub = hub
    this.fn = fn
    this.resolve = resolve
    this.reject = reject
    this.open = false
    this.value = undefined
    this.failed = false
    this.error = undefined
  }

  begin(id) {
    this.open = true
    let result
    try {
      result = writing.run(this, this.fn, new Transaction(this.db, id))
    } catch (error) {
      this.finish(id, false, error)
      return
    }
    if (isThenable(result)) {
      Promise.resolve(result).then(
        (value) => this.finish(id, true, value),
        (error) => this.finish(id, false, error),
      )
    } else {
      this.finish(id, true, result)
    }
  }

  finish(id, succeeded, outcome) {
    this.open = false
    if (succeeded) {
      this.value = outcome
    } else {
      this.failed = true
      this.error = outcome
    }
    this.db.finishWrite(id, succeeded)
  }

  settle(committed, payload) {
    this.db.settled()
    if (committed) this.hub.seen = Math.max(this.hub.seen, payload)
    this.hub.notify()
    if (this.failed) this.reject(this.error)
    else if (committed) this.resolve({ value: this.value, version: payload })
    else this.reject(payload ?? new Error('the write failed'))
  }
}

class VacuumJob {
  constructor(db, hub, resolve, reject) {
    this.db = db
    this.hub = hub
    this.resolve = resolve
    this.reject = reject
  }

  settle(committed, payload) {
    this.db.settled()
    if (committed) this.hub.seen = Math.max(this.hub.seen, payload)
    this.hub.notify()
    if (committed) this.resolve(payload)
    else this.reject(payload ?? new Error('the vacuum failed'))
  }
}

/** A write's open transaction, valid until its callback finishes. */
class Transaction {
  #db
  #job

  constructor(db, job) {
    this.#db = db
    this.#job = job
  }

  /** Runs a statement; returns how many rows it changed and the last rowid it inserted. */
  run(sql, params) {
    checkSql(sql)
    return this.#db.txRun(this.#job, sql, params)
  }

  /** Every row a statement returns, as objects keyed by column name. */
  all(sql, params) {
    checkSql(sql)
    const parts = this.#db.txQuery(this.#job, sql, params, undefined, scratch.buffer)
    return toRows(viewsOf(parts[0]), parts)
  }

  /** The first row a statement returns, or `undefined`. */
  get(sql, params) {
    checkSql(sql)
    const parts = this.#db.txQuery(this.#job, sql, params, 1, scratch.buffer)
    return toRows(viewsOf(parts[0]), parts)[0]
  }

  /** Runs a script of statements separated by semicolons, without parameters. */
  exec(sql) {
    checkSql(sql)
    this.#db.txExec(this.#job, sql)
  }
}

/** A read transaction on one version of the database. */
class Snapshot {
  #native
  #hub

  constructor(snapshot, hub) {
    this.#native = snapshot
    this.#hub = hub
  }

  get version() {
    return this.#native.version
  }

  query(sql, params) {
    checkSql(sql)
    return toResult(this.#native.query(sql, params, undefined, scratch.buffer))
  }

  rerun(sql, params, hash) {
    checkSql(sql)
    return toResult(this.#native.query(sql, params, hash, scratch.buffer))
  }

  /** Ends the read transaction. */
  close() {
    this.#native.close()
  }

  [Symbol.dispose]() {
    this.close()
  }
}

// Subscriptions. One follower per database reads its change stream, asks a
// shared index which subscriptions each commit affects, and re-runs those.

class Subscription {
  #live
  #id
  #hash
  #latest
  #pending
  #error
  #waiters = []
  #done = false

  constructor(live, id, sql, params, result) {
    this.#live = live
    this.#id = id
    this.sql = sql
    this.params = params
    this.#hash = result.hash
    this.#latest = result
    this.#pending = result
  }

  /** The newest result, whose rows the subscription last delivered or will deliver next. */
  get result() {
    return this.#latest
  }

  get hash() {
    return this.#hash
  }

  get closed() {
    return this.#done
  }

  next() {
    if (this.#pending !== undefined) {
      const value = this.#pending
      this.#pending = undefined
      return Promise.resolve({ value, done: false })
    }
    if (this.#error !== undefined) {
      const error = this.#error
      this.#error = undefined
      return Promise.reject(error)
    }
    if (this.#done) return Promise.resolve(DONE)
    return new Promise((resolve, reject) => this.#waiters.push({ resolve, reject }))
  }

  return() {
    this.close()
    return Promise.resolve(DONE)
  }

  /** Stops the subscription; pending `next()` calls resolve as done. */
  close() {
    if (this.#done) return
    this.#live.remove(this.#id)
    this.#end()
  }

  [Symbol.asyncIterator]() {
    return this
  }

  [Symbol.dispose]() {
    this.close()
  }

  _update(result) {
    this.#hash = result.hash
    if (result.unchanged) return
    this.#latest = result
    const waiter = this.#waiters.shift()
    if (waiter !== undefined) waiter.resolve({ value: result, done: false })
    else this.#pending = result
  }

  _fail(error) {
    this.#done = true
    const waiter = this.#waiters.shift()
    if (waiter !== undefined) waiter.reject(error)
    else this.#error = error
    for (const rest of this.#waiters.splice(0)) rest.resolve(DONE)
  }

  _end() {
    this.#end()
  }

  #end() {
    this.#done = true
    for (const waiter of this.#waiters.splice(0)) waiter.resolve(DONE)
  }
}

class Live {
  #db
  #native
  #index = new NativeIndex()
  #subscriptions = new Map()
  #stream = null
  #lastId = 0

  constructor(db, native) {
    this.#db = db
    this.#native = native
  }

  /** Runs a subscription's query, storing its read set in the index. */
  #query(id, sql, params, hash) {
    return toResult(this.#native.queryInto(this.#index, id, sql, params, hash, scratch.buffer))
  }

  subscribe(sql, params) {
    // Re-runs bind these values, so a caller's later change to its array or
    // object must not reach them.
    if (Array.isArray(params)) params = [...params]
    else if (params !== null && typeof params === 'object' && !ArrayBuffer.isView(params)) params = { ...params }
    const id = ++this.#lastId
    const result = this.#query(id, sql, params, undefined)
    const subscription = new Subscription(this, id, sql, params, result)
    this.#subscriptions.set(id, subscription)
    if (this.#stream === null) this.#follow(this.#db.changesSince(result.version))
    return subscription
  }

  remove(id) {
    if (!this.#subscriptions.delete(id)) return
    this.#index.delete(id)
    if (this.#subscriptions.size === 0 && this.#stream !== null) {
      const stream = this.#stream
      this.#stream = null
      stream.close()
    }
  }

  #follow(stream) {
    this.#stream = stream
    this.#run(stream).catch((error) => this.#failAll(error))
  }

  async #run(stream) {
    for (;;) {
      let step
      try {
        step = await stream.next()
      } catch (error) {
        if (stream !== this.#stream) return
        if (error.code === 'ERR_SQLIVE_LAGGED') {
          this.#resync()
          return
        }
        throw error
      }
      if (stream !== this.#stream) return
      if (step.done) {
        this.#endAll()
        return
      }
      for (const id of this.#index.affected(step.value)) this.#rerun(id)
    }
  }

  #rerun(id) {
    const subscription = this.#subscriptions.get(id)
    if (subscription === undefined) return
    let result
    try {
      result = this.#query(id, subscription.sql, subscription.params, subscription.hash)
    } catch (error) {
      this.remove(id)
      subscription._fail(error)
      return
    }
    subscription._update(result)
  }

  /** After the stream falls behind the log: follow from now and re-run everything. */
  #resync() {
    if (this.#db.closed) {
      this.#endAll()
      return
    }
    this.#follow(this.#db.changesSince(this.#db.version))
    for (const id of [...this.#subscriptions.keys()]) this.#rerun(id)
  }

  #failAll(error) {
    this.#stream = null
    for (const [id, subscription] of [...this.#subscriptions]) {
      this.remove(id)
      subscription._fail(error)
    }
  }

  #endAll() {
    this.#stream = null
    for (const [id, subscription] of [...this.#subscriptions]) {
      this.remove(id)
      subscription._end()
    }
  }
}

/** A SQLite database whose writes publish commit events. */
class LiveDb {
  #native
  #hub = new Hub()
  #live = null

  /**
   * Opens or creates the database at `path`.
   * @param {string} path
   * @param {object} [options]
   */
  constructor(path, options) {
    if (typeof path !== 'string') throw new TypeError(`path must be a string, not ${typeof path}`)
    this.#native = new NativeDb(path, options ?? undefined)
  }

  static open(path, options) {
    return new LiveDb(path, options)
  }

  get path() {
    return this.#native.path
  }

  /** The latest version the log has reached. */
  get version() {
    return this.#native.version
  }

  get closed() {
    return this.#native.closed
  }

  /**
   * Runs `fn(tx)` inside a transaction and commits it. The callback runs on
   * this thread; the commit and its fsync run on the writer thread.
   */
  write(fn) {
    if (typeof fn !== 'function') return Promise.reject(new TypeError('write takes a callback'))
    const outer = writing.getStore()
    if (outer !== undefined && outer.open && outer.db === this.#native) return Promise.reject(nestedWrite())
    return new Promise((resolve, reject) => {
      const id = nextJob()
      jobs.set(id, new WriteJob(this.#native, this.#hub, fn, resolve, reject))
      try {
        this.#native.queueWrite(id, dispatch)
      } catch (error) {
        jobs.delete(id)
        reject(error)
      }
    })
  }

  /** Runs `fn(tx)` inside a transaction and commits it, blocking until the commit's fsync returns. */
  writeSync(fn) {
    if (typeof fn !== 'function') throw new TypeError('writeSync takes a callback')
    const id = nextJob()
    const tx = new Transaction(this.#native, id)
    let value
    try {
      const version = this.#native.writeSync(id, () => {
        const result = fn(tx)
        if (isThenable(result)) {
          throw new TypeError('a writeSync callback must be synchronous; use write for an async callback')
        }
        value = result
      })
      this.#hub.seen = Math.max(this.#hub.seen, version)
      return { value, version }
    } finally {
      this.#hub.notify()
    }
  }

  /** Runs `VACUUM` on the writer thread and resolves with the version of its event. */
  vacuum() {
    const outer = writing.getStore()
    if (outer !== undefined && outer.open && outer.db === this.#native) return Promise.reject(nestedWrite())
    return new Promise((resolve, reject) => {
      const id = nextJob()
      jobs.set(id, new VacuumJob(this.#native, this.#hub, resolve, reject))
      try {
        this.#native.queueVacuum(id, dispatch)
      } catch (error) {
        jobs.delete(id)
        reject(error)
      }
    })
  }

  query(sql, params) {
    checkSql(sql)
    const result = toResult(this.#native.query(sql, params, undefined, scratch.buffer))
    this.#hub.read(result.version)
    return result
  }

  rerun(sql, params, hash) {
    checkSql(sql)
    const result = toResult(this.#native.query(sql, params, hash, scratch.buffer))
    this.#hub.read(result.version)
    return result
  }

  queryAsync(sql, params) {
    checkSql(sql)
    return this.#native.queryAsync(sql, params, undefined).then((parts) => {
      const result = toResult(parts)
      this.#hub.read(result.version)
      return result
    })
  }

  rerunAsync(sql, params, hash) {
    checkSql(sql)
    return this.#native.queryAsync(sql, params, hash).then((parts) => {
      const result = toResult(parts)
      this.#hub.read(result.version)
      return result
    })
  }

  snapshot() {
    const snapshot = new Snapshot(this.#native.snapshot(), this.#hub)
    this.#hub.read(snapshot.version)
    return snapshot
  }

  /** A stream of the events committed after now. */
  changes() {
    return new ChangeStream(this.#native.changes(), this.#hub)
  }

  /** A stream of the events after `version`, starting with any the log retains. */
  changesSince(version) {
    return new ChangeStream(this.#native.changesSince(version), this.#hub)
  }

  /** A live query: an async iterable of its results, the first at once and then each one whose rows change. */
  subscribe(sql, params) {
    checkSql(sql)
    if (this.#live === null) this.#live = new Live(this, this.#native)
    return this.#live.subscribe(sql, params)
  }

  /** Closes the database once its queued writes finish. Streams and subscriptions end. */
  close() {
    this.#native.close()
    this.#hub.close()
  }

  [Symbol.dispose]() {
    this.close()
  }
}

module.exports = {
  LiveDb,
  Transaction,
  Snapshot,
  ChangeStream,
  Subscription,
  CommitEvent,
  ReadSet,
  ReadSetIndex,
}
