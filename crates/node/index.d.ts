/**
 * SqLive: live queries for SQLite. Every query returns its rows plus a read
 * set, the tables, columns and key ranges the result depends on. Every
 * committed write publishes a commit event listing the rows it changed.
 * `readSet.intersects(event)` says which queries to re-run, and
 * `db.subscribe()` re-runs them for you.
 */

/** A value SQLite stores, as a query returns it. Integers are numbers, or bigints with `safeIntegers`. */
export type Value = null | number | bigint | string | Buffer

/** A value a parameter binds. Booleans bind as 0 and 1; byte arrays bind as blobs. */
export type Param = null | undefined | number | bigint | boolean | string | ArrayBuffer | ArrayBufferView

/**
 * Parameters for `?`, `?N`, `:name`, `@name` and `$name`: an array of
 * positional values, or an object of named ones. An object key may carry
 * its prefix; a bare key takes the prefix the SQL uses.
 */
export type Params = readonly Param[] | { readonly [name: string]: Param }

/** A row, keyed by column name. */
export type Row = Record<string, Value>

export interface Options {
  /** Read-only connections kept open for queries. Default 4. */
  readers?: number
  /** Commit events the log retains for `changesSince`. Default 4,096. */
  logCapacity?: number
  /** Bytes of commit events the log retains. Default 64 MiB. */
  logBytes?: number
  /** Rows of one table an event lists before it names the table whole. Default 10,000; `Infinity` lists every row. */
  tableChanges?: number
  /** Milliseconds a connection waits for a lock. Default 5,000. */
  busyTimeout?: number
  /** WAL pages after which a commit checkpoints. Default 1,000. */
  checkpointPages?: number
  /** The SQLite VFS to open the database with. */
  vfs?: string
  /** Read every integer as a bigint. Otherwise an integer past 53 bits throws a `RangeError`. */
  safeIntegers?: boolean
}

/** A query's rows, the version of the snapshot it read, what the rows depend on, and their hash. */
export interface QueryResult<R = Row> {
  /** Empty when the result is `unchanged`. */
  rows: R[]
  version: number
  /** 64 hex digits of a BLAKE3 digest of the column names and rows. */
  hash: string
  readSet: ReadSet
  /** Set by `rerun` when the rows hash the same as the previous result's. */
  unchanged: boolean
}

/** A subscription's result. The subscription keeps the read set itself. */
export interface SubscriptionResult<R = Row> {
  rows: R[]
  version: number
  hash: string
  unchanged: false
}

export interface RunResult {
  /** Rows the statement inserted, updated or deleted. */
  changes: number
  /** The rowid of the connection's last insert. */
  lastInsertRowid: number | bigint
}

export interface WriteResult<T> {
  /** What the callback returned. */
  value: T
  /** The commit's version. */
  version: number
}

/** A write's open transaction, valid until its callback finishes. */
export declare class Transaction {
  private constructor()
  /** Runs a statement, stepping through any rows it returns. */
  run(sql: string, params?: Params): RunResult
  /** Every row a statement returns. */
  all<R = Row>(sql: string, params?: Params): R[]
  /** The first row a statement returns, or `undefined`. */
  get<R = Row>(sql: string, params?: Params): R | undefined
  /** Runs a script of statements separated by semicolons, without parameters. */
  exec(sql: string): void
}

/** A SQLite database whose writes publish commit events. */
export declare class LiveDb {
  /** Opens or creates the database at `path`, in WAL mode with `synchronous = FULL`. */
  constructor(path: string, options?: Options)
  static open(path: string, options?: Options): LiveDb
  readonly path: string
  /** The latest version the log has reached. */
  readonly version: number
  readonly closed: boolean
  /**
   * Runs `fn(tx)` inside a transaction and commits it. The callback runs on
   * this thread and may be async, holding the transaction across its
   * awaits; the writer thread commits, so the fsync never blocks the event
   * loop. A callback that throws or rejects rolls the transaction back, and
   * the promise rejects with its error.
   */
  write<T>(fn: (tx: Transaction) => T | PromiseLike<T>): Promise<WriteResult<Awaited<T>>>
  /**
   * Runs `fn(tx)` inside a transaction and commits it on this thread,
   * blocking until the commit's fsync returns. Throws while an
   * asynchronous write is pending.
   */
  writeSync<T>(fn: (tx: Transaction) => T): WriteResult<T>
  /** Runs `VACUUM` on the writer thread; resolves with the version of its event. */
  vacuum(): Promise<number>
  /** Runs a read-only query on this thread, in a fresh snapshot. */
  query<R = Row>(sql: string, params?: Params): QueryResult<R>
  /** Runs a query again; when its rows hash as `hash` does, the result is `unchanged` and carries no rows. */
  rerun<R = Row>(sql: string, params: Params | undefined, hash: string): QueryResult<R>
  /** Runs a query on libuv's thread pool, so several run at once across reader connections. */
  queryAsync<R = Row>(sql: string, params?: Params): Promise<QueryResult<R>>
  rerunAsync<R = Row>(sql: string, params: Params | undefined, hash: string): Promise<QueryResult<R>>
  /** A read transaction on the latest version, for several queries that must agree. */
  snapshot(): Snapshot
  /** A stream of the events committed after now. */
  changes(): ChangeStream
  /** A stream of the events after `version`, starting with any the log retains. */
  changesSince(version: number): ChangeStream
  /**
   * A live query: an async iterable of results, the first at once, then each
   * one whose rows changed. A slow consumer gets the newest result.
   */
  subscribe<R = Row>(sql: string, params?: Params): Subscription<R>
  /** Closes the database once its queued writes finish. Streams and subscriptions end. */
  close(): void
  [Symbol.dispose](): void
}

/** A read transaction on one version of the database. */
export declare class Snapshot {
  private constructor()
  readonly version: number
  query<R = Row>(sql: string, params?: Params): QueryResult<R>
  rerun<R = Row>(sql: string, params: Params | undefined, hash: string): QueryResult<R>
  /** Ends the read transaction. */
  close(): void
  [Symbol.dispose](): void
}

export type Op = 'insert' | 'update' | 'delete'

/** One row's net change within a transaction. */
export interface RowChange {
  table: string
  op: Op
  /** `null` for a `WITHOUT ROWID` table, whose primary key identifies the row. */
  rowid: number | bigint | null
  /** The row's stored columns before the transaction; `null` for an insert. */
  old: Row | null
  /** The row's stored columns after the transaction; `null` for a delete. */
  new: Row | null
}

/** The net row changes of one committed transaction. */
export declare class CommitEvent {
  private constructor()
  readonly version: number
  /** The transaction changed the schema or ran `VACUUM`; `changes` is empty. Reload what you hold. */
  readonly schemaChanged: boolean
  /** One entry per changed row, in the order the transaction first touched it. Converted on first access. */
  readonly changes: RowChange[]
  /** Tables the transaction changed more than `tableChanges` rows of; `changes` lists none of their rows. */
  readonly wholeTables: string[]
  toJSON(): { version: number; schemaChanged: boolean; changes: RowChange[]; wholeTables: string[] }
}

/**
 * Commit events in version order. Iterating waits for the next commit and
 * ends when the database closes. A stream that falls behind the retained
 * log rejects once with `ERR_SQLIVE_LAGGED`, then ends.
 */
export declare class ChangeStream implements AsyncIterableIterator<CommitEvent> {
  private constructor()
  /** The version of the event this stream yields next. */
  readonly nextVersion: number
  /** The next event if one has committed, or `undefined`. */
  tryNext(): CommitEvent | undefined
  next(): Promise<IteratorResult<CommitEvent, undefined>>
  return(): Promise<IteratorResult<CommitEvent, undefined>>
  /** Ends the stream; pending `next()` calls resolve as done. */
  close(): void
  [Symbol.asyncIterator](): ChangeStream
}

/** A live query. Iterate it for results; `break` or `close()` ends it. */
export declare class Subscription<R = Row> implements AsyncIterableIterator<SubscriptionResult<R>> {
  private constructor()
  readonly sql: string
  readonly params: Params | undefined
  /** The newest result, whose rows the subscription delivered or will deliver next. */
  readonly result: SubscriptionResult<R>
  readonly hash: string
  readonly closed: boolean
  next(): Promise<IteratorResult<SubscriptionResult<R>, undefined>>
  return(): Promise<IteratorResult<SubscriptionResult<R>, undefined>>
  close(): void
  [Symbol.asyncIterator](): Subscription<R>
  [Symbol.dispose](): void
}

/** One table a read set covers. */
export interface TableRead {
  name: string
  /** The columns read, in lowercase. */
  columns: string[]
  /** An update to any column counts. */
  everyColumn: boolean
  /** The rows the result depends on, as SQL; `TRUE` for every row. */
  filter: string
}

/** The tables, columns and rows a query result depends on, as of the snapshot version it read. */
export declare class ReadSet {
  private constructor()
  readonly version: number
  /** Every commit after the version intersects. */
  readonly everyCommit: boolean
  readonly tables: TableRead[]
  /** Whether `event` may have changed the result. False for an event at or below the read set's version. */
  intersects(event: CommitEvent): boolean
  /** Whether the result may depend on some row of `table`. */
  readsRowsOf(table: string): boolean
  toString(): string
  toJSON(): { version: number; everyCommit: boolean; tables: TableRead[] }
}

/** Read sets stored under keys you choose; `affected(event)` returns the keys whose queries the commit may change. */
export declare class ReadSetIndex<K extends number | string = number | string> {
  readonly size: number
  /** Stores `readSet` under `key`, replacing the key's previous one. */
  insert(key: K, readSet: ReadSet): this
  /** Drops the key's read set; false if the key held none. */
  delete(key: K): boolean
  has(key: K): boolean
  get(key: K): ReadSet | undefined
  /** The keys whose queries `event` may change, in key order: numbers first, then strings. */
  affected(event: CommitEvent): K[]
}

/**
 * The `code` an error carries: SQLite's name for a SQLite failure, such as
 * `SQLITE_CONSTRAINT_UNIQUE`, or one of SqLive's own.
 */
export type ErrorCode =
  | 'ERR_SQLIVE_LAGGED'
  | 'ERR_SQLIVE_FUTURE_VERSION'
  | 'ERR_SQLIVE_ROLLED_BACK'
  | 'ERR_SQLIVE_SNAPSHOT_ENDED'
  | 'ERR_SQLIVE_UNSUPPORTED'
  | 'ERR_SQLIVE_CLOSED'
  | 'ERR_SQLIVE_TRANSACTION_ENDED'
  | 'ERR_SQLIVE_WRITE_PENDING'
  | 'ERR_SQLIVE_PARAMETERS'
  | `SQLITE_${string}`

export interface SqliveError extends Error {
  code: ErrorCode | 'ERR_INVALID_ARG_TYPE' | 'ERR_OUT_OF_RANGE'
  /** `ERR_SQLIVE_LAGGED` and `ERR_SQLIVE_FUTURE_VERSION`: the version asked for. */
  requested?: number
  /** `ERR_SQLIVE_LAGGED`: the log holds events after this version. */
  oldest?: number
  /** `ERR_SQLIVE_FUTURE_VERSION`: the latest committed version. */
  latest?: number
}
