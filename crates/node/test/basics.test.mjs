import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { createRequire } from 'node:module'
import { join } from 'node:path'
import { test } from 'node:test'
import { LiveDb, ReadSet } from '../index.mjs'
import { tempDb } from './helpers.mjs'

const require = createRequire(import.meta.url)

async function withTable(t, options) {
  const opened = tempDb(t, options)
  await opened.db.write((tx) => tx.exec('CREATE TABLE v (id INTEGER PRIMARY KEY, x)'))
  return opened
}

test('the CommonJS and ES module entries export the same classes', () => {
  const cjs = require('../index.js')
  assert.equal(cjs.LiveDb, LiveDb)
  assert.equal(cjs.ReadSet, ReadSet)
})

test('values round-trip with their storage classes', async (t) => {
  const { db } = await withTable(t)
  const bytes = Buffer.from([0, 1, 2, 255])
  const cases = [
    [null, null, 'null'],
    [undefined, null, 'null'],
    [0, 0, 'integer'],
    [-7, -7, 'integer'],
    [Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER, 'integer'],
    [1.5, 1.5, 'real'],
    [-0, -0, 'real'],
    [2 ** 60, 2 ** 60, 'real'],
    [true, 1, 'integer'],
    [false, 0, 'integer'],
    [12n, 12, 'integer'],
    ['', '', 'text'],
    ['héllo 🌍', 'héllo 🌍', 'text'],
    [bytes, bytes, 'blob'],
    [new Uint8Array([9, 8]), Buffer.from([9, 8]), 'blob'],
    [new Uint16Array([1]).buffer, Buffer.from([1, 0]), 'blob'],
    [new DataView(new Uint8Array([4, 5, 6]).buffer, 1, 2), Buffer.from([5, 6]), 'blob'],
    [new Uint8Array(0), Buffer.alloc(0), 'blob'],
  ]
  await db.write((tx) => {
    cases.forEach(([input], i) => tx.run('INSERT INTO v (id, x) VALUES (?, ?)', [i, input]))
  })
  const { rows } = db.query('SELECT id, x, typeof(x) AS type FROM v ORDER BY id')
  cases.forEach(([, expected, type], i) => {
    assert.equal(rows[i].type, type, `case ${i}`)
    assert.deepEqual(rows[i].x, expected, `case ${i}`)
    if (Object.is(expected, -0)) assert.ok(Object.is(rows[i].x, -0), 'the sign of -0 survives')
  })
  assert.ok(Buffer.isBuffer(rows[13].x))
})

test('a typed array binds exactly its own bytes', async (t) => {
  const { db } = await withTable(t)
  const arrays = [new Float64Array([1.5]), new BigInt64Array([1n, 2n]), new Int16Array([1, 2, 3])]
  if (typeof Float16Array === 'function') arrays.push(new Float16Array(4))
  await db.write((tx) => arrays.forEach((array, i) => tx.run('INSERT INTO v VALUES (?, ?)', [i, array])))
  const { rows } = db.query('SELECT length(x) AS n FROM v ORDER BY id')
  assert.deepEqual(rows.map((row) => row.n), arrays.map((array) => array.byteLength))
})

test('text across many chunks comes back whole', async (t) => {
  const { db } = await withTable(t)
  // Long and non-ASCII values, past the 4,096 UTF-16 units one chunk holds.
  const texts = Array.from({ length: 300 }, (_, i) => `${i}:${'é🌍x'.repeat(i % 7)}${'y'.repeat(i % 50)}`)
  // An empty value between two that each fill a chunk alone.
  texts.push('z'.repeat(9000), '', 'w'.repeat(5000), '', '', 'end')
  await db.write((tx) => texts.forEach((text, i) => tx.run('INSERT INTO v VALUES (?, ?)', [i, text])))
  const { rows } = db.query('SELECT x, length(x) AS n FROM v ORDER BY id')
  assert.deepEqual(rows.map((row) => row.x), texts)
  const async = await db.queryAsync('SELECT x FROM v ORDER BY id')
  assert.deepEqual(async.rows.map((row) => row.x), texts)
  const inside = (await db.write((tx) => tx.all('SELECT x FROM v ORDER BY id'))).value
  assert.deepEqual(inside.map((row) => row.x), texts)
})

test('integers past 53 bits need safeIntegers', async (t) => {
  const { db, path } = await withTable(t)
  await db.write((tx) => tx.run('INSERT INTO v VALUES (1, ?)', [2n ** 62n]))
  assert.throws(() => db.query('SELECT x FROM v'), { name: 'RangeError', code: 'ERR_OUT_OF_RANGE' })
  db.close()
  const big = new LiveDb(path, { safeIntegers: true })
  t.after(() => big.close())
  const { rows } = big.query('SELECT id, x, 1.5 AS r FROM v')
  assert.deepEqual(rows, [{ id: 1n, x: 2n ** 62n, r: 1.5 }])
  await assert.rejects(
    big.write((tx) => tx.run('INSERT INTO v VALUES (2, ?)', [2n ** 64n])),
    { name: 'RangeError' },
  )
})

test('parameters bind by position, number and name', async (t) => {
  const { db } = await withTable(t)
  await db.write((tx) => {
    tx.run('INSERT INTO v VALUES (?, ?)', [1, 'a'])
    tx.run('INSERT INTO v VALUES (?2, ?1)', ['b', 2])
    tx.run('INSERT INTO v VALUES (:id, :x)', { id: 3, x: 'c' })
    tx.run('INSERT INTO v VALUES (@id, $x)', { id: 4, x: 'd' })
    tx.run('INSERT INTO v VALUES (@id, :x)', { '@id': 5, ':x': 'e' })
  })
  const { rows } = db.query('SELECT x FROM v WHERE id >= $low ORDER BY id', { low: 1 })
  assert.deepEqual(rows.map((r) => r.x), ['a', 'b', 'c', 'd', 'e'])
  assert.equal(db.query('SELECT x FROM v WHERE id = @id', { id: 4 }).rows[0].x, 'd')
  assert.throws(() => db.query('SELECT ?, ?', [1]), { code: 'ERR_SQLIVE_PARAMETERS' })
  assert.throws(() => db.query('SELECT :a', { b: 1 }), { code: 'ERR_SQLIVE_PARAMETERS' })
  assert.throws(() => db.query('SELECT ?', [{}]), { name: 'TypeError', code: 'ERR_INVALID_ARG_TYPE' })
  assert.throws(() => db.query('SELECT ?', 5), { name: 'TypeError' })
  assert.throws(() => db.query(5), { name: 'TypeError' })
})

test('a lone column with an empty name', (t) => {
  const { db } = tempDb(t)
  assert.deepEqual(db.query('SELECT 1 AS ""').rows, [{ '': 1 }])
  assert.deepEqual(db.query('SELECT 1 AS "", 2 AS ""').rows, [{ '': 2 }])
})

test('a bare key ignores prefixes inside literals and comments', (t) => {
  const { db } = tempDb(t)
  assert.deepEqual(db.query("SELECT 'a:id' AS s, $id AS v /* @id */", { id: 5 }).rows, [{ s: 'a:id', v: 5 }])
})

test('rows key by column name, keeping the last of duplicate names', async (t) => {
  const { db } = tempDb(t)
  const { rows } = db.query(`SELECT 1 AS a, 2 AS a, 3 AS "__proto__", 4 AS "it's ""quoted"""`)
  assert.equal(rows[0].a, 2)
  assert.equal(Object.getPrototypeOf(rows[0]), Object.prototype)
  assert.equal(rows[0].__proto__, 3)
  assert.equal(rows[0][`it's "quoted"`], 4)
})

test('write returns the callback value and the commit version', async (t) => {
  const { db } = await withTable(t)
  const first = await db.write((tx) => tx.run('INSERT INTO v VALUES (1, 1)'))
  assert.deepEqual(first, { value: { changes: 1, lastInsertRowid: 1 }, version: 2 })
  const second = await db.write(async (tx) => {
    await new Promise((resolve) => setTimeout(resolve, 5))
    tx.run('INSERT INTO v VALUES (2, 2)')
    return tx.get('SELECT count(*) AS n FROM v').n
  })
  assert.deepEqual(second, { value: 2, version: 3 })
  assert.equal(db.version, 3)
})

test('writes queued together commit in the order they were called', async (t) => {
  const { db } = await withTable(t)
  const writes = Array.from({ length: 20 }, (_, i) =>
    db.write((tx) => {
      tx.run('INSERT INTO v (x) VALUES (?)', [i])
      return i
    }),
  )
  const results = await Promise.all(writes)
  results.forEach((result, i) => {
    assert.equal(result.value, i)
    assert.equal(result.version, i + 2)
  })
  const { rows } = db.query('SELECT x FROM v ORDER BY id')
  assert.deepEqual(rows.map((r) => r.x), results.map((r) => r.value))
})

test('a callback that throws rolls back and rejects with its own error', async (t) => {
  const { db } = await withTable(t)
  const thrown = new Error('nope')
  await assert.rejects(
    db.write((tx) => {
      tx.run('INSERT INTO v VALUES (1, 1)')
      throw thrown
    }),
    (error) => error === thrown,
  )
  await assert.rejects(
    db.write(async (tx) => {
      tx.run('INSERT INTO v VALUES (2, 2)')
      await null
      throw thrown
    }),
    (error) => error === thrown,
  )
  assert.throws(
    () =>
      db.writeSync((tx) => {
        tx.run('INSERT INTO v VALUES (3, 3)')
        throw thrown
      }),
    (error) => error === thrown,
  )
  assert.throws(
    () => db.writeSync(async () => {}),
    { name: 'TypeError', message: /must be synchronous/ },
  )
  assert.deepEqual(db.query('SELECT * FROM v').rows, [])
  assert.equal(db.version, 1, 'rolled-back writes publish nothing')
})

test('a SQLite error inside a write carries its code', async (t) => {
  const { db } = await withTable(t)
  await db.write((tx) => tx.run('INSERT INTO v VALUES (1, 1)'))
  await assert.rejects(
    db.write((tx) => tx.run('INSERT INTO v VALUES (1, 1)')),
    { code: 'SQLITE_CONSTRAINT_PRIMARYKEY' },
  )
  // A failed statement the callback catches leaves the transaction open.
  const { value } = await db.write((tx) => {
    try {
      tx.run('INSERT INTO v VALUES (1, 1)')
    } catch (error) {
      tx.run('INSERT INTO v VALUES (2, 2)')
      return error.code
    }
  })
  assert.equal(value, 'SQLITE_CONSTRAINT_PRIMARYKEY')
  assert.equal(db.query('SELECT count(*) AS n FROM v').rows[0].n, 2)
})

test('a transaction ends with its callback', async (t) => {
  const { db } = await withTable(t)
  let kept
  await db.write((tx) => {
    kept = tx
  })
  assert.throws(() => kept.run('INSERT INTO v VALUES (1, 1)'), { code: 'ERR_SQLIVE_TRANSACTION_ENDED' })
  // A kept transaction cannot reach into a later write.
  await db.write(() => {
    assert.throws(() => kept.run('INSERT INTO v VALUES (1, 1)'), { code: 'ERR_SQLIVE_TRANSACTION_ENDED' })
  })
  assert.deepEqual(db.query('SELECT * FROM v').rows, [])
})

test('a write inside a write refuses instead of deadlocking', async (t) => {
  const { db } = await withTable(t)
  await db.write(async () => {
    await assert.rejects(db.write(() => {}), { code: 'ERR_SQLIVE_WRITE_PENDING' })
    await assert.rejects(db.vacuum(), { code: 'ERR_SQLIVE_WRITE_PENDING' })
    assert.throws(() => db.writeSync(() => {}), { code: 'ERR_SQLIVE_WRITE_PENDING' })
  })
  const pending = db.write(() => {})
  assert.throws(() => db.writeSync(() => {}), { code: 'ERR_SQLIVE_WRITE_PENDING' })
  await pending
  assert.equal(db.writeSync(() => 'ok').value, 'ok')
})

test('transactions read their own writes', async (t) => {
  const { db } = await withTable(t)
  const { value } = await db.write((tx) => {
    tx.run('INSERT INTO v VALUES (1, ?)', ['a'])
    const returned = tx.all('INSERT INTO v VALUES (2, ?) RETURNING id, x', ['b'])
    return { all: tx.all('SELECT * FROM v ORDER BY id'), one: tx.get('SELECT x FROM v WHERE id = ?', [2]), none: tx.get('SELECT x FROM v WHERE id = 9'), returned }
  })
  assert.deepEqual(value.all, [{ id: 1, x: 'a' }, { id: 2, x: 'b' }])
  assert.deepEqual(value.one, { x: 'b' })
  assert.equal(value.none, undefined)
  assert.deepEqual(value.returned, [{ id: 2, x: 'b' }])
})

test('queries refuse to write and report SQL errors', async (t) => {
  const { db } = await withTable(t)
  assert.throws(() => db.query('INSERT INTO v VALUES (1, 1)'), { code: 'ERR_SQLIVE_UNSUPPORTED' })
  assert.throws(() => db.query('SELEC 1'), { code: 'SQLITE_ERROR' })
  await assert.rejects(db.queryAsync('SELECT * FROM missing'), { code: 'SQLITE_ERROR', message: /no such table/ })
})

test('queryAsync returns what query returns', async (t) => {
  const { db } = await withTable(t)
  await db.write((tx) => {
    for (let i = 0; i < 100; i++) tx.run('INSERT INTO v VALUES (?, ?)', [i, `row ${i}`])
  })
  const sql = 'SELECT * FROM v WHERE id >= ? ORDER BY id LIMIT 10'
  const results = await Promise.all(Array.from({ length: 32 }, (_, i) => db.queryAsync(sql, [i])))
  results.forEach((result, i) => {
    const sync = db.query(sql, [i])
    assert.deepEqual(result.rows, sync.rows)
    assert.equal(result.hash, sync.hash)
    assert.equal(String(result.readSet), String(sync.readSet))
  })
  const again = await db.rerunAsync(sql, [0], results[0].hash)
  assert.equal(again.unchanged, true)
  assert.deepEqual(again.rows, [])
})

test('rerun reports an unchanged result by its hash', async (t) => {
  const { db } = await withTable(t)
  await db.write((tx) => tx.run('INSERT INTO v VALUES (1, 1)'))
  const first = db.query('SELECT * FROM v')
  assert.match(first.hash, /^[0-9a-f]{64}$/)
  await db.write((tx) => tx.run('INSERT INTO v VALUES (2, 2)'))
  const changed = db.rerun('SELECT * FROM v', undefined, first.hash)
  assert.equal(changed.unchanged, false)
  assert.equal(changed.rows.length, 2)
  const same = db.rerun('SELECT * FROM v', undefined, changed.hash)
  assert.equal(same.unchanged, true)
  assert.deepEqual(same.rows, [])
  assert.equal(same.hash, changed.hash)
  assert.throws(() => db.rerun('SELECT * FROM v', undefined, 'abc'), { name: 'TypeError' })
})

test('a snapshot reads one version across queries', async (t) => {
  const { db } = await withTable(t)
  await db.write((tx) => tx.run('INSERT INTO v VALUES (1, 1)'))
  const snapshot = db.snapshot()
  await db.write((tx) => tx.run('INSERT INTO v VALUES (2, 2)'))
  const a = snapshot.query('SELECT count(*) AS n FROM v')
  const b = snapshot.rerun('SELECT count(*) AS n FROM v', undefined, a.hash)
  assert.equal(a.rows[0].n, 1)
  assert.equal(a.version, snapshot.version)
  assert.equal(b.unchanged, true)
  snapshot[Symbol.dispose]()
  assert.throws(() => snapshot.query('SELECT 1'), { code: 'ERR_SQLIVE_CLOSED' })
  assert.equal(db.query('SELECT count(*) AS n FROM v').rows[0].n, 2)
})

test('vacuum runs on the writer thread and publishes a schema change', async (t) => {
  const { db } = await withTable(t)
  const stream = db.changes()
  const version = await db.vacuum()
  const event = stream.tryNext()
  assert.equal(event.version, version)
  assert.equal(event.schemaChanged, true)
})

test('closing finishes queued writes, then refuses new work', async (t) => {
  const { db } = await withTable(t)
  const queued = db.write((tx) => tx.run('INSERT INTO v VALUES (1, 1)'))
  db.close()
  assert.equal(db.closed, true)
  assert.equal((await queued).version, 2)
  assert.throws(() => db.query('SELECT 1'), { code: 'ERR_SQLIVE_CLOSED' })
  await assert.rejects(db.write(() => {}), { code: 'ERR_SQLIVE_CLOSED' })
  assert.throws(() => db.version, { code: 'ERR_SQLIVE_CLOSED' })
})

test('options are checked', (t) => {
  const { dir } = tempDb(t)
  assert.throws(() => new LiveDb(join(dir, 'a.db'), { logCapacity: -1 }), { name: 'RangeError' })
  assert.throws(() => new LiveDb(join(dir, 'b.db'), { busyTimeout: Number.NaN }), { name: 'RangeError' })
  assert.throws(() => new LiveDb(join(dir, 'b.db'), { busyTimeout: 1e300 }), { name: 'RangeError' })
  const open = new LiveDb(join(dir, 'c.db'), { tableChanges: Infinity, logBytes: 1 << 20, readers: 2 })
  open.close()
})

test('the process exits once no write is pending, and not before', (t) => {
  const { dir } = tempDb(t)
  const script = `
    const { LiveDb } = require(${JSON.stringify(require.resolve('../index.js'))})
    const db = new LiveDb(${JSON.stringify(join(dir, 'exit.db'))})
    // Twenty megabytes, so the vacuum still runs after the write settles.
    db.write((tx) => tx.exec(\`
      CREATE TABLE t (x);
      WITH RECURSIVE n (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 20000)
      INSERT INTO t SELECT randomblob(1000) FROM n;
    \`)).then(({ version }) => console.log('committed', version))
    db.vacuum().then((version) => console.log('vacuumed', version))
  `
  const child = spawnSync(process.execPath, ['-e', script], { encoding: 'utf8', timeout: 20_000 })
  assert.equal(child.signal, null, 'the process exited on its own')
  assert.equal(child.status, 0, child.stderr)
  assert.deepEqual(child.stdout.trim().split('\n'), ['committed 1', 'vacuumed 2'])
})

test('a worker that ends inside a write rolls it back and lets the lock go', async (t) => {
  const { dir } = tempDb(t)
  const path = join(dir, 'worker.db')
  const setup = new LiveDb(path)
  setup.writeSync((tx) => tx.exec('CREATE TABLE t (x)'))
  setup.close()
  const { Worker } = await import('node:worker_threads')
  const worker = new Worker(
    `
    const { parentPort } = require('node:worker_threads')
    const { LiveDb } = require(${JSON.stringify(require.resolve('../index.js'))})
    const db = new LiveDb(${JSON.stringify(path)})
    db.write(async (tx) => {
      tx.run('INSERT INTO t VALUES (1)')
      parentPort.postMessage('open')
      await new Promise(() => {})
    })
    `,
    { eval: true },
  )
  await new Promise((resolve) => worker.once('message', resolve))
  await worker.terminate()
  const db = new LiveDb(path)
  t.after(() => db.close())
  const { version } = await db.write((tx) => tx.run('INSERT INTO t VALUES (2)'))
  assert.equal(version, 2)
  assert.deepEqual(db.query('SELECT x FROM t').rows, [{ x: 2 }])
})
