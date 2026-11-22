import assert from 'node:assert/strict'
import { test } from 'node:test'
import { inspect } from 'node:util'
import { ReadSetIndex } from '../index.mjs'
import { settle, tempDb } from './helpers.mjs'

const SCHEMA = `
  CREATE TABLE todos (id INTEGER PRIMARY KEY, list_id INTEGER NOT NULL, title TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0);
  CREATE TABLE tags (name TEXT PRIMARY KEY, n INTEGER) WITHOUT ROWID;
`

async function todos(t, options) {
  const opened = tempDb(t, options)
  await opened.db.write((tx) => tx.exec(SCHEMA))
  return opened
}

test('an event lists each row change with its old and new values', async (t) => {
  const { db } = await todos(t)
  const stream = db.changes()
  await db.write((tx) => {
    tx.run('INSERT INTO todos (list_id, title) VALUES (5, ?)', ['milk'])
    tx.run('INSERT INTO todos (list_id, title) VALUES (5, ?)', ['eggs'])
    tx.run('INSERT INTO tags VALUES (?, ?)', ['home', 1])
  })
  await db.write((tx) => {
    tx.run('UPDATE todos SET done = 1 WHERE id = 1')
    tx.run('DELETE FROM todos WHERE id = 2')
    tx.run('UPDATE tags SET n = 2')
  })
  const inserted = stream.tryNext()
  const changed = stream.tryNext()
  assert.equal(stream.tryNext(), undefined)
  assert.equal(inserted.version, 2)
  assert.equal(inserted.schemaChanged, false)
  assert.deepEqual(inserted.wholeTables, [])
  assert.deepEqual(inserted.changes, [
    { table: 'todos', op: 'insert', rowid: 1, old: null, new: { id: 1, list_id: 5, title: 'milk', done: 0 } },
    { table: 'todos', op: 'insert', rowid: 2, old: null, new: { id: 2, list_id: 5, title: 'eggs', done: 0 } },
    { table: 'tags', op: 'insert', rowid: null, old: null, new: { name: 'home', n: 1 } },
  ])
  assert.deepEqual(changed.changes, [
    {
      table: 'todos',
      op: 'update',
      rowid: 1,
      old: { id: 1, list_id: 5, title: 'milk', done: 0 },
      new: { id: 1, list_id: 5, title: 'milk', done: 1 },
    },
    { table: 'todos', op: 'delete', rowid: 2, old: { id: 2, list_id: 5, title: 'eggs', done: 0 }, new: null },
    { table: 'tags', op: 'update', rowid: null, old: { name: 'home', n: 1 }, new: { name: 'home', n: 2 } },
  ])
  assert.equal(changed.changes, changed.changes, 'changes convert once')
  assert.match(inspect(changed), /CommitEvent \{\s+version: 3,/)
  assert.deepEqual(JSON.parse(JSON.stringify(changed)).version, 3)
})

test('a stream yields retained events, then waits for new ones', async (t) => {
  const { db } = await todos(t)
  const before = db.version
  await db.write((tx) => tx.run('INSERT INTO todos (list_id, title) VALUES (1, ?)', ['a']))
  const seen = []
  const reading = (async () => {
    for await (const event of db.changesSince(before)) {
      seen.push(event.version)
      if (seen.length === 3) break
    }
  })()
  await settle()
  assert.deepEqual(seen, [before + 1])
  await db.write((tx) => tx.run('INSERT INTO todos (list_id, title) VALUES (1, ?)', ['b']))
  db.writeSync((tx) => tx.run('INSERT INTO todos (list_id, title) VALUES (1, ?)', ['c']))
  await reading
  assert.deepEqual(seen, [before + 1, before + 2, before + 3])
})

test('several pending next() calls resolve in order', async (t) => {
  const { db } = await todos(t)
  const stream = db.changes()
  const first = stream.next()
  const second = stream.next()
  await db.write((tx) => tx.run('INSERT INTO todos (list_id, title) VALUES (1, ?)', ['a']))
  await db.write((tx) => tx.run('INSERT INTO todos (list_id, title) VALUES (1, ?)', ['b']))
  assert.equal((await first).value.version, 2)
  assert.equal((await second).value.version, 3)
})

test('a stream that falls behind the log lags once, with where it stood', async (t) => {
  const { db } = await todos(t, { logCapacity: 2 })
  const stream = db.changesSince(db.version)
  for (let i = 0; i < 4; i++) {
    await db.write((tx) => tx.run('INSERT INTO todos (list_id, title) VALUES (1, ?)', [`t${i}`]))
  }
  await assert.rejects(stream.next(), { code: 'ERR_SQLIVE_LAGGED', requested: 1, oldest: 3 })
  assert.deepEqual(await stream.next(), { value: undefined, done: true })
  assert.throws(() => db.changesSince(99), { code: 'ERR_SQLIVE_FUTURE_VERSION', requested: 99, latest: 5 })
})

test('a subscription keeps the parameters it was given', async (t) => {
  const { db } = await todos(t)
  const params = [5]
  const subscription = db.subscribe('SELECT title FROM todos WHERE list_id = ? ORDER BY id', params)
  await subscription.next()
  params[0] = 6
  await db.write((tx) => tx.run('INSERT INTO todos (list_id, title) VALUES (5, ?)', ['five']))
  assert.deepEqual((await subscription.next()).value.rows, [{ title: 'five' }])
  assert.deepEqual(subscription.params, [5])
  subscription.close()
})

test('a subscription that falls behind the log re-runs and catches up', async (t) => {
  const { db } = await todos(t, { logCapacity: 2 })
  const subscription = db.subscribe('SELECT title FROM todos ORDER BY id')
  assert.deepEqual((await subscription.next()).value.rows, [])
  // The follower receives the first event, which the subscription does not
  // read; the synchronous writes after it leave the log before the follower
  // runs again, so only a re-run after the lag can show them.
  db.writeSync((tx) => tx.run('INSERT INTO tags VALUES (?, 1)', ['other']))
  for (let i = 0; i < 5; i++) db.writeSync((tx) => tx.run('INSERT INTO todos (list_id, title) VALUES (1, ?)', [`t${i}`]))
  const { value } = await subscription.next()
  assert.deepEqual(value.rows.map((row) => row.title), ['t0', 't1', 't2', 't3', 't4'])
  await db.write((tx) => tx.run('DELETE FROM todos WHERE title = ?', ['t0']))
  assert.equal((await subscription.next()).value.rows.length, 4)
  subscription.close()
  assert.deepEqual(await subscription.next(), { value: undefined, done: true })
})

test('a waiting stream ends when the database closes', async (t) => {
  const { db } = await todos(t)
  const stream = db.changes()
  const pending = stream.next()
  db.close()
  assert.deepEqual(await pending, { value: undefined, done: true })
})

test('a transaction past the change cap names its table whole', async (t) => {
  const { db } = await todos(t, { tableChanges: 3 })
  const stream = db.changes()
  await db.write((tx) => {
    for (let i = 0; i < 5; i++) tx.run('INSERT INTO todos (list_id, title) VALUES (1, ?)', [`t${i}`])
    tx.run('INSERT INTO tags VALUES (?, 1)', ['x'])
  })
  const event = stream.tryNext()
  assert.deepEqual(event.wholeTables, ['todos'])
  assert.deepEqual(event.changes.map((change) => change.table), ['tags'])
})

test('DDL publishes a schema change with no rows', async (t) => {
  const { db } = await todos(t)
  const stream = db.changes()
  await db.write((tx) => tx.exec('CREATE INDEX todos_by_list ON todos (list_id)'))
  const event = stream.tryNext()
  assert.equal(event.schemaChanged, true)
  assert.deepEqual(event.changes, [])
})

test('read sets say which events can change their results', async (t) => {
  const { db } = await todos(t)
  await db.write((tx) => {
    for (let i = 1; i <= 12; i++) tx.run('INSERT INTO todos (list_id, title) VALUES (?, ?)', [i % 2 === 0 ? 5 : 6, `t${i}`])
  })
  const { readSet } = db.query('SELECT id, title FROM todos WHERE list_id = ? ORDER BY id DESC LIMIT 3', [5])
  assert.equal(readSet.everyCommit, false)
  assert.equal(String(readSet), 'todos[id, list_id, title] where list_id = 5 AND id >= 8')
  assert.deepEqual(readSet.tables, [
    { name: 'todos', columns: ['id', 'list_id', 'title'], everyColumn: false, filter: 'list_id = 5 AND id >= 8' },
  ])
  assert.equal(readSet.readsRowsOf('todos'), true)
  assert.equal(readSet.readsRowsOf('tags'), false)
  const stream = db.changesSince(readSet.version)
  await db.write((tx) => tx.run('INSERT INTO todos (list_id, title) VALUES (6, ?)', ['other list']))
  await db.write((tx) => tx.run('UPDATE todos SET done = 1 WHERE id = 12'))
  await db.write((tx) => tx.run('UPDATE todos SET title = ? WHERE id = 2', ['outside the window']))
  await db.write((tx) => tx.run('UPDATE todos SET title = ? WHERE id = 10', ['inside']))
  const results = [stream.tryNext(), stream.tryNext(), stream.tryNext(), stream.tryNext()].map((event) =>
    readSet.intersects(event),
  )
  assert.deepEqual(results, [false, false, false, true])
  assert.equal(db.query("SELECT name FROM pragma_table_info('todos')").readSet.everyCommit, true)
})

test('a read-set index finds the keys an event affects, by number or string', async (t) => {
  const { db } = await todos(t)
  await db.write((tx) => {
    for (let list = 1; list <= 4; list++) tx.run('INSERT INTO todos (list_id, title) VALUES (?, ?)', [list, 'x'])
  })
  const index = new ReadSetIndex()
  const sql = 'SELECT * FROM todos WHERE list_id = ?'
  for (let list = 1; list <= 4; list++) {
    index.insert(list, db.query(sql, [list]).readSet)
    index.insert(`list ${list}`, db.query(sql, [list]).readSet)
  }
  assert.equal(index.size, 8)
  assert.equal(index.has(2), true)
  assert.equal(String(index.get('list 3')), 'todos[done, id, list_id, title] where list_id = 3')
  const stream = db.changes()
  await db.write((tx) => tx.run('UPDATE todos SET done = 1 WHERE list_id IN (2, 4)'))
  assert.deepEqual(index.affected(stream.tryNext()), [2, 4, 'list 2', 'list 4'])
  assert.equal(index.delete(2), true)
  assert.equal(index.delete(2), false)
  assert.equal(index.size, 7)
})
