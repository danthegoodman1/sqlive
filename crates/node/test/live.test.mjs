// Properties over random workloads, checked against SQLite itself through
// node:sqlite: subscriptions hold the rows a fresh query returns, a replica
// built from events matches a dump of the tables, and the read-set index
// agrees with testing every read set.
//
//     SQLIVE_SEED=7 SQLIVE_SEEDS=1 node --test test/live.test.mjs

import assert from 'node:assert/strict'
import { test } from 'node:test'
import { ReadSetIndex } from '../index.mjs'
import { Prng, oracle, plain, seed, settle, tempDb } from './helpers.mjs'

const STEPS = Number.parseInt(process.env.SQLIVE_STEPS ?? '200', 10)
const SEEDS = Number.parseInt(process.env.SQLIVE_SEEDS ?? '3', 10)

const SCHEMA = `
  CREATE TABLE items (id INTEGER PRIMARY KEY, grp INTEGER, score REAL, name TEXT, data BLOB);
  CREATE INDEX items_by_grp ON items (grp, id);
  CREATE TABLE tags (k TEXT PRIMARY KEY, v INTEGER, note TEXT) WITHOUT ROWID;
`

const NAMES = ['ant', 'bee', 'cat', 'dog', 'eel', null]
// Halves, so sums are exact whatever order SQLite adds them in.
const SCORES = [0, -0, 0.5, 1, 1.5, -2.5, 3, null]

const QUERIES = [
  (r) => ['SELECT * FROM items WHERE grp = ? ORDER BY id DESC LIMIT ?', [r.below(6), 1 + r.below(4)]],
  (r) => {
    const low = r.pick(SCORES) ?? 0
    return ['SELECT id, name FROM items WHERE score BETWEEN ? AND ? ORDER BY score, id', [low, low + r.below(4)]]
  },
  () => ['SELECT grp, count(*) AS n, total(score) AS s FROM items GROUP BY grp ORDER BY grp', []],
  (r) => ['SELECT * FROM tags WHERE k >= ? ORDER BY k LIMIT 3', [r.pick(NAMES) ?? '']],
  (r) => [
    'SELECT i.id, t.k, t.v FROM items AS i JOIN tags AS t ON t.k = i.name WHERE i.grp = ? ORDER BY i.id, t.k',
    [r.below(6)],
  ],
  (r) => ['SELECT * FROM items WHERE name IN (?, ?) ORDER BY id', [r.pick(NAMES), r.pick(NAMES)]],
  () => ['SELECT count(*) AS n FROM items WHERE data IS NULL', []],
  (r) => ['SELECT id, data FROM items WHERE id > ? ORDER BY id LIMIT 2', [r.below(40)]],
]

function randomQuery(r) {
  return r.pick(QUERIES)(r)
}

function blob(r) {
  if (r.chance(0.3)) return null
  return Buffer.from(Array.from({ length: r.below(4) }, () => r.below(256)))
}

/** One random statement, as `[sql, params]`. */
function statement(r) {
  switch (r.below(7)) {
    case 0:
    case 1:
      return ['INSERT INTO items (grp, score, name, data) VALUES (?, ?, ?, ?)', [r.below(6), r.pick(SCORES), r.pick(NAMES), blob(r)]]
    case 2:
      return ['UPDATE items SET grp = ?, score = ? WHERE id = ?', [r.below(6), r.pick(SCORES), 1 + r.below(40)]]
    case 3:
      return ['UPDATE items SET name = ? WHERE grp = ?', [r.pick(NAMES), r.below(6)]]
    case 4:
      return ['DELETE FROM items WHERE id = ? OR grp = ?', [1 + r.below(40), 6 + r.below(30)]]
    case 5:
      return ['INSERT OR REPLACE INTO tags VALUES (?, ?, ?)', [r.pick(NAMES) ?? 'none', r.below(10), r.chance(0.5) ? 'n' : null]]
    default:
      return ['DELETE FROM tags WHERE k = ?', [r.pick(NAMES) ?? 'none']]
  }
}

/** Runs one random transaction in one of the three write modes; some fail and roll back. */
async function randomWrite(db, r) {
  const statements = Array.from({ length: 1 + r.below(4) }, () => statement(r))
  const fails = r.chance(0.1)
  const broken = new Error('planned rollback')
  const mode = r.below(5)
  try {
    if (mode === 0) {
      db.writeSync((tx) => {
        for (const [sql, params] of statements) tx.run(sql, params)
        if (fails) throw broken
      })
    } else if (mode === 1) {
      await db.write(async (tx) => {
        for (const [sql, params] of statements) {
          tx.run(sql, params)
          await null
        }
        if (fails) throw broken
      })
    } else {
      await db.write((tx) => {
        for (const [sql, params] of statements) tx.run(sql, params)
        // A statement that fails on a duplicate key, caught inside.
        if (r.chance(0.2)) {
          try {
            tx.run('INSERT INTO items (id) VALUES ((SELECT min(id) FROM items))')
          } catch (error) {
            assert.equal(error.code, 'SQLITE_CONSTRAINT_PRIMARYKEY')
          }
        }
        if (fails) throw broken
      })
    }
    assert.equal(fails, false, 'a failing callback rejects')
  } catch (error) {
    if (error !== broken) throw error
  }
}

class Replica {
  items = new Map()
  tags = new Map()

  apply(event) {
    assert.equal(event.schemaChanged, false)
    assert.deepEqual(event.wholeTables, [])
    for (const change of event.changes) {
      const row = change.new ?? change.old
      if (change.table === 'items') {
        assert.equal(change.rowid, row.id, 'an items row carries its rowid')
        if (change.op === 'delete') this.items.delete(row.id)
        else this.items.set(row.id, change.new)
      } else {
        assert.equal(change.table, 'tags')
        assert.equal(change.rowid, null, 'a WITHOUT ROWID row has no rowid')
        if (change.op === 'delete') this.tags.delete(row.k)
        else this.tags.set(row.k, change.new)
      }
    }
  }

  check(sqlite, label) {
    const items = [...this.items.values()].sort((a, b) => a.id - b.id)
    const tags = [...this.tags.values()].sort((a, b) => (a.k < b.k ? -1 : 1))
    assert.deepEqual(plain(items), plain(sqlite.prepare('SELECT * FROM items ORDER BY id').all()), `${label}: items replica`)
    assert.deepEqual(plain(tags), plain(sqlite.prepare('SELECT * FROM tags ORDER BY k').all()), `${label}: tags replica`)
  }
}

for (let s = seed(); s < seed() + SEEDS; s++) {
  test(`live results match fresh queries (seed ${s}; replay with SQLIVE_SEED=${s} SQLIVE_SEEDS=1)`, async (t) => {
    const { db, path } = tempDb(t, { logCapacity: 64 })
    await db.write((tx) => tx.exec(SCHEMA))
    const sqlite = await oracle(path)
    t.after(() => sqlite.close())
    const queries = Prng.stream(s, 'live queries')
    const writes = Prng.stream(s, 'live writes')
    const churn = Prng.stream(s, 'live churn')

    const subscriptions = Array.from({ length: 12 }, () => db.subscribe(...randomQuery(queries)))
    const replica = new Replica()
    const events = db.changesSince(db.version)
    const index = new ReadSetIndex()
    const readSets = new Map()
    const deliveries = { count: 0 }
    const consumers = subscriptions.map((subscription) => consume(subscription, deliveries))
    let checks = 0

    for (let step = 0; step < STEPS; step++) {
      const label = `seed ${s} step ${step}`
      // Some read sets join the index before the write, from a fresh query.
      for (let i = 0; i < 2; i++) {
        const key = churn.below(8)
        const [sql, params] = randomQuery(queries)
        const readSet = db.query(sql, params).readSet
        index.insert(key, readSet)
        readSets.set(key, readSet)
      }
      await randomWrite(db, writes)
      await settle()
      await settle()

      for (let event = events.tryNext(); event !== undefined; event = events.tryNext()) {
        replica.apply(event)
        const expected = [...readSets].filter(([, readSet]) => readSet.intersects(event)).map(([key]) => key).sort((a, b) => a - b)
        assert.deepEqual(index.affected(event), expected, `${label}: index agrees with intersects`)
      }
      replica.check(sqlite, label)

      for (const subscription of subscriptions) {
        const fresh = sqlite.prepare(subscription.sql).all(...subscription.params)
        assert.deepEqual(plain(subscription.result.rows), plain(fresh), `${label}: ${subscription.sql} ${JSON.stringify(subscription.params)}`)
        checks++
      }

      // Subscriptions come and go while the follower runs.
      if (churn.chance(0.1)) {
        const i = churn.below(subscriptions.length)
        subscriptions[i].close()
        await consumers[i]
        subscriptions[i] = db.subscribe(...randomQuery(queries))
        consumers[i] = consume(subscriptions[i], deliveries)
      }
    }
    for (const subscription of subscriptions) subscription.close()
    await Promise.all(consumers)
    assert.ok(checks >= STEPS * subscriptions.length)
    assert.ok(deliveries.count > subscriptions.length, 'subscriptions delivered changed results')
  })
}

async function consume(subscription, deliveries) {
  let last
  for await (const result of subscription) {
    assert.equal(result.unchanged, false)
    assert.notEqual(result.hash, last, 'a subscription delivers only results whose rows changed')
    last = result.hash
    deliveries.count++
  }
}
