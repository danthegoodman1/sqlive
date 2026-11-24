// The Node binding's benchmarks, matching `sqlive-bench`'s workloads so the
// two compare directly: write throughput, query latency, and the read-set
// index.
//
//     npm run bench [-- writes|queries|index]
//
// Databases live in target/bench at the repository root, on disk, since
// every commit waits for fsync; `SQLIVE_BENCH_DIR` moves them. Run with
// `UV_THREADPOOL_SIZE=8` (the npm script sets it) so `queryAsync` can use
// eight reader connections.

import { mkdirSync, rmSync, statSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { LiveDb, ReadSetIndex } from '../index.mjs'

const here = dirname(fileURLToPath(import.meta.url))
const which = process.argv[2]
const runs = (name) => which === undefined || which === name

const CHATS = `CREATE TABLE chats (id INTEGER PRIMARY KEY, room INTEGER, created_at INTEGER, body TEXT);
  CREATE INDEX chats_by_room ON chats (room, created_at);`
const QUERY = 'SELECT * FROM chats WHERE room = ?1 ORDER BY created_at DESC LIMIT 10'
const BODY = 'a chat message of typical length, sixty bytes long, give or '
const INSERT = 'INSERT INTO chats (room, created_at, body) VALUES (?1, ?2, ?3)'

function freshDir(name) {
  const root = process.env.SQLIVE_BENCH_DIR ?? resolve(here, '../../../target/bench')
  const dir = join(root, `node-${name}`)
  rmSync(dir, { recursive: true, force: true })
  mkdirSync(dir, { recursive: true })
  return dir
}

function fill(tx, rooms) {
  for (let room = 0; room < rooms; room++) {
    for (let createdAt = 0; createdAt < 12; createdAt++) tx.run(INSERT, [room, createdAt, BODY])
  }
}

function now() {
  return process.hrtime.bigint()
}

function seconds(start) {
  return Number(now() - start) / 1e9
}

const perSecond = (count, elapsed) => Math.round(count / elapsed).toLocaleString('en-US')
const micros = (elapsed, count) => ((elapsed * 1e6) / count).toFixed(1)

async function writes() {
  const dir = freshDir('writes')
  const SINGLES = 1000
  const BATCHES = 100
  const BATCH = 1000
  const filled = (tx) => {
    for (let i = 0; i < BATCHES * BATCH; i++) tx.run(INSERT, [i % 100, i, BODY])
  }
  const workloads = [
    { name: 'Insert 1 row per commit', commits: SINGLES, rows: 1, setup: () => {}, work: (tx, i) => tx.run(INSERT, [i % 100, i, BODY]) },
    {
      name: 'Insert 1,000 rows per commit',
      commits: BATCHES,
      rows: BATCH,
      setup: () => {},
      work: (tx, i) => {
        for (let j = i * BATCH; j < (i + 1) * BATCH; j++) tx.run(INSERT, [j % 100, j, BODY])
      },
    },
    {
      name: 'Update 1,000 rows per commit',
      commits: BATCHES,
      rows: BATCH,
      setup: filled,
      work: (tx, i) => tx.run("UPDATE chats SET body = 'changed ' || id WHERE id BETWEEN ?1 AND ?2", [i * BATCH + 1, (i + 1) * BATCH]),
    },
    {
      name: 'Delete 1,000 rows per commit',
      commits: BATCHES,
      rows: BATCH,
      setup: filled,
      work: (tx, i) => tx.run('DELETE FROM chats WHERE id BETWEEN ?1 AND ?2', [i * BATCH + 1, (i + 1) * BATCH]),
    },
  ]
  console.log(`\n## writes: WAL, synchronous = FULL, in ${dir}\n`)
  console.log('| Workload | `write` commits/s | `write` rows/s | `writeSync` commits/s | `writeSync` rows/s |')
  console.log('| --- | ---: | ---: | ---: | ---: |')
  let n = 0
  for (const workload of workloads) {
    const timed = async (sync) => {
      const db = new LiveDb(join(dir, `live-${n++}.db`))
      db.writeSync((tx) => {
        tx.exec(CHATS)
        workload.setup(tx)
      })
      const start = now()
      for (let i = 0; i < workload.commits; i++) {
        if (sync) db.writeSync((tx) => workload.work(tx, i))
        else await db.write((tx) => workload.work(tx, i))
      }
      const elapsed = seconds(start)
      db.close()
      return elapsed
    }
    const async = await timed(false)
    const sync = await timed(true)
    const rows = workload.commits * workload.rows
    console.log(
      `| ${workload.name} | ${perSecond(workload.commits, async)} | ${perSecond(rows, async)} | ${perSecond(workload.commits, sync)} | ${perSecond(rows, sync)} |`,
    )
  }

  // How long the event loop stalls while single-row commits run back to
  // back: the gaps between ticks of a 1 ms timer, which stands in for the
  // rest of a server's work.
  console.log('\n| 1,000 single-row commits | Timer ticks | Gap between ticks p50 | p99 | max |')
  console.log('| --- | ---: | ---: | ---: | ---: |')
  for (const sync of [false, true]) {
    const db = new LiveDb(join(dir, `stall-${sync}.db`))
    db.writeSync((tx) => tx.exec(CHATS))
    const gaps = []
    let last = performance.now()
    const ticker = setInterval(() => {
      const at = performance.now()
      gaps.push(at - last)
      last = at
    }, 1)
    for (let i = 0; i < SINGLES; i++) {
      if (sync) {
        db.writeSync((tx) => tx.run(INSERT, [i % 100, i, BODY]))
        // Yields between commits, as a server between requests.
        await new Promise((resolve) => setImmediate(resolve))
      } else {
        await db.write((tx) => tx.run(INSERT, [i % 100, i, BODY]))
      }
    }
    clearInterval(ticker)
    gaps.sort((a, b) => a - b)
    const at = (q) => `${gaps[Math.min(gaps.length - 1, Math.floor(q * gaps.length))].toFixed(2)} ms`
    console.log(`| ${sync ? '`writeSync`' : '`write`'} | ${gaps.length} | ${at(0.5)} | ${at(0.99)} | ${gaps.at(-1).toFixed(2)} ms |`)
    db.close()
  }
}

async function queries() {
  const ROOMS = 10_000
  const QUERIES = 50_000
  const SCANS = 5
  const IN_FLIGHT = 64
  const dir = freshDir('queries')
  const path = join(dir, 'bench.db')
  const db = new LiveDb(path, { readers: 8 })
  db.writeSync((tx) => {
    tx.exec(CHATS)
    fill(tx, ROOMS)
  })
  const room = (i) => (i * 7919) % ROOMS
  const hashes = Array.from({ length: ROOMS }, (_, r) => db.query(QUERY, [r]).hash)

  const timed = (query) => {
    for (let i = 0; i < 5000; i++) query(i)
    const start = now()
    for (let i = 0; i < QUERIES; i++) query(i)
    return seconds(start)
  }
  let sink = 0
  const query = timed((i) => {
    sink += db.query(QUERY, [room(i)]).rows.length
  })
  const rerun = timed((i) => {
    const r = room(i)
    if (!db.rerun(QUERY, [r], hashes[r]).unchanged) throw new Error('a re-run changed')
  })
  const { DatabaseSync } = await import('node:sqlite')
  const plain = new DatabaseSync(path, { readOnly: true })
  const statement = plain.prepare(QUERY)
  const nodeSqlite = timed((i) => {
    sink += statement.all(room(i)).length
  })

  // Queries in flight on libuv's thread pool, IN_FLIGHT at a time.
  const many = async () => {
    let next = 0
    const worker = async () => {
      while (next < QUERIES) {
        const i = next++
        sink += (await db.queryAsync(QUERY, [room(i)])).rows.length
      }
    }
    const start = now()
    await Promise.all(Array.from({ length: IN_FLIGHT }, worker))
    return seconds(start)
  }
  await many()
  const parallel = await many()

  console.log(`\n## queries: the chat query over ${ROOMS} rooms of 12 chats\n`)
  console.log('| Query | SqLive µs | SqLive queries/s | node:sqlite µs | node:sqlite queries/s |')
  console.log('| --- | ---: | ---: | ---: | ---: |')
  console.log(`| \`query\`, 1 thread | ${micros(query, QUERIES)} | ${perSecond(QUERIES, query)} | ${micros(nodeSqlite, QUERIES)} | ${perSecond(QUERIES, nodeSqlite)} |`)
  console.log(`| \`rerun\`, unchanged, 1 thread | ${micros(rerun, QUERIES)} | ${perSecond(QUERIES, rerun)} | | |`)
  console.log(`| \`queryAsync\`, ${IN_FLIGHT} in flight, ${process.env.UV_THREADPOOL_SIZE ?? 4} pool threads | | ${perSecond(QUERIES, parallel)} | | |`)
  console.log(`\nThe ${(statSync(path).size / 1e6).toFixed(1)} MB database is hot.`)

  const rows = ROOMS * 12
  const scan = (run) => {
    run()
    const start = now()
    for (let i = 0; i < SCANS; i++) run()
    return seconds(start)
  }
  const liveScan = scan(() => {
    sink += db.query('SELECT * FROM chats').rows.length
  })
  const all = plain.prepare('SELECT * FROM chats')
  const plainScan = scan(() => {
    sink += all.all().length
  })
  console.log(`\n| Full scan of ${rows} rows | SqLive rows/s | node:sqlite rows/s |`)
  console.log('| --- | ---: | ---: |')
  console.log(`| \`SELECT * FROM chats\` | ${perSecond(rows * SCANS, liveScan)} | ${perSecond(rows * SCANS, plainScan)} |`)
  plain.close()
  db.close()
  if (sink === 0) throw new Error('no rows')
}

async function index() {
  const ROOMS = 10_000
  const SUBSCRIPTIONS = 100_000
  const EVENTS = 2000
  const BRUTE = 50
  const dir = freshDir('index')
  const db = new LiveDb(join(dir, 'bench.db'))
  db.writeSync((tx) => {
    tx.exec(CHATS)
    fill(tx, ROOMS)
  })
  let start = now()
  const keys = new ReadSetIndex()
  const readSets = []
  for (let s = 0; s < SUBSCRIPTIONS; s++) {
    const readSet = db.query(QUERY, [s % ROOMS]).readSet
    keys.insert(s, readSet)
    readSets.push(readSet)
  }
  const built = seconds(start)
  const stream = db.changes()
  const events = []
  for (let i = 0; i < EVENTS; i++) {
    db.writeSync((tx) => tx.run("INSERT INTO chats (room, created_at, body) VALUES (?1, ?2, 'new')", [(i * 7919) % ROOMS, 100 + i]))
    events.push(stream.tryNext())
  }
  start = now()
  let affected = 0
  for (const event of events) affected += keys.affected(event).length
  const indexed = seconds(start)
  start = now()
  let tested = 0
  for (const event of events.slice(0, BRUTE)) {
    for (const readSet of readSets) if (readSet.intersects(event)) tested++
  }
  const brute = seconds(start)
  if (tested !== (affected / EVENTS) * BRUTE) throw new Error('the index and brute force disagree')
  console.log(`\n## index: ${SUBSCRIPTIONS} subscriptions over ${ROOMS} rooms, built in ${built.toFixed(2)} s\n`)
  console.log('| Method | Events per second | Affected per event |')
  console.log('| --- | ---: | ---: |')
  console.log(`| \`ReadSetIndex.affected\` | ${perSecond(EVENTS, indexed)} | ${affected / EVENTS} |`)
  console.log(`| \`intersects\` on every read set | ${perSecond(BRUTE, brute)} | ${tested / BRUTE} |`)
  db.close()
}

if (runs('writes')) await writes()
if (runs('queries')) await queries()
if (runs('index')) await index()
