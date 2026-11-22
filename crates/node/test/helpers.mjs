// Shared test helpers: temporary databases, a seeded PRNG, and a plain
// SQLite connection (node:sqlite) that serves as the oracle.

import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { LiveDb } from '../index.mjs'

/** A fresh database in its own directory, closed and removed after the test. */
export function tempDb(t, options) {
  const dir = mkdtempSync(join(tmpdir(), 'sqlive-test-'))
  const path = join(dir, 'test.db')
  const db = new LiveDb(path, options)
  t.after(() => {
    db.close()
    try {
      rmSync(dir, { recursive: true, force: true, maxRetries: 5 })
    } catch {
      // Windows keeps a file busy until the writer thread lets the database
      // go; a leftover temporary directory fails nothing.
    }
  })
  return { db, path, dir }
}

/** Waits until every callback the event loop has queued has run. */
export function settle() {
  return new Promise((resolve) => setImmediate(resolve))
}

/** The seed a run uses: `SQLIVE_SEED`, or a fixed default. */
export function seed() {
  return Number.parseInt(process.env.SQLIVE_SEED ?? '1', 10) >>> 0
}

function fnv1a(text) {
  let hash = 0x811c9dc5
  for (let i = 0; i < text.length; i++) {
    hash ^= text.charCodeAt(i)
    hash = Math.imul(hash, 0x01000193) >>> 0
  }
  return hash
}

/** sfc32 seeded through splitmix32. Each purpose draws from its own stream. */
export class Prng {
  #a
  #b
  #c
  #d

  static stream(seed, purpose) {
    return new Prng((seed ^ fnv1a(purpose)) >>> 0)
  }

  constructor(seed) {
    let state = seed >>> 0
    const next = () => {
      state = (state + 0x9e3779b9) >>> 0
      let z = state
      z = Math.imul(z ^ (z >>> 16), 0x85ebca6b) >>> 0
      z = Math.imul(z ^ (z >>> 13), 0xc2b2ae35) >>> 0
      return (z ^ (z >>> 16)) >>> 0
    }
    this.#a = next()
    this.#b = next()
    this.#c = next()
    this.#d = next()
  }

  u32() {
    const t = (((this.#a + this.#b) >>> 0) + this.#d) >>> 0
    this.#d = (this.#d + 1) >>> 0
    this.#a = this.#b ^ (this.#b >>> 9)
    this.#b = (this.#c + (this.#c << 3)) >>> 0
    this.#c = (this.#c << 21) | (this.#c >>> 11)
    this.#c = (this.#c + t) >>> 0
    return t
  }

  /** An integer in [0, n). */
  below(n) {
    return this.u32() % n
  }

  chance(p) {
    return this.u32() / 0x100000000 < p
  }

  pick(items) {
    return items[this.below(items.length)]
  }
}

/** A plain SQLite connection to `path`, for fresh queries and table dumps. */
export async function oracle(path) {
  const { DatabaseSync } = await import('node:sqlite')
  return new DatabaseSync(path, { readOnly: true })
}

/** Rows as plain data that compares across both libraries: blobs as byte arrays. */
export function plain(rows) {
  return rows.map((row) => {
    const out = {}
    for (const [key, value] of Object.entries(row)) {
      out[key] = value instanceof Uint8Array ? [...value] : value
    }
    return out
  })
}
