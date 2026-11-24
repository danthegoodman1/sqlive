#!/usr/bin/env node
// Fails if a Linux addon needs a newer glibc than the baseline its
// platform package promises.
//
//     node scripts/glibc.mjs sqlive.linux-x64-gnu.node 2.34

import { execFileSync } from 'node:child_process'

const [addon, baseline] = process.argv.slice(2)
if (!addon || !baseline) {
  console.error('usage: glibc.mjs ADDON BASELINE')
  process.exit(1)
}

const parts = (version) => version.split('.').map(Number)
const newer = (a, b) => {
  const [x, y] = [parts(a), parts(b)]
  for (let i = 0; i < Math.max(x.length, y.length); i++) {
    if ((x[i] ?? 0) !== (y[i] ?? 0)) return (x[i] ?? 0) > (y[i] ?? 0)
  }
  return false
}

const symbols = execFileSync('readelf', ['--version-info', '--wide', addon], { encoding: 'utf8' })
const versions = [...new Set([...symbols.matchAll(/GLIBC_(\d+(?:\.\d+)+)/g)].map((match) => match[1]))]
if (versions.length === 0) throw new Error(`${addon} names no glibc symbol versions`)
const highest = versions.reduce((a, b) => (newer(b, a) ? b : a))
if (newer(highest, baseline)) {
  console.error(`${addon} needs GLIBC_${highest}, past the GLIBC_${baseline} baseline`)
  process.exit(1)
}
console.log(`${addon} needs at most GLIBC_${highest} (baseline ${baseline})`)
