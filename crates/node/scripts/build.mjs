#!/usr/bin/env node
// Builds the addon for this machine and copies it next to index.js as
// sqlive.<target>.node, the name binding.js loads.
//
//     node scripts/build.mjs [--debug | --profile NAME]
//
// The default build uses the workspace's `node` profile: stripped, with
// link-time optimization. Cargo builds for the host without `--target`, so
// build scripts share the target's settings; on musl that keeps them able
// to load libclang, which libsqlite3-sys's bindgen needs.

import { execFileSync } from 'node:child_process'
import { copyFileSync, readFileSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const packageDir = resolve(here, '..')
const workspace = resolve(packageDir, '../..')

function isMusl() {
  if (process.platform !== 'linux') return false
  const report = process.report?.getReport?.()
  if (report?.header?.glibcVersionRuntime) return false
  try {
    return readFileSync('/usr/bin/ldd', 'utf8').includes('musl')
  } catch {
    return true
  }
}

function target() {
  const { platform, arch } = process
  if (platform === 'darwin' && arch === 'arm64') return 'darwin-arm64'
  if (platform === 'win32' && arch === 'x64') return 'win32-x64-msvc'
  if (platform === 'linux' && (arch === 'x64' || arch === 'arm64')) return `linux-${arch}-${isMusl() ? 'musl' : 'gnu'}`
  throw new Error(`sqlive has no build for ${platform}-${arch}`)
}

const named = process.argv.indexOf('--profile')
const profile = process.argv.includes('--debug') ? 'dev' : named > 0 ? process.argv[named + 1] : 'node'
execFileSync('cargo', ['build', '--profile', profile, '-p', 'sqlive-node'], { cwd: workspace, stdio: 'inherit' })

const targetDir = process.env.CARGO_TARGET_DIR ? resolve(workspace, process.env.CARGO_TARGET_DIR) : join(workspace, 'target')
const library = { darwin: 'libsqlive_node.dylib', win32: 'sqlive_node.dll' }[process.platform] ?? 'libsqlive_node.so'
const built = join(targetDir, profile === 'dev' ? 'debug' : profile, library)
const addon = join(packageDir, `sqlive.${target()}.node`)
copyFileSync(built, addon)
console.log(`built ${addon}`)
