#!/usr/bin/env node
// Keeps the platform packages in step with the main package, stages built
// addons into them, and publishes any package version npm lacks.
//
//     node scripts/release.mjs check             # npm/* match package.json
//     node scripts/release.mjs version 0.2.0     # sets every package's version
//     node scripts/release.mjs stage DIR         # copies each sqlive.<target>.node from DIR
//     node scripts/release.mjs publish [--dry-run]
//
// The main package names each platform package as an optional dependency
// only once `stage` has run, so the lockfile never refers to a package
// that is not published yet.

import { execFileSync } from 'node:child_process'
import { copyFileSync, existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

export const TARGETS = [
  { triple: 'aarch64-apple-darwin', target: 'darwin-arm64', os: ['darwin'], cpu: ['arm64'] },
  { triple: 'aarch64-unknown-linux-gnu', target: 'linux-arm64-gnu', os: ['linux'], cpu: ['arm64'], libc: ['glibc'] },
  { triple: 'aarch64-unknown-linux-musl', target: 'linux-arm64-musl', os: ['linux'], cpu: ['arm64'], libc: ['musl'] },
  { triple: 'x86_64-pc-windows-msvc', target: 'win32-x64-msvc', os: ['win32'], cpu: ['x64'] },
  { triple: 'x86_64-unknown-linux-gnu', target: 'linux-x64-gnu', os: ['linux'], cpu: ['x64'], libc: ['glibc'] },
  { triple: 'x86_64-unknown-linux-musl', target: 'linux-x64-musl', os: ['linux'], cpu: ['x64'], libc: ['musl'] },
]

const defaultRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')

const readJson = (path) => JSON.parse(readFileSync(path, 'utf8'))
const writeJson = (path, value) => writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`)

export const addonName = (target) => `sqlive.${target}.node`

/** The manifest of a target's platform package, derived from the main one. */
export function platformManifest(main, { target, os, cpu, libc }) {
  const manifest = {
    name: `${main.name}-${target}`,
    version: main.version,
    description: `The ${target} native addon of ${main.name}`,
    author: main.author,
    homepage: main.homepage,
    bugs: main.bugs,
    repository: main.repository,
    main: addonName(target),
    files: [addonName(target)],
    os,
    cpu,
    engines: main.engines,
  }
  if (main.license !== undefined) manifest.license = main.license
  if (libc !== undefined) manifest.libc = libc
  return manifest
}

function fail(message) {
  throw new Error(message)
}

function semver(version) {
  if (!/^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(-[0-9A-Za-z.-]+)?$/.test(version)) {
    fail(`${version} is not a semver version`)
  }
}

/** The dist-tag a version publishes under: `next` for a prerelease. */
export function distTag(version) {
  return version.includes('-') ? 'next' : 'latest'
}

export function check(root = defaultRoot) {
  const main = readJson(join(root, 'package.json'))
  semver(main.version)
  const triples = TARGETS.map(({ triple }) => triple)
  if (JSON.stringify(main.napi?.targets) !== JSON.stringify(triples)) {
    fail(`package.json napi.targets must list ${triples.join(', ')}`)
  }
  if (main.optionalDependencies !== undefined) {
    fail('package.json names its platform packages only when `stage` runs')
  }
  if (main.files.some((file) => file.endsWith('.node'))) fail('the main package ships no addon')
  const dirs = readdirSync(join(root, 'npm')).sort()
  const expected = TARGETS.map(({ target }) => target).sort()
  if (JSON.stringify(dirs) !== JSON.stringify(expected)) fail(`npm/ must hold exactly ${expected.join(', ')}`)
  for (const target of TARGETS) {
    const path = join(root, 'npm', target.target, 'package.json')
    const actual = readJson(path)
    const wanted = platformManifest(main, target)
    if (JSON.stringify(actual) !== JSON.stringify(wanted)) {
      fail(`${path} differs from what package.json implies; run \`node scripts/release.mjs version ${main.version}\``)
    }
  }
  const lock = readJson(join(root, 'package-lock.json'))
  if (lock.version !== main.version || lock.packages?.['']?.version !== main.version) {
    fail(`package-lock.json is not at version ${main.version}`)
  }
  return main.version
}

/** Sets every package's version and rewrites the platform manifests. */
export function setVersion(version, root = defaultRoot) {
  semver(version)
  const mainPath = join(root, 'package.json')
  const main = readJson(mainPath)
  main.version = version
  writeJson(mainPath, main)
  for (const target of TARGETS) {
    mkdirSync(join(root, 'npm', target.target), { recursive: true })
    writeJson(join(root, 'npm', target.target, 'package.json'), platformManifest(main, target))
  }
  const lockPath = join(root, 'package-lock.json')
  const lock = readJson(lockPath)
  lock.version = version
  lock.packages[''].version = version
  writeJson(lockPath, lock)
}

/** Copies each target's addon from `dir` into its package and names the packages in package.json. */
export function stage(dir, root = defaultRoot) {
  const version = check(root)
  for (const { target } of TARGETS) {
    const source = join(dir, addonName(target))
    if (!existsSync(source)) fail(`missing ${source}`)
    copyFileSync(source, join(root, 'npm', target, addonName(target)))
  }
  const mainPath = join(root, 'package.json')
  const main = readJson(mainPath)
  main.optionalDependencies = Object.fromEntries(TARGETS.map(({ target }) => [`${main.name}-${target}`, version]))
  writeJson(mainPath, main)
}

function published(name, version, exec) {
  try {
    exec('npm', ['view', `${name}@${version}`, 'version'])
    return true
  } catch {
    return false
  }
}

/** Publishes each platform package, then the main one, skipping versions npm has. */
export function publish({ root = defaultRoot, dryRun = false, exec = run } = {}) {
  const main = readJson(join(root, 'package.json'))
  if (main.optionalDependencies === undefined) fail('run `stage` before `publish`')
  const tag = distTag(main.version)
  const packages = [...TARGETS.map(({ target }) => join(root, 'npm', target)), root]
  const done = []
  for (const dir of packages) {
    const { name, version } = readJson(join(dir, 'package.json'))
    if (published(name, version, exec)) {
      console.log(`${name}@${version} is already published`)
      continue
    }
    const args = ['publish', dir, '--access', 'public', '--tag', tag]
    if (dryRun) args.push('--dry-run')
    else args.push('--provenance')
    exec('npm', args)
    done.push(`${name}@${version}`)
  }
  return done
}

function run(command, args) {
  return execFileSync(command, args, { stdio: ['ignore', 'pipe', 'inherit'], encoding: 'utf8' })
}

const invoked = process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)
if (invoked) {
  const [command, arg] = process.argv.slice(2)
  try {
    if (command === 'check') console.log(`packages agree at ${check()}`)
    else if (command === 'version' && arg) setVersion(arg)
    else if (command === 'stage' && arg) stage(resolve(arg))
    else if (command === 'publish') for (const name of publish({ dryRun: process.argv.includes('--dry-run') })) console.log(`published ${name}`)
    else fail('usage: release.mjs check | version VERSION | stage DIR | publish [--dry-run]')
  } catch (error) {
    console.error(error.message)
    process.exit(1)
  }
}
