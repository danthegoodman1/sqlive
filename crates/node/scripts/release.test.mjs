import assert from 'node:assert/strict'
import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'
import { TARGETS, addonName, check, distTag, publish, setVersion, stage } from './release.mjs'

const packageDir = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const readJson = (path) => JSON.parse(readFileSync(path, 'utf8'))

/** A copy of the package's manifests to change. */
function copy(t) {
  const root = mkdtempSync(join(tmpdir(), 'sqlive-release-'))
  t.after(() => rmSync(root, { recursive: true, force: true }))
  for (const file of ['package.json', 'package-lock.json']) cpSync(join(packageDir, file), join(root, file))
  cpSync(join(packageDir, 'npm'), join(root, 'npm'), { recursive: true })
  return root
}

test('the checked-in packages agree', () => {
  assert.equal(check(packageDir), readJson(join(packageDir, 'package.json')).version)
})

test('a version change reaches every package and the lockfile', (t) => {
  const root = copy(t)
  setVersion('1.2.3-beta.1', root)
  assert.equal(check(root), '1.2.3-beta.1')
  for (const { target } of TARGETS) {
    assert.equal(readJson(join(root, 'npm', target, 'package.json')).version, '1.2.3-beta.1')
  }
  assert.throws(() => setVersion('1.2', root), /not a semver version/)
})

test('check catches a platform package that drifted', (t) => {
  const root = copy(t)
  const path = join(root, 'npm', 'linux-x64-gnu', 'package.json')
  const manifest = readJson(path)
  manifest.version = '9.9.9'
  writeFileSync(path, JSON.stringify(manifest))
  assert.throws(() => check(root), /differs from what package.json implies/)
})

test('stage copies every addon and names the platform packages', (t) => {
  const root = copy(t)
  const built = mkdtempSync(join(tmpdir(), 'sqlive-built-'))
  t.after(() => rmSync(built, { recursive: true, force: true }))
  for (const { target } of TARGETS.slice(1)) writeFileSync(join(built, addonName(target)), target)
  assert.throws(() => stage(built, root), /missing/)
  writeFileSync(join(built, addonName(TARGETS[0].target)), TARGETS[0].target)
  stage(built, root)
  const main = readJson(join(root, 'package.json'))
  assert.deepEqual(Object.keys(main.optionalDependencies), TARGETS.map(({ target }) => `${main.name}-${target}`))
  for (const { target } of TARGETS) {
    assert.equal(readFileSync(join(root, 'npm', target, addonName(target)), 'utf8'), target)
  }
})

test('publish skips published versions and finishes with the main package', (t) => {
  const root = copy(t)
  assert.throws(() => publish({ root, exec: () => '' }), /run `stage` before `publish`/)
  const main = readJson(join(root, 'package.json'))
  main.optionalDependencies = {}
  writeFileSync(join(root, 'package.json'), JSON.stringify(main))
  const calls = []
  const exec = (command, args) => {
    calls.push(args)
    if (args[0] === 'view' && args[1].startsWith(`${main.name}-darwin-arm64@`)) return main.version
    if (args[0] === 'view') throw new Error('E404')
    return ''
  }
  const done = publish({ root, exec })
  assert.equal(done.length, TARGETS.length)
  assert.equal(done.at(-1), `${main.name}@${main.version}`)
  assert.ok(!done.some((name) => name.startsWith(`${main.name}-darwin-arm64@`)))
  const published = calls.filter((args) => args[0] === 'publish')
  assert.ok(published.every((args) => args.includes('--provenance') && args.includes('--access')))
  assert.equal(distTag('1.0.0'), 'latest')
  assert.equal(distTag('1.0.0-rc.1'), 'next')
})
