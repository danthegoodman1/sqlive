'use strict'

// Loads the native addon: a local build in this directory first, then the
// platform package npm installed as an optional dependency.

const { readFileSync } = require('node:fs')
const { join } = require('node:path')

const errors = []

function isMusl() {
  if (process.platform !== 'linux') return false
  const report = typeof process.report?.getReport === 'function' ? process.report.getReport() : undefined
  if (report?.header?.glibcVersionRuntime) return false
  if (Array.isArray(report?.sharedObjects) && report.sharedObjects.some((file) => file.includes('musl'))) {
    return true
  }
  try {
    return readFileSync('/usr/bin/ldd', 'utf8').includes('musl')
  } catch {
    return false
  }
}

function target() {
  const { platform, arch } = process
  if (platform === 'darwin' && arch === 'arm64') return 'darwin-arm64'
  if (platform === 'win32' && arch === 'x64') return 'win32-x64-msvc'
  if (platform === 'linux' && (arch === 'x64' || arch === 'arm64')) {
    return `linux-${arch}-${isMusl() ? 'musl' : 'gnu'}`
  }
  throw new Error(`sqlive has no native build for ${platform}-${arch}`)
}

function attempt(id) {
  try {
    return require(id)
  } catch (error) {
    errors.push(error)
    return undefined
  }
}

function load() {
  if (process.env.SQLIVE_NATIVE_PATH) {
    return require(process.env.SQLIVE_NATIVE_PATH)
  }
  const name = target()
  const local = attempt(join(__dirname, `sqlive.${name}.node`))
  if (local) return local
  const main = require('./package.json')
  const platform = `${main.name}-${name}`
  const packaged = attempt(platform)
  if (packaged) {
    const actual = require(`${platform}/package.json`).version
    if (actual !== main.version) {
      throw new Error(`${main.name} ${main.version} found ${platform} ${actual}; reinstall so their versions match`)
    }
    return packaged
  }
  const reasons = errors.map((error) => `\n- ${error.message}`).join('')
  throw new Error(
    `sqlive could not load its native build for ${name}. Reinstall it with optional dependencies enabled.${reasons}`,
  )
}

module.exports = load()
