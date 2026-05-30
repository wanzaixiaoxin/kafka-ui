import { cpSync, existsSync, mkdirSync } from 'fs'
import { resolve, dirname } from 'path'
import { fileURLToPath } from 'url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const root = resolve(__dirname, '..')
const dist = resolve(root, 'dist')

if (!existsSync(dist)) mkdirSync(dist)

const installerDir = resolve(root, 'src-tauri/target/release/bundle/nsis')
const exe = resolve(root, 'src-tauri/target/release/kafka-client.exe')

if (existsSync(installerDir)) {
  for (const f of ['exe', 'msi']) {
    try { cpSync(resolve(installerDir, `Kafka Client_1.0.0_x64-setup.${f}`), resolve(dist, `Kafka Client Setup.${f}`)) } catch {}
  }
}

if (existsSync(exe)) {
  cpSync(exe, resolve(dist, 'kafka-client.exe'))
}

console.log('Copied to dist/')
