#!/usr/bin/env node
// A stand-in `ubit` for the art/identify fixtures: replays plan.json beside this script, then records the argv and
// UBIT_CONFIG_DIR it was called with in observed.json so both the JS and the Rust port can be checked against it.
import { readFileSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'

const here = dirname(process.argv[1])
const plan = JSON.parse(readFileSync(join(here, 'plan.json'), 'utf8'))
writeFileSync(join(here, 'observed.json'), JSON.stringify({
  argv: process.argv.slice(2),
  configDir: process.env.UBIT_CONFIG_DIR ?? null
}))

const gap = Number(plan.gapMs) || 0
const pause = () => new Promise(resolve => setTimeout(resolve, gap))
const hang = plan.hangAfterStdout == null ? Infinity : Number(plan.hangAfterStdout)
const stdout = Array.isArray(plan.stdout) ? plan.stdout : []
const stderr = Array.isArray(plan.stderr) ? plan.stderr : []

for (const [i, chunk] of stdout.entries()) {
  process.stdout.write(chunk)
  if (i + 1 >= hang) await new Promise(() => setInterval(() => {}, 1000))
  await pause()
}
for (const chunk of stderr) {
  process.stderr.write(chunk)
  await pause()
}
process.exit(Number(plan.exit) || 0)
