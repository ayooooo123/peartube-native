// `downloadPage` for the acquire scenario harness: takes the next planned page, writes it into the temp
// directory the acquirer made, reports its progress, and answers the file the way `ubit get` would.
import { setFile } from './acquire-mirror-stubs.mjs'

export async function downloadPage (url, dir, options = {}) {
  const plan = globalThis.__pages?.shift()
  if (!plan) throw new Error('Source stalled')
  for (const progress of plan.progress || []) options.onProgress?.(progress)
  if (options.controller?.aborted) throw new Error('Cancelled')
  const path = `${dir}/clip.mp4`
  setFile(path, plan.body)
  return { path, size: plan.size ?? null }
}
