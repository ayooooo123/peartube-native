// The cast controller: `onchange`, `onCommand` and the receiver's answers are scripted.
import { clone, fixtures } from '../state.mjs'

export async function resolvePeerStorage (candidate, fallback) {
  return candidate || `${fallback}/peer`
}

export function reclaimLowDiskReceiverStorage () {}

export function createCastController (options) {
  const status = () => clone(fixtures.castStatus)
  const change = () => options.onchange?.()
  return {
    async initHub () {},
    async setLanHost (host) { this.lanHost = host },
    status,
    async pairReceiver () { change(); return clone(fixtures.receiver) },
    removeReceiver () { change(); return status() },
    async castPlay () { return clone(fixtures.castPlay) },
    async castAttach () { return clone(fixtures.castAttach) },
    async castControl () { return clone(fixtures.castControl) },
    async castStop () { return clone(fixtures.castStop) },
    async startLocalReceiver ({ onCommand }) {
      await onCommand(clone(fixtures.receiverCommand))
      return clone(fixtures.receiverStart)
    },
    async reportLocalReceiverState () { return clone(fixtures.receiverState) },
    async close () {}
  }
}
