#!/bin/sh
# Writes STACK.lock: the commit of each sibling crate that release-ffi.yml builds against.
# Run it after a sibling change the next release must include; every commit must be pushed.
# A sibling's own Cargo.lock does not count: peartube-native's lock resolves the build.
#
#   sh ffi/stack-lock.sh
set -eu
repo=$(cd "$(dirname "$0")/.." && pwd)
out=$repo/STACK.lock
tmp=$out.tmp
: > "$tmp"
for r in compact-encoding-rs protomux-rs protomux-wakeup-rs hyperschema-rs hypercore-id-encoding-rs \
  hypercore-storage-rs hypercore-rs corestore-rs hyperbee-rs hyperblobs-rs byte-stream-rs autobee-rs peeroxide; do
  dir=$repo/../$r
  sha=$(git -C "$dir" rev-parse HEAD)
  [ -z "$(git -C "$dir" status --porcelain --untracked-files=no -- . ':!Cargo.lock')" ] || { echo "$r has uncommitted changes" >&2; rm -f "$tmp"; exit 1; }
  [ -n "$(git -C "$dir" branch -r --contains "$sha" 2>/dev/null)" ] || { echo "$r $sha is not pushed" >&2; rm -f "$tmp"; exit 1; }
  echo "$r $sha" >> "$tmp"
done
mv "$tmp" "$out"
cat "$out"
