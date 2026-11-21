#!/bin/sh
# Diagnostic only. Builds the blobs test, runs http_stream_survives_cache_ceiling, and samples
# its threads with gdb from the moment the HTTP request starts.
# Usage: run.sh <dir of sample.py> [gdb prefix, e.g. sudo]
set -u
HERE=$1
AS=${2:-}
OUT=${DIAG_OUT:-/tmp/diag}
mkdir -p "$OUT"
BIN=$(cargo test -p peartube-native --test blobs --no-run --message-format=json 2>"$OUT/build.log" |
  grep -o '"executable":"[^"]*blobs-[^"]*"' | tail -1 | cut -d'"' -f4)
echo "bin=$BIN"
[ -x "$BIN" ] || {
  tail -30 "$OUT/build.log"
  exit 2
}
: >"$OUT/test.log"
"$BIN" http_stream_survives_cache_ceiling --include-ignored --nocapture >"$OUT/test.log" 2>&1 &
T=$!
i=0
while ! grep -q 'cache-freeze: request start' "$OUT/test.log"; do
  sleep 0.02
  i=$((i + 1))
  if [ $i -gt 9000 ] || ! kill -0 $T 2>/dev/null; then
    echo "no start marker"
    tail -30 "$OUT/test.log"
    kill $T 2>/dev/null
    exit 3
  fi
done
if [ "${DIAG_GDB:-1}" = 1 ]; then
  $AS gdb -p "$T" -batch -x "$HERE/sample.py" >"$OUT/gdb.log" 2>&1
  echo "gdb samples: $(grep -c '^=== sample' "$OUT/gdb.log")"
fi
# Let the stream run on for a while, then stop: the first minute is the subject.
sleep "${DIAG_TAIL:-45}"
kill $T 2>/dev/null
pkill -f cache-ceiling.mjs 2>/dev/null
echo "===== test output"
grep -v '^cache-freeze: watch' "$OUT/test.log" | cut -c1-240 | head -120
echo "===== watch"
grep '^cache-freeze: watch' "$OUT/test.log" | head -130
if [ -f "$OUT/gdb.log" ]; then
  echo "===== gdb"
  cut -c1-200 "$OUT/gdb.log"
fi
