#!/usr/bin/env bash
set -euo pipefail

PROF_DIR="${1:-metrics/profiles}"
PLOT_DIR="${2:-metrics/plots}"
PORT=18095

mkdir -p "$PLOT_DIR"

f="${PROF_DIR}/cpu_query_idx.prof"
if [[ ! -f "$f" ]]; then
  echo "нет $f — сначала make profile"
  exit 0
fi

fuser -k ${PORT}/tcp 2>/dev/null || true
sleep 1
go tool pprof -http=":${PORT}" "$f" &
PID=$!
sleep 2

curl -sf "http://localhost:${PORT}/ui/flamegraph" \
    -o "${PLOT_DIR}/flamegraph_cpu_query_idx.html" || true

kill $PID 2>/dev/null || true
wait $PID 2>/dev/null || true
