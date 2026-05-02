#!/usr/bin/env bash
set -euo pipefail

PROF_DIR="${1:-metrics/profiles}"
PLOT_DIR="${2:-metrics/plots}"
PORT=18092

mkdir -p "$PLOT_DIR"

profiles=("cpu_parallel_get_conc" "cpu_parallel_get_plain")

for prof in "${profiles[@]}"; do
  f="${PROF_DIR}/${prof}.prof"
  if [[ ! -f "$f" ]]; then
    echo "skip flamegraph (${prof}: нет профиля)"
    continue
  fi
  echo "→ flamegraph: ${prof}"

  fuser -k ${PORT}/tcp 2>/dev/null || true
  sleep 1

  go tool pprof -http=":${PORT}" "$f" &
  PPROF_PID=$!

  sleep 2

  if curl -sf "http://localhost:${PORT}/ui/flamegraph" \
      -o "${PLOT_DIR}/flamegraph_${prof}.html"; then
    :
  fi

  kill $PPROF_PID 2>/dev/null || true
  wait $PPROF_PID 2>/dev/null || true

  sleep 1

done
