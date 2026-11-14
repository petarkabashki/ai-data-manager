#!/bin/bash
# One IBKR stock download worker: passes over 1d/1w, then 8h/4h, then 1h.
#
#   tools/ibkr_worker.sh <worker-index> <worker-count> [universe]
#
# Symbols are split by index modulo worker-count. Each pass runs twice: the
# second round only retries what the first left (the planner reads the files,
# so finished work is skipped). When the downloader exits 75 (Gateway session
# dead and not coming back, or the deployed python code changed under it) this
# waits for the API to answer and relaunches, so the next item runs on new code.
set -u
cd "$(dirname "$0")/.."
W=${1:?worker index}; N=${2:?worker count}; UNIVERSE=${3:-russell2000}
PORT=${IBKR_PORT:-4001}
UV=${UV:-$(command -v uv || echo "$HOME/.local/bin/uv")}
mkdir -p tmp

SYMS=$($UV run python -c "
import yaml
k=list(yaml.safe_load(open('price-data/$UNIVERSE.yaml')))
print(' '.join(s for i,s in enumerate(k) if i%$N==$W))")

[ -n "$SYMS" ] || { echo "no symbols for worker $W/$N"; exit 2; }

gateway_answers() {
  python3 - "$PORT" <<'PY'
import socket, struct, sys
try:
    s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=5); s.settimeout(8)
    m = b"v100..176"; s.sendall(b"API\0" + struct.pack(">I", len(m)) + m)
    sys.exit(0 if s.recv(64) else 1)
except Exception:
    sys.exit(1)
PY
}

run_pass() {  # run_pass <tag> <downloader args...>
  local tag=$1; shift
  local log=tmp/ibkr_${UNIVERSE}_w${W}_${tag}.out
  local rc
  while true; do
    nice -n 5 $UV run python -u tools/download_ibkr_stock_overnight.py \
      --universe "$UNIVERSE" --session all --port "$PORT" --client-id $((81+W)) \
      --sleep-seconds 0.4 --max-chunk-retries 2 --since 2000-01-01 \
      --resume-log tmp/ibkr_stock_resume_${UNIVERSE}_w${W}_${tag}.jsonl \
      --progress-file tmp/ibkr_stock_progress_${UNIVERSE}_w${W}_${tag}.json \
      "$@" --symbols $SYMS >> "$log" 2>&1
    rc=$?
    [ $rc -ne 75 ] && return $rc
    echo "$(date -u +%FT%TZ) exit 75 (Gateway or code change): waiting for the Gateway API" >> "$log"
    until gateway_answers; do sleep 30; done
    echo "$(date -u +%FT%TZ) Gateway answers again, relaunching" >> "$log"
  done
}

for round in 1 2; do
  run_pass a${round} --timeframes 1d 1w --request-timeout 60
  run_pass b${round} --timeframes 8h 4h --chunk-days 180 --request-timeout 90
  run_pass c${round} --timeframes 1h --chunk-days 90 --request-timeout 120
done
