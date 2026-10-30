#!/usr/bin/env bash
# Start IB Gateway with plain `docker run` (same settings as compose.yaml, for hosts
# without the compose plugin, e.g. the rootless docker ag-fleet sets up for `agfleet`).
# Usage: ./run.sh [pull|up|down|logs]
set -euo pipefail
cd "$(dirname "$0")"
export DOCKER_HOST="${DOCKER_HOST:-unix:///run/user/$(id -u)/docker.sock}"
IMAGE=ghcr.io/gnzsnz/ib-gateway:stable
NAME=ib-gateway

case "${1:-up}" in
  pull) docker pull "$IMAGE" ;;
  up)
    [ -f ib-gateway.env ] || { echo "missing ib-gateway.env (copy ib-gateway.env.example, chmod 600)" >&2; exit 1; }
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker run -d --name "$NAME" --restart unless-stopped \
      --env-file ib-gateway.env \
      -e READ_ONLY_API=yes -e TIME_ZONE=Etc/UTC -e AUTO_RESTART_TIME="11:45 PM" \
      -e TWOFA_TIMEOUT_ACTION=restart -e JAVA_HEAP_SIZE=768 \
      -p 127.0.0.1:4001:4003 -p 127.0.0.1:4002:4004 \
      --memory 1536m "$IMAGE" ;;
  down) docker rm -f "$NAME" ;;
  logs) docker logs -f --tail 50 "$NAME" ;;
  *) echo "usage: $0 [pull|up|down|logs]" >&2; exit 2 ;;
esac
