#!/usr/bin/env bash
# Always-on foreground launcher: Cloudflare tunnel + Flask (same as start.sh, documented for deploy/always-on).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

PORT="${PORT:-5001}"
TUNNEL_NAME="${CLOUDFLARED_TUNNEL_NAME:-sarraficars}"

echo "[always-on] Launching cloudflared tunnel → http://127.0.0.1:${PORT} (${TUNNEL_NAME})..."
cloudflared tunnel run --url "http://127.0.0.1:${PORT}" "${TUNNEL_NAME}" &
TUNNEL_PID=$!

echo "[always-on] Starting Flask (PUBLIC=1, PORT=${PORT})..."
PUBLIC=1 PORT="${PORT}" python3 run.py &
SERVER_PID=$!

cleanup() {
  echo "[always-on] Shutting down..."
  kill "${TUNNEL_PID}" "${SERVER_PID}" 2>/dev/null || true
  wait "${SERVER_PID}" 2>/dev/null || true
}
trap cleanup INT TERM

echo "[always-on] Running. Ctrl-C to stop."
echo "[always-on]   tunnel PID: ${TUNNEL_PID}"
echo "[always-on]   server PID: ${SERVER_PID}"

wait "${SERVER_PID}"
