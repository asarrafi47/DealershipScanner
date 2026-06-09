#!/usr/bin/env bash
# Start the Cloudflare tunnel so https://sarraficars.com serves your local Flask app.
#
# Prerequisite: Flask must already be listening on PORT (default 5001).
# In another terminal:
#   PUBLIC=1 python3 run.py
#
# Optional: START_WEB=1 ./start.sh  — also launch run.py if nothing is listening yet.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

export PYTHONPATH="${SCRIPT_DIR}/backend${PYTHONPATH:+:${PYTHONPATH}}"

PORT="${PORT:-5001}"
TUNNEL_NAME="${CLOUDFLARED_TUNNEL_NAME:-sarraficars}"
PUBLIC_URL="${PUBLIC_SITE_URL:-https://sarraficars.com}"

SERVER_PID=""

_web_ready() {
    curl -sf --max-time 2 "http://127.0.0.1:${PORT}/login" >/dev/null 2>&1
}

_wait_for_web() {
    local tries="${1:-30}"
    local i
    for ((i = 1; i <= tries; i++)); do
        if _web_ready; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

_start_web_if_requested() {
    if _web_ready; then
        echo "[start] Flask already responding on http://127.0.0.1:${PORT}"
        return 0
    fi
    if lsof -tiTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "[start] Port $PORT is in use but /login is not ready yet — waiting..."
        _wait_for_web 20
        return $?
    fi
    if [ "${START_WEB:-0}" != "1" ]; then
        echo "[start] ERROR: No Flask server on http://127.0.0.1:${PORT}"
        echo "[start] Start the app in another terminal, then re-run ./start.sh:"
        echo "        PUBLIC=1 python3 run.py"
        echo "[start] Or start both here: START_WEB=1 ./start.sh"
        exit 1
    fi
    echo "[start] START_WEB=1 — launching Flask on port $PORT..."
    PUBLIC=1 PORT="$PORT" python3 run.py &
    SERVER_PID=$!
    if ! _wait_for_web 40; then
        echo "[start] ERROR: Flask did not become ready on port $PORT."
        kill "$SERVER_PID" 2>/dev/null || true
        exit 1
    fi
}

_stop_old_tunnels() {
    local pattern="cloudflared tunnel.*${TUNNEL_NAME}"
    if pgrep -f "$pattern" >/dev/null 2>&1; then
        echo "[start] Stopping previous cloudflared tunnel(s) (${TUNNEL_NAME})..."
        pkill -f "$pattern" 2>/dev/null || true
        sleep 0.5
    fi
}

_start_web_if_requested
_stop_old_tunnels

echo "[start] Launching cloudflared tunnel → http://127.0.0.1:${PORT} (${TUNNEL_NAME})..."
cloudflared tunnel --loglevel warn run --url "http://127.0.0.1:${PORT}" "${TUNNEL_NAME}" &
TUNNEL_PID=$!

cleanup() {
    echo "[start] Shutting down..."
    kill "$TUNNEL_PID" 2>/dev/null || true
    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
    fi
    wait "$TUNNEL_PID" 2>/dev/null || true
}
trap cleanup INT TERM

echo "[start] Tunnel running. Ctrl-C to stop tunnel only."
echo "[start]   tunnel PID: $TUNNEL_PID"
if [ -n "$SERVER_PID" ]; then
    echo "[start]   server PID: $SERVER_PID"
fi
echo "[start] Local:  http://127.0.0.1:${PORT}"
echo "[start] Public: ${PUBLIC_URL}"

if _wait_for_web 1; then
    if curl -sf --max-time 15 "${PUBLIC_URL}/login" >/dev/null 2>&1; then
        echo "[start] Public site OK: ${PUBLIC_URL}"
    else
        echo "[start] Tunnel started; waiting for Cloudflare edge (try ${PUBLIC_URL} in ~30s)."
    fi
fi

wait "$TUNNEL_PID"
