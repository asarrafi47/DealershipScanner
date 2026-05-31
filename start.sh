#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load .env
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

export PYTHONPATH="${SCRIPT_DIR}/backend${PYTHONPATH:+:${PYTHONPATH}}"

echo "[start] Launching cloudflared tunnel..."
# --loglevel is a `tunnel` command flag (before `run`), not a `run` subcommand flag.
cloudflared tunnel --loglevel warn run --url http://localhost:5001 sarraficars &
TUNNEL_PID=$!

echo "[start] Starting Flask server (PUBLIC=1)..."
PUBLIC=1 python3 run.py &
SERVER_PID=$!

# Forward signals so both children die cleanly on Ctrl-C
trap "echo '[start] Shutting down...'; kill $TUNNEL_PID $SERVER_PID 2>/dev/null; wait" INT TERM

echo "[start] Running. Ctrl-C to stop."
echo "[start]   tunnel PID: $TUNNEL_PID"
echo "[start]   server PID: $SERVER_PID"

wait $SERVER_PID
