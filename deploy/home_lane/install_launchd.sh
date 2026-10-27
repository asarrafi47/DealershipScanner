#!/usr/bin/env bash
#
# install_launchd.sh — render com.sarraficars.home-lane.plist.template into
# ~/Library/LaunchAgents. Renders only by default; --load also loads it (owner action).
#
# Usage: install_launchd.sh [--load] [--hour H] [--minute M]     (default 02:00 local;
# set the Mac to America/New_York so the run ends before Railway's 09:00 UTC window)

set -eu

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SELF_DIR/../.." && pwd)"
TEMPLATE="$SELF_DIR/com.sarraficars.home-lane.plist.template"
AGENT_DIR="${HOME_LANE_AGENT_DIR:-$HOME/Library/LaunchAgents}"
LABEL="com.sarraficars.home-lane"
HOUR=2
MINUTE=0
LOAD=0

while [ $# -gt 0 ]; do
  case "$1" in
    --load) LOAD=1 ;;
    --hour) HOUR="${2:?--hour needs a value}"; shift ;;
    --minute) MINUTE="${2:?--minute needs a value}"; shift ;;
    *) echo "usage: install_launchd.sh [--load] [--hour H] [--minute M]" >&2; exit 64 ;;
  esac
  shift
done
case "$HOUR" in ''|*[!0-9]*) echo "bad --hour" >&2; exit 64 ;; esac
case "$MINUTE" in ''|*[!0-9]*) echo "bad --minute" >&2; exit 64 ;; esac
[ "$HOUR" -le 23 ] && [ "$MINUTE" -le 59 ] || { echo "time out of range" >&2; exit 64; }

[ -f "$REPO_ROOT/.prod_lane_clone" ] || { echo "REFUSED: $REPO_ROOT is not a prod-lane clone (no .prod_lane_clone marker)" >&2; exit 2; }

mkdir -p "$AGENT_DIR"
OUT="$AGENT_DIR/$LABEL.plist"
sed -e "s|__REPO_ROOT__|$REPO_ROOT|g" -e "s|__HOME__|$HOME|g" \
    -e "s|__HOUR__|$HOUR|g" -e "s|__MINUTE__|$MINUTE|g" "$TEMPLATE" >"$OUT"
echo "rendered $OUT"

if [ "$LOAD" = "1" ]; then
  launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
  launchctl bootstrap "gui/$(id -u)" "$OUT"
  echo "loaded $LABEL"
else
  echo "not loaded; re-run with --load to activate"
fi
