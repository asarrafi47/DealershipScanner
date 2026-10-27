#!/bin/sh
# Web container entrypoint: optional vault preload, then gunicorn on Railway/Docker PORT.
set -eu

export KMAC_VAULT_AUTO="${KMAC_VAULT_AUTO:-1}"
export PYTHONPATH="${PYTHONPATH:-/app}"

# Fail-fast reachability check only: this subprocess's env writes die with it
# and are never inherited by the gunicorn process below, so it exists purely
# to surface a vault problem early via the WARN line. The real, single load
# that workers actually run on happens later, once, in the gunicorn master
# (see --preload on the exec gunicorn call).
if [ "${KMAC_VAULT_AUTO}" != "0" ]; then
  python3 -c "from backend.utils.kmac_vault import load_kmac_vault_secrets; load_kmac_vault_secrets()" \
    || echo "WARN: kmac vault preload skipped (vault unreachable or token missing)" >&2
fi

# Converge the schema before serving: the versioned chain in migrations/ is the
# authoritative schema source; runtime CREATE TABLE passes only re-assert it.
# Idempotent (checksummed schema_migrations tracking). Opt out: MIGRATE_ON_BOOT=0.
if [ "${MIGRATE_ON_BOOT:-1}" != "0" ]; then
  python3 -m backend.scripts.migrate --apply \
    || echo "WARN: migration apply FAILED — booting on the existing schema" >&2
fi

# Boot-time DB init (D-DB4): users, admin, inventory check, dealer portal, job queue.
# A failure in production refuses to serve; elsewhere it warns and boots.
if ! python3 -m backend.web.db_bootstrap; then
  if [ "${FLASK_ENV:-}" = "production" ]; then
    echo "ERROR: db_bootstrap failed in production -- not serving" >&2
    exit 1
  fi
  echo "WARN: db_bootstrap failed -- booting anyway (non-production)" >&2
fi

python3 scripts/bootstrap_site_admin.py \
  || echo "WARN: site admin bootstrap skipped" >&2

PORT="${PORT:-8000}"
GUNICORN_RELOAD=""
if [ "${FLASK_ENV:-}" = "development" ]; then
  # poll: reliable reload when /app is a Docker bind mount (inotify misses host edits on macOS)
  GUNICORN_RELOAD="--reload --reload-engine poll"
fi
# -w 1 means every CPU-bound request (e.g. serializing the full listings
# catalog to JSON, or a heavy Jinja render) holds the GIL and blocks every
# other request, regardless of thread count -- threads only help I/O waits.
# More worker PROCESSES parallelize, but each duplicates this process's
# in-memory caches (listings grid, filter options, etc.), and --preload does
# not share them (refcounting breaks copy-on-write). Railway bills memory, so
# PRODUCTION RUNS GUNICORN_WORKERS=1 (set in the service variables): 3 workers
# held 13.7 GB flat, 1 worker 4.92 GB, and with the arbiter no longer
# prewarming (gunicorn.conf.py) ~3.3 GB warm. The default below is only the
# fallback for an unset variable (local/Docker runs); an earlier note here
# sizing 3 workers at ~18 GB against a 24 GB limit predates that measurement.
# Re-measure RSS before raising it, and check `cat /sys/fs/cgroup/memory.max`.
GUNICORN_WORKERS="${GUNICORN_WORKERS:-3}"

# Multi-worker fallout #1: the in-memory IP rate limiter (backend/utils/ip_rate_limit.py)
# keeps its sliding-window state per-process (N37.11: the shared SQLite file option is
# gone), so with >1 worker the effective ceiling on login/register/smart-search/recalls
# is the configured limit times GUNICORN_WORKERS. Set GUNICORN_WORKERS=1 for exact limits.

# Multi-worker fallout #2: --max-requests recycles a worker mid-run with no
# awareness of the daemon threads it owns (bulk import queue, vector reindex,
# listings/grid cache rebuild) — a job in flight on that worker just vanishes,
# no error ever reaches the job dict. Default to disabled (0 = never recycle)
# so background jobs are safe by default; opt in via env only if you've moved
# those jobs off in-process daemon threads or accept the risk.
GUNICORN_MAX_REQUESTS="${GUNICORN_MAX_REQUESTS:-0}"
GUNICORN_MAX_REQUESTS_JITTER="${GUNICORN_MAX_REQUESTS_JITTER:-0}"

# --preload loads backend.main (and its module-level load_kmac_vault_secrets()
# call) once in the gunicorn master before forking, so every worker inherits
# the SAME populated os.environ instead of each independently re-fetching
# vault secrets and possibly diverging on a transient vault failure.
# -c gunicorn.conf.py: moves the listings prewarm out of the --preload arbiter
# (which otherwise builds a ~2GB grid it never serves) into post_fork per worker.
exec gunicorn -c "${GUNICORN_CONF:-/app/gunicorn.conf.py}" \
  -w "${GUNICORN_WORKERS}" --threads 4 -b "0.0.0.0:${PORT}" \
  --preload \
  --timeout 120 --graceful-timeout 30 \
  --max-requests "${GUNICORN_MAX_REQUESTS}" --max-requests-jitter "${GUNICORN_MAX_REQUESTS_JITTER}" \
  ${GUNICORN_RELOAD} \
  backend.main:app
