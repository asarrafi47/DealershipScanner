"""gunicorn server hooks for the web service (scripts/docker-entrypoint-web.sh passes -c).

Why this file exists: the entrypoint runs gunicorn with ``--preload`` so that
``backend.main`` -- and its one-time kmac vault load -- is imported ONCE in the
arbiter and every worker inherits the same populated environ. That is the right
call for secrets. It was the wrong place for the listings prewarm.

``backend/main.py`` used to start ``_prewarm_listings_inventory_cache()`` at
module level. Under ``--preload`` that thread starts in the ARBITER; the arbiter
forks its workers within milliseconds, the prewarm's 3 s head-start elapses
after the fork, and threads do not survive fork. Net effect: the arbiter builds
the entire ~2 GB listings grid that no request is ever served from, and each
worker then builds its own on first hit. Two full copies for one worker's worth
of serving. Railway bills memory.

ORDERING MATTERS -- verified against gunicorn 26.0.0 source:
  Application.__init__ -> do_load_config()      <- THIS FILE is exec'd here
  Arbiter.__init__ -> setup() -> app.wsgi()     <- --preload imports backend.main
  Arbiter.start()  -> cfg.on_starting()         <- too late; the import already ran
So the arbiter mark is set at MODULE LEVEL below, not in an on_starting hook.
(An earlier revision used on_starting and silently changed nothing.)
"""
from __future__ import annotations

import os

# Read by backend/main.py at import: when set, skip the module-level prewarm.
# Set here, at config-load time, so it is already in the environment when the
# --preload import of backend.main happens a few lines later in the arbiter.
ARBITER_MARK = "DS_GUNICORN_ARBITER"
os.environ[ARBITER_MARK] = "1"


def post_fork(server, worker) -> None:  # runs in each worker, after fork
    # Workers inherit ARBITER_MARK from the arbiter's environ, which is exactly
    # why main.py's import-time guard cannot be what starts the prewarm for them:
    # this hook is the one place that runs in the worker, so it starts it.
    from backend.main import _prewarm_listings_inventory_cache

    _prewarm_listings_inventory_cache()
