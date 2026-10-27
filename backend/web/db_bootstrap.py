"""Boot-time database init for the web app, run as an explicit launcher step (D-DB4 a).

``python3 -m backend.web.db_bootstrap`` runs, in order: the production security
config check, ``init_users_db``, ``init_admin_db``, ``init_inventory_db`` (on
Postgres a check against the migration chain, nothing is created),
``init_dealer_portal_db`` and ``init_job_queue_schema``. Exit status 0 when all
five ran, 1 on the first failure (the failing step is named on stderr).

Every launcher calls it before serving: ``scripts/docker-entrypoint-web.sh``,
``start.sh`` (START_WEB), ``scripts/run-web-local.sh``, ``run.py`` and the
always-on launchd/systemd units (``tests/test_db_bootstrap.py`` greps for it).
``backend.main`` still runs the same inits at import for now (P13A.6 removes
that), so running both is expected: every step is idempotent.

It does NOT rebuild the incomplete-listings index; that full rebuild stays with
the scanner and worker init paths.
"""
from __future__ import annotations

import sys
from collections.abc import Callable

# (name, dotted module, attribute): imported lazily so a failed import is a named
# step failure, and so tests can patch the attribute on its own module.
STEPS: tuple[tuple[str, str, str], ...] = (
    ("users", "backend.db.users_db", "init_users_db"),
    ("admin", "backend.db.admin_users_db", "init_admin_db"),
    ("inventory", "backend.db.inventory_db", "init_inventory_db"),
    ("dealer_portal", "backend.db.dealer_portal_db", "init_dealer_portal_db"),
    ("job_queue", "backend.scanner.job_queue", "init_job_queue_schema"),
)


def _resolve(module: str, attr: str) -> Callable[[], None]:
    import importlib

    return getattr(importlib.import_module(module), attr)


def run_bootstrap() -> list[str]:
    """Run the config guards and every init step; return the step names that ran.

    Raises whatever the failing step raised, after naming it on stderr."""
    from backend.utils.production_security import assert_production_security_config

    assert_production_security_config()
    from backend.db.inventory_pg import assert_inventory_backend_configured

    assert_inventory_backend_configured()

    ran: list[str] = []
    for name, module, attr in STEPS:
        try:
            _resolve(module, attr)()
        except Exception:
            print(f"db_bootstrap: step {name!r} failed", file=sys.stderr)
            raise
        ran.append(name)
    return ran


def main() -> int:
    try:
        ran = run_bootstrap()
    except Exception as exc:
        print(f"db_bootstrap: FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print("db_bootstrap: ok (" + ", ".join(ran) + ")")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
