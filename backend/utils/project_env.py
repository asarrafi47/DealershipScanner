"""Load repository ``.env`` into the process environment (optional ``python-dotenv``)."""

from __future__ import annotations

import sys
from pathlib import Path


def ensure_backend_on_sys_path() -> None:
    """
    Insert ``backend/`` on ``sys.path`` so bare imports (``scraping``, ``oem``, …) resolve.

    Matches ``conftest.py`` and script entrypoints; safe to call repeatedly.
    """
    backend_dir = str(Path(__file__).resolve().parents[1])
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)


def load_project_dotenv(*, override: bool = False) -> None:
    """
    Load ``<repo>/.env`` if the file exists. Shell-exported variables win when
    ``override=False`` (default), matching common dev expectations.

    Exception: ANTHROPIC_API_KEY is always taken from .env when the shell
    value is clearly invalid (too short to be a real key), so Claude features
    work even when the shell has a placeholder value.
    """
    import os

    try:
        from dotenv import load_dotenv, dotenv_values
    except ImportError:
        ensure_backend_on_sys_path()
        return
    root = Path(__file__).resolve().parents[2]
    env_path = root / ".env"
    if env_path.is_file():
        load_dotenv(env_path, override=override)
        # Patch keys that look invalid after merge
        if not override:
            dot_vals = dotenv_values(env_path)
            for key in ("ANTHROPIC_API_KEY",):
                shell_val = os.environ.get(key, "")
                dot_val = dot_vals.get(key, "")
                if dot_val and len(shell_val) < 40 < len(dot_val):
                    os.environ[key] = dot_val
    ensure_backend_on_sys_path()


def bootstrap_inventory_script(*, init_db: bool = True) -> None:
    """
    Load ``.env`` / vault secrets, then optionally ``init_inventory_db()``.

    Use at the top of CLI scripts that need Postgres inventory.
    """
    load_project_dotenv()
    import os

    # Prefer explicit inventory DSN; fall back to DATABASE_URL when it is Postgres.
    if not (os.environ.get("INVENTORY_DATABASE_URL") or "").strip():
        db_url = (os.environ.get("DATABASE_URL") or "").strip()
        if db_url.startswith(("postgresql://", "postgres://")):
            os.environ["INVENTORY_DATABASE_URL"] = db_url
    try:
        from backend.utils.kmac_vault import load_kmac_vault_secrets

        load_kmac_vault_secrets()
    except Exception:
        pass
    if init_db:
        from backend.db.inventory_db import init_inventory_db

        init_inventory_db()
