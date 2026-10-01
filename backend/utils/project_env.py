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


DOTENV_DISABLE_ENV = "PROJECT_DOTENV_DISABLE"


def dotenv_disabled() -> bool:
    """
    True when ``PROJECT_DOTENV_DISABLE`` is set (the root ``conftest.py`` sets it
    for every pytest session). Tests blank database URLs and point user DBs at
    tmp files; ``.env`` must neither refill a blanked key nor override a value a
    test set, so loading it is skipped entirely while the flag is on.
    """
    import os

    return (os.environ.get(DOTENV_DISABLE_ENV) or "").strip().lower() in ("1", "true", "yes", "on")


def load_project_dotenv(*, override: bool = False) -> None:
    """
    Load ``<repo>/.env`` if the file exists. Shell-exported variables win when
    ``override=False`` (default), matching common dev expectations.

    Exception: ANTHROPIC_API_KEY is always taken from .env when the shell
    value is clearly invalid (too short to be a real key), so Claude features
    work even when the shell has a placeholder value.

    No-op (beyond the ``sys.path`` fix-up) while :func:`dotenv_disabled`.
    """
    import os

    if dotenv_disabled():
        ensure_backend_on_sys_path()
        return
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
