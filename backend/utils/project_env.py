"""Load repository ``.env`` into the process environment (optional ``python-dotenv``)."""

from __future__ import annotations

from pathlib import Path


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
        return
    root = Path(__file__).resolve().parents[2]
    env_path = root / ".env"
    if not env_path.is_file():
        return
    load_dotenv(env_path, override=override)
    # Patch keys that look invalid after merge
    if not override:
        dot_vals = dotenv_values(env_path)
        for key in ("ANTHROPIC_API_KEY",):
            shell_val = os.environ.get(key, "")
            dot_val = dot_vals.get(key, "")
            if dot_val and len(shell_val) < 40 < len(dot_val):
                os.environ[key] = dot_val
