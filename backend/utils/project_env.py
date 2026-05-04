"""Load repository ``.env`` into the process environment (optional ``python-dotenv``)."""

from __future__ import annotations

from pathlib import Path


def load_project_dotenv(*, override: bool = False) -> None:
    """
    Load ``<repo>/.env`` if the file exists. Shell-exported variables win when
    ``override=False`` (default), matching common dev expectations.
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    root = Path(__file__).resolve().parents[2]
    env_path = root / ".env"
    if env_path.is_file():
        load_dotenv(env_path, override=override)
