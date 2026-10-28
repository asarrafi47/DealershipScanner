"""
Where runtime-written state files live (scan counter, live gas prices).

These files used to sit under ``backend/dictionary/derived/``, a tracked tree, so
every scan dirtied ``git status``. They now resolve to ``$RUNTIME_STATE_DIR`` or
``<repo>/workspace/state/`` (untracked; on Railway ``/app/workspace`` is a symlink
to the ``/data`` volume).
"""
from __future__ import annotations

import contextlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Iterator

ROOT = Path(__file__).resolve().parents[2]


def state_dir() -> Path:
    raw = (os.environ.get("RUNTIME_STATE_DIR") or "").strip()
    return Path(raw) if raw else ROOT / "workspace" / "state"


def state_path(name: str) -> Path:
    """Path of runtime state file ``name`` (the directory is not created here)."""
    return state_dir() / name


def write_json_atomic(path: Path, payload: Any) -> Path:
    """Write ``payload`` as JSON via a per-process tmp name, then rename over ``path``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        os.replace(tmp_name, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_name)
        raise
    return path


@contextlib.contextmanager
def file_lock(path: Path) -> Iterator[None]:
    """Exclusive advisory lock on ``<path>.lock`` (no-op where fcntl is missing)."""
    try:
        import fcntl
    except ImportError:  # pragma: no cover - non-POSIX
        yield
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(path.name + ".lock")
    with open(lock_path, "a", encoding="utf-8") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)
