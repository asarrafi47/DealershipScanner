"""The recipe file cache: atomic writes, a per-file lock, and the guard backup.

The ``dealer_recipes`` row is the authority for a dealer's recipes (N13 P7.2);
``workspace/recipes/<slug>.json`` is a cache of it, refreshed only when the row's
content differs. This module holds the file mechanics ``backend.scanner.recipes``
uses for that, and nothing that knows what a recipe is:

- ``atomic_write_json``: temp file in the same directory, fsync, ``os.replace``.
  Readers see the old file or the new one, never a torn one.
- ``cache_lock``: ``fcntl.flock`` on ``<slug>.json.lock`` beside the cache file, so
  two processes on one host (shards) do not interleave a compare-and-rewrite. The
  lock name ends ``.lock``, so the ``*.json`` globs over the cache dir skip it.
- ``is_reconciled`` / ``backup_cache_file``: until ``<cache>/_reconciled`` names the
  current store's fingerprint (written by ``backend/scripts/reconcile_recipe_store.py``),
  a cache file that differs from the store is copied to ``workspace/backups`` before
  the store's copy overwrites it, so cache-side evidence is never destroyed unseen.
- ``oplog_*`` (N13 P7.3): the offline op-log, ``<cache>/_pending/<store fingerprint>/<slug>``,
  one JSON object per line. A recipe mutation the store could not take is appended
  here and replayed on the next load that reaches the store. The fingerprint
  directory keeps an entry from ever being replayed against another store. Callers
  hold ``cache_lock(oplog_path(...))`` around a read-modify-write.
"""
from __future__ import annotations

import json
import os
import shutil
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:  # POSIX only; elsewhere the lock is a no-op (the atomic write still holds)
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]

RECONCILED_MARKER = "_reconciled"
PENDING_DIR = "_pending"
BACKUP_SUBDIR = "recipe_cache_differing"


def atomic_write_json(path: Path, obj: Any) -> None:
    """Write ``obj`` as JSON (``indent=1``) to ``path`` atomically.

    The bytes go to ``<name>.tmp.<pid>.<thread>`` in the same directory, are
    flushed and fsynced, then ``os.replace``d over ``path``. On any failure the temp
    file is removed and the previous ``path`` is left byte-identical. Temp names end
    in ``.tmp.<pid>.<thread>``, so the ``*.json`` globs over the cache dir never
    match them.
    """
    payload = json.dumps(obj, indent=1)
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}.{threading.get_ident()}")
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def lock_path(path: Path) -> Path:
    return path.with_name(path.name + ".lock")


@contextmanager
def cache_lock(path: Path) -> Iterator[None]:
    """Hold an exclusive ``flock`` on ``<path>.lock`` for the block.

    Best-effort: where the lock file cannot be opened (read-only dir, no ``fcntl``)
    the block runs unlocked, as a cache write always did.
    """
    fh = None
    if fcntl is not None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            fh = open(lock_path(path), "a+")
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        except OSError:
            if fh is not None:
                fh.close()
            fh = None
    try:
        yield
    finally:
        if fh is not None:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            finally:
                fh.close()


def read_rows(path: Path) -> list[dict] | None:
    """The cache file's rows, or None when it is missing, unreadable or not a list."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return raw if isinstance(raw, list) else None


def is_reconciled(cache_dir: Path, fingerprint: str | None) -> bool:
    """True when ``<cache_dir>/_reconciled`` records a reconcile for ``fingerprint``."""
    if not fingerprint:
        return False
    try:
        marker = json.loads((cache_dir / RECONCILED_MARKER).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return isinstance(marker, dict) and marker.get("fingerprint") == fingerprint


def backup_cache_file(path: Path) -> Path | None:
    """Copy ``path`` to ``<workspace>/backups/recipe_cache_differing/<name>.<stamp>``.

    ``<workspace>`` is the cache dir's parent (``workspace/recipes`` -> ``workspace``).
    Returns the copy, or None when the file does not exist. Raises OSError when the
    copy fails (the caller then leaves the cache alone).
    """
    if not path.exists():
        return None
    dest_dir = path.parent.parent / "backups" / BACKUP_SUBDIR
    dest_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    dest = dest_dir / f"{path.stem}.{stamp}.json"
    shutil.copy2(path, dest)
    return dest


def oplog_path(cache_dir: Path, fingerprint: str, slug: str) -> Path:
    """The op-log file of ``slug`` for the store ``fingerprint``."""
    return cache_dir / PENDING_DIR / fingerprint / slug


def oplog_read(path: Path) -> list[dict]:
    """The log's entries in order. A missing file is an empty log; a line that is not
    a JSON object is skipped (a torn last line from a crash must not wedge the log)."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return []
    out: list[dict] = []
    for line in text.splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if isinstance(entry, dict):
            out.append(entry)
    return out


def oplog_append(path: Path, entry: dict) -> None:
    """Append one entry (a single ``write`` of one line, fsynced)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def oplog_rewrite(path: Path, entries: list[dict]) -> None:
    """Replace the log with ``entries`` atomically; remove the file (and the then-empty
    fingerprint dir) when ``entries`` is empty."""
    if not entries:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        try:
            path.parent.rmdir()
        except OSError:
            pass
        return
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}.{threading.get_ident()}")
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write("".join(json.dumps(e, ensure_ascii=False) + "\n" for e in entries))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
