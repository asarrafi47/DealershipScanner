"""
Simple PID-file lock so two full scanner runs never overlap on one host.

A stale lock (holder PID no longer alive, or an unreadable/empty file) is
reclaimed automatically rather than wedging every future run.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from backend.scanner.constants import ROOT

logger = logging.getLogger("scanner")

LOCK_PATH = ROOT / "workspace" / "scanner.lock"


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Exists, just owned by someone else — still alive.
        return True
    except OSError:
        return False
    return True


def current_lock_holder(path: Path | None = None) -> int | None:
    """PID recorded in the lock file, or None if absent/unreadable."""
    lock_path = Path(path) if path else LOCK_PATH
    try:
        return int(lock_path.read_text().strip())
    except (ValueError, OSError):
        return None


def acquire_scan_lock(path: Path | None = None) -> bool:
    """
    Claim the scanner run lock for this process.

    Returns True once acquired (including reclaiming a stale lock). Returns
    False when another *live* process already holds it — caller should refuse
    to start a second concurrent scan.
    """
    lock_path = Path(path) if path else LOCK_PATH
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    held_pid = current_lock_holder(lock_path)
    if held_pid and held_pid != os.getpid() and _pid_alive(held_pid):
        return False
    lock_path.write_text(str(os.getpid()))
    return True


def release_scan_lock(path: Path | None = None) -> None:
    """Release the lock, but only if this process is still the recorded holder."""
    lock_path = Path(path) if path else LOCK_PATH
    try:
        if lock_path.is_file() and current_lock_holder(lock_path) == os.getpid():
            lock_path.unlink()
    except OSError:
        logger.warning("Failed to release scan lock at %s", lock_path)
