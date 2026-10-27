"""
Scanner run lock so two full scanner runs never overlap on one host.

The lock is an ``fcntl.flock`` on a descriptor kept open for the life of the
process; the kernel drops it when the process dies (SIGKILL included), so a
dead holder never wedges the next run and a recycled PID cannot fake a live
one. The file's PID text is diagnostic only (messages, ``current_lock_holder``).
A lock refused because a live process holds it raises ``ScanLockHeld`` in the
orchestrator, and the CLI turns that into exit code ``EXIT_LOCK_HELD`` (75).
"""
from __future__ import annotations

import fcntl
import logging
import os
from pathlib import Path

from backend.scanner.constants import ROOT

logger = logging.getLogger("scanner")

# EX_TEMPFAIL: "try again later". No other script in this repo exits 75.
EXIT_LOCK_HELD = 75


class ScanLockHeld(RuntimeError):
    """Another live process holds the scanner lock."""

    def __init__(self, holder: int | None, path: Path):
        self.holder = holder
        self.path = Path(path)
        super().__init__(f"scanner lock {self.path} held by pid {holder}")


def default_lock_path() -> Path:
    """One lock per *shard*: ``SCANNER_LOCK_PATH`` names this process's lock file.

    Unset, it is ``workspace/scanner.lock`` — one scanner per machine, the
    historical rule. Since 2026-09-28 the fleet runs several pipeline shards on
    one host (the scanner is CPU-bound in one interpreter, batch size past ~8
    buys nothing); each shard scans a disjoint roster under its own lock file.
    """
    raw = (os.environ.get("SCANNER_LOCK_PATH") or "").strip()
    if raw:
        p = Path(raw)
        return p if p.is_absolute() else ROOT / p
    return ROOT / "workspace" / "scanner.lock"


def __getattr__(name: str):
    # ``LOCK_PATH`` stays importable but is resolved at access time, so a
    # SCANNER_LOCK_PATH change after import takes effect without a reload.
    if name == "LOCK_PATH":
        return default_lock_path()
    raise AttributeError(name)


# path -> open descriptor for the locks this process holds. The descriptor
# stays open for the process lifetime; closing it would drop the flock.
_held: dict[Path, int] = {}


def _resolve(path: Path | None) -> Path:
    return Path(path) if path else default_lock_path()


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
    """PID recorded in the lock file (diagnostic), or None if absent/unreadable."""
    try:
        return int(_resolve(path).read_text().strip())
    except (ValueError, OSError):
        return None


def lock_holder_alive(path: Path | None = None) -> int | None:
    """PID of the live holder, or None when the lock is free.

    Liveness is the flock itself: probe it (try, then release) and read the
    PID only for the message. A holder whose pid we may not signal is still a
    holder. ``0`` stands for "held, pid unreadable".
    """
    lock_path = _resolve(path)
    if lock_path in _held:
        return os.getpid()
    try:
        fd = os.open(lock_path, os.O_RDWR)
    except OSError:
        return None
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return current_lock_holder(lock_path) or 0
        fcntl.flock(fd, fcntl.LOCK_UN)
        return None
    finally:
        os.close(fd)


def acquire_scan_lock(path: Path | None = None) -> bool:
    """
    Claim the scanner run lock for this process.

    Returns True once acquired (a dead holder's lock is simply free). Returns
    False when another *live* process holds it — caller should refuse to
    start a second concurrent scan. Taking a lock this process already holds
    succeeds.
    """
    lock_path = _resolve(path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if lock_path in _held:
        return True
    while True:
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            return False
        # The previous holder unlinks on release: if the name now points at a
        # different file (or none) we locked an orphan inode — retry on the new one.
        try:
            same = os.fstat(fd).st_ino == os.stat(lock_path).st_ino
        except OSError:
            same = False
        if not same:
            os.close(fd)
            continue
        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode())
        _held[lock_path] = fd
        return True


def release_scan_lock(path: Path | None = None) -> None:
    """Release the lock, removing the file only if this process holds it."""
    lock_path = _resolve(path)
    fd = _held.pop(lock_path, None)
    if fd is None:
        return
    try:
        lock_path.unlink()  # while still locked, so no one else's file is removed
    except OSError:
        logger.warning("Failed to release scan lock at %s", lock_path)
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
