"""P11A.4: the scanner lock is an flock, not a PID file."""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap
import time

from backend.scanner import scan_lock
from backend.scanner.constants import ROOT

_CHILD = textwrap.dedent(
    """
    import sys, time
    from backend.scanner import scan_lock
    ok = scan_lock.acquire_scan_lock(sys.argv[1])
    print("won" if ok else "lost", flush=True)
    if ok:
        sys.stdin.readline()   # hold until the parent says go
        scan_lock.release_scan_lock(sys.argv[1])
    """
)


def _spawn(path):
    return subprocess.Popen([sys.executable, "-c", _CHILD, str(path)], cwd=str(ROOT),
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)


def test_race_has_exactly_one_winner_per_round(tmp_path):
    lock = tmp_path / "race.lock"
    for _ in range(50):
        a, b = _spawn(lock), _spawn(lock)
        results = sorted([a.stdout.readline().strip(), b.stdout.readline().strip()])
        # the winner holds until released, so the loser always sees it held
        assert results == ["lost", "won"]
        for p in (a, b):
            try:  # the loser has already exited
                p.stdin.write("\n")
                p.stdin.flush()
            except BrokenPipeError:
                pass
            p.wait(timeout=20)
        assert not lock.exists()


def test_sigkill_holder_frees_lock_immediately(tmp_path):
    lock = tmp_path / "kill.lock"
    child = _spawn(lock)
    assert child.stdout.readline().strip() == "won"
    assert scan_lock.lock_holder_alive(lock) == child.pid
    assert scan_lock.acquire_scan_lock(lock) is False
    child.send_signal(signal.SIGKILL)
    child.wait()
    assert lock.exists()  # SIGKILL left the file (and its pid text) behind
    assert scan_lock.lock_holder_alive(lock) is None
    assert scan_lock.acquire_scan_lock(lock) is True
    assert scan_lock.current_lock_holder(lock) == os.getpid()
    scan_lock.release_scan_lock(lock)


def test_recycled_pid_in_stale_file_is_ignored(tmp_path):
    lock = tmp_path / "recycled.lock"
    lock.write_text(str(os.getppid()))  # a live pid, but nobody holds the flock
    assert scan_lock.lock_holder_alive(lock) is None
    assert scan_lock.acquire_scan_lock(lock) is True
    scan_lock.release_scan_lock(lock)


def test_release_removes_only_own_file(tmp_path):
    lock = tmp_path / "own.lock"
    scan_lock.release_scan_lock(lock)  # never held: no-op
    lock.write_text("1")
    scan_lock.release_scan_lock(lock)
    assert lock.exists()
    assert scan_lock.acquire_scan_lock(lock)
    assert scan_lock.lock_holder_alive(lock) == os.getpid()
    scan_lock.release_scan_lock(lock)
    assert not lock.exists()
    assert scan_lock.lock_holder_alive(lock) is None
