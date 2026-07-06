"""
Guard for schema-DDL scripts: refuse to alter shared tables while a scanner runs.

A live scanner process holds its INSERT/UPDATE SQL in memory; dropping a column
under it fails every subsequent upsert (2026-07-04: 11 dealers lost to a
mid-scan ``DROP COLUMN``). Call :func:`abort_if_scanner_running` before DDL.
"""
from __future__ import annotations

import subprocess
import sys


def scanner_pids() -> list[int]:
    """PIDs of running scanner entrypoints (scanner.py / scanner_mac_mini.py / backend.scanner.cli)."""
    try:
        out = subprocess.run(
            ["pgrep", "-f", "scanner.py|scanner_mac_mini.py|backend.scanner.cli"],
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout
    except (OSError, subprocess.TimeoutExpired):
        return []
    return [int(line) for line in out.split() if line.strip().isdigit()]


def abort_if_scanner_running(*, force: bool = False, action: str = "schema change") -> None:
    """Exit(3) when a scanner is alive, unless *force*."""
    pids = scanner_pids()
    if not pids:
        return
    msg = (
        f"REFUSING {action}: scanner process(es) alive (pid {', '.join(map(str, pids))}). "
        "A live scanner holds its SQL in memory — DDL now breaks every remaining upsert. "
        "Wait for the scan to finish or pass --force."
    )
    if force:
        print(f"WARNING: {msg} Proceeding due to --force.", file=sys.stderr)
        return
    print(msg, file=sys.stderr)
    sys.exit(3)
