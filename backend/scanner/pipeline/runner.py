"""Step 2: scanner lock, discovery capture and HTTP-only scan subprocesses, retry batches. Moved verbatim from backend/scripts/dealer_pipeline.py (audit F11)."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.scanner.pipeline.constants import HTTP_ONLY_ENV, LOG_ROOT, ROOT
from backend.scanner.pipeline.db import chromium_process_count, wait_for_db


# --------------------------------------------------------------------------
# 2. scan
# --------------------------------------------------------------------------

from backend.scanner.scan_lock import default_lock_path as _default_lock_path

# Per-shard lock (SCANNER_LOCK_PATH); the scanner subprocesses inherit the env,
# so this pipeline and the scanners it spawns agree on the file.
LOCK_FILE = _default_lock_path()


def _lock_holder_alive() -> int | None:
    """PID holding the scanner lock, or None when free / stale."""
    try:
        txt = LOCK_FILE.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return None
    import re

    m = re.search(r"\d+", txt)
    if not m:
        return None
    pid = int(m.group(0))
    try:
        os.kill(pid, 0)
    except OSError:
        return None
    return pid


def run_discovery_capture(dealer_id: str, timeout_sec: int = 900) -> dict[str, Any]:
    """Run ``discovery_probe --browser-capture`` for one dealer in a separate process
    (so SCANNER_ALLOW_BROWSER never enters this one). Bounded to one capture per
    dealer per UTC day via a marker file; returns a small summary for the triage.

    The scanner image (Dockerfile.scanner, Railway) carries no browser: there the
    capture is skipped without spending the day's marker, so a machine that has
    Playwright can still run it for this dealer today."""
    import importlib.util

    if importlib.util.find_spec("playwright") is None:
        return {"skipped": "no browser in this image (Dockerfile.discovery runs captures)", "recipes_after": 0}
    marker_dir = LOG_ROOT / dealer_id
    marker_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    marker = marker_dir / f".capture_{today}"
    if marker.exists():
        return {"skipped": "already captured today", "recipes_after": 0}
    marker.write_text(datetime.now(timezone.utc).isoformat())
    py = str(ROOT / ".venv" / "bin" / "python") if (ROOT / ".venv" / "bin" / "python").exists() else sys.executable
    env = dict(os.environ)
    env.pop("SCANNER_ALLOW_BROWSER", None)  # the probe sets it for itself
    env["DEALER_LOGS_ROOT"] = str(LOG_ROOT)  # the probe's discovery.md is this run's discovery.md
    t0 = time.time()
    try:
        proc = subprocess.run([py, "-m", "backend.scripts.discovery_probe", "--no-paths", "--browser-capture", "--dealers", dealer_id],
                              cwd=str(ROOT), env=env, capture_output=True, text=True, timeout=timeout_sec)
        tail = (proc.stdout or "").strip().splitlines()[-2:]
    except subprocess.TimeoutExpired:
        return {"error": f"capture timed out after {timeout_sec}s", "recipes_after": 0, "seconds": round(time.time() - t0)}
    out: dict[str, Any] = {"seconds": round(time.time() - t0), "rc": proc.returncode, "stdout_tail": tail, "recipes_after": 0}
    caps = sorted(marker_dir.glob("capture_*.json"))
    if caps:
        try:
            cap = json.loads(caps[-1].read_text())
            out.update({k: cap.get(k) for k in ("records", "recipes_before", "recipes_after", "profile", "errors", "validation") if k in cap})
            out["endpoints"] = len(cap.get("endpoints") or [])
        except Exception as exc:  # noqa: BLE001
            out["error"] = f"capture report unreadable: {str(exc)[:80]}"
    print(f"discover {dealer_id:36s} {json.dumps(out)[:220]}", flush=True)
    return out


def wait_for_scanner_lock(max_wait_sec: int, poll_sec: int = 20) -> bool:
    """The scanner refuses to start while another run holds workspace/scanner.lock
    (one process per machine). Wait for it instead of failing the batch."""
    waited = 0
    while waited < max_wait_sec:
        pid = _lock_holder_alive()
        if pid is None:
            return True
        if waited == 0:
            print(f"scan    waiting for scanner lock held by pid {pid}", flush=True)
        time.sleep(poll_sec)
        waited += poll_sec
    return _lock_holder_alive() is None


def run_http_only_scan(dealer_ids: list[str], *, concurrency: int, log_path: Path, timeout_sec: int) -> int:
    env = dict(os.environ)
    env.update(HTTP_ONLY_ENV)
    # scanner.py rosters from dealers.json by default and exits 1 on an id it
    # does not hold ("No dealer(s) matching …"): 69 of the 72 not-in-manifest
    # dealers failed that way on 2026-09-26 AFTER their recipes were synthesized.
    # Roster from active inventory ∪ stored recipes instead — every dealer this
    # pipeline can scan is in one of those.
    env.setdefault("DEALERS_FROM_SCANNABLE", "1")
    py = str(ROOT / ".venv" / "bin" / "python") if (ROOT / ".venv" / "bin" / "python").exists() else sys.executable
    cmd = [py, "scanner.py", "--dealer-id", ",".join(dealer_ids), "--dealer-concurrency", str(concurrency), "--scan-only"]
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("ab") as fh:
        try:
            proc = subprocess.run(cmd, cwd=str(ROOT), env=env, stdout=fh, stderr=subprocess.STDOUT, timeout=timeout_sec)
            return proc.returncode
        except subprocess.TimeoutExpired:
            return -9


def scan_retry_batch(dealer_ids: list[str], *, out_dir: Path, batch: int, scan_timeout: int, lock_wait: int) -> dict[str, Any]:
    """Step 3's scan: the retried dealers in one final HTTP-only pass (chunked by
    --batch like the main run). Returns {"rc": [...], "chromium_leaks": [...]}."""
    rcs: list[int] = []
    leaks: list[dict[str, Any]] = []
    for i in range(0, len(dealer_ids), max(1, batch)):
        chunk = dealer_ids[i:i + max(1, batch)]
        if not wait_for_db(lock_wait):
            print("retry   database unreachable; retry batch abandoned", flush=True)
            rcs.append(-1)
            break
        if not wait_for_scanner_lock(lock_wait):
            print("retry   scanner lock still held; retry batch abandoned", flush=True)
            rcs.append(-1)
            break
        before = chromium_process_count()
        t0 = time.time()
        rc = run_http_only_scan(chunk, concurrency=len(chunk), log_path=out_dir / "scanner.log", timeout_sec=scan_timeout)
        after = chromium_process_count()
        if after > max(before, 0):
            print(f"retry   BROWSER LEAK: chromium processes {before} -> {after} during retry {', '.join(chunk)[:80]}", flush=True)
            leaks.append({"batch": chunk, "before": before, "after": after, "retry": True})
        print(f"retry   batch: {', '.join(chunk)[:120]} rc={rc} in {time.time() - t0:.0f}s", flush=True)
        rcs.append(rc)
    return {"rc": rcs, "chromium_leaks": leaks}
