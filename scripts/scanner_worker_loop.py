#!/usr/bin/env python3
"""Scanner worker: claim dealer_jobs and run per-dealer scrape."""
from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_log = logging.getLogger("scanner-worker")

POLL_SEC = float(os.environ.get("SCANNER_WORKER_POLL_SEC", "10"))


def _playwright_chromium_path() -> str | None:
    import glob

    home = os.path.expanduser("~")
    pattern = os.path.join(home, ".cache", "ms-playwright", "chromium-*", "chrome-linux", "chrome")
    matches = sorted(glob.glob(pattern))
    return matches[-1] if matches else None


def _subprocess_env(payload: dict[str, Any]) -> dict[str, str]:
    env = {k: str(v) for k, v in os.environ.items()}
    # Scans are HTTP-only (docs/HTTP_ONLY_SCANS_PLAN.md); the discovery capture
    # sets SCANNER_ALLOW_BROWSER for itself, so it never leaks into a scan job.
    env.pop("SCANNER_ALLOW_BROWSER", None)
    for key, val in (payload.get("retry_env") or {}).items():
        if key:
            env[str(key)] = str(val)
    return env


def _run_dealer_scan(dealer_id: str, job_type: str, payload: dict) -> tuple[bool, str, dict]:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    pl = payload or {}
    if job_type == "onboard" and dealer_id:
        # Onboarding = discovery: HTTP probe + template synthesis, and the one
        # sanctioned browser capture when no template describes the site. It
        # writes recipes and discovery logs, never car rows; the follow-up scan
        # job replays the recipe over HTTP. (Replaced the Puppeteer scanner.js
        # --smart-import path on 2026-09-26.)
        cmd = [sys.executable, "-m", "backend.scripts.discovery_probe", "--no-paths", "--browser-capture", "--dealers", str(dealer_id)]
    else:
        cmd = [sys.executable, os.path.join(root, "scanner.py"), "--dealer-id", dealer_id, "--scan-only"]
    _log.info("Running: %s", " ".join(cmd))
    env = _subprocess_env(pl)
    try:
        proc = subprocess.run(
            cmd,
            cwd=root,
            capture_output=True,
            text=True,
            timeout=int(os.environ.get("SCANNER_JOB_TIMEOUT_SEC", "3600")),
            env=env,
        )
    except subprocess.TimeoutExpired:
        return False, "job_timeout", {}
    except FileNotFoundError as ex:
        return False, str(ex), {}
    tail = (proc.stdout or "")[-1500:] + (proc.stderr or "")[-500:]
    from backend.scanner.scrape_confidence import merge_result_with_scanner_stdout, soft_scrape_failure

    if proc.returncode != 0:
        result = merge_result_with_scanner_stdout({"log_tail": tail}, proc.stdout or "")
        return False, f"exit_{proc.returncode}", result
    result = merge_result_with_scanner_stdout({"log_tail": tail[-800:]}, proc.stdout or "")
    if soft_scrape_failure(job_type, result):
        return False, "scrape_empty", result
    return True, "", result


def main() -> int:
    from backend.utils.kmac_vault import load_kmac_vault_secrets

    load_kmac_vault_secrets()
    from backend.db.inventory_db import init_inventory_db
    from backend.db.inventory_pg import is_inventory_postgres
    from backend.scanner.job_queue import (
        claim_next_job,
        finish_job,
        init_job_queue_schema,
        reap_stale_running_jobs,
    )

    if not is_inventory_postgres():
        _log.error("INVENTORY_DATABASE_URL must point at Postgres for scanner-worker.")
        return 1

    init_inventory_db()
    init_job_queue_schema()
    wid = os.environ.get("SCANNER_WORKER_ID", "worker")
    pw = _playwright_chromium_path()
    if pw:
        _log.info("Using Playwright Chromium at %s", pw)
    _log.info("Scanner worker %s started (poll=%ss)", wid, POLL_SEC)
    # A previous incarnation of this replica may have died mid-job (redeploy,
    # OOM); its row is still `running` and would otherwise never be retried.
    try:
        reaped = reap_stale_running_jobs()
        if reaped:
            _log.warning("Reaped %d stale running job(s) left by a lost worker", reaped)
    except Exception:
        _log.exception("Stale-job reap failed; continuing")

    while True:
        job = claim_next_job()
        if not job:
            time.sleep(POLL_SEC)
            continue
        _log.info("Claimed job id=%s dealer=%s type=%s", job["id"], job["dealer_id"], job["job_type"])
        ok, err, result = _run_dealer_scan(job["dealer_id"], job["job_type"], job.get("payload") or {})
        if ok:
            from backend.scanner.job_queue import record_catalog_after_success

            record_catalog_after_success(
                dealer_id=job["dealer_id"],
                job_type=job["job_type"],
                payload=job.get("payload") or {},
            )
        finish_job(job["id"], ok=ok, error=err or None, result=result)
        _log.info("Job id=%s finished ok=%s", job["id"], ok)


if __name__ == "__main__":
    raise SystemExit(main())
