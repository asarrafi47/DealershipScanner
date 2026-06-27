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
    if not env.get("PUPPETEER_EXECUTABLE_PATH"):
        pw = _playwright_chromium_path()
        if pw:
            env["PUPPETEER_EXECUTABLE_PATH"] = pw
    for key, val in (payload.get("retry_env") or {}).items():
        if key:
            env[str(key)] = str(val)
    return env


def _run_dealer_scan(dealer_id: str, job_type: str, payload: dict) -> tuple[bool, str, dict]:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    pl = payload or {}
    if pl.get("use_python_scanner") or (job_type != "onboard"):
        cmd = [sys.executable, os.path.join(root, "scanner.py"), "--dealer-id", dealer_id, "--scan-only"]
    elif job_type == "onboard" and pl.get("url"):
        cmd = [
            "node",
            os.path.join(root, "backend", "scanner", "scanner.js"),
            "--url",
            str(pl["url"]),
            "--smart-import",
        ]
        profile = (pl.get("profile") or "").strip()
        if profile:
            cmd.extend(["--profile", profile])
        if dealer_id:
            cmd.extend(["--dealer-id", str(dealer_id)])
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
    _sync_dealer_sqlite_to_postgres(dealer_id)
    return True, "", result


def _sync_dealer_sqlite_to_postgres(dealer_id: str) -> None:
    """scanner.js writes SQLite; upsert this dealer's rows into Postgres when configured."""
    from backend.db.inventory_pg import is_inventory_postgres

    if not is_inventory_postgres() or not dealer_id:
        return
    db_path = (os.environ.get("INVENTORY_DB_PATH") or "").strip()
    if not db_path or not os.path.isfile(db_path):
        return
    try:
        import sqlite3

        from backend.db.inventory_pg import init_postgres_inventory, pg_connect

        sq = sqlite3.connect(db_path)
        sq.row_factory = sqlite3.Row
        try:
            sq_cols = [r[1] for r in sq.execute("PRAGMA table_info(cars)")]
            rows = sq.execute(
                f"SELECT {', '.join(sq_cols)} FROM cars WHERE dealer_id = ?",
                (dealer_id,),
            ).fetchall()
        finally:
            sq.close()
        if not rows:
            return
        pg = pg_connect()
        try:
            init_postgres_inventory(pg)
            cur = pg.cursor()
            pg_cols = {
                r[0]
                for r in cur.execute(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = 'cars'
                    """
                ).fetchall()
            }
            use_cols = [c for c in sq_cols if c in pg_cols and c != "id"]
            col_list = ", ".join(f'"{c}"' for c in use_cols)
            placeholders = ", ".join("%s" for _ in use_cols)
            sql = (
                f'INSERT INTO cars ({col_list}) VALUES ({placeholders}) '
                "ON CONFLICT (vin) DO NOTHING"
            )
            batch = [tuple(row[c] for c in use_cols) for row in rows]
            cur.executemany(sql, batch)
            pg.commit()
            _log.info("Synced %s vehicle(s) for %s to Postgres", len(batch), dealer_id)
        finally:
            pg.close()
    except Exception as ex:
        _log.warning("Postgres inventory sync failed for %s: %s", dealer_id, ex)


def main() -> int:
    from backend.utils.kmac_vault import load_kmac_vault_secrets

    load_kmac_vault_secrets()
    from backend.db.inventory_db import init_inventory_db
    from backend.db.inventory_pg import is_inventory_postgres
    from backend.scanner.job_queue import claim_next_job, finish_job, init_job_queue_schema

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

    while True:
        job = claim_next_job()
        if not job:
            time.sleep(POLL_SEC)
            continue
        _log.info("Claimed job id=%s dealer=%s type=%s", job["id"], job["dealer_id"], job["job_type"])
        ok, err, result = _run_dealer_scan(job["dealer_id"], job["job_type"], job.get("payload") or {})
        if ok:
            from backend.scanner.job_queue import record_dealer_scan_registry

            record_dealer_scan_registry(
                dealer_id=job["dealer_id"],
                job_type=job["job_type"],
                payload=job.get("payload") or {},
            )
        finish_job(job["id"], ok=ok, error=err or None, result=result)
        _log.info("Job id=%s finished ok=%s", job["id"], ok)


if __name__ == "__main__":
    raise SystemExit(main())
