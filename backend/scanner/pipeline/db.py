"""Process / DB plumbing of the dealer pipeline. Moved verbatim from backend/scripts/dealer_pipeline.py (audit F11)."""
from __future__ import annotations

import subprocess
import time
from typing import Any

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

from backend.db.inventory_db import get_conn  # noqa: E402


def chromium_process_count() -> int:
    """Headless Chromium processes alive on this machine (the fleet's browser-free
    contract; deploy/nightly_http_refresh.sh keeps the same counter)."""
    try:
        out = subprocess.run(["pgrep", "-f", "ms-playwright|headless_shell|chrome-headless"], capture_output=True, text=True, timeout=10)
        return len([ln for ln in out.stdout.splitlines() if ln.strip()])
    except Exception:  # noqa: BLE001
        return -1


def wait_for_db(max_wait: int, poll: int = 30) -> bool:
    """Block until the inventory database answers, up to *max_wait* seconds.

    The mini reaches the MBP's Postgres through an SSH reverse tunnel; when it
    dropped mid-fleet on 2026-09-27 the pipeline burned through 55 batches in
    seconds (each scanner exited 1 on connect) and both shards reported DONE.
    Waiting keeps the roster intact for when the tunnel comes back."""
    deadline = time.time() + max(0, max_wait)
    warned = False
    while True:
        try:
            conn = get_conn()
            try:
                conn.execute("SELECT 1").fetchone()
            finally:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass
            if warned:
                print("scan    database reachable again", flush=True)
            return True
        except Exception as exc:  # noqa: BLE001
            if time.time() >= deadline:
                return False
            if not warned:
                print(f"scan    database unreachable ({str(exc).splitlines()[0][:120]}); waiting", flush=True)
                warned = True
            time.sleep(poll)



def _assess_conn():
    """Connection for the assess/reconcile loops, in autocommit.

    Those loops only read, plus single-statement UPDATEs in reconcile_dealer, but
    between queries they do slow non-DB work (catalog load in verify_accuracy,
    log writes). With autocommit off every SELECT opened a transaction that sat
    idle through that work; production's idle_in_transaction_session_timeout
    (5 min) killed three shards' connections on the 2026-09-28 Railway fleet run
    ("the connection is closed", 223 dealers never assessed)."""
    conn = get_conn()
    raw = getattr(conn, "_raw", None)
    if raw is not None and hasattr(raw, "autocommit"):
        try:
            raw.autocommit = True
        except Exception:  # noqa: BLE001 - sqlite / already in a transaction
            pass
    return conn


def _conn_alive(conn) -> bool:
    raw = getattr(conn, "_raw", conn)
    closed = getattr(raw, "closed", False)
    return not closed

def _rows(conn, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]
