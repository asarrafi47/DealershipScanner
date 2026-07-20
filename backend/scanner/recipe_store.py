"""
Postgres persistence for endpoint-replay recipes (``dealer_recipes`` table).

The recipe *files* under ``workspace/recipes/`` stay the local hot cache —
this store exists for portability and durability: the HTTP shortcut for a
dealer follows the dealer to every machine (Railway workers, CI, a second
laptop, a fresh checkout) instead of living only on whichever box ran the
discovery scan. One row per dealer mirroring the file exactly, plus derived
health columns for the scanner-ops views.

Sync model (see ``backend.scanner.recipes``):
  - ``save_recipes`` writes the file, then write-through here;
  - ``load_recipes`` compares file vs DB freshness (max ``saved_at``): a newer
    DB row materializes the local file (new machine); a newer file lazily
    pushes to the DB (covers scans that wrote files with pre-DB code).

Every DB touch is best-effort: a scan must never fail because Postgres was
unreachable, so errors log and fall back to file behavior. Set
``RECIPES_DB_DISABLED=1`` to opt out entirely.
"""
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("scanner")

_table_lock = threading.Lock()

_DDL_PG = """
CREATE TABLE IF NOT EXISTS dealer_recipes (
    dealer_id     TEXT PRIMARY KEY,
    recipes_json  TEXT NOT NULL,
    recipe_count  INTEGER NOT NULL DEFAULT 0,
    provider_hint TEXT,
    max_saved_at  DOUBLE PRECISION NOT NULL DEFAULT 0,
    last_ok_at    DOUBLE PRECISION NOT NULL DEFAULT 0,
    stale_count   INTEGER NOT NULL DEFAULT 0,
    updated_at    TEXT,
    scan_hints    TEXT
)
"""

# Per-dealer scan instructions (scan_hints JSON) — the containment mechanism:
# platform handlers keep their proven default behavior for every dealer that
# works; a dealer that needs different navigation carries its own override so
# fixing one dealer can never regress another. Recognized keys:
#   needs_http_proxy    bool   — inventory params are HTTP-walled; set
#                                SCANNER_HTTP_PROXY for browser-free capture
#   price_source        str    — "list" (default) | "vdp" | "second_endpoint":
#                                where prices actually live on this dealer
#   requires_browser    bool   — no HTTP fingerprint; go straight to Playwright
#   skip_reason         str    — why this dealer is out of scope (e.g. not a
#                                car dealer) — scanner may skip entirely
#   notes               str    — freeform operator note shown in ops views
#   hint_source         str    — who wrote the hint (scan run / manual / ops)
SCAN_HINT_KEYS = (
    "needs_http_proxy", "price_source", "requires_browser",
    "skip_reason", "notes", "hint_source",
)

_DDL_SQLITE = _DDL_PG.replace("DOUBLE PRECISION", "REAL")

_table_ready = False


def _enabled() -> bool:
    return (os.environ.get("RECIPES_DB_DISABLED") or "").strip().lower() not in (
        "1", "true", "yes", "on",
    )


def _conn():
    from backend.db.inventory_db import get_conn

    return get_conn()


def _ensure_table(conn) -> None:
    global _table_ready
    if _table_ready:
        return
    # Serialize first-use DDL: concurrent CREATEs from parallel scan threads
    # would abort each other's transactions on Postgres.
    with _table_lock:
        if _table_ready:
            return
        cur = conn.cursor()
        try:
            cur.execute(_DDL_PG)
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            cur.execute(_DDL_SQLITE)
        try:
            cur.execute("ALTER TABLE dealer_recipes ADD COLUMN IF NOT EXISTS scan_hints TEXT")
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                cur.execute("SELECT scan_hints FROM dealer_recipes LIMIT 1")
            except Exception:
                cur.execute("ALTER TABLE dealer_recipes ADD COLUMN scan_hints TEXT")
        conn.commit()
        _table_ready = True


def _derive_meta(rows: list[dict[str, Any]]) -> dict[str, Any]:
    saved = [float(r.get("saved_at") or 0) for r in rows]
    ok = [float(r.get("last_ok_at") or 0) for r in rows]
    hints = [str(r.get("provider_hint") or "").strip() for r in rows]
    return {
        "recipe_count": len(rows),
        "provider_hint": next((h for h in hints if h), None),
        "max_saved_at": max(saved) if saved else 0.0,
        "last_ok_at": max(ok) if ok else 0.0,
        "stale_count": sum(1 for r in rows if r.get("stale")),
    }


def db_save_recipes(dealer_id: str, rows: list[dict[str, Any]]) -> bool:
    """Upsert the dealer's full recipe list. Best-effort; returns success."""
    if not _enabled() or not dealer_id:
        return False
    try:
        conn = _conn()
        try:
            _ensure_table(conn)
            cur = conn.cursor()
            meta = _derive_meta(rows)
            now = datetime.now(timezone.utc).isoformat()
            payload = json.dumps(rows, ensure_ascii=False)
            cur.execute("SELECT 1 FROM dealer_recipes WHERE dealer_id = ?", (dealer_id,))
            if cur.fetchone():
                cur.execute(
                    "UPDATE dealer_recipes SET recipes_json=?, recipe_count=?, provider_hint=?, "
                    "max_saved_at=?, last_ok_at=?, stale_count=?, updated_at=? WHERE dealer_id=?",
                    (payload, meta["recipe_count"], meta["provider_hint"], meta["max_saved_at"],
                     meta["last_ok_at"], meta["stale_count"], now, dealer_id),
                )
            else:
                cur.execute(
                    "INSERT INTO dealer_recipes (dealer_id, recipes_json, recipe_count, provider_hint, "
                    "max_saved_at, last_ok_at, stale_count, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (dealer_id, payload, meta["recipe_count"], meta["provider_hint"],
                     meta["max_saved_at"], meta["last_ok_at"], meta["stale_count"], now),
                )
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 — never let DB issues break a scan
        logger.debug("dealer_recipes save skipped for %s: %s", dealer_id, exc)
        return False


def db_load_recipes(dealer_id: str) -> tuple[list[dict[str, Any]], float] | None:
    """(recipe rows, max_saved_at) from the DB, or None. Best-effort."""
    if not _enabled() or not dealer_id:
        return None
    try:
        conn = _conn()
        try:
            _ensure_table(conn)
            cur = conn.cursor()
            cur.execute(
                "SELECT recipes_json, max_saved_at FROM dealer_recipes WHERE dealer_id = ?",
                (dealer_id,),
            )
            row = cur.fetchone()
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        logger.debug("dealer_recipes load skipped for %s: %s", dealer_id, exc)
        return None
    if not row or not row[0]:
        return None
    try:
        rows = json.loads(row[0])
    except (TypeError, ValueError):
        return None
    if not isinstance(rows, list):
        return None
    try:
        max_saved = float(row[1] or 0.0)
    except (TypeError, ValueError):
        max_saved = 0.0
    return rows, max_saved


def get_scan_hints(dealer_id: str) -> dict[str, Any]:
    """Per-dealer scan instructions ({} when none / DB unavailable)."""
    if not _enabled() or not dealer_id:
        return {}
    try:
        conn = _conn()
        try:
            _ensure_table(conn)
            cur = conn.cursor()
            cur.execute("SELECT scan_hints FROM dealer_recipes WHERE dealer_id = ?", (dealer_id,))
            row = cur.fetchone()
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        logger.debug("scan_hints load skipped for %s: %s", dealer_id, exc)
        return {}
    if not row or not row[0]:
        return {}
    try:
        hints = json.loads(row[0])
        return hints if isinstance(hints, dict) else {}
    except (TypeError, ValueError):
        return {}


def set_scan_hints(dealer_id: str, hints: dict[str, Any], *, merge: bool = True) -> bool:
    """
    Write per-dealer scan instructions. ``merge=True`` (default) overlays onto
    existing hints; a key set to None removes it. Creates the dealer row (with
    an empty recipe list) when the dealer has no recipes yet — hints often
    exist precisely BECAUSE recipe capture failed.
    """
    if not _enabled() or not dealer_id:
        return False
    try:
        conn = _conn()
        try:
            _ensure_table(conn)
            cur = conn.cursor()
            # Read on the SAME connection the write happens on — a separate
            # get_scan_hints() connection widens the read-modify-write window
            # for concurrent writers to drop each other's keys.
            base: dict[str, Any] = {}
            if merge:
                cur.execute("SELECT scan_hints FROM dealer_recipes WHERE dealer_id = ?", (dealer_id,))
                row0 = cur.fetchone()
                if row0 and row0[0]:
                    try:
                        parsed0 = json.loads(row0[0])
                        if isinstance(parsed0, dict):
                            base = parsed0
                    except (TypeError, ValueError):
                        base = {}
            for k, v in hints.items():
                if v is None:
                    base.pop(k, None)
                else:
                    base[k] = v
            payload = json.dumps(base, ensure_ascii=False)
            now = datetime.now(timezone.utc).isoformat()
            cur.execute("SELECT 1 FROM dealer_recipes WHERE dealer_id = ?", (dealer_id,))
            if cur.fetchone():
                cur.execute(
                    "UPDATE dealer_recipes SET scan_hints=?, updated_at=? WHERE dealer_id=?",
                    (payload, now, dealer_id),
                )
            else:
                cur.execute(
                    "INSERT INTO dealer_recipes (dealer_id, recipes_json, recipe_count, updated_at, scan_hints) "
                    "VALUES (?, '[]', 0, ?, ?)",
                    (dealer_id, now, payload),
                )
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        logger.debug("scan_hints save skipped for %s: %s", dealer_id, exc)
        return False
