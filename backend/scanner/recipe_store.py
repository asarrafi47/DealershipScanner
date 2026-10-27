"""
Postgres persistence for endpoint-replay recipes (``dealer_recipes`` table).

The recipe *files* under ``workspace/recipes/`` stay the local hot cache —
this store exists for portability and durability: the HTTP shortcut for a
dealer follows the dealer to every machine (Railway workers, CI, a second
laptop, a fresh checkout) instead of living only on whichever box ran the
discovery scan. One row per dealer mirroring the file exactly, plus derived
health columns for the scanner-ops views.

Sync model (see ``backend.scanner.recipes``):
  - ``saved_at`` on a recipe row means "last write of this dealer's set".
    ``save_recipes`` stamps every row with one ``time.time()`` per call, before
    it writes the file and then writes through here, so the file and the DB row
    carry the same stamp and ``max_saved_at`` is that stamp. It stays a
    per-write stamp in later phases: hosts running older code still compare it.
    This module stores rows as given and never stamps them.
  - ``load_recipes`` compares file vs DB freshness (max ``saved_at``): a newer
    DB row materializes the local file (another host wrote the set since); a
    newer file is pushed up as it is (a write-through that failed, or a file
    written by pre-DB code), with a WARNING naming the dealer and the DB copy it
    replaced. Neither direction re-stamps.
  - ``updated_at`` is not a recipe-write stamp: every ``set_scan_hints`` bumps it.

Writes are compare-and-set (V034, N13 P7.1): ``db_mutate`` / ``mutate_scan_hints`` read
the row, compute the change, and write ``WHERE rev = <rev read>``, retrying when another
host got in between. ``db_save_recipes`` and ``set_scan_hints`` are wrappers on them.
``recipes_written_at`` is stamped by recipe writes only (a hint write bumps ``rev``).

Every DB touch is best-effort: a scan must never fail because Postgres was
unreachable, so errors log and fall back to file behavior. A failed
write-through (``db_save_recipes``) logs at WARNING the first time each error
class is seen in a process, then at DEBUG. Set ``RECIPES_DB_DISABLED=1`` to opt
out entirely.

One read is not best-effort: ``db_load_recipes(..., strict=True)`` raises
``RecipeStoreReadError`` when it cannot tell whether the store holds a copy (the
read raised, or the row's ``recipes_json`` is not a JSON list), where the
default read returns ``None`` for both that and a missing row. A recipe cache
tagged for another store (``backend.scanner.recipes.check_cache_store``) uses
it: there the store is the only copy a save makes, so a save must never act on
an absence the read did not prove.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

logger = logging.getLogger("scanner")

_table_lock = threading.Lock()

# Error classes whose write-through failure was already logged at WARNING in
# this process (rate limit: one WARNING per class, the rest at DEBUG).
_save_failure_warned: set[str] = set()
_save_failure_lock = threading.Lock()


def _log_save_failure(dealer_id: str, exc: BaseException, *, file_written: bool = True) -> None:
    """Log a failed ``dealer_recipes`` write.

    Normally the recipe file was written but the shared store was not, so other
    hosts will not see this save. With ``file_written=False`` (a save on a recipe
    cache tagged for another store, which writes the store only) nothing was
    written and the save is lost. Either is worth a WARNING, but a dead DB fails
    every save of a fleet run, so only the first failure of each error class per
    process logs at WARNING; repeats log at DEBUG.
    """
    cls = type(exc)
    key = f"{cls.__module__}.{cls.__qualname__}"
    with _save_failure_lock:
        first = key not in _save_failure_warned
        _save_failure_warned.add(key)
    if first:
        if file_written:
            write, what = "write-through", "the recipe file was written but the shared store was not"
        else:
            write, what = "write", ("the recipe cache is tagged for another store, so no recipe "
                                    "file was written either and this save was lost")
        logger.warning(
            "dealer_recipes %s failed for %s (%s: %s); %s. Further %s failures in "
            "this process log at DEBUG.",
            write, dealer_id, cls.__name__, exc, what, cls.__name__,
        )
    else:
        logger.debug("dealer_recipes save skipped for %s: %s", dealer_id, exc)


class RecipeStoreReadError(Exception):
    """A strict ``db_load_recipes`` could not tell whether the store holds a copy:
    the read raised, or the stored ``recipes_json`` is not a JSON list."""


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
# True when dealer_recipes has the V034 columns (rev, recipes_written_at,
# superseded_json); decided once per process by _ensure_table. False only on a
# database behind the chain, where writes fall back to the unversioned UPDATE.
_cas_columns = True

# Counters behind store_stats(): CAS writes, lost races that were retried, writes that
# gave up after the retries, and writes that raised.
_stats_lock = threading.Lock()
_stats: dict[str, int] = {"writes": 0, "conflicts": 0, "exhausted": 0, "failures": 0}


def _enabled() -> bool:
    return (os.environ.get("RECIPES_DB_DISABLED") or "").strip().lower() not in (
        "1", "true", "yes", "on",
    )


def _conn():
    from backend.db.inventory_db import get_conn

    return get_conn()


def _ensure_table(conn) -> None:
    """Make sure ``dealer_recipes`` (with ``scan_hints``) exists; once per process.

    Postgres (P4.6): ``migrations/`` owns the table (V001, ``scan_hints``
    included). At the expected migration version this issues no statement. Behind
    the chain (warn mode) the table is created only when ``to_regclass`` cannot find
    it, after ``SET LOCAL lock_timeout`` (``backend.db.runtime_ddl``), and the
    ``scan_hints`` column is checked in ``information_schema`` but never added here:
    ``ALTER TABLE ... ADD COLUMN`` takes ACCESS EXCLUSIVE on ``dealer_recipes`` even
    with IF NOT EXISTS, and a missing column is a migration's job. Strict mode on a
    behind database raises ``SchemaNotMigratedError`` (every caller is best-effort).

    SQLite (pytest): unchanged, create the table and add ``scan_hints`` when a
    probe shows it missing.
    """
    global _table_ready
    if _table_ready:
        return
    # Serialize first use: concurrent CREATEs from parallel scan threads would
    # abort each other's transactions on Postgres.
    with _table_lock:
        if _table_ready:
            return
        from backend.db import runtime_ddl

        if runtime_ddl.is_sqlite_connection(conn):
            _ensure_table_sqlite(conn)
        else:
            _ensure_table_pg(conn)
        _table_ready = True


def _ensure_table_sqlite(conn) -> None:
    cur = conn.cursor()
    cur.execute(_DDL_SQLITE)
    try:
        cur.execute("SELECT scan_hints FROM dealer_recipes LIMIT 1")
    except Exception:
        cur.execute("ALTER TABLE dealer_recipes ADD COLUMN scan_hints TEXT")
    for col, ddl in (("rev", "INTEGER NOT NULL DEFAULT 0"), ("recipes_written_at", "REAL NOT NULL DEFAULT 0"),
                     ("superseded_json", "TEXT NOT NULL DEFAULT '[]'")):
        try:
            cur.execute(f"SELECT {col} FROM dealer_recipes LIMIT 1")
        except Exception:
            cur.execute(f"ALTER TABLE dealer_recipes ADD COLUMN {col} {ddl}")
    conn.commit()


def _ensure_table_pg(conn) -> None:
    from backend.db import runtime_ddl

    global _cas_columns
    _cas_columns = True
    if not runtime_ddl.pg_legacy_ddl_needed():
        return
    cur = conn.cursor()
    try:
        if runtime_ddl.pg_missing_relations(cur, ["dealer_recipes"]):
            runtime_ddl.run_pg_legacy_ddl(cur, [_DDL_PG])
        if not runtime_ddl.pg_column_exists(cur, "dealer_recipes", "rev"):
            _cas_columns = False
            logger.warning(
                "dealer_recipes has no rev column (V034); recipe writes are not compare-and-set "
                "until the database is migrated (python -m backend.scripts.migrate --apply)."
            )
        if not runtime_ddl.pg_column_exists(cur, "dealer_recipes", "scan_hints"):
            logger.warning(
                "dealer_recipes has no scan_hints column; scan hints will not load or "
                "save until the database is migrated (python -m backend.scripts.migrate "
                "--apply). Not adding it at runtime: ALTER TABLE takes ACCESS EXCLUSIVE."
            )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


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


@dataclass(frozen=True)
class StoreRow:
    """One ``dealer_recipes`` row as ``db_read`` / ``db_mutate`` see it.

    ``rev`` is the compare-and-set counter (0 on a row written before V034, or on a
    database behind the chain). A row is authoritative for its recipes only when
    ``recipes_written_at`` is set (``authoritative``): a hints-only row holds ``[]``.
    """
    rows: list[dict[str, Any]]
    rev: int
    hints: dict[str, Any]
    updated_at: str | None
    max_saved_at: float
    recipes_written_at: float

    @property
    def authoritative(self) -> bool:
        return self.recipes_written_at > 0


def store_stats() -> dict[str, int]:
    """Copy of the CAS counters: ``writes``, ``conflicts`` (lost races, retried),
    ``exhausted`` (gave up after the retries) and ``failures`` (raised)."""
    with _stats_lock:
        return dict(_stats)


def _bump(name: str) -> None:
    with _stats_lock:
        _stats[name] += 1


def _parse_json(text: Any, kind: type) -> Any:
    """``text`` parsed when it is JSON of ``kind`` (list/dict), else None."""
    if not text:
        return None
    try:
        out = json.loads(text)
    except (TypeError, ValueError):
        return None
    return out if isinstance(out, kind) else None


def _snapshot(cur, dealer_id: str) -> StoreRow | None:
    """The row as stored (unreadable JSON reads as empty), or None when absent."""
    cols = "recipes_json, max_saved_at, scan_hints, updated_at"
    if _cas_columns:
        cols += ", rev, recipes_written_at"
    cur.execute(f"SELECT {cols} FROM dealer_recipes WHERE dealer_id = ?", (dealer_id,))
    row = cur.fetchone()
    if not row:
        return None
    return StoreRow(
        rows=_parse_json(row[0], list) or [],
        rev=int(row[4] or 0) if _cas_columns else 0,
        hints=_parse_json(row[2], dict) or {},
        updated_at=row[3],
        max_saved_at=float(row[1] or 0.0),
        recipes_written_at=float(row[5] or 0.0) if _cas_columns else 0.0,
    )


def _cas_write(cur, dealer_id: str, current: StoreRow | None, cols: dict[str, Any]) -> bool:
    """One compare-and-set write of ``cols`` (column -> value) against ``current``.

    No row: INSERT ... ON CONFLICT DO NOTHING, which counts as written only when
    it inserted (rowcount 1). A row: UPDATE ... SET rev = rev + 1 WHERE dealer_id = ?
    AND rev = <rev read>, written only when it changed exactly one row. A rowcount
    other than 1 is a lost race, never a success. A database behind the chain has no
    ``rev``: the UPDATE is then keyed on ``dealer_id`` alone.
    """
    names = list(cols)
    vals = [cols[n] for n in names]
    if current is None:
        extra, extra_v = (["rev"], [1]) if _cas_columns else ([], [])
        allc = names + extra
        cur.execute(
            f"INSERT INTO dealer_recipes (dealer_id, {', '.join(allc)}) "
            f"VALUES ({', '.join('?' * (len(allc) + 1))}) ON CONFLICT (dealer_id) DO NOTHING",
            (dealer_id, *vals, *extra_v),
        )
        return cur.rowcount == 1
    sets = ", ".join(f"{n}=?" for n in names)
    if _cas_columns:
        cur.execute(
            f"UPDATE dealer_recipes SET {sets}, rev=rev+1 WHERE dealer_id=? AND rev=?",
            (*vals, dealer_id, current.rev),
        )
    else:
        cur.execute(f"UPDATE dealer_recipes SET {sets} WHERE dealer_id=?", (*vals, dealer_id))
    return cur.rowcount == 1


def _written(current: StoreRow | None, cols: dict[str, Any]) -> StoreRow:
    """The row as ``_cas_write`` left it, built from what was read and what was written."""
    return StoreRow(
        rows=json.loads(cols["recipes_json"]) if "recipes_json" in cols else (current.rows if current else []),
        rev=(current.rev if current else 0) + 1 if _cas_columns else 0,
        hints=json.loads(cols["scan_hints"]) if "scan_hints" in cols else (current.hints if current else {}),
        updated_at=cols.get("updated_at"),
        max_saved_at=cols.get("max_saved_at", current.max_saved_at if current else 0.0),
        recipes_written_at=cols.get("recipes_written_at", current.recipes_written_at if current else 0.0),
    )


def _cas_loop(
    dealer_id: str,
    plan: Callable[[StoreRow | None], dict[str, Any] | None],
    *,
    create: bool,
    retries: int,
    file_written: bool = True,
) -> StoreRow | None:
    """Read, ``plan`` the column changes, CAS-write; retry on a lost race.

    ``plan(current)`` returns the columns to write, or None to leave the row alone
    (the current row is then the result). Returns the row as written (``rev`` bumped),
    the unchanged row for a no-op, or None: store disabled, no row with ``create``
    False, retries used up, or an error (logged, rate-limited; never raised).
    """
    if not _enabled() or not dealer_id:
        return None
    try:
        conn = _conn()
        try:
            _ensure_table(conn)
            cur = conn.cursor()
            for _ in range(max(1, retries)):
                current = _snapshot(cur, dealer_id)
                if current is None and not create:
                    conn.rollback()
                    return None
                cols = plan(current)
                if cols is None:
                    conn.rollback()
                    return current
                if _cas_write(cur, dealer_id, current, cols):
                    conn.commit()
                    _bump("writes")
                    return _written(current, cols)
                conn.rollback()
                _bump("conflicts")
            _bump("exhausted")
            logger.warning(
                "dealer_recipes write for %s lost the compare-and-set race %d times; not written",
                dealer_id, max(1, retries),
            )
            return None
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 — never let DB issues break a scan
        _bump("failures")
        _log_save_failure(dealer_id, exc, file_written=file_written)
        return None


def db_read(dealer_id: str, *, strict: bool = False) -> StoreRow | None:
    """The dealer's ``StoreRow``, or None. Best-effort; alias fallback like
    ``db_load_recipes`` (reads follow ``_aliases.json``, writes never do).

    Unlike ``db_load_recipes`` a hints-only row is returned (``authoritative`` False).
    ``strict`` is not supported here; use ``db_load_recipes(strict=True)``."""
    found = _db_read_exact(dealer_id)
    if found is not None:
        return found
    old_slug = _alias_read_slug(dealer_id)
    return _db_read_exact(old_slug) if old_slug else None


def _db_read_exact(dealer_id: str) -> StoreRow | None:
    if not _enabled() or not dealer_id:
        return None
    try:
        conn = _conn()
        try:
            _ensure_table(conn)
            return _snapshot(conn.cursor(), dealer_id)
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        logger.debug("dealer_recipes read skipped for %s: %s", dealer_id, exc)
        return None


def db_mutate(
    dealer_id: str,
    fn: Callable[[StoreRow | None], list[dict[str, Any]] | None],
    *,
    create: bool = True,
    retries: int = 8,
    file_written: bool = True,
) -> StoreRow | None:
    """Compare-and-set the dealer's recipe list: ``fn(current)`` returns the new list
    (None leaves the row alone); ``current`` is None when the dealer has no row.

    Retried up to ``retries`` times when another writer got in between (``fn`` runs
    again on the fresh row, so it must be re-runnable). Stamps ``recipes_written_at``
    and bumps ``rev``; the rows themselves are stored as given (``saved_at`` is the
    caller's). Returns the row as written, or None (see ``_cas_loop``)."""
    def plan(current: StoreRow | None) -> dict[str, Any] | None:
        rows = fn(current)
        if rows is None:
            return None
        meta = _derive_meta(rows)
        cols = {
            "recipes_json": json.dumps(rows, ensure_ascii=False),
            "recipe_count": meta["recipe_count"], "provider_hint": meta["provider_hint"],
            "max_saved_at": meta["max_saved_at"], "last_ok_at": meta["last_ok_at"],
            "stale_count": meta["stale_count"],
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if _cas_columns:
            cols["recipes_written_at"] = time.time()
        return cols

    return _cas_loop(dealer_id, plan, create=create, retries=retries, file_written=file_written)


def db_save_recipes(dealer_id: str, rows: list[dict[str, Any]], *, file_written: bool = True) -> bool:
    """Replace the dealer's full recipe list. Best-effort; returns success.

    Thin wrapper over ``db_mutate``. The rows are stored as given: ``save_recipes``
    has already stamped their ``saved_at``, and a ``load_recipes`` push-up must keep
    the file's stamp. ``file_written=False`` says the caller wrote no recipe file (a
    cache tagged for another store), so a failure is logged as a lost save."""
    return db_mutate(dealer_id, lambda _cur: rows, file_written=file_written) is not None


def mutate_scan_hints(
    dealer_id: str,
    fn: Callable[[dict[str, Any]], dict[str, Any] | None],
    *,
    retries: int = 8,
) -> bool:
    """Compare-and-set the dealer's scan hints: ``fn(current_hints)`` returns the new
    dict (None leaves them alone). ``fn`` may build nested values (P7.6) and must be
    re-runnable. Creates the row (with an empty recipe list) when the dealer has none.
    A hint write bumps ``rev`` and ``updated_at`` but never ``recipes_written_at``.
    Returns True when the hints are stored (including unchanged)."""
    def plan(current: StoreRow | None) -> dict[str, Any] | None:
        new = fn(dict(current.hints) if current else {})
        if new is None:
            return None
        cols: dict[str, Any] = {
            "scan_hints": json.dumps(new, ensure_ascii=False),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if current is None:
            cols.update(recipes_json="[]", recipe_count=0)
        return cols

    return _cas_loop(dealer_id, plan, create=True, retries=retries) is not None


def _alias_read_slug(dealer_id: str) -> str | None:
    """Retired slug to fall back to on READS after a dealer-URL rekey.

    Delegates to ``backend.scanner.recipes.resolve_alias_slug`` (the
    ``workspace/recipes/_aliases.json`` map) — no schema change needed: the
    old ``dealer_recipes`` row is simply queried under its old key. Writes
    never follow aliases, so the rekeyed dealer's row heals to the new key on
    its next save. Best-effort like every other touch in this module.
    """
    try:
        from backend.scanner.recipes import resolve_alias_slug

        return resolve_alias_slug(dealer_id)
    except Exception:  # noqa: BLE001
        return None


def db_load_recipes(
    dealer_id: str, *, strict: bool = False,
) -> tuple[list[dict[str, Any]], float] | None:
    """(recipe rows, max_saved_at) from the DB, or None. Best-effort.

    Falls back to the dealer's pre-rename key (``_aliases.json``) when the
    current key has no row.

    By default ``None`` means "no copy was read": no row, or the read failed
    (logged at DEBUG). With ``strict=True`` ``None`` means the store has no row
    under either key, and a read that cannot show that (it raised, or a row's
    ``recipes_json`` is not a JSON list) raises ``RecipeStoreReadError``.
    """
    found = _db_load_recipes_exact(dealer_id, strict=strict)
    if found is not None:
        return found
    old_slug = _alias_read_slug(dealer_id)
    if old_slug:
        return _db_load_recipes_exact(old_slug, strict=strict)
    return None


def _db_load_recipes_exact(
    dealer_id: str, *, strict: bool = False,
) -> tuple[list[dict[str, Any]], float] | None:
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
        if strict:
            # The class only: the error text (logged above at DEBUG) can quote the URL.
            raise RecipeStoreReadError(
                f"dealer_recipes read for {dealer_id} failed ({type(exc).__name__})"
            ) from exc
        return None
    if not row:
        return None
    unreadable = f"dealer_recipes row for {dealer_id} has unreadable recipes_json"
    if not row[0]:
        if strict:
            raise RecipeStoreReadError(unreadable + " (empty)")
        return None
    try:
        rows = json.loads(row[0])
    except (TypeError, ValueError) as exc:
        if strict:
            raise RecipeStoreReadError(unreadable + " (not JSON)") from exc
        return None
    if not isinstance(rows, list):
        if strict:
            raise RecipeStoreReadError(unreadable + " (not a list)")
        return None
    try:
        max_saved = float(row[1] or 0.0)
    except (TypeError, ValueError):
        max_saved = 0.0
    return rows, max_saved


def get_scan_hints(dealer_id: str) -> dict[str, Any]:
    """Per-dealer scan instructions ({} when none / DB unavailable).

    Falls back to the dealer's pre-rename key (``_aliases.json``) when the
    current key has no hints (read-side only; ``set_scan_hints`` always
    writes the current key).
    """
    hints = _get_scan_hints_exact(dealer_id)
    if hints is not None:
        # An explicitly stored {} means CLEARED — it must not fall through to
        # the alias slug, or a rekeyed dealer's stale old-slug hints resurrect
        # forever every time the current key's hints are cleared.
        return hints
    old_slug = _alias_read_slug(dealer_id)
    if old_slug:
        return _get_scan_hints_exact(old_slug) or {}
    return {}


def _get_scan_hints_exact(dealer_id: str) -> dict[str, Any] | None:
    """The stored hints dict, {} when explicitly cleared, None when no row."""
    if not _enabled() or not dealer_id:
        return None
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
        return None
    if not row or not row[0]:
        return None
    try:
        hints = json.loads(row[0])
        return hints if isinstance(hints, dict) else None
    except (TypeError, ValueError):
        return None


def set_scan_hints(dealer_id: str, hints: dict[str, Any], *, merge: bool = True) -> bool:
    """
    Write per-dealer scan instructions. ``merge=True`` (default) overlays onto
    existing hints; a key set to None removes it. Creates the dealer row (with
    an empty recipe list) when the dealer has no recipes yet — hints often
    exist precisely BECAUSE recipe capture failed. Runs on ``mutate_scan_hints``,
    so concurrent writers of different keys both survive.
    """
    def apply(current: dict[str, Any]) -> dict[str, Any]:
        base = current if merge else {}
        for k, v in hints.items():
            if v is None:
                base.pop(k, None)
            else:
                base[k] = v
        return base

    return mutate_scan_hints(dealer_id, apply)
