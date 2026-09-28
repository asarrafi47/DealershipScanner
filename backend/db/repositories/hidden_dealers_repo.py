"""Per-user hidden dealerships (account profile -> "Hidden dealerships").

Mirrors saved_searches_repo.py: a thin CRUD layer over the inventory DB
connection (SQLite in tests, Postgres in prod via backend.db.inventory_compat).
``dealer_id`` is the hostname-derived ``cars.dealer_id`` key, stored lower-case
so the exclusion clause in search_repo (``LOWER(dealer_id) NOT IN (...)``) and
the client-side set in main.js compare like with like. Schema: migrations/
V021__user_hidden_dealers.sql (+ the runtime CREATE TABLE copies in
inventory_pg.py / schema_repo.py).
"""
from __future__ import annotations

from backend.db.repositories.base_repo import db_conn

_MAX_DEALER_ID_LEN = 128
_MAX_DEALER_NAME_LEN = 200
# Keep one user's list bounded: nobody hides more rooftops than this by hand,
# and the exclusion clause is inlined into every search that user runs.
MAX_HIDDEN_DEALERS_PER_USER = 200


def normalize_dealer_id(dealer_id) -> str:
    """Canonical form of a ``cars.dealer_id`` key ('' when unusable)."""
    key = str(dealer_id or "").strip().lower()
    if not key or len(key) > _MAX_DEALER_ID_LEN:
        return ""
    return key


def hide_dealer(user_id: int, dealer_id: str, dealer_name: str | None = None) -> bool:
    """Add ``dealer_id`` to the user's hidden list. Returns True when a row was added
    (False when it was already hidden). Raises ValueError on a bad id or a full list."""
    key = normalize_dealer_id(dealer_id)
    if not key:
        raise ValueError("dealer_id required")
    name = (str(dealer_name or "").strip() or None)
    if name and len(name) > _MAX_DEALER_NAME_LEN:
        name = name[:_MAX_DEALER_NAME_LEN]
    with db_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM user_hidden_dealers WHERE user_id = ? AND dealer_id = ? LIMIT 1",
            (int(user_id), key),
        ).fetchone()
        if row is not None:
            return False
        cnt_row = conn.execute(
            "SELECT COUNT(*) FROM user_hidden_dealers WHERE user_id = ?",
            (int(user_id),),
        ).fetchone()
        count = int(_first(cnt_row) or 0)
        if count >= MAX_HIDDEN_DEALERS_PER_USER:
            raise ValueError("hidden dealer list is full")
        conn.execute(
            "INSERT INTO user_hidden_dealers (user_id, dealer_id, dealer_name) "
            "VALUES (?, ?, ?) ON CONFLICT (user_id, dealer_id) DO NOTHING",
            (int(user_id), key, name),
        )
        conn.commit()
    return True


def unhide_dealer(user_id: int, dealer_id: str) -> bool:
    """Remove ``dealer_id`` from the user's hidden list. Returns True if a row was removed."""
    key = normalize_dealer_id(dealer_id)
    if not key:
        return False
    with db_conn() as conn:
        cur = conn.execute(
            "DELETE FROM user_hidden_dealers WHERE user_id = ? AND dealer_id = ?",
            (int(user_id), key),
        )
        conn.commit()
        deleted = getattr(cur, "rowcount", None)
    if deleted is not None and deleted >= 0:
        return deleted > 0
    # Backends whose cursor doesn't report rowcount reliably: re-check existence.
    return not is_dealer_hidden(user_id, key)


def is_dealer_hidden(user_id: int, dealer_id: str) -> bool:
    key = normalize_dealer_id(dealer_id)
    if not key:
        return False
    with db_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM user_hidden_dealers WHERE user_id = ? AND dealer_id = ? LIMIT 1",
            (int(user_id), key),
        ).fetchone()
    return row is not None


def list_hidden_dealers(user_id: int) -> list[dict]:
    """[{dealer_id, dealer_name, created_at}, ...] newest first."""
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT dealer_id, dealer_name, created_at FROM user_hidden_dealers "
            "WHERE user_id = ? ORDER BY created_at DESC, id DESC",
            (int(user_id),),
        ).fetchall()
    out: list[dict] = []
    for r in rows:
        if isinstance(r, dict):
            did, name, created = r["dealer_id"], r["dealer_name"], r["created_at"]
        else:
            did, name, created = r[0], r[1], r[2]
        out.append(
            {
                "dealer_id": str(did),
                "dealer_name": (name or None),
                "created_at": created,
            }
        )
    return out


def hidden_dealer_ids_for_user(user_id) -> set[str]:
    """Lower-cased ``cars.dealer_id`` keys this user hid. Empty set for anonymous
    callers (``user_id`` falsy) so callers can pass ``session.get("user_id")`` straight in."""
    if not user_id:
        return set()
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return set()
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT dealer_id FROM user_hidden_dealers WHERE user_id = ?",
            (uid,),
        ).fetchall()
    out: set[str] = set()
    for r in rows:
        did = r["dealer_id"] if isinstance(r, dict) else r[0]
        key = normalize_dealer_id(did)
        if key:
            out.add(key)
    return out


def _first(row):
    if row is None:
        return None
    if isinstance(row, dict):
        return next(iter(row.values()), None)
    return row[0]
