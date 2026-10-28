"""Persistence for user-authored comments (``car_comments``, ``dealer_comments``)
and the per-dealership Google rating cache (``dealer_ratings``).

Schema of record is ``migrations/V002__community.sql`` (``comment_flags`` V004,
``comment_attachments`` V005). The tables are never created at runtime: a database
that has not run the migration chain fails loudly instead of growing its own
copy, the arrangement ``backend/reviews/store.py`` uses for ``dealer_reviews``.

Reads and writes go through the shared inventory connection
(:func:`backend.db.inventory_db.get_conn`), so ``?`` placeholders are adapted to
``%s`` by the compat cursor. Rows come back as dicts (``psycopg`` ``dict_row``).

Two things this module deliberately owns rather than leaving to callers:

* **Body normalization.** ``sanitize_body`` is the single place text is cleaned
  (NUL/control characters stripped, length capped). A comment that reaches the
  table is already storable.
* **Escaping.** Rows are returned with both ``body`` (the plain text as typed)
  and ``body_html`` (HTML-escaped, newlines to ``<br>``). Storing the raw text
  keeps one canonical form -- double-escaped text is unrecoverable -- while
  ``body_html`` gives the UI something it can drop into ``innerHTML`` without
  the caller having to remember to escape. Nothing in here ever stores markup.
"""

from __future__ import annotations

import html
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from psycopg.rows import dict_row

from backend.db.inventory_db import get_conn

# Free text, not an essay: long enough for "this dealership adds bogus packages,
# avoid" plus context, short enough that one post cannot dominate a page.
MAX_BODY_CHARS = 4000
MIN_BODY_CHARS = 2

# Flags needed before a comment auto-hides. Matches dealer_reviews'
# REPORT_FLAG_THRESHOLD so moderation behaves the same across both surfaces.
FLAG_HIDE_THRESHOLD = 3

# Per-user posting cap enforced in SQL (the per-IP limiter is per-worker memory
# and resets on restart; this one survives).
USER_POST_LIMIT = 10
USER_POST_WINDOW_SECONDS = 3600.0

SCOPE_CAR = "car"
SCOPE_DEALER = "dealer"

# scope -> (table, subject column). Every accessor below is written once against
# this map: car and dealer comments differ only in which id they hang off, and
# two hand-copied halves would drift the first time moderation changes.
_SCOPES: dict[str, tuple[str, str]] = {
    SCOPE_CAR: ("car_comments", "car_id"),
    SCOPE_DEALER: ("dealer_comments", "dealership_id"),
}

_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_EXCESS_BLANK_LINES_RE = re.compile(r"\n{3,}")


class CommentError(ValueError):
    """Rejected before it reached the table; ``code`` is the API error string."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _scope_spec(scope: str) -> tuple[str, str]:
    try:
        return _SCOPES[scope]
    except KeyError:
        raise CommentError("bad_scope", f"Unknown comment scope {scope!r}.") from None


def sanitize_body(raw: str | None) -> str:
    """Normalize submitted text or raise :class:`CommentError`.

    Strips control characters (a NUL truncates the value inside Postgres' text
    protocol, and the rest only ever arrive from a paste or a bot), normalizes
    line endings, and collapses runs of blank lines so one post cannot push the
    rest of the thread off screen. Markup is left alone here on purpose --
    escaping happens on the way out, see the module docstring.
    """
    text = (raw or "")
    if not isinstance(text, str):
        raise CommentError("bad_body", "Comment must be text.")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = _CONTROL_CHARS_RE.sub("", text)
    text = _EXCESS_BLANK_LINES_RE.sub("\n\n", text).strip()
    if len(text) < MIN_BODY_CHARS:
        raise CommentError("empty_body", "Write something first.")
    if len(text) > MAX_BODY_CHARS:
        raise CommentError(
            "body_too_long", f"Comments are limited to {MAX_BODY_CHARS} characters."
        )
    return text


def body_to_html(body: str) -> str:
    """HTML-escaped rendering of a stored comment (newlines become ``<br>``)."""
    return html.escape(body or "", quote=False).replace("\n", "<br>")


def _iso(value: Any) -> str | None:
    """Timestamps read back as ``datetime`` (``timestamptz``) or ``str`` (TEXT columns)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def _row_to_comment(row: Any, scope: str) -> dict[str, Any]:
    d = dict(row)
    subject_col = _SCOPES[scope][1]
    body = d.get("body") or ""
    return {
        "id": int(d["id"]),
        "scope": scope,
        "subject_id": int(d[subject_col]),
        "user_id": int(d["user_id"]),
        "body": body,
        "body_html": body_to_html(body),
        "is_hidden": bool(d.get("is_hidden")),
        "flag_count": int(d.get("flag_count") or 0),
        # Always present so callers never branch on the key's existence;
        # populated by attach_attachments on the read paths that need it.
        "attachments": [],
        "created_at": _iso(d.get("created_at")),
        "updated_at": _iso(d.get("updated_at")),
        "deleted_at": _iso(d.get("deleted_at")),
    }


_SELECT_COLUMNS = (
    "id, {subject}, user_id, body, is_hidden, flag_count, created_at, updated_at, deleted_at"
)


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def list_comments(
    scope: str,
    subject_id: int,
    *,
    limit: int = 100,
    offset: int = 0,
    include_hidden: bool = False,
) -> list[dict[str, Any]]:
    """Visible comments for one car/dealership, newest first.

    Returns ``[]`` rather than raising when the table does not exist yet: a car
    page rendered on a database that has never had a comment is not an error.
    """
    table, subject_col = _scope_spec(scope)
    cap = max(1, min(int(limit), 500))
    skip = max(0, int(offset))
    hidden_clause = "" if include_hidden else " AND is_hidden = FALSE"
    conn = get_conn()
    try:
        cur = conn.cursor(row_factory=dict_row)
        cur.execute(
            f"""
            SELECT {_SELECT_COLUMNS.format(subject=subject_col)}
            FROM {table}
            WHERE {subject_col} = ? AND deleted_at IS NULL{hidden_clause}
            ORDER BY created_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (int(subject_id), cap, skip),
        )
        rows = [_row_to_comment(r, scope) for r in cur.fetchall()]
    except Exception:
        return []
    finally:
        conn.close()
    # Outside the try: attachments open their own connection, and a thread whose
    # photos cannot be read is still a thread worth rendering.
    from backend.db.comment_attachments_db import attach_attachments  # lazy: avoids an import cycle

    return attach_attachments(scope, rows)


def count_comments(scope: str, subject_id: int, *, include_hidden: bool = False) -> int:
    """Number of visible comments on a subject (for a badge next to the tab)."""
    table, subject_col = _scope_spec(scope)
    hidden_clause = "" if include_hidden else " AND is_hidden = FALSE"
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT COUNT(*) FROM {table} "
            f"WHERE {subject_col} = ? AND deleted_at IS NULL{hidden_clause}",
            (int(subject_id),),
        )
        row = cur.fetchone()
        return int((row[0] if row else 0) or 0)
    except Exception:
        return 0
    finally:
        conn.close()


def get_comment(scope: str, comment_id: int) -> dict[str, Any] | None:
    """One comment by id, including soft-deleted and hidden rows."""
    table, subject_col = _scope_spec(scope)
    conn = get_conn()
    try:
        cur = conn.cursor(row_factory=dict_row)
        cur.execute(
            f"SELECT {_SELECT_COLUMNS.format(subject=subject_col)} FROM {table} WHERE id = ?",
            (int(comment_id),),
        )
        row = cur.fetchone()
        comment = _row_to_comment(row, scope) if row else None
    except Exception:
        return None
    finally:
        conn.close()
    from backend.db.comment_attachments_db import attach_attachments  # lazy: avoids an import cycle

    return attach_attachments(scope, [comment])[0] if comment else None


def count_recent_comments_by_user(
    user_id: int, *, within_seconds: float = USER_POST_WINDOW_SECONDS
) -> int:
    """Comments this user posted across both surfaces inside the window.

    Deleted rows still count -- otherwise "post, delete, repeat" would be an
    unlimited spam loop.
    """
    since = (
        datetime.now(timezone.utc) - timedelta(seconds=max(1.0, float(within_seconds)))
    ).isoformat()
    total = 0
    conn = get_conn()
    try:
        cur = conn.cursor()
        for table, _subject_col in _SCOPES.values():
            cur.execute(
                f"SELECT COUNT(*) FROM {table} WHERE user_id = ? AND created_at >= ?",
                (int(user_id), since),
            )
            row = cur.fetchone()
            total += int((row[0] if row else 0) or 0)
        return total
    except Exception:
        # A missing table means nothing has been posted yet, not "block the user".
        return total
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------

def create_comment(
    scope: str,
    subject_id: int,
    user_id: int,
    body: str,
    *,
    ip_hash: str | None = None,
) -> dict[str, Any]:
    """Insert one comment and return it. Raises :class:`CommentError` on bad text."""
    table, subject_col = _scope_spec(scope)
    clean = sanitize_body(body)
    now = _now_iso()
    conn = get_conn()
    try:
        cur = conn.cursor(row_factory=dict_row)
        cur.execute(
            f"""
            INSERT INTO {table} (
                {subject_col}, user_id, body, ip_hash, is_hidden, flag_count,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, FALSE, 0, ?, ?)
            RETURNING {_SELECT_COLUMNS.format(subject=subject_col)}
            """,
            (int(subject_id), int(user_id), clean, ip_hash, now, now),
        )
        row = cur.fetchone()
        conn.commit()
        return _row_to_comment(row, scope)
    finally:
        conn.close()


def delete_own_comment(scope: str, comment_id: int, user_id: int) -> str:
    """Soft-delete a comment the caller owns.

    Returns ``"deleted"``, ``"not_found"`` (unknown id or already gone) or
    ``"forbidden"`` (someone else's). The ownership test is part of the UPDATE
    predicate, not a read-then-write, so two concurrent requests cannot race
    into deleting a row the second one does not own.
    """
    table, subject_col = _scope_spec(scope)
    now = _now_iso()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            UPDATE {table}
            SET deleted_at = ?, updated_at = ?
            WHERE id = ? AND user_id = ? AND deleted_at IS NULL
            RETURNING id
            """,
            (now, now, int(comment_id), int(user_id)),
        )
        if cur.fetchone():
            conn.commit()
            return "deleted"
        # Nothing updated: distinguish "not yours" from "not there" for the API.
        cur.execute(
            f"SELECT user_id, deleted_at FROM {table} WHERE id = ?", (int(comment_id),)
        )
        existing = cur.fetchone()
        conn.commit()
        if not existing:
            return "not_found"
        owner = int(existing[0])
        already_deleted = existing[1] is not None
        if owner != int(user_id):
            return "forbidden"
        return "not_found" if already_deleted else "forbidden"
    finally:
        conn.close()


def flag_comment(scope: str, comment_id: int, *, reporter_hash: str | None = None) -> dict[str, Any] | None:
    """Increment a comment's flag count, auto-hiding it at the threshold.

    ``reporter_hash`` (the salted ip_hash from :func:`backend.reviews.guards.hash_ip`)
    makes a report idempotent per actor: the first one bumps ``flag_count``, later
    ones return the row unchanged. Without it a single visitor could reach
    :data:`FLAG_HIDE_THRESHOLD` alone and hide any comment on the site, which is
    exactly the hole ``review_reports`` closes for dealer reviews.

    Passing ``reporter_hash=None`` keeps the old unconditional bump and is only
    for callers that are not a public request (moderation scripts, tests).
    """
    table, subject_col = _scope_spec(scope)
    now = _now_iso()
    conn = get_conn()
    try:
        cur = conn.cursor(row_factory=dict_row)
        if reporter_hash:
            # Confirm the comment exists first: a flag on a missing/deleted row is a
            # 404, not a silent no-op that would still burn this reporter's one slot.
            cur.execute(
                f"SELECT {_SELECT_COLUMNS.format(subject=subject_col)} FROM {table} "
                "WHERE id = ? AND deleted_at IS NULL",
                (int(comment_id),),
            )
            current = cur.fetchone()
            if current is None:
                conn.commit()
                return None
            cur = conn.cursor(row_factory=dict_row)
            cur.execute(
                "INSERT INTO comment_flags (scope, comment_id, ip_hash, created_at) "
                "VALUES (?, ?, ?, ?) ON CONFLICT (scope, comment_id, ip_hash) DO NOTHING",
                (scope, int(comment_id), reporter_hash, now),
            )
            if not cur.rowcount:
                # Already reported by this actor — current state, no bump.
                conn.commit()
                return _row_to_comment(current, scope)
            cur = conn.cursor(row_factory=dict_row)
        cur.execute(
            f"""
            UPDATE {table}
            SET flag_count = flag_count + 1,
                is_hidden = (flag_count + 1 >= {int(FLAG_HIDE_THRESHOLD)}),
                updated_at = ?
            WHERE id = ? AND deleted_at IS NULL
            RETURNING {_SELECT_COLUMNS.format(subject=subject_col)}
            """,
            (now, int(comment_id)),
        )
        row = cur.fetchone()
        conn.commit()
        return _row_to_comment(row, scope) if row else None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Subject existence (so routes 404 instead of writing orphan rows)
# ---------------------------------------------------------------------------

def car_exists(car_id: int) -> bool:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM cars WHERE id = ?", (int(car_id),))
        return cur.fetchone() is not None
    except Exception:
        return False
    finally:
        conn.close()


def dealership_exists(dealership_id: int) -> bool:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM dealerships WHERE id = ?", (int(dealership_id),))
        return cur.fetchone() is not None
    except Exception:
        return False
    finally:
        conn.close()


def subject_exists(scope: str, subject_id: int) -> bool:
    _scope_spec(scope)  # validate
    if scope == SCOPE_CAR:
        return car_exists(subject_id)
    return dealership_exists(subject_id)


# Attachments and dealer ratings live in their own modules. Re-exported lazily
# (they import helpers from here, so an eager import at module end would cycle
# when one of them is imported first).
_MOVED = {
    "backend.db.comment_attachments_db": (
        "list_attachments", "attach_attachments", "get_attachment",
        "add_attachments", "delete_attachments_for_comment",
        "_ATTACHMENT_COLUMNS", "_row_to_attachment",
    ),
    "backend.db.dealer_ratings_db": (
        "upsert_dealer_rating", "get_dealer_rating",
        "list_active_dealership_websites", "list_dealerships_needing_rating",
    ),
}
_MOVED_BY_NAME = {n: m for m, names in _MOVED.items() for n in names}


def __getattr__(name: str) -> Any:
    mod = _MOVED_BY_NAME.get(name)
    if mod is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    value = getattr(importlib.import_module(mod), name)
    globals()[name] = value
    return value
