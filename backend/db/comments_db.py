"""Persistence for user-authored comments (``car_comments``, ``dealer_comments``)
and the per-dealership Google rating cache (``dealer_ratings``).

Schema of record is ``migrations/V002__community.sql``. The ``ensure_*`` calls
here are the same DDL expressed idempotently, so a database that has not run
the migration yet (a fresh dev box, a SQLite test) still works -- exactly the
arrangement ``backend/reviews/store.py`` uses for ``dealer_reviews``.

Writes go through the shared inventory connection
(:func:`backend.db.inventory_db.get_conn`), so ``?`` placeholders are adapted to
``%s`` on Postgres by the compat cursor and the module reads the same on both
backends. Production is Postgres; SQLite exists only for pytest isolation.

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
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from backend.db import inventory_pg
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


# Parent table each scope's subject id points at, so the runtime DDL declares the
# same foreign key the migration does.
_SUBJECT_PARENT: dict[str, str] = {"car_id": "cars", "dealership_id": "dealerships"}


# Index name -> (columns, partial predicate). Shared by the runtime DDL below and
# asserted equal to migrations/V002__community.sql by test_comments_db.py, because
# whichever of the two runs first wins: an index created here without V002's partial
# predicate would leave a database that passes ``migrate`` yet reads differently.
_VISIBLE_ROWS = "deleted_at IS NULL AND is_hidden = false"
_FLAGGED_ROWS = "flag_count > 0"


def _comment_indexes(table: str, subject_col: str) -> list[tuple[str, str, str]]:
    """``(name, columns, where)`` for one comment table, matching V002 exactly."""
    return [
        # The read path: visible comments for one subject, newest first.
        (f"idx_{table}_thread", f"{subject_col}, created_at DESC", _VISIBLE_ROWS),
        # Ownership checks on delete, and the per-user posting-volume cap.
        (f"idx_{table}_user_recent", "user_id, created_at DESC", ""),
        (f"idx_{table}_moderation", "flag_count DESC, created_at DESC", _FLAGGED_ROWS),
    ]


# One reporter counts once per comment. Schema of record is
# migrations/V004__comment_flag_dedupe.sql; this is the same DDL expressed
# idempotently, the arrangement described in the module docstring. Deliberately
# NOT created by ``ensure_comment_tables`` -- that function's output is compared
# statement-for-statement against V002 by test_comments_db.py.
_DDL_COMMENT_FLAGS_PG = """
CREATE TABLE IF NOT EXISTS comment_flags (
    scope TEXT NOT NULL,
    comment_id BIGINT NOT NULL,
    ip_hash TEXT NOT NULL,
    created_at TEXT,
    UNIQUE (scope, comment_id, ip_hash)
)
"""

_DDL_COMMENT_FLAGS_SQLITE = """
CREATE TABLE IF NOT EXISTS comment_flags (
    scope TEXT NOT NULL,
    comment_id INTEGER NOT NULL,
    ip_hash TEXT NOT NULL,
    created_at TEXT,
    UNIQUE (scope, comment_id, ip_hash)
)
"""


# Photo attachments. Schema of record is
# migrations/V005__comment_attachments.sql; as with comment_flags this is the
# same DDL expressed idempotently and is deliberately NOT created by
# ``ensure_comment_tables``, whose output is compared statement-for-statement
# against V002 by test_comments_db.py.
#
# Scope-polymorphic like comment_flags, and for the same reason: car_comments.id
# and dealer_comments.id are independent sequences, so no single foreign key can
# point at both parents. ``storage_key`` is the opaque server-minted name from
# backend/utils/comment_images.py -- never a user-supplied filename -- and it is
# UNIQUE so a collision fails the INSERT instead of quietly re-pointing one
# comment's photo at another's file.
_DDL_COMMENT_ATTACHMENTS_PG = """
CREATE TABLE IF NOT EXISTS comment_attachments (
    id BIGSERIAL PRIMARY KEY,
    scope TEXT NOT NULL,
    comment_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    storage_key TEXT NOT NULL UNIQUE,
    mime_type TEXT NOT NULL,
    width INTEGER NOT NULL,
    height INTEGER NOT NULL,
    byte_size INTEGER NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT,
    deleted_at TEXT,
    CONSTRAINT comment_attachments_scope_known CHECK (scope IN ('car', 'dealer')),
    CONSTRAINT comment_attachments_mime_allowed
        CHECK (mime_type IN ('image/jpeg', 'image/png', 'image/webp')),
    CONSTRAINT comment_attachments_dims_positive CHECK (width > 0 AND height > 0),
    CONSTRAINT comment_attachments_size_positive CHECK (byte_size > 0),
    CONSTRAINT comment_attachments_sort_order_nonneg CHECK (sort_order >= 0)
)
"""

_DDL_COMMENT_ATTACHMENTS_SQLITE = """
CREATE TABLE IF NOT EXISTS comment_attachments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scope TEXT NOT NULL,
    comment_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    storage_key TEXT NOT NULL UNIQUE,
    mime_type TEXT NOT NULL,
    width INTEGER NOT NULL,
    height INTEGER NOT NULL,
    byte_size INTEGER NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT,
    deleted_at TEXT,
    CONSTRAINT comment_attachments_scope_known CHECK (scope IN ('car', 'dealer')),
    CONSTRAINT comment_attachments_mime_allowed
        CHECK (mime_type IN ('image/jpeg', 'image/png', 'image/webp')),
    CONSTRAINT comment_attachments_dims_positive CHECK (width > 0 AND height > 0),
    CONSTRAINT comment_attachments_size_positive CHECK (byte_size > 0),
    CONSTRAINT comment_attachments_sort_order_nonneg CHECK (sort_order >= 0)
)
"""

_ATTACHMENT_INDEXES: list[tuple[str, str, str]] = [
    ("idx_comment_attachments_comment", "scope, comment_id, sort_order", "deleted_at IS NULL"),
    ("idx_comment_attachments_user", "user_id, created_at DESC", ""),
]


# Same shape for dealer_ratings.
_RATING_INDEXES: list[tuple[str, str, str]] = [
    ("idx_dealer_ratings_place", "google_place_id", ""),
    ("idx_dealer_ratings_fetched", "fetched_at", ""),
]


def _create_index_sql(name: str, table: str, columns: str, where: str) -> str:
    stmt = f"CREATE INDEX IF NOT EXISTS {name} ON {table} ({columns})"
    return f"{stmt} WHERE ({where})" if where else stmt


def _ddl_comments(table: str, subject_col: str, *, postgres: bool) -> str:
    """DDL kept semantically identical to migrations/V002__community.sql.

    Not "close enough": whichever of the two runs first wins, and a table created
    here without V002's foreign key and CHECK constraints would leave a database
    that passes ``migrate`` while silently accepting orphan rows and blank
    comments. The two are not byte-for-byte — V002 is Postgres-only and written
    in pg_dump's spelling (``public.`` schema, ``bigserial``, ``::text`` casts),
    while this has to emit a SQLite variant for pytest as well. What must match,
    and what ``test_ddl_matches_v002_migration`` checks, is the column set, the
    constraint names, and the index definitions including their partial
    predicates.
    """
    parent = _SUBJECT_PARENT[subject_col]
    if postgres:
        return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id BIGSERIAL PRIMARY KEY,
            {subject_col} BIGINT NOT NULL REFERENCES {parent}(id) ON DELETE CASCADE,
            user_id BIGINT NOT NULL,
            body TEXT NOT NULL,
            ip_hash TEXT,
            is_hidden BOOLEAN NOT NULL DEFAULT FALSE,
            flag_count INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            deleted_at TIMESTAMPTZ,
            CONSTRAINT {table}_body_not_blank CHECK (btrim(body) <> ''),
            CONSTRAINT {table}_body_length CHECK (char_length(body) <= {MAX_BODY_CHARS}),
            CONSTRAINT {table}_flag_count_nonneg CHECK (flag_count >= 0)
        )
        """
    return f"""
    CREATE TABLE IF NOT EXISTS {table} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        {subject_col} INTEGER NOT NULL REFERENCES {parent}(id) ON DELETE CASCADE,
        user_id INTEGER NOT NULL,
        body TEXT NOT NULL,
        ip_hash TEXT,
        is_hidden INTEGER NOT NULL DEFAULT 0,
        flag_count INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        deleted_at TEXT,
        CONSTRAINT {table}_body_not_blank CHECK (trim(body) <> ''),
        CONSTRAINT {table}_body_length CHECK (length(body) <= {MAX_BODY_CHARS}),
        CONSTRAINT {table}_flag_count_nonneg CHECK (flag_count >= 0)
    )
    """


_DDL_RATINGS_PG = """
CREATE TABLE IF NOT EXISTS dealer_ratings (
    dealership_id BIGINT PRIMARY KEY REFERENCES dealerships(id) ON DELETE CASCADE,
    google_place_id TEXT,
    google_rating NUMERIC(2,1),
    google_review_count INTEGER,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    source TEXT NOT NULL DEFAULT 'google_places',
    CONSTRAINT dealer_ratings_rating_range
        CHECK (google_rating IS NULL OR (google_rating >= 0 AND google_rating <= 5)),
    CONSTRAINT dealer_ratings_review_count_nonneg
        CHECK (google_review_count IS NULL OR google_review_count >= 0)
)
"""

_DDL_RATINGS_SQLITE = """
CREATE TABLE IF NOT EXISTS dealer_ratings (
    dealership_id INTEGER PRIMARY KEY REFERENCES dealerships(id) ON DELETE CASCADE,
    google_place_id TEXT,
    google_rating REAL,
    google_review_count INTEGER,
    fetched_at TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'google_places',
    CONSTRAINT dealer_ratings_rating_range
        CHECK (google_rating IS NULL OR (google_rating >= 0 AND google_rating <= 5)),
    CONSTRAINT dealer_ratings_review_count_nonneg
        CHECK (google_review_count IS NULL OR google_review_count >= 0)
)
"""


# Per-process "already ensured" flags, the same guard
# ``inventory_pg.init_postgres_inventory`` uses for ``_PG_INV_SCHEMA_OK``.
# Without them, every comment post/delete/flag/attach and every dealer-rating
# write re-issues `CREATE TABLE IF NOT EXISTS` + `CREATE INDEX IF NOT EXISTS`
# against the shared Postgres inventory connection -- and `CREATE INDEX IF NOT
# EXISTS` still takes a lock even when the index already exists, so a long
# transaction elsewhere on these tables queues every later comment write
# behind it. Gated on Postgres specifically: SQLite is pytest-only, a fresh
# empty file per test, so that path must keep creating its tables every call.
_COMMENT_TABLES_OK = False
_COMMENT_FLAGS_TABLE_OK = False
_COMMENT_ATTACHMENTS_TABLE_OK = False
_DEALER_RATINGS_TABLE_OK = False


def ensure_comment_tables(cur: Any) -> None:
    """Create both comment tables + indexes if absent (idempotent, no commit)."""
    global _COMMENT_TABLES_OK
    postgres = inventory_pg.is_inventory_postgres()
    if postgres and _COMMENT_TABLES_OK:
        return
    for table, subject_col in _SCOPES.values():
        cur.execute(_ddl_comments(table, subject_col, postgres=postgres))
        for name, columns, where in _comment_indexes(table, subject_col):
            cur.execute(_create_index_sql(name, table, columns, where))
    if postgres:
        _COMMENT_TABLES_OK = True


def ensure_comment_flags_table(cur: Any) -> None:
    """Create the ``comment_flags`` dedup table if absent (idempotent, no commit)."""
    global _COMMENT_FLAGS_TABLE_OK
    postgres = inventory_pg.is_inventory_postgres()
    if postgres and _COMMENT_FLAGS_TABLE_OK:
        return
    cur.execute(_DDL_COMMENT_FLAGS_PG if postgres else _DDL_COMMENT_FLAGS_SQLITE)
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_comment_flags_comment ON comment_flags (scope, comment_id)"
    )
    if postgres:
        _COMMENT_FLAGS_TABLE_OK = True


def ensure_comment_attachments_table(cur: Any) -> None:
    """Create ``comment_attachments`` if absent (idempotent, no commit)."""
    global _COMMENT_ATTACHMENTS_TABLE_OK
    postgres = inventory_pg.is_inventory_postgres()
    if postgres and _COMMENT_ATTACHMENTS_TABLE_OK:
        return
    cur.execute(_DDL_COMMENT_ATTACHMENTS_PG if postgres else _DDL_COMMENT_ATTACHMENTS_SQLITE)
    for name, columns, where in _ATTACHMENT_INDEXES:
        cur.execute(_create_index_sql(name, "comment_attachments", columns, where))
    if postgres:
        _COMMENT_ATTACHMENTS_TABLE_OK = True


def ensure_dealer_ratings_table(cur: Any) -> None:
    """Create ``dealer_ratings`` if absent (idempotent, no commit)."""
    global _DEALER_RATINGS_TABLE_OK
    postgres = inventory_pg.is_inventory_postgres()
    if postgres and _DEALER_RATINGS_TABLE_OK:
        return
    cur.execute(_DDL_RATINGS_PG if postgres else _DDL_RATINGS_SQLITE)
    for name, columns, where in _RATING_INDEXES:
        cur.execute(_create_index_sql(name, "dealer_ratings", columns, where))
    if postgres:
        _DEALER_RATINGS_TABLE_OK = True


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
    """Timestamps read back as ``datetime`` on Postgres, ``str`` on SQLite."""
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
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
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
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
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
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        ensure_comment_tables(cur)
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
        ensure_comment_tables(cur)
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
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        ensure_comment_tables(cur)
        if reporter_hash:
            ensure_comment_flags_table(cur)
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
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO comment_flags (scope, comment_id, ip_hash, created_at) "
                "VALUES (?, ?, ?, ?) ON CONFLICT (scope, comment_id, ip_hash) DO NOTHING",
                (scope, int(comment_id), reporter_hash, now),
            )
            if not cur.rowcount:
                # Already reported by this actor — current state, no bump.
                conn.commit()
                return _row_to_comment(current, scope)
            cur = conn.cursor()
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
# Photo attachments
#
# The bytes are on local disk (backend/utils/comment_images.py); these rows are
# what survives a restart and what the serving route resolves an id against.
# ``storage_key`` stays inside the server -- backend/routes/community_api.py
# turns a row into ``{"id", "url", "width", "height"}`` and nothing else, so the
# on-disk layout is never a fact a client knows.
# ---------------------------------------------------------------------------

_ATTACHMENT_COLUMNS = (
    "id, scope, comment_id, user_id, storage_key, mime_type, width, height, "
    "byte_size, sort_order, created_at"
)


def _row_to_attachment(row: Any) -> dict[str, Any]:
    d = dict(row)
    return {
        "id": int(d["id"]),
        "scope": d["scope"],
        "comment_id": int(d["comment_id"]),
        "user_id": int(d["user_id"]),
        "storage_key": d["storage_key"],
        "mime_type": d["mime_type"],
        "width": int(d["width"] or 0),
        "height": int(d["height"] or 0),
        "byte_size": int(d["byte_size"] or 0),
        "sort_order": int(d["sort_order"] or 0),
        "created_at": _iso(d.get("created_at")),
    }


def list_attachments(scope: str, comment_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    """``{comment_id: [attachment, ...]}`` for a page of comments, in one query.

    Batched because the alternative is a query per row on every thread render.
    Returns ``{}`` rather than raising when the table does not exist yet, the
    same way :func:`list_comments` treats a database that has never had a
    comment: a car page is not an error just because nobody has uploaded a photo.
    """
    ids = [int(c) for c in comment_ids or []]
    if not ids:
        return {}
    _scope_spec(scope)  # validate
    placeholders = ", ".join("?" for _ in ids)
    out: dict[int, list[dict[str, Any]]] = {}
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT {_ATTACHMENT_COLUMNS}
            FROM comment_attachments
            WHERE scope = ? AND deleted_at IS NULL AND comment_id IN ({placeholders})
            ORDER BY comment_id ASC, sort_order ASC, id ASC
            """,
            (scope, *ids),
        )
        for row in cur.fetchall():
            att = _row_to_attachment(row)
            out.setdefault(att["comment_id"], []).append(att)
        return out
    except Exception:
        return {}
    finally:
        conn.close()


def attach_attachments(scope: str, comments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Add an ``attachments`` list to each serialized comment (in place)."""
    by_comment = list_attachments(scope, [c["id"] for c in comments])
    for c in comments:
        c["attachments"] = by_comment.get(c["id"], [])
    return comments


def get_attachment(attachment_id: int) -> dict[str, Any] | None:
    """One live attachment by id -- what the serving route looks a URL up in.

    Soft-deleted rows read as missing so a deleted comment's photos stop being
    reachable by anyone who kept the URL.
    """
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT {_ATTACHMENT_COLUMNS} FROM comment_attachments "
            "WHERE id = ? AND deleted_at IS NULL",
            (int(attachment_id),),
        )
        row = cur.fetchone()
        return _row_to_attachment(row) if row else None
    except Exception:
        return None
    finally:
        conn.close()


def add_attachments(
    scope: str, comment_id: int, user_id: int, items: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Record already-written files against a comment, in the order given.

    ``items`` carry ``storage_key``/``mime_type``/``width``/``height``/
    ``byte_size`` from :mod:`backend.utils.comment_images`. Every row commits
    together: a half-attached comment would render with photos missing and no
    way to tell that from the user having attached fewer.
    """
    _scope_spec(scope)  # validate
    if not items:
        return []
    now = _now_iso()
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    out: list[dict[str, Any]] = []
    try:
        cur = conn.cursor()
        ensure_comment_attachments_table(cur)
        for order, item in enumerate(items):
            cur.execute(
                f"""
                INSERT INTO comment_attachments (
                    scope, comment_id, user_id, storage_key, mime_type,
                    width, height, byte_size, sort_order, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING {_ATTACHMENT_COLUMNS}
                """,
                (
                    scope,
                    int(comment_id),
                    int(user_id),
                    item["storage_key"],
                    item["mime_type"],
                    int(item["width"]),
                    int(item["height"]),
                    int(item["byte_size"]),
                    order,
                    now,
                ),
            )
            out.append(_row_to_attachment(cur.fetchone()))
        conn.commit()
        return out
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def delete_attachments_for_comment(scope: str, comment_id: int) -> list[str]:
    """Soft-delete a comment's attachment rows; return the keys to unlink.

    Rows are kept (the comment itself is only soft-deleted) but the files are
    the caller's to remove -- returning the keys rather than deleting here keeps
    this module free of filesystem concerns, and an unlink that fails must not
    roll back the row that already stopped serving.
    """
    _scope_spec(scope)  # validate
    now = _now_iso()
    conn = get_conn()
    try:
        cur = conn.cursor()
        ensure_comment_attachments_table(cur)
        cur.execute(
            """
            UPDATE comment_attachments SET deleted_at = ?
            WHERE scope = ? AND comment_id = ? AND deleted_at IS NULL
            RETURNING storage_key
            """,
            (now, scope, int(comment_id)),
        )
        keys = [str(r[0]) for r in cur.fetchall()]
        conn.commit()
        return keys
    except Exception:
        conn.rollback()
        return []
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


# ---------------------------------------------------------------------------
# Dealer ratings (Google Places cache)
# ---------------------------------------------------------------------------

def upsert_dealer_rating(
    dealership_id: int,
    *,
    place_id: str | None,
    rating: float | None,
    review_count: int | None,
    source: str = "google_places",
    fetched_at: str | None = None,
) -> None:
    """Store the current rating for a dealership (insert or replace in place)."""
    now = fetched_at or _now_iso()
    conn = get_conn()
    try:
        cur = conn.cursor()
        ensure_dealer_ratings_table(cur)
        cur.execute(
            """
            INSERT INTO dealer_ratings (
                dealership_id, google_place_id, google_rating, google_review_count,
                fetched_at, source
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (dealership_id) DO UPDATE SET
                google_place_id = EXCLUDED.google_place_id,
                google_rating = EXCLUDED.google_rating,
                google_review_count = EXCLUDED.google_review_count,
                fetched_at = EXCLUDED.fetched_at,
                source = EXCLUDED.source
            """,
            (
                int(dealership_id),
                (place_id or "").strip() or None,
                rating,
                review_count,
                now,
                source,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_dealer_rating(dealership_id: int) -> dict[str, Any] | None:
    """Cached Google rating for one dealership, or ``None`` if never fetched."""
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT dealership_id, google_place_id, google_rating, google_review_count,
                   fetched_at, source
            FROM dealer_ratings WHERE dealership_id = ?
            """,
            (int(dealership_id),),
        )
        row = cur.fetchone()
        if not row:
            return None
        d = dict(row)
        raw_rating = d.get("google_rating")
        return {
            "dealership_id": int(d["dealership_id"]),
            "google_place_id": d.get("google_place_id"),
            "google_rating": float(raw_rating) if raw_rating is not None else None,
            "google_review_count": (
                int(d["google_review_count"]) if d.get("google_review_count") is not None else None
            ),
            "fetched_at": _iso(d.get("fetched_at")),
            "source": d.get("source"),
        }
    except Exception:
        return None
    finally:
        conn.close()


def list_active_dealership_websites() -> list[dict[str, Any]]:
    """Every active rooftop's id, website columns and coordinates.

    The rating importer folds this into "which domains does more than one
    rooftop share" (echopark.com, carmax.com): on those, matching a website
    proves only that Google found the right *group*, so it also needs each
    sibling's position to work out which store a hit belongs to. Read once per
    run, not once per dealer — it is a few hundred short rows.
    """
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, website_url, dealer_website_url, latitude, longitude
            FROM dealerships
            WHERE is_active = 1 AND duplicate_of_id IS NULL
            """
        )
        return [dict(r) for r in cur.fetchall()]
    except Exception:
        # No registry table (fresh test DB) means no shared domains to guard.
        return []
    finally:
        conn.close()


def list_dealerships_needing_rating(
    *, limit: int = 25, refresh_after_days: int | None = None
) -> list[dict[str, Any]]:
    """The importer's work queue: active registry rows with no (or stale) rating.

    ``refresh_after_days=None`` means "never refresh what we already have" --
    the cheapest mode, and the default, because Google Places bills per call.
    """
    cap = max(1, int(limit))
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        ensure_dealer_ratings_table(cur)
        params: list[Any] = []
        if refresh_after_days is None:
            freshness = "r.dealership_id IS NULL"
        else:
            cutoff = (
                datetime.now(timezone.utc) - timedelta(days=max(1, int(refresh_after_days)))
            ).isoformat()
            freshness = "(r.dealership_id IS NULL OR r.fetched_at < ?)"
            params.append(cutoff)
        params.append(cap)
        cur.execute(
            f"""
            SELECT d.id, d.name, d.city, d.state, d.latitude, d.longitude,
                   d.website_url, d.dealer_website_url,
                   COALESCE(r.google_place_id, d.google_place_id) AS google_place_id
            FROM dealerships d
            LEFT JOIN dealer_ratings r ON r.dealership_id = d.id
            WHERE d.is_active = 1
              AND d.duplicate_of_id IS NULL
              AND {freshness}
            ORDER BY d.id ASC
            LIMIT ?
            """,
            tuple(params),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
