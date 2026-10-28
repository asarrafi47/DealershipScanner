"""Photo-attachment rows for comments (``comment_attachments``).

Split out of :mod:`backend.db.comments_db`, which re-exports every public name
here; callers keep importing from ``comments_db``.
"""

from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row

from backend.db.comments_db import _iso, _now_iso, _scope_spec
from backend.db.inventory_db import get_conn


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
    try:
        cur = conn.cursor(row_factory=dict_row)
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
    try:
        cur = conn.cursor(row_factory=dict_row)
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
    out: list[dict[str, Any]] = []
    try:
        cur = conn.cursor(row_factory=dict_row)
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
