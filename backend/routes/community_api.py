"""JSON endpoints for car and dealership comments, plus the cached Google rating.

    GET    /api/cars/<car_id>/comments
    POST   /api/cars/<car_id>/comments
    DELETE /api/cars/<car_id>/comments/<comment_id>
    POST   /api/cars/<car_id>/comments/<comment_id>/flag

    GET    /api/dealerships/<dealership_id>/comments
    POST   /api/dealerships/<dealership_id>/comments
    DELETE /api/dealerships/<dealership_id>/comments/<comment_id>
    POST   /api/dealerships/<dealership_id>/comments/<comment_id>/flag

    GET    /api/dealerships/<dealership_id>/rating

    GET    /comment-images/<attachment_id>

Reading is public; writing requires a signed-in user (``session['user_id']``,
the same check as ``listings_api.api_saved_cars``).

CSRF is enforced *in this module* rather than through ``backend/main.py``'s
``_csrf_mutating_requests`` hook, which matches endpoints against a hardcoded
list and does not cover DELETE at all. Clients must send ``X-CSRF-Token``
(from ``GET /api/auth/csrf`` or the ``csrf_token`` template global) on every
POST and DELETE here.

Three independent limits sit in front of a write, because this is the only
public surface in the app where an authenticated user creates unbounded rows:

1. per-IP volume (``ip_rate_limit.allow_request``) — bounds an abusive host,
2. per-user volume in SQL (``comments_db.count_recent_comments_by_user``) —
   survives a worker restart, which the in-process limiter does not,
3. body validation in ``comments_db.sanitize_body``.

Text is stored as typed and returned as both ``body`` (plain) and ``body_html``
(escaped). Nothing here ever renders or stores user-supplied HTML.

Two properties the JSON contract owes the reader, both enforced here rather than
in ``comments_db`` (which is the persistence layer and keeps the real columns):

* **Authors are anonymous.** A serialized comment carries ``is_mine``, never the
  author's ``user_id``. The UI labels everyone "Shopper"; shipping a stable id
  alongside that would let any reader of the public GET stitch one person's
  comments together across every car and dealership thread.
* **A report counts once per reporter.** ``flag_comment`` is given the caller's
  salted ``ip_hash``, so replaying the flag request cannot drive a comment past
  ``comments_db.FLAG_HIDE_THRESHOLD`` on its own.

Photo attachments
-----------------

A POST may be ``multipart/form-data`` instead of JSON, with up to
:data:`comment_images.MAX_ATTACHMENTS_PER_COMMENT` files under ``images``. Auth
is unchanged -- same login requirement, same ``X-CSRF-Token`` header (the widget
sends it explicitly, so multipart needs nothing new) -- and every decision about
what a file is allowed to be lives in :mod:`backend.utils.comment_images`, which
decodes and re-encodes rather than trusting a filename or a client Content-Type.

Two properties this module owns:

* **Uploads never reach a filesystem path a client chose.** The stored name is
  minted server-side and the serving route takes an integer id, not a path
  segment, so ``storage_key`` only ever comes back out of the database.
* **The body cap is raised for this endpoint alone.** ``MAX_CONTENT_LENGTH`` is
  9 MB app-wide, which four 8 MB photos exceed. ``request.max_content_length``
  is raised per request, only when the request is actually multipart, and only
  before the body is first touched.
"""

from __future__ import annotations

import os

from flask import Response, abort, jsonify, request, send_file, session, url_for

from backend.db import comments_db
from backend.db.comments_db import SCOPE_CAR, SCOPE_DEALER, CommentError
from backend.routes._shared import _client_ip
from backend.utils import comment_images
from backend.utils.comment_images import AttachmentError
from backend.utils.csrf import validate_csrf_header
from backend.utils.ip_rate_limit import allow_request

_DEFAULT_COMMENT_POST_RPM = 6
_DEFAULT_COMMENT_FLAG_RPM = 20


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return max(1, int(raw.split()[0]))
    except (TypeError, ValueError, IndexError):
        return default


def _ip_hash() -> str | None:
    from backend.reviews.guards import hash_ip

    return hash_ip(_client_ip())


def _require_user() -> tuple[int | None, tuple]:
    """``(user_id, ())`` when signed in, ``(None, (response, 401))`` when not."""
    uid = session.get("user_id")
    if not uid:
        return None, (jsonify({"ok": False, "error": "not_logged_in"}), 401)
    try:
        return int(uid), ()
    except (TypeError, ValueError):
        return None, (jsonify({"ok": False, "error": "not_logged_in"}), 401)


def _public_comment(comment: dict, viewer_id: int | None) -> dict:
    """Strip the author's ``user_id`` and replace it with ``is_mine``.

    Comments render as an anonymous "Shopper" (frontend/static/ds_comments.js),
    so shipping the raw ``users.id`` would undo that: it is stable across every
    car and dealership thread, so anyone reading the public list endpoint could
    group a person's whole comment history and cross-reference it with the
    ``user_id`` other surfaces expose. The client only ever needed the one bit it
    was computing from it -- whether to draw Delete or Report.
    """
    out = dict(comment)
    author = out.pop("user_id", None)
    out["is_mine"] = viewer_id is not None and author is not None and int(author) == int(viewer_id)
    out["attachments"] = [_public_attachment(a) for a in (comment.get("attachments") or [])]
    return out


def _public_attachment(att: dict) -> dict:
    """The four fields a client needs, and nothing that describes the disk.

    ``storage_key`` stays server-side: it is the only value that maps to a real
    path, and the URL is built from the row id instead, so the serving route has
    no path segment to defend. ``width``/``height`` are the re-encoded file's, so
    the widget can reserve layout space before the image loads.
    """
    aid = int(att["id"])
    return {
        "id": aid,
        "url": url_for("serve_comment_image", attachment_id=aid),
        "width": int(att.get("width") or 0),
        "height": int(att.get("height") or 0),
    }


def _viewer_id() -> int | None:
    try:
        uid = session.get("user_id")
        return int(uid) if uid else None
    except (TypeError, ValueError):
        return None


def _json_body() -> dict:
    data = request.get_json(silent=True)
    if isinstance(data, dict):
        return data
    # Tolerate a plain form post so a no-JS fallback form still works. This is
    # also the multipart path: the comment text arrives as an ordinary field
    # next to the ``images`` parts.
    return {k: v for k, v in request.form.items()}


def _raise_body_cap_for_uploads() -> None:
    """Let a multipart comment POST carry photos past the 9 MB app-wide cap.

    ``MAX_CONTENT_LENGTH`` is set once for the whole app in ``backend/main.py``
    and is smaller than four maximum-size photos, so an upload would be answered
    413 before any of this module's validation ran. Werkzeug applies the limit
    when the body is first read, so raising it here -- per request, only for a
    multipart POST, and before ``request.form``/``request.files`` are touched --
    widens exactly this endpoint and leaves every other route at the app cap.
    """
    if request.mimetype == "multipart/form-data":
        request.max_content_length = comment_images.MAX_REQUEST_BYTES


def _uploaded_images() -> list:
    """Files posted under ``images`` (or ``images[]``), capped by count.

    The count check happens before anything is decoded so a caller cannot make
    the server do 50 decodes to learn it may only send four.
    """
    if request.mimetype != "multipart/form-data":
        return []
    files = request.files.getlist("images") + request.files.getlist("images[]")
    if len(files) > comment_images.MAX_ATTACHMENTS_PER_COMMENT:
        raise AttachmentError(
            "too_many_images",
            f"Attach at most {comment_images.MAX_ATTACHMENTS_PER_COMMENT} photos per comment.",
        )
    return files


def _list_comments(scope: str, subject_id: int):
    limit = _env_int("COMMENTS_PAGE_SIZE", 100)
    try:
        offset = max(0, int(request.args.get("offset") or 0))
    except (TypeError, ValueError):
        offset = 0
    viewer = _viewer_id()
    comments = [
        _public_comment(c, viewer)
        for c in comments_db.list_comments(scope, subject_id, limit=limit, offset=offset)
    ]
    return jsonify(
        {
            "ok": True,
            "scope": scope,
            "subject_id": subject_id,
            "count": comments_db.count_comments(scope, subject_id),
            "offset": offset,
            "comments": comments,
            # So the UI can grey out the composer without a second request.
            "can_post": bool(session.get("user_id")),
        }
    )


def _create_comment(scope: str, subject_id: int):
    ip = _client_ip()
    rpm = _env_int("COMMENT_POST_RPM", _DEFAULT_COMMENT_POST_RPM)
    if not allow_request(f"comment_post:{ip}", max_events=rpm, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    uid, err = _require_user()
    if uid is None:
        return err
    validate_csrf_header()
    # Before the first read of the body, and after the auth checks that need no
    # body at all -- an anonymous caller never gets the larger cap.
    _raise_body_cap_for_uploads()

    if not comments_db.subject_exists(scope, subject_id):
        return jsonify({"ok": False, "error": "not_found"}), 404

    posted = comments_db.count_recent_comments_by_user(uid)
    if posted >= _env_int("COMMENT_USER_HOURLY_CAP", comments_db.USER_POST_LIMIT):
        return jsonify({"ok": False, "error": "too_many_comments"}), 429

    # Photos are validated and re-encoded before the comment row exists, so a
    # rejected upload leaves nothing behind -- not a bodyless comment, not a
    # thread entry whose photos silently went missing.
    try:
        body = _json_body().get("body")
        prepared = comment_images.prepare_uploads(_uploaded_images())
    except AttachmentError as exc:
        return jsonify({"ok": False, "error": exc.code, "message": exc.message}), 400

    try:
        comment = comments_db.create_comment(
            scope, subject_id, uid, body, ip_hash=_ip_hash()
        )
    except CommentError as exc:
        return jsonify({"ok": False, "error": exc.code, "message": exc.message}), 400

    comment["attachments"] = _persist_attachments(scope, comment["id"], uid, prepared)
    return jsonify({"ok": True, "comment": _public_comment(comment, uid)}), 201


def _persist_attachments(scope: str, comment_id: int, uid: int, prepared: list) -> list:
    """Write the files, then the rows; unwind the files if the rows do not land.

    Files first because a row pointing at a missing file renders as a broken
    image on every future page view, while a file with no row is invisible and
    removable. If the INSERT fails the files are unlinked here, so the failure
    mode is "the comment posted without its photos", not orphaned disk.
    """
    if not prepared:
        return []
    written: list[str] = []
    try:
        for item in prepared:
            comment_images.store(item)
            written.append(item.storage_key)
        return comments_db.add_attachments(
            scope,
            comment_id,
            uid,
            [
                {
                    "storage_key": p.storage_key,
                    "mime_type": p.mime_type,
                    "width": p.width,
                    "height": p.height,
                    "byte_size": p.byte_size,
                }
                for p in prepared
            ],
        )
    except Exception:
        for key in written:
            comment_images.discard(key)
        return []


def _delete_comment(scope: str, subject_id: int, comment_id: int):
    uid, err = _require_user()
    if uid is None:
        return err
    validate_csrf_header()

    existing = comments_db.get_comment(scope, comment_id)
    if not existing or existing["subject_id"] != subject_id:
        return jsonify({"ok": False, "error": "not_found"}), 404

    outcome = comments_db.delete_own_comment(scope, comment_id, uid)
    if outcome == "deleted":
        # Only after the ownership-checked UPDATE succeeded: the same request
        # that removes the comment removes its photos, so a deleted comment's
        # images stop resolving even for someone who kept the URL.
        for key in comments_db.delete_attachments_for_comment(scope, comment_id):
            comment_images.discard(key)
        return jsonify({"ok": True, "deleted": comment_id})
    if outcome == "forbidden":
        return jsonify({"ok": False, "error": "not_your_comment"}), 403
    return jsonify({"ok": False, "error": "not_found"}), 404


def _flag_comment(scope: str, subject_id: int, comment_id: int):
    """Flagging needs no login (an unregistered reader still sees the abuse), so
    the count is deduplicated per reporter and capped harder per IP than posting.

    The rate limit alone is not a defence. ``comments_db.FLAG_HIDE_THRESHOLD`` is
    3 and the limiter allows 20 requests a minute, so before the ``ip_hash`` was
    threaded through, replaying this request three times removed any comment on
    the site from every reader. ``flag_comment`` now counts each reporter once,
    the same guarantee ``review_reports`` gives dealer reviews.
    """
    ip = _client_ip()
    rpm = _env_int("COMMENT_FLAG_RPM", _DEFAULT_COMMENT_FLAG_RPM)
    if not allow_request(f"comment_flag:{ip}", max_events=rpm, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429
    validate_csrf_header()

    existing = comments_db.get_comment(scope, comment_id)
    if not existing or existing["subject_id"] != subject_id:
        return jsonify({"ok": False, "error": "not_found"}), 404

    reporter = _ip_hash()
    if not reporter:
        # No identifiable reporter means no way to count them once. Accept the
        # report so the caller cannot probe for it, but do not bump the count --
        # an unattributable flag is exactly the unlimited path this closes.
        return jsonify(
            {
                "ok": True,
                "flag_count": existing["flag_count"],
                "is_hidden": existing["is_hidden"],
            }
        )

    updated = comments_db.flag_comment(scope, comment_id, reporter_hash=reporter)
    if not updated:
        return jsonify({"ok": False, "error": "not_found"}), 404
    return jsonify(
        {"ok": True, "flag_count": updated["flag_count"], "is_hidden": updated["is_hidden"]}
    )


# --- car comments ----------------------------------------------------------

def api_car_comments(car_id: int):
    if request.method == "POST":
        return _create_comment(SCOPE_CAR, int(car_id))
    return _list_comments(SCOPE_CAR, int(car_id))


def api_car_comment_delete(car_id: int, comment_id: int):
    return _delete_comment(SCOPE_CAR, int(car_id), int(comment_id))


def api_car_comment_flag(car_id: int, comment_id: int):
    return _flag_comment(SCOPE_CAR, int(car_id), int(comment_id))


# --- dealership comments ---------------------------------------------------

def api_dealer_comments(dealership_id: int):
    if request.method == "POST":
        return _create_comment(SCOPE_DEALER, int(dealership_id))
    return _list_comments(SCOPE_DEALER, int(dealership_id))


def api_dealer_comment_delete(dealership_id: int, comment_id: int):
    return _delete_comment(SCOPE_DEALER, int(dealership_id), int(comment_id))


def api_dealer_comment_flag(dealership_id: int, comment_id: int):
    return _flag_comment(SCOPE_DEALER, int(dealership_id), int(comment_id))


# --- comment photos --------------------------------------------------------

def serve_comment_image(attachment_id: int):
    """Serve one stored comment photo.

    Traversal is not defended against here; it is designed out. The URL carries
    an integer id and nothing else -- ``<int:attachment_id>`` will not match a
    request containing ``..`` or a slash at all -- and the filesystem path comes
    from :func:`comment_images.resolve_stored_path`, which re-checks the stored
    key against its ``<2 hex>/<32 hex>.<ext>`` pattern and re-checks containment
    in the upload directory. No user-controlled string is ever joined to a path.

    Reading is public, like the comment thread itself. The content is immutable
    (a new upload is a new id), so it is cached hard and served ``nosniff`` with
    a Content-Type drawn from the closed set in ``comment_images``.
    """
    att = comments_db.get_attachment(int(attachment_id))
    if not att:
        abort(404)
    if att.get("mime_type") not in comment_images.ALLOWED_MIME_TYPES:
        abort(404)
    path = comment_images.resolve_stored_path(att.get("storage_key"))
    if path is None or not path.is_file():
        abort(404)
    resp: Response = send_file(
        path,
        mimetype=att["mime_type"],
        conditional=True,
        max_age=31536000,
    )
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["Content-Disposition"] = "inline"
    return resp


# --- dealership rating -----------------------------------------------------

def api_dealer_rating(dealership_id: int):
    """Cached Google star rating. Read-only: never calls Google on a page view.

    ``rating: null`` means the importer has not run for this dealer (or Google
    had nothing) — the UI should show no stars at all rather than zero stars.
    """
    rating = comments_db.get_dealer_rating(int(dealership_id))
    if not rating:
        return jsonify(
            {
                "ok": True,
                "dealership_id": int(dealership_id),
                "rating": None,
                "review_count": None,
                "fetched_at": None,
                "source": None,
            }
        )
    return jsonify(
        {
            "ok": True,
            "dealership_id": rating["dealership_id"],
            "rating": rating["google_rating"],
            "review_count": rating["google_review_count"],
            "fetched_at": rating["fetched_at"],
            "source": rating["source"],
            "google_place_id": rating["google_place_id"],
        }
    )


def register(app) -> None:
    """Attach the community routes (additive; bare endpoint names)."""
    app.add_url_rule(
        "/api/cars/<int:car_id>/comments",
        endpoint="api_car_comments",
        view_func=api_car_comments,
        methods=["GET", "POST"],
    )
    app.add_url_rule(
        "/api/cars/<int:car_id>/comments/<int:comment_id>",
        endpoint="api_car_comment_delete",
        view_func=api_car_comment_delete,
        methods=["DELETE"],
    )
    app.add_url_rule(
        "/api/cars/<int:car_id>/comments/<int:comment_id>/flag",
        endpoint="api_car_comment_flag",
        view_func=api_car_comment_flag,
        methods=["POST"],
    )
    app.add_url_rule(
        "/api/dealerships/<int:dealership_id>/comments",
        endpoint="api_dealer_comments",
        view_func=api_dealer_comments,
        methods=["GET", "POST"],
    )
    app.add_url_rule(
        "/api/dealerships/<int:dealership_id>/comments/<int:comment_id>",
        endpoint="api_dealer_comment_delete",
        view_func=api_dealer_comment_delete,
        methods=["DELETE"],
    )
    app.add_url_rule(
        "/api/dealerships/<int:dealership_id>/comments/<int:comment_id>/flag",
        endpoint="api_dealer_comment_flag",
        view_func=api_dealer_comment_flag,
        methods=["POST"],
    )
    app.add_url_rule(
        "/api/dealerships/<int:dealership_id>/rating",
        endpoint="api_dealer_rating",
        view_func=api_dealer_rating,
        methods=["GET"],
    )
    # Integer converter on purpose: the route has no path segment a caller can
    # steer, so there is nothing for a traversal payload to reach.
    app.add_url_rule(
        "/comment-images/<int:attachment_id>",
        endpoint="serve_comment_image",
        view_func=serve_comment_image,
        methods=["GET"],
    )
