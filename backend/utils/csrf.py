"""Double-submit CSRF: session token + form field or X-CSRF-Token header."""

from __future__ import annotations

import secrets

from flask import abort, request, session

_SESSION_KEY = "_csrf_token"


def ensure_csrf_token() -> str:
    cur = session.get(_SESSION_KEY)
    if isinstance(cur, str) and len(cur) >= 32:
        return cur
    t = secrets.token_urlsafe(32)
    session[_SESSION_KEY] = t
    return t


def validate_csrf_form() -> None:
    expected = session.get(_SESSION_KEY)
    supplied = (request.form.get("csrf_token") or "").strip()
    if not expected or not supplied:
        abort(403)
    if not secrets.compare_digest(supplied, expected):
        abort(403)


def validate_csrf_header() -> None:
    expected = session.get(_SESSION_KEY)
    supplied = (request.headers.get("X-CSRF-Token") or "").strip()
    if not expected or not supplied:
        abort(403)
    if not secrets.compare_digest(supplied, expected):
        abort(403)
