"""Double-submit CSRF: session token + form field or X-CSRF-Token header."""

from __future__ import annotations

import secrets

from flask import Response, abort, redirect, request, session, url_for

_SESSION_KEY = "_csrf_token"


def ensure_csrf_token() -> str:
    cur = session.get(_SESSION_KEY)
    if isinstance(cur, str) and len(cur) >= 32:
        return cur
    t = secrets.token_urlsafe(32)
    session[_SESSION_KEY] = t
    return t


def validate_csrf_form() -> Response | None:
    """Return a redirect response on login CSRF failure; otherwise abort(403) or return None."""
    expected = session.get(_SESSION_KEY)
    supplied = (request.form.get("csrf_token") or "").strip()
    if not expected or not supplied or not secrets.compare_digest(supplied, expected):
        ep = request.endpoint or ""
        if ep in ("dev.admin_login", "dev.admin_register"):
            return redirect(url_for("dev.admin_login", _error="session_expired"))
        if "login" in ep:
            return redirect(url_for("login_page", _error="session_expired"))
        abort(403)
    return None


def validate_csrf_header(*_args: object, **_kwargs: object) -> None:
    """Header must match session token (extra positional/keyword args ignored; legacy callers passed ``request``)."""
    expected = session.get(_SESSION_KEY)
    supplied = (request.headers.get("X-CSRF-Token") or "").strip()
    if not expected or not supplied:
        abort(403)
    if not secrets.compare_digest(supplied, expected):
        abort(403)
