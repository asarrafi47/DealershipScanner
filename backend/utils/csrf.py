"""Double-submit CSRF: session token + form field or X-CSRF-Token header."""

from __future__ import annotations

import secrets

from flask import Response, abort, redirect, request, session, url_for
from markupsafe import escape

_SESSION_KEY = "_csrf_token"


def ensure_csrf_token() -> str:
    cur = session.get(_SESSION_KEY)
    if isinstance(cur, str) and len(cur) >= 32:
        return cur
    t = secrets.token_urlsafe(32)
    session[_SESSION_KEY] = t
    return t


class LazyCsrfToken:
    """The template global ``csrf_token``, minted only when a template renders it.

    The context processor used to call :func:`ensure_csrf_token` for every rendered
    template, so every anonymous page wrote the session and sent a ``session`` cookie,
    which made no HTML page cacheable at the edge (perf 2026-11-20). This object mints
    (and stores in the session) on first use: ``{{ csrf_token }}`` in a form field or in
    ``<meta name="csrf-token">`` works exactly as before, and a page that never renders
    it leaves the session untouched. One instance per render context; the value is
    memoised so a page that renders it twice gets the same token.
    """

    __slots__ = ("_value",)

    def __init__(self) -> None:
        self._value: str | None = None

    def _get(self) -> str:
        if self._value is None:
            self._value = ensure_csrf_token()
        return self._value

    def __str__(self) -> str:
        return self._get()

    def __html__(self) -> str:
        return str(escape(self._get()))

    def __bool__(self) -> bool:
        return True

    def __len__(self) -> int:
        return len(self._get())

    def __eq__(self, other: object) -> bool:
        if isinstance(other, LazyCsrfToken):
            other = str(other)
        return self._get() == other

    def __hash__(self) -> int:
        return hash(self._get())

    def __repr__(self) -> str:
        return "LazyCsrfToken(minted)" if self._value is not None else "LazyCsrfToken(unminted)"


def validate_csrf_form() -> Response | None:
    """Return a redirect response on login CSRF failure; otherwise abort(403) or return None."""
    expected = session.get(_SESSION_KEY)
    supplied = (request.form.get("csrf_token") or "").strip()
    if not expected or not supplied or not secrets.compare_digest(supplied.encode(), expected.encode()):
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
    if not secrets.compare_digest(supplied.encode(), expected.encode()):
        abort(403)
