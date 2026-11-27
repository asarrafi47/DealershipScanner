"""Per-request scope for the web app: ``Server-Timing`` and inventory connection reuse.

:func:`register_request_scope` wraps ``app.wsgi_app`` (outermost, so ``app`` covers every
before/after hook) in a middleware that, for each request:

1. enters the inventory request scope (``inventory_pg.begin_request_scope``): sequential
   ``get_conn()`` calls on this thread share one Postgres connection, and the opt-in
   ``WEB_PG_STATEMENT_TIMEOUT_MS`` applies (see the block comment in ``inventory_pg``);
2. starts the duration accumulator (``backend.utils.request_timing``) and, when the
   response starts, adds ``Server-Timing`` to HTML and JSON responses: ``app`` (wall time to
   the first byte), ``db`` (inventory statements), ``db_connect`` (Postgres connects),
   ``users_db`` (SQLCipher users-DB opens) and ``render`` (Jinja), each with its count;
3. leaves both in Flask's teardown (after a ``stream_with_context`` body has streamed),
   when the response body is closed, or on error, whichever comes first.

Only durations and counts are sent: no SQL text, no parameters, no host names. Nothing
here may fail a request: every step is guarded.

Environment: ``SERVER_TIMING=0`` drops the header (and the accumulator);
``INVENTORY_REQUEST_CONN_REUSE=0`` keeps connect-per-call.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any, Callable, Iterable

from backend.db import inventory_pg
from backend.utils import request_timing

_log = logging.getLogger(__name__)

_TIMED_TYPES = ("text/html", "application/json")

# A view sets ``request.environ[NO_SERVER_TIMING_KEY] = True`` to keep the header off its answer (the
# phone sign-in routes do, so connection counts cannot tell a known number from an unknown one).
NO_SERVER_TIMING_KEY = "ds.no_server_timing"


def server_timing_enabled() -> bool:
    raw = (os.environ.get("SERVER_TIMING") or "").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _shared_cacheable(headers: list[tuple[str, str]]) -> bool:
    """True for a response a CDN may store (``public`` / ``s-maxage``).

    Such a response gets no ``Server-Timing``: an edge cache would replay one request's
    timings to every visitor (wrong numbers, and a cleaner timing oracle). Review 2026-11-20.
    """
    for k, v in headers:
        if k.lower() == "cache-control":
            cc = v.lower()
            return "public" in cc or "s-maxage" in cc
    return False


def _content_type(headers: list[tuple[str, str]]) -> str:
    for k, v in headers:
        if k.lower() == "content-type":
            return v.split(";", 1)[0].strip().lower()
    return ""


class _ClosingBody:
    """Iterates the app's body, then runs ``on_close`` exactly once from ``close()``."""

    def __init__(self, body: Iterable[bytes], on_close: Callable[[], None]):
        self._body = body
        self._on_close = on_close

    def __iter__(self):
        return iter(self._body)

    def close(self) -> None:
        try:
            close = getattr(self._body, "close", None)
            if close is not None:
                close()
        finally:
            cb, self._on_close = self._on_close, None
            if cb is not None:
                cb()


# The request that owns this thread's scope (a fresh object per request). Leaving is
# token-checked, so a late close() of an earlier response body (a test client that
# never closed it, collected during a later request) cannot end the current scope.
_owner = threading.local()


def _end_scope() -> None:
    _owner.token = None
    try:
        inventory_pg.end_request_scope()
    except Exception:  # noqa: BLE001
        _log.debug("end_request_scope failed", exc_info=True)
    request_timing.end()


def leave_current_scope() -> None:
    """End this thread's request scope now (the Flask teardown hook calls it)."""
    if getattr(_owner, "token", None) is not None:
        _end_scope()


class RequestScopeMiddleware:
    def __init__(self, wsgi_app: Callable[..., Any], *, timing: bool):
        self.wsgi_app = wsgi_app
        self.timing = timing

    @staticmethod
    def _leave(token: object) -> None:
        if getattr(_owner, "token", None) is token:
            _end_scope()

    def __call__(self, environ: dict, start_response: Callable[..., Any]):
        token = object()
        try:
            if getattr(_owner, "token", None) is not None:
                _end_scope()  # a scope an earlier request never closed
            _owner.token = token
            inventory_pg.begin_request_scope()
            if self.timing:
                request_timing.begin()
        except Exception:  # noqa: BLE001 - never fail a request over the scope
            _log.debug("begin_request_scope failed", exc_info=True)

        timing = self.timing

        def _start_response(status: str, headers: list, exc_info: Any = None):
            if timing:
                try:
                    if (
                        not environ.get(NO_SERVER_TIMING_KEY)
                        and _content_type(headers) in _TIMED_TYPES
                        and not _shared_cacheable(headers)
                        and not any(k.lower() == "server-timing" for k, _ in headers)
                    ):
                        value = request_timing.header_value()
                        if value:
                            headers.append(("Server-Timing", value))
                except Exception:  # noqa: BLE001
                    pass
            if exc_info is not None:
                return start_response(status, headers, exc_info)
            return start_response(status, headers)

        try:
            body = self.wsgi_app(environ, _start_response)
        except BaseException:
            self._leave(token)
            raise
        file_wrapper = environ.get("wsgi.file_wrapper")
        if isinstance(file_wrapper, type) and isinstance(body, file_wrapper):
            # send_file bodies: no app code runs while they stream, and wrapping them
            # would cost gunicorn its sendfile() fast path for static files.
            self._leave(token)
            return body
        return _ClosingBody(body, lambda: self._leave(token))


def _hook_render_timing(app: Any) -> None:
    from flask import before_render_template, template_rendered

    def _before(sender: Any, **_extra: Any) -> None:
        request_timing.push_mark()

    def _after(sender: Any, **_extra: Any) -> None:
        request_timing.pop_mark("render")

    before_render_template.connect(_before, app, weak=False)
    template_rendered.connect(_after, app, weak=False)


def _hook_users_db_timing() -> None:
    """Time SQLCipher users-DB opens (PBKDF2 key derivation is the cost) as ``users_db``.

    Wraps ``users_sqlite._connect_sqlcipher`` from outside (that module is not changed
    here); skipped quietly when the function is missing or already wrapped.
    """
    try:
        from backend.db import users_sqlite
    except Exception:  # noqa: BLE001
        return
    orig = getattr(users_sqlite, "_connect_sqlcipher", None)
    if orig is None or getattr(orig, "_server_timing_wrapped", False):
        return

    def _connect_sqlcipher(*args: Any, **kwargs: Any) -> Any:
        with request_timing.timed("users_db"):
            return orig(*args, **kwargs)

    _connect_sqlcipher._server_timing_wrapped = True  # type: ignore[attr-defined]
    _connect_sqlcipher.__wrapped__ = orig  # type: ignore[attr-defined]
    users_sqlite._connect_sqlcipher = _connect_sqlcipher


def register_request_scope(app: Any) -> None:
    """Wrap ``app.wsgi_app`` in :class:`RequestScopeMiddleware` (call once, last)."""
    timing = server_timing_enabled()
    if timing:
        try:
            _hook_render_timing(app)
        except Exception:  # noqa: BLE001
            _log.warning("Server-Timing render hook unavailable", exc_info=True)
        _hook_users_db_timing()
    # Sit just inside ProxyFix when it is installed, so ``app.wsgi_app`` stays the ProxyFix
    # (test_proxy_fix_keeps_https_behind_trusted_proxy) and the scope sees the fixed environ.
    from werkzeug.middleware.proxy_fix import ProxyFix

    if isinstance(app.wsgi_app, ProxyFix):
        app.wsgi_app.app = RequestScopeMiddleware(app.wsgi_app.app, timing=timing)
    else:
        app.wsgi_app = RequestScopeMiddleware(app.wsgi_app, timing=timing)

    # Registered last, so it runs first among the teardown functions. All of a normal
    # response's DB work is done by then, and the scope must not outlive the request when
    # a server (or a test client) is late to close the body.
    @app.teardown_request
    def _leave_request_scope(_exc: Any) -> None:
        leave_current_scope()
