"""``Server-Timing`` on HTML and JSON responses (backend/web/request_scope.py).

Pinned: the header appears on HTML and JSON only; it carries durations and counts and
nothing else (no SQL, no hosts); ``SERVER_TIMING=0`` removes it; a failing hook never
fails a request; send_file bodies keep gunicorn's sendfile path; the request scope ends
only when the body is closed. The real app registers the middleware outermost.
"""

from __future__ import annotations

import re

import pytest
from flask import Flask, jsonify, render_template_string

from backend.db import inventory_pg
from backend.utils import request_timing
from backend.web import request_scope

_PART = re.compile(r'^[a-z_]+;dur=\d+\.\d(;desc="\d+x")?$')


def _app(monkeypatch, *, enabled: bool = True) -> Flask:
    monkeypatch.setenv("SERVER_TIMING", "1" if enabled else "0")
    app = Flask(__name__)

    @app.get("/html")
    def html():
        request_timing.add("db", 4.25)
        request_timing.add("db", 1.0)
        request_timing.add("db_connect", 3.0)
        return render_template_string("<p>{{ x }}</p>", x="SELECT secret FROM cars")

    @app.get("/json")
    def json_route():
        request_timing.add("users_db", 130.0)
        return jsonify(ok=True)

    @app.get("/public")
    def public_route():
        return "<p>x</p>", 200, {"Content-Type": "text/html", "Cache-Control": "public, max-age=0, s-maxage=300"}

    @app.get("/text")
    def text():
        return "plain", 200, {"Content-Type": "text/plain"}

    request_scope.register_request_scope(app)
    return app


def _parts(value: str) -> dict[str, str]:
    out = {}
    for part in value.split(", "):
        assert _PART.match(part), part
        out[part.split(";", 1)[0]] = part
    return out


def test_html_response_carries_durations_and_counts(monkeypatch):
    r = _app(monkeypatch).test_client().get("/html")
    parts = _parts(r.headers["Server-Timing"])
    assert list(parts)[:3] == ["app", "db", "db_connect"]
    assert parts["db"].startswith("db;dur=5.") and parts["db"].endswith('desc="2x"')
    assert "render" in parts  # Jinja signals
    assert "SELECT" not in r.headers["Server-Timing"] and "127.0.0.1" not in r.headers["Server-Timing"]


def test_json_response_carries_the_header(monkeypatch):
    r = _app(monkeypatch).test_client().get("/json")
    parts = _parts(r.headers["Server-Timing"])
    assert parts["users_db"] == 'users_db;dur=130.0;desc="1x"'


def test_other_content_types_do_not(monkeypatch):
    r = _app(monkeypatch).test_client().get("/text")
    assert "Server-Timing" not in r.headers


def test_shared_cacheable_responses_do_not(monkeypatch):
    # An edge cache would replay one request's timings to every visitor.
    r = _app(monkeypatch).test_client().get("/public")
    assert "Server-Timing" not in r.headers


def test_env_switch_turns_it_off(monkeypatch):
    r = _app(monkeypatch, enabled=False).test_client().get("/html")
    assert r.status_code == 200 and "Server-Timing" not in r.headers


def test_a_broken_timing_hook_never_fails_the_request(monkeypatch):
    app = _app(monkeypatch)

    def boom():
        raise RuntimeError("bug")

    monkeypatch.setattr(request_timing, "header_value", boom)
    r = app.test_client().get("/html")
    assert r.status_code == 200 and "Server-Timing" not in r.headers


def test_accumulator_is_inert_outside_a_request():
    request_timing.end()
    request_timing.add("db", 1.0)  # the scanner path: no state, no error
    with request_timing.timed("db"):
        pass
    assert request_timing.header_value() is None and request_timing.snapshot() == {}


def test_scope_lasts_until_the_body_is_closed():
    seen = []

    def inner(environ, start_response):
        start_response("200 OK", [("Content-Type", "text/html")])

        def body():
            seen.append(inventory_pg.in_request_scope())
            yield b"x"

        return body()

    mw = request_scope.RequestScopeMiddleware(inner, timing=True)
    headers_out = {}
    body = mw({}, lambda status, headers, exc_info=None: headers_out.update(headers))
    assert "Server-Timing" in headers_out
    assert list(body) == [b"x"] and seen == [True]
    assert inventory_pg.in_request_scope()
    body.close()
    assert not inventory_pg.in_request_scope() and not request_timing.active()


def test_scope_ends_when_the_app_raises():
    def inner(environ, start_response):
        raise ValueError("handler crashed")

    mw = request_scope.RequestScopeMiddleware(inner, timing=True)
    with pytest.raises(ValueError):
        mw({}, lambda *a: None)
    assert not inventory_pg.in_request_scope() and not request_timing.active()


def test_file_wrapper_bodies_pass_through_untouched():
    class FileWrapper:
        def __init__(self, f):
            self.f = f

        def __iter__(self):
            return iter([b"static"])

    wrapped = FileWrapper(None)

    def inner(environ, start_response):
        start_response("200 OK", [("Content-Type", "text/css")])
        return wrapped

    mw = request_scope.RequestScopeMiddleware(inner, timing=True)
    body = mw({"wsgi.file_wrapper": FileWrapper}, lambda *a: None)
    assert body is wrapped  # gunicorn's sendfile() needs its own wrapper type
    assert not inventory_pg.in_request_scope()


def test_users_db_hook_wraps_once(monkeypatch):
    from backend.db import users_sqlite

    calls = []
    monkeypatch.setattr(users_sqlite, "_connect_sqlcipher", lambda key: calls.append(key) or "conn")
    request_scope._hook_users_db_timing()
    request_scope._hook_users_db_timing()  # idempotent
    request_timing.begin()
    try:
        assert users_sqlite._connect_sqlcipher("k") == "conn"
        assert request_timing.snapshot()["users_db"][1] == 1
    finally:
        request_timing.end()
    assert calls == ["k"]


def test_the_app_registers_the_middleware_outermost():
    import ast
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "main.py").read_text(encoding="utf-8")
    calls = [
        n.func.id
        for n in ast.walk(ast.parse(src))
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id.startswith("register_")
    ]
    assert calls[-1] == "register_request_scope"


def test_flask_teardown_ends_the_scope_even_if_the_body_is_never_closed(monkeypatch):
    app = _app(monkeypatch)
    resp = app.test_client().get("/html", buffered=False)  # body left open, as some clients do
    assert resp.status_code == 200
    assert not inventory_pg.in_request_scope() and not request_timing.active()
    resp.close()


def test_a_late_close_of_an_old_body_does_not_end_the_current_scope():
    def inner(environ, start_response):
        start_response("200 OK", [("Content-Type", "text/html")])
        return [b"x"]

    mw = request_scope.RequestScopeMiddleware(inner, timing=True)
    old = mw({}, lambda *a: None)
    current = mw({}, lambda *a: None)  # the next request on this thread
    old.close()
    assert inventory_pg.in_request_scope() and request_timing.active()
    current.close()
    assert not inventory_pg.in_request_scope()
