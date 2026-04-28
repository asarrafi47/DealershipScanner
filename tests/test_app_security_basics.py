"""App-level security: logout, body limits, shared rate limit, CSP header shape."""

from __future__ import annotations



def test_get_logout_not_allowed() -> None:
    from backend.main import app

    with app.test_client() as c:
        rv = c.get("/logout")
        assert rv.status_code == 405


def test_logout_post_requires_csrf() -> None:
    from backend.main import app

    with app.test_client() as c:
        rv = c.post("/logout")
        assert rv.status_code == 403


def test_smart_search_payload_too_large() -> None:
    from backend import main
    from backend.utils.csrf import _SESSION_KEY

    tok = "z" * 32
    with main.app.test_client() as c:
        with c.session_transaction() as sess:
            sess[_SESSION_KEY] = tok
        big = "x" * (main._CHAT_MAX_BODY + 8)
        rv = c.post(
            "/api/search/smart",
            json={"q": big},
            headers={"X-CSRF-Token": tok},
        )
        assert rv.status_code == 413


def test_ip_rate_limit_sqlite_shared(tmp_path, monkeypatch) -> None:
    from backend.utils import ip_rate_limit as m

    dbf = tmp_path / "rl.db"
    monkeypatch.setenv("RATE_LIMIT_SQLITE_PATH", str(dbf))
    assert m.allow_request("k1", max_events=2, window_seconds=60.0)
    assert m.allow_request("k1", max_events=2, window_seconds=60.0)
    assert not m.allow_request("k1", max_events=2, window_seconds=60.0)
    assert m.allow_request("k2", max_events=2, window_seconds=60.0)


def test_csp_enforce_header_when_enabled(monkeypatch) -> None:
    monkeypatch.setenv("CSP_ENFORCE", "1")
    from backend.main import _csp_enforce_wanted, app

    assert _csp_enforce_wanted() is True
    with app.test_client() as c:
        rv = c.get("/login")
    assert rv.status_code == 200
    csp = rv.headers.get("Content-Security-Policy", "")
    assert "default-src" in csp
    assert "script-src" in csp
    assert "nonce-" in csp
