"""Car chat policy, rate limits, and web-research URL guards."""

from __future__ import annotations

import pytest


def test_web_research_playwright_auto_prod_requires_login(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.utils import car_chat_policy as m

    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.delenv("CAR_CHAT_WEB_RESEARCH", raising=False)
    monkeypatch.delenv("CAR_CHAT_WEB_RESEARCH_PUBLIC", raising=False)
    assert m.web_research_playwright_allowed(None) is False
    assert m.web_research_playwright_allowed(1) is True


def test_web_research_playwright_public_override(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.utils import car_chat_policy as m

    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.setenv("CAR_CHAT_WEB_RESEARCH_PUBLIC", "1")
    assert m.web_research_playwright_allowed(None) is True


def test_web_research_playwright_force_off(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.utils import car_chat_policy as m

    monkeypatch.setenv("CAR_CHAT_WEB_RESEARCH", "0")
    assert m.web_research_playwright_allowed(99) is False


def test_web_research_playwright_force_on(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.utils import car_chat_policy as m

    monkeypatch.setenv("CAR_CHAT_WEB_RESEARCH", "1")
    monkeypatch.setenv("FLASK_ENV", "production")
    assert m.web_research_playwright_allowed(None) is True


def test_href_blocks_private_and_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.utils import web_researcher as wr

    monkeypatch.delenv("WEB_RESEARCH_ALLOWED_HOSTS", raising=False)
    assert wr.href_is_acceptable_result("http://127.0.0.1/foo", allowed_hosts=None) is False
    assert wr.href_is_acceptable_result("http://192.168.1.1/", allowed_hosts=None) is False
    assert wr.href_is_acceptable_result("http://localhost/foo", allowed_hosts=None) is False

    monkeypatch.setenv("WEB_RESEARCH_ALLOWED_HOSTS", "good.example,*.trusted.test")
    allow = wr.allowed_hosts_from_env()
    assert allow is not None
    assert wr.href_is_acceptable_result("https://good.example/path", allowed_hosts=allow) is True
    assert wr.href_is_acceptable_result("https://sub.trusted.test/x", allowed_hosts=allow) is True
    assert wr.href_is_acceptable_result("https://evil.com/", allowed_hosts=allow) is False


def test_car_chat_global_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RATE_LIMIT_CAR_CHAT_GLOBAL_PER_MIN", "2")
    monkeypatch.setenv("RATE_LIMIT_CAR_CHAT_PER_IP_PER_MIN", "500")
    monkeypatch.setenv("RATE_LIMIT_CAR_CHAT_PER_MIN", "500")

    import backend.main as main

    dummy_car = {
        "id": 1,
        "year": 2020,
        "make": "Test",
        "model": "Car",
        "trim": None,
        "vin": "X",
        "inactive": 0,
    }

    monkeypatch.setattr(main, "get_car_by_id", lambda *_a, **_k: dummy_car)
    monkeypatch.setattr(
        main,
        "run_car_page_chat",
        lambda *_a, **_k: {"reply": "ok", "error": None, "discrepancy_flags": []},
    )

    from backend.utils.csrf import _SESSION_KEY

    tok = "t" * 32
    with main.app.test_client() as c:
        with c.session_transaction() as sess:
            sess[_SESSION_KEY] = tok
        hdr = {"X-CSRF-Token": tok, "Content-Type": "application/json"}
        for _ in range(2):
            rv = c.post("/api/car/1/chat", json={"message": "price"}, headers=hdr)
            assert rv.status_code == 200
        rv = c.post("/api/car/1/chat", json={"message": "price"}, headers=hdr)
        assert rv.status_code == 429
