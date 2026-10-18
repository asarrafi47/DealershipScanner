"""Car chat policy, rate limits, and web-research URL guards."""

from __future__ import annotations

import pytest

from backend.routes import cars_pages as _cars_pages_routes


def test_web_research_playwright_auto_prod_requires_login(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.utils import car_chat_policy as m

    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.delenv("CAR_CHAT_WEB_RESEARCH", raising=False)
    monkeypatch.delenv("CAR_CHAT_WEB_RESEARCH_PUBLIC", raising=False)
    assert m.web_research_playwright_allowed(None) is False
    assert m.web_research_playwright_allowed(1) is True


def test_web_research_playwright_public_override_dev_only(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.utils import car_chat_policy as m

    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("CAR_CHAT_WEB_RESEARCH_PUBLIC", "1")
    assert m.web_research_playwright_allowed(None) is True

    monkeypatch.setenv("FLASK_ENV", "production")
    assert m.web_research_playwright_allowed(None) is False


def test_web_research_playwright_force_off(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.utils import car_chat_policy as m

    monkeypatch.setenv("CAR_CHAT_WEB_RESEARCH", "0")
    assert m.web_research_playwright_allowed(99) is False


def test_web_research_playwright_force_on(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.utils import car_chat_policy as m

    monkeypatch.setenv("CAR_CHAT_WEB_RESEARCH", "1")
    monkeypatch.setenv("FLASK_ENV", "production")
    assert m.web_research_playwright_allowed(None) is True


def test_href_blocks_private_and_allowlist(monkeypatch: pytest.MonkeyPatch, fake_dns) -> None:
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


def test_user_daily_limit_default_ignores_listing_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """Audit D #39: the whole-account budget must not be sized by the per-listing knob."""
    from backend.utils import car_chat_policy as m

    # setenv("") not delenv: delenv + dotenv reload repopulates from .env in this repo.
    monkeypatch.setenv("CAR_CHAT_MAX_PER_USER_DAILY", "")
    monkeypatch.setenv("CAR_CHAT_MAX_PER_LISTING_DAILY", "3")
    assert m.car_chat_user_daily_limit() == 40
    assert m.car_chat_listing_daily_limit() == 3


def test_daily_limit_defaults_and_garbage(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.utils import car_chat_policy as m

    monkeypatch.setenv("CAR_CHAT_MAX_PER_USER_DAILY", "")
    monkeypatch.setenv("CAR_CHAT_MAX_PER_LISTING_DAILY", "")
    assert m.car_chat_user_daily_limit() == 40
    assert m.car_chat_listing_daily_limit() == 10

    monkeypatch.setenv("CAR_CHAT_MAX_PER_USER_DAILY", "not-a-number")
    monkeypatch.setenv("CAR_CHAT_MAX_PER_LISTING_DAILY", "also-bad")
    assert m.car_chat_user_daily_limit() == 40
    assert m.car_chat_listing_daily_limit() == 10

    monkeypatch.setenv("CAR_CHAT_MAX_PER_USER_DAILY", "0")
    monkeypatch.setenv("CAR_CHAT_MAX_PER_LISTING_DAILY", "0")
    assert m.car_chat_user_daily_limit() == 0
    assert m.car_chat_listing_daily_limit() == 0

    monkeypatch.setenv("CAR_CHAT_MAX_PER_USER_DAILY", "77")
    monkeypatch.setenv("CAR_CHAT_MAX_PER_LISTING_DAILY", "5")
    assert m.car_chat_user_daily_limit() == 77
    assert m.car_chat_listing_daily_limit() == 5


def test_car_chat_global_rate_limit(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    # Set before import: load_project_dotenv() must not re-enable billing from .env.
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "0")
    monkeypatch.setenv("CAR_CHAT_MAX_PER_USER_DAILY", "0")
    monkeypatch.setenv("RATE_LIMIT_CAR_CHAT_GLOBAL_PER_MIN", "2")
    monkeypatch.setenv("RATE_LIMIT_CAR_CHAT_PER_IP_PER_MIN", "500")
    monkeypatch.setenv("RATE_LIMIT_CAR_CHAT_PER_MIN", "500")
    monkeypatch.setenv("RATE_LIMIT_SQLITE_PATH", str(tmp_path / "rate_limits.db"))

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

    monkeypatch.setattr(_cars_pages_routes, "get_car_by_id", lambda *_a, **_k: dummy_car)
    monkeypatch.setattr(
        _cars_pages_routes,
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


# --- P3.8 (D-SEC1 (a)): car-chat research is HTTP-only ----------------------

_CHAT_CAR = {
    "id": 136451,
    "year": 2021,
    "make": "BMW",
    "model": "3 Series",
    "trim": "330i xDrive",
    "price": 31500,
    "mileage": 22000,
    "vin": "3MW5R7J00M8B00000",
    "condition": "Used",
}
_RESEARCH_URL = "https://www.jdpower.com/cars/2021/bmw/3-series"
_RESEARCH_TEXT = " ".join(
    ["J.D. Power owners rate the 2021 BMW 3 Series above average for quality and reliability."] * 4
)


@pytest.fixture
def http_only_research(monkeypatch: pytest.MonkeyPatch) -> dict:
    """Stub the network (one DuckDuckGo candidate, one HTTP page) and the LLM; record
    every WebResearcher the chat builds and every playwright import (which raises)."""
    import builtins

    from backend.intelligence.ai import agent
    from backend.utils import web_researcher as wr

    seen: dict = {"researchers": [], "playwright_imports": [], "fetched": [], "system": []}
    real_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if level == 0 and (name == "playwright" or name.startswith("playwright.")):
            seen["playwright_imports"].append(name)
            raise ImportError(f"P3.8 test: playwright import blocked ({name})")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    class _Recorded(wr.WebResearcher):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            seen["researchers"].append(self)

    def fetch(url: str, *, timeout_sec: int = 20):
        seen["fetched"].append(url)
        return "2021 BMW 3 Series quality and reliability", _RESEARCH_TEXT

    monkeypatch.setattr(wr, "WebResearcher", _Recorded)
    monkeypatch.setattr(wr, "duckduckgo_html_result_links", lambda *a, **k: [_RESEARCH_URL])
    monkeypatch.setattr(wr, "fetch_page_text_http", fetch)

    monkeypatch.setattr(agent, "prepare_car_detail_context", lambda car: {})
    monkeypatch.setattr("backend.intelligence.deal_score_cache.public_deal_score", lambda car: None)
    monkeypatch.setattr("backend.vector.pgvector_service.get_model_knowledge", lambda **k: ("", ""), raising=False)
    monkeypatch.setattr("backend.vector.pgvector_service.add_model_knowledge", lambda **k: None, raising=False)

    def fake_generate(system, msg, *, max_tokens=1024):
        seen["system"].append(system)
        return "Owners rate it above average.\nSource: " + _RESEARCH_URL

    monkeypatch.setattr(agent, "_generate_reply", fake_generate)
    return seen


def test_car_chat_research_answer_is_served_from_http(http_only_research: dict) -> None:
    from backend.intelligence.ai import agent

    out = agent.run_car_page_chat(dict(_CHAT_CAR), "how reliable is this car long term?", allow_web_research=True)
    seen = http_only_research
    assert out["error"] is None
    assert out["web_research_used"] is True
    assert out["web_research_url"] == _RESEARCH_URL
    assert seen["fetched"] == [_RESEARCH_URL]
    assert any("above average for quality and reliability" in s for s in seen["system"])
    assert [r.allow_browser for r in seen["researchers"]] == [False]
    assert seen["playwright_imports"] == []


def test_compare_chat_research_answer_is_served_from_http(http_only_research: dict) -> None:
    from backend.intelligence.ai import agent

    other = dict(_CHAT_CAR, id=136452, make="Audi", model="A4", trim="Premium", vin="WAUENAF40MN000000")
    out = agent.run_compare_chat([dict(_CHAT_CAR), other], "which of these is more reliable?", allow_web_research=True)
    seen = http_only_research
    assert out["error"] is None
    assert out["web_research_used"] is True
    assert out["web_research_url"] == _RESEARCH_URL
    assert [r.allow_browser for r in seen["researchers"]] == [False]
    assert seen["playwright_imports"] == []


def test_car_chat_failed_http_research_does_not_fall_back_to_a_browser(
    http_only_research: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The case that used to launch Chromium: the only candidate answers 403 over HTTP."""
    import urllib.error

    from backend.intelligence.ai import agent
    from backend.utils import web_researcher as wr

    def forbidden(url: str, *, timeout_sec: int = 20):
        raise urllib.error.HTTPError(url, 403, "Forbidden", hdrs=None, fp=None)

    monkeypatch.setattr(wr, "fetch_page_text_http", forbidden)
    out = agent.run_car_page_chat(dict(_CHAT_CAR), "how reliable is this car long term?", allow_web_research=True)
    seen = http_only_research
    assert out["error"] is None
    assert out["web_research_used"] is False
    assert [r.allow_browser for r in seen["researchers"]] == [False]
    assert seen["playwright_imports"] == []
