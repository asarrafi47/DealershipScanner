"""Card C3: the four chat routes run through the chat gate (docs/auth_sms/TASKS.md, CHAT_LIMITS.md).

The Postgres tier runs the real gate against a fresh database (the acceptance bullets); the offline
tests cover what needs no database (the burst guard, the narration checks, the sticker wiring, the
chat panel script). No network, no model: the model functions are replaced by fakes.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from backend import settings_store as ss
from backend.chat_budget import gate
from backend.chat_budget import messages as msg
from backend.chat_budget import web as chat_web
from backend.db.repositories import chat_usage_repo as repo
from backend.routes import cars_pages as _cars_pages_routes
from backend.utils.csrf import _SESSION_KEY

ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc
NY = "America/New_York"
NOW = datetime(2026, 11, 13, 19, 35, tzinfo=UTC)  # 2:35 PM Eastern
TOP = datetime(2026, 11, 13, 20, 0, tzinfo=UTC)  # 3:00 PM Eastern, the next top of the hour
MIDNIGHT = datetime(2026, 11, 14, 5, 0, tzinfo=UTC)  # 12:00 AM Eastern
TOKEN = "c" * 32
CAR = {"id": 1, "year": 2020, "make": "Test", "model": "Car", "vin": "X", "inactive": 0}
pg = pytest.mark.pg


# ---------------------------------------------------------------- shared set-up


@pytest.fixture
def settings(monkeypatch):
    """No settings rows and no chat environment: the code defaults (rows are added per test)."""
    for spec in ss.SPECS.values():
        monkeypatch.delenv(spec.env, raising=False)
    rows: dict[str, str] = {}
    ss.set_loader(lambda: dict(rows))
    yield rows
    ss.set_loader(None)


@pytest.fixture
def clock(monkeypatch):
    monkeypatch.setattr(gate, "_utcnow", lambda: NOW)
    return NOW


@pytest.fixture
def fakes(monkeypatch):
    """Every model function the four routes reach, faked; ``calls`` counts them."""
    calls: list[str] = []

    def car_chat(*_a, **_k):
        calls.append("car")
        return {"reply": "ok", "error": None, "discrepancy_flags": []}

    def compare_chat(*_a, **_k):
        calls.append("compare")
        return {"reply": "ok", "error": None}

    def general(prompt, *_a, **_k):
        calls.append("general")
        return "NONE" if "A car shopper said" in prompt else "A plain answer."

    def narrate(_row):
        calls.append("narrate")
        return "A tidy car."

    monkeypatch.setattr(_cars_pages_routes, "get_car_by_id", lambda cid, **_k: dict(CAR, id=cid) if cid < 900 else None)
    monkeypatch.setattr(_cars_pages_routes, "get_cars_by_ids", lambda ids: [dict(CAR, id=i) for i in ids])
    monkeypatch.setattr(_cars_pages_routes, "run_car_page_chat", car_chat)
    monkeypatch.setattr(_cars_pages_routes, "run_compare_chat", compare_chat)
    monkeypatch.setattr("backend.intelligence.ai.agent.run_car_page_chat", car_chat)
    monkeypatch.setattr("backend.db.inventory_db.get_car_by_id", lambda cid, **_k: dict(CAR, id=cid))
    monkeypatch.setattr("backend.utils.llm_client.complete", general)
    monkeypatch.setattr("backend.utils.query_parser.parse_natural_query", lambda m: {})
    import backend.routes.ai_narrate_bp as narrate_bp

    monkeypatch.setattr(narrate_bp, "get_car_by_id", lambda cid, **_k: dict(CAR, id=cid) if cid < 900 else None)
    monkeypatch.setattr(narrate_bp, "narrate_vehicle", narrate)
    narrate_bp._cache.clear()
    from backend.utils.ip_rate_limit import clear_rate_limit_state

    clear_rate_limit_state()
    return calls


def _app(app_factory, *, production=False, **env):
    base = dict(
        ALLOW_DEFAULT_APP_USER="0",
        BILLING_STRIPE_ENABLED="0",
        ALLOW_DEV_PHONE_PEPPER="1",
        RATE_LIMIT_CAR_CHAT_GLOBAL_PER_MIN="1000",
        RATE_LIMIT_CAR_CHAT_PER_IP_PER_MIN="1000",
        RATE_LIMIT_CAR_CHAT_PER_MIN="1000",
        CAR_CHAT_WEB_RESEARCH="0",
    )
    if production:
        base["PHONE_HASH_PEPPER"] = "p" * 40
    base.update(env)
    return app_factory(production=production, **base)


def _client(main, user_id=42):
    c = main.app.test_client()
    with c.session_transaction() as sess:
        sess[_SESSION_KEY] = TOKEN
        if user_id is not None:
            sess["user_id"] = user_id
    return c


H = {"X-CSRF-Token": TOKEN, "Content-Type": "application/json"}


def car(c, cid=1, text="hello"):
    return c.post(f"/api/car/{cid}/chat", json={"message": text}, headers=H)


def compare(c, text="which one?"):
    return c.post("/api/compare/chat", json={"car_ids": [1, 2], "message": text}, headers=H)


def sidebar(c, text="how does financing work?", car_id=None):
    body = {"message": text}
    if car_id is not None:
        body["car_id"] = car_id
    return c.post("/api/ai/chat", json=body, headers=H)


def narrate(c, cid=1):
    return c.get(f"/api/car/{cid}/narrate", headers={"X-CSRF-Token": TOKEN})


ROUTES = {"car": car, "compare": compare, "sidebar": sidebar, "narrate": narrate}


def _user_day_messages(uid="42"):
    rows = repo.read_day_totals(now=NOW, tz_name=NY, kind="user", key=uid)
    return rows[0]["messages"] if rows else 0


# ---------------------------------------------------------------- acceptance (Postgres tier)


@pg
def test_eleventh_question_in_an_hour_is_refused_with_the_hour_sentence_and_a_reset_on_the_hour(
    pg_db, settings, clock, fakes, app_factory
):
    c = _client(_app(app_factory))
    for i in range(10):
        assert car(c, cid=1 + i % 3, text=f"q{i}").status_code == 200, i
    rv = car(c, cid=4, text="q11")
    assert rv.status_code == 429
    body = rv.get_json()
    assert body["error"] == "user_chat_limit_reached"
    assert body["message"] == msg.hour_limit_text(10, TOP, NOW, NY)
    assert "3:00 PM Eastern" in body["message"]
    assert body["resets_at"] == "2026-11-13T20:00:00Z"  # the next top of the hour, in UTC
    assert fakes.count("car") == 10  # the refused question never reached the model


@pg
def test_a_restart_with_the_same_database_does_not_reset_the_counts(pg_db, settings, clock, fakes, app_factory):
    first = _client(_app(app_factory))
    for i in range(6):
        assert car(first, cid=1 + i, text=f"a{i}").status_code == 200
    # a new app instance (a restart or a second worker), the same Postgres database
    second = _client(_app(app_factory))
    for i in range(4):
        assert car(second, cid=10 + i, text=f"b{i}").status_code == 200
    assert car(second, cid=20).status_code == 429
    assert _user_day_messages() == 10


@pg
def test_sidebar_compare_and_car_page_share_one_allowance(pg_db, settings, clock, fakes, app_factory):
    c = _client(_app(app_factory))
    for i in range(4):
        assert car(c, cid=1 + i).status_code == 200
    for _ in range(3):
        assert sidebar(c).get_json()["ok"] is True
    for _ in range(3):
        assert compare(c).status_code == 200
    # ten messages from three routes: the eleventh is refused on any of them
    for route in (car, compare, sidebar, narrate):
        rv = route(c)
        assert rv.status_code == 429, route.__name__
        assert rv.get_json()["error"] == "user_chat_limit_reached"
    assert _user_day_messages() == 10


@pg
def test_the_per_car_limit_is_shared_by_the_car_page_and_the_sidebar(pg_db, settings, clock, fakes, app_factory):
    settings["chat.user_per_hour"] = "100"
    c = _client(_app(app_factory))
    for i in range(5):
        assert car(c, cid=7).status_code == 200
        assert sidebar(c, car_id=7).get_json()["context"] == "car"
    rv = sidebar(c, car_id=7)
    assert rv.status_code == 429
    assert rv.get_json()["error"] == "listing_chat_limit_reached"
    assert car(c, cid=7).get_json()["error"] == "listing_chat_limit_reached"
    assert car(c, cid=8).status_code == 200  # another car still works


@pg
@pytest.mark.parametrize("route", sorted(ROUTES))
def test_the_site_budget_stops_every_chat_route(pg_db, settings, clock, fakes, app_factory, route):
    settings["chat.budget_tokens_per_day"] = "1000"  # under one message's estimate: the cap is reached
    c = _client(_app(app_factory))
    rv = ROUTES[route](c)
    assert rv.status_code == 429
    body = rv.get_json()
    assert body["error"] == "chat_budget_reached"
    assert body["message"] == msg.budget_text(MIDNIGHT, NY)
    assert body["resets_at"] == "2026-11-14T05:00:00Z"
    assert fakes == []


@pg
@pytest.mark.parametrize("route", sorted(ROUTES))
def test_a_refusal_is_429_with_the_old_shape_plus_message_and_resets_at(pg_db, settings, clock, fakes, app_factory, route):
    settings["chat.user_per_day"] = "1"
    c = _client(_app(app_factory))
    assert ROUTES[route](c).status_code == 200
    if route == "narrate":
        import backend.routes.ai_narrate_bp as narrate_bp

        narrate_bp._cache.clear()
    rv = ROUTES[route](c)
    assert rv.status_code == 429
    body = rv.get_json()
    assert set(body) == {"ok", "error", "message", "resets_at"}
    assert body["ok"] is False and body["error"] == "user_chat_limit_reached"
    assert body["message"] == msg.day_limit_text(1, MIDNIGHT, NOW, NY)
    assert body["resets_at"] == "2026-11-14T05:00:00Z"


@pg
def test_the_paused_setting_answers_chat_paused_on_every_route(pg_db, settings, clock, fakes, app_factory):
    settings["chat.enabled"] = "off"
    c = _client(_app(app_factory))
    for name, route in ROUTES.items():
        body = route(c).get_json()
        assert (body["error"], body["message"], body["resets_at"]) == ("chat_paused", msg.PAUSED_TEXT, None), name


@pg
@pytest.mark.parametrize("route", sorted(ROUTES))
def test_with_default_limits_and_an_empty_settings_table_nobody_is_locked_out(pg_db, fakes, app_factory, route, monkeypatch):
    """Rule 7: the gate is on by default but lets a normal shopper through on every route."""
    for spec in ss.SPECS.values():
        monkeypatch.delenv(spec.env, raising=False)
    ss.reset_cache()
    c = _client(_app(app_factory))
    rv = ROUTES[route](c)
    assert rv.status_code == 200, rv.get_json()
    assert _user_day_messages_now() == 1


def _user_day_messages_now(uid="42"):
    rows = repo.read_day_totals(now=datetime.now(UTC), tz_name=NY, kind="user", key=uid)
    return rows[0]["messages"] if rows else 0


@pg
def test_a_404_or_an_empty_message_costs_no_message(pg_db, settings, clock, fakes, app_factory):
    c = _client(_app(app_factory))
    assert car(c, cid=999).status_code == 404
    assert car(c, text="  ").status_code == 400
    assert narrate(c, cid=999).status_code == 404
    assert sidebar(c, text="  ").status_code == 400
    assert _user_day_messages() == 0


@pg
def test_the_sidebar_counts_one_message_for_its_two_model_calls(pg_db, settings, clock, fakes, app_factory):
    c = _client(_app(app_factory))
    body = sidebar(c).get_json()
    assert body["context"] == "general"
    assert fakes.count("general") == 2  # the search rewrite, then the answer
    assert _user_day_messages() == 1


@pg
def test_a_local_search_in_the_sidebar_costs_nothing(pg_db, settings, clock, fakes, app_factory, monkeypatch):
    monkeypatch.setattr("backend.utils.query_parser.parse_natural_query", lambda m: {"make": "BMW", "max_price": 50000})
    c = _client(_app(app_factory))
    assert sidebar(c, text="BMW under 50k").get_json()["context"] == "search"
    assert _user_day_messages() == 0


@pg
def test_narration_cache_hit_costs_nothing(pg_db, settings, clock, fakes, app_factory):
    c = _client(_app(app_factory))
    assert narrate(c).status_code == 200
    second = narrate(c)
    assert second.get_json().get("cached") is True
    assert fakes.count("narrate") == 1
    assert _user_day_messages() == 1


@pg
def test_a_network_cap_counts_a_whole_slash_24(pg_db, settings, clock, fakes, app_factory):
    """Per-/24 back-stop: different accounts on neighbouring addresses share one counter."""
    settings["chat.ip_net_per_day"] = "2"
    settings["chat.ip_per_day"] = "100"
    main = _app(app_factory)
    for uid, addr in ((1, "198.51.100.7"), (2, "198.51.100.8")):
        c = _client(main, user_id=uid)
        rv = c.post("/api/car/1/chat", json={"message": "hi"}, headers=H, environ_base={"REMOTE_ADDR": addr})
        assert rv.status_code == 200
    c = _client(main, user_id=3)
    rv = c.post("/api/car/1/chat", json={"message": "hi"}, headers=H, environ_base={"REMOTE_ADDR": "198.51.100.9"})
    assert rv.status_code == 429
    assert rv.get_json()["error"] == "user_chat_limit_reached"  # the hidden counter is never named
    other = c.post("/api/car/1/chat", json={"message": "hi"}, headers=H, environ_base={"REMOTE_ADDR": "203.0.113.9"})
    assert other.status_code == 200


@pg
def test_a_refusal_logs_no_address_number_or_long_hash(pg_db, settings, clock, fakes, app_factory, caplog):
    settings["chat.user_per_day"] = "1"
    caplog.set_level(logging.DEBUG)
    c = _client(_app(app_factory))
    car(c)
    rv = c.post("/api/car/1/chat", json={"message": "hi +12025550123"}, headers=H,
                environ_base={"REMOTE_ADDR": "198.51.100.77"})
    assert rv.status_code == 429
    text = caplog.text + repr(rv.get_json())
    assert "198.51.100.77" not in text
    assert "2025550123" not in text
    assert not re.search(r"\d{7,}", rv.get_data(as_text=True))
    assert not re.search(r"[0-9a-f]{64}", text)


# ---------------------------------------------------------------- signed out (offline)


@pytest.mark.parametrize("route", sorted(ROUTES))
def test_signed_out_requests_get_the_existing_login_response(fakes, app_factory, route):
    main = _app(app_factory, production=True)
    c = _client(main, user_id=None)
    rv = ROUTES[route](c)
    assert rv.get_json()["error"] == "login_required"
    # car and compare chat answered 403 before card C3, the sidebar 401; narration now asks for sign-in too
    assert rv.status_code == (403 if route in ("car", "compare") else 401)
    assert fakes == []


def test_narration_needs_the_csrf_header(fakes, app_factory, chat_gate_open):
    """A GET carries the Lax session cookie on a cross-site link: the header is what stops that."""
    c = _client(_app(app_factory))
    rv = c.get("/api/car/1/narrate")
    assert (rv.status_code, rv.get_json()["error"]) == (403, "csrf_required")
    assert fakes == [] and chat_gate_open == []
    assert narrate(c).status_code == 200
    assert chat_gate_open == [("narrate", {"listing_id": None, "weight": 1})]


def test_postgres_down_closes_chat_on_every_route(fakes, app_factory, monkeypatch, settings):
    def down(*_a, **_k):
        raise OSError("connection refused")

    monkeypatch.setattr(repo, "reserve", down)
    c = _client(_app(app_factory))
    for name, route in ROUTES.items():
        rv = route(c)
        assert rv.status_code == 503, name
        assert rv.get_json()["error"] == "chat_unavailable"
    assert fakes == []


# ---------------------------------------------------------------- the burst guard (offline)


def test_one_burst_function_guards_all_four_routes(fakes, app_factory, monkeypatch, chat_gate_open):
    """Card R3 swaps the burst backend in one place: chat_budget.web._allow."""
    seen: list[str] = []

    def refuse(key, max_events):
        seen.append(key)
        return False

    monkeypatch.setattr(chat_web, "_allow", refuse)
    c = _client(_app(app_factory))
    for name, route in ROUTES.items():
        rv = route(c)
        assert (rv.status_code, rv.get_json()["error"]) == (429, "rate_limited"), name
    assert len(seen) == 4 and fakes == [] and chat_gate_open == []
    for path in ("backend/routes/cars_pages.py", "backend/routes/ai_chat_bp.py", "backend/routes/ai_narrate_bp.py"):
        src = (ROOT / path).read_text(encoding="utf-8")
        assert "allow_request" not in src, path
        assert ":daily:" not in src, path


def test_the_burst_guard_keeps_the_old_per_minute_numbers(monkeypatch):
    from backend.utils.ip_rate_limit import clear_rate_limit_state

    clear_rate_limit_state()
    monkeypatch.setenv("RATE_LIMIT_CAR_CHAT_PER_MIN", "2")
    monkeypatch.setenv("RATE_LIMIT_CAR_CHAT_PER_IP_PER_MIN", "50")
    monkeypatch.setenv("RATE_LIMIT_CAR_CHAT_GLOBAL_PER_MIN", "0")
    assert chat_web.burst_allowed(chat_web.BURST_CAR, "192.0.2.1", pair="5")
    assert chat_web.burst_allowed(chat_web.BURST_CAR, "192.0.2.1", pair="5")
    assert not chat_web.burst_allowed(chat_web.BURST_CAR, "192.0.2.1", pair="5")
    assert chat_web.burst_allowed(chat_web.BURST_CAR, "192.0.2.1", pair="6")  # another car


# ---------------------------------------------------------------- weights, networks, stickers (offline)


def test_web_research_questions_weigh_three(settings):
    q = "what do owners say about reliability?"
    from backend.intelligence.ai.agent import _needs_web_research

    assert _needs_web_research(q)
    assert chat_web.message_weight(q, True) == 3
    assert chat_web.message_weight(q, False) == 1
    assert chat_web.message_weight("hello", True) == 1


@pytest.mark.parametrize(
    "ip,net",
    [
        ("198.51.100.7", "198.51.100.0/24"),
        ("2001:db8:1:2::", "2001:db8:1::/48"),
        ("::ffff:198.51.100.7", "198.51.100.0/24"),
        ("junk", None),
    ],
)
def test_network_of_is_slash_24_or_slash_48(ip, net):
    assert chat_web.network_of(ip) == net


def test_the_network_counter_is_a_charge_with_its_own_limit(settings):
    settings["chat.ip_net_per_day"] = "7"
    g = gate.guard("car", 1, None, None, "iphash", None, 1, net_hash="nethash")
    charges = {(c.kind, c.key): c for c in g._charges()}
    assert charges[("ip", "nethash")].day_limit == 7
    assert charges[("ip", "iphash")].day_limit == 100


class _Recorder:
    def __init__(self, deny=False):
        self.opened = []
        self.deny = deny

    def factory(self, kind, user_id, phone_hash, device_hash, ip_hash, listing_id, weight, **kw):
        rec = self

        class G:
            def __enter__(self):
                rec.opened.append((kind, weight, user_id))
                if rec.deny:
                    raise gate.ChatDenied(gate.Denial("user_day", msg.ERR_USER, "You have used your 40 questions for today."))
                return self

            def __exit__(self, *exc):
                rec.opened.append("exit")
                return False

        return G()


def test_sticker_model_calls_from_a_shopper_request_run_under_the_guard_with_weight_3(monkeypatch, app_factory):
    rec = _Recorder()
    monkeypatch.setattr(chat_web, "guard", rec.factory)
    main = _app(app_factory)
    from backend.enrichment import window_sticker_service as ws

    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setattr("backend.llm.client.complete", lambda *a, **k: type("R", (), {
        "text": '{"options": []}', "require_complete": lambda self: None})())
    with main.app.test_request_context("/api/cars/1/packages/ensure", method="POST"):
        from flask import session

        session["user_id"] = 42
        ctx = chat_web.bind_sticker_guard()
    out = ctx.run(ws._analyze_sticker_text_with_claude, "MSRP 30,000 sticker text", dict(CAR))
    assert out == {"options": []}
    assert rec.opened == [("sticker", 3, 42), "exit"]
    # the scanner (no shopper request) is never counted
    rec.opened.clear()
    assert ws._analyze_sticker_text_with_claude("MSRP 30,000 sticker text", dict(CAR)) == {"options": []}
    assert rec.opened == []


def test_a_refused_sticker_call_skips_the_model_and_keeps_the_sentence(monkeypatch, app_factory):
    rec = _Recorder(deny=True)
    monkeypatch.setattr(chat_web, "guard", rec.factory)
    main = _app(app_factory)
    from backend.enrichment import window_sticker_service as ws

    called = []
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setattr("backend.llm.client.complete", lambda *a, **k: called.append(1))
    with main.app.test_request_context("/api/cars/1/packages/ensure", method="POST"):
        ctx = chat_web.bind_sticker_guard()
    assert ctx.run(ws._analyze_sticker_text_with_claude, "text", dict(CAR)) is None
    assert ctx.run(ws._vision_fallback_packages, dict(CAR)) is None
    assert called == []
    denial = chat_web.sticker_denial(ctx)
    assert denial is not None and denial.error == "user_chat_limit_reached"


def test_the_packages_route_returns_its_payload_with_the_sentence_when_refused(monkeypatch, app_factory):
    main = _app(app_factory)
    monkeypatch.setattr(_cars_pages_routes, "get_car_by_id", lambda *_a, **_k: dict(CAR))
    monkeypatch.setattr(_cars_pages_routes.paid_access, "check_feature", lambda *_a: (True, ""))
    monkeypatch.setattr(_cars_pages_routes, "_packages_ensure_panel_snapshot", lambda *_a, **_k: {"ok": True})
    monkeypatch.setattr(_cars_pages_routes, "_packages_ensure_payload", lambda *_a, **_k: {"ok": True})
    monkeypatch.setattr(_cars_pages_routes, "_packages_ensure_in_cooldown", lambda *_a: False)
    monkeypatch.setattr(_cars_pages_routes, "_packages_ensure_inflight_snapshot", lambda *_a: None)

    def pipeline(car_id, **_k):
        from backend.chat_budget.web import _sticker_denial

        _sticker_denial.get().append(gate.Denial("budget", msg.ERR_BUDGET, "The assistant has reached its budget."))
        return {}, _cars_pages_routes.ENSURE_COMPLETED

    monkeypatch.setattr(_cars_pages_routes, "_run_packages_ensure_with_budget", pipeline)
    c = _client(main)
    rv = c.post("/api/cars/1/packages/ensure?vision=1", headers=H)
    assert rv.status_code == 200
    body = rv.get_json()
    assert body["chat_limit_error"] == "chat_budget_reached"
    assert body["chat_limit_message"] == "The assistant has reached its budget."
    assert "window_sticker_status" in body  # the existing payload, not a refusal


def test_the_packages_worker_thread_runs_in_the_request_context():
    src = (ROOT / "backend/routes/car_detail/packages_ensure.py").read_text(encoding="utf-8")
    assert "contextvars.copy_context()" in src and "target=work_ctx.run" in src


# ---------------------------------------------------------------- the chat panels (offline)


def test_the_compare_panel_prints_the_server_sentence_as_text():
    js = (ROOT / "frontend/static/compare_chat.js").read_text(encoding="utf-8")
    assert "Daily chat limit reached" not in js
    assert "appendBubble(data.message" in js
    assert "div.textContent = body" in js  # plain text, never HTML


def test_the_sidebar_panel_prints_the_server_sentence_as_text():
    js = (ROOT / "frontend/static/ai_chatbot.js").read_text(encoding="utf-8")
    assert "data.message" in js
    assert "d.textContent = text" in js
