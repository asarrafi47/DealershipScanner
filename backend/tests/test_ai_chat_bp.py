"""Global AI chat endpoint: gating, car-context routing, general fallback."""
from __future__ import annotations

import flask
import pytest

import backend.routes.ai_chat_bp as bp


@pytest.fixture
def client(monkeypatch):
    app = flask.Flask(__name__)
    app.register_blueprint(bp.ai_chat_bp)
    # allow_request always passes in tests
    monkeypatch.setattr(bp, "allow_request", lambda *a, **k: True)
    return app.test_client()


def _allow_feature(monkeypatch, ok=True):
    monkeypatch.setattr(bp, "require_feature", lambda *a, **k: (ok, "" if ok else "not_entitled"))


def test_gated_without_entitlement(client, monkeypatch):
    _allow_feature(monkeypatch, ok=False)
    r = client.post("/api/ai/chat", json={"message": "hi"})
    assert r.status_code == 403
    assert r.get_json()["ok"] is False


def test_empty_message_rejected(client, monkeypatch):
    _allow_feature(monkeypatch)
    r = client.post("/api/ai/chat", json={"message": "   "})
    assert r.status_code == 400


def test_car_context_routes_to_car_chat(client, monkeypatch):
    _allow_feature(monkeypatch)
    monkeypatch.setattr("backend.db.inventory_db.get_car_by_id",
                        lambda cid, include_inactive=False: {"id": cid, "make": "Toyota"})
    monkeypatch.setattr("backend.intelligence.ai.agent.run_car_page_chat",
                        lambda car, msg, **k: {"reply": f"About car {car['id']}: yes.", "error": None})
    r = client.post("/api/ai/chat", json={"message": "good deal?", "car_id": 42})
    assert r.status_code == 200
    data = r.get_json()
    assert data["ok"] and data["context"] == "car" and "car 42" in data["reply"]


def test_general_path_without_car(client, monkeypatch):
    _allow_feature(monkeypatch)
    monkeypatch.setattr("backend.utils.llm_client.complete",
                        lambda *a, **k: "Financing is typically arranged with the dealer.")
    r = client.post("/api/ai/chat", json={"message": "how does financing work?"})
    assert r.status_code == 200
    data = r.get_json()
    assert data["ok"] and data["context"] == "general" and "Financing" in data["reply"]


def test_rate_limited(client, monkeypatch):
    _allow_feature(monkeypatch)
    monkeypatch.setattr(bp, "allow_request", lambda *a, **k: False)
    r = client.post("/api/ai/chat", json={"message": "hi"})
    assert r.status_code == 429


def test_search_intent_returns_listings_link(client, monkeypatch):
    _allow_feature(monkeypatch)
    monkeypatch.setattr("backend.utils.query_parser.parse_natural_query",
                        lambda m: {"make": "BMW", "max_price": 50000})
    r = client.post("/api/ai/chat", json={"message": "BMW under 50k"})
    data = r.get_json()
    assert data["ok"] and data["context"] == "search"
    assert data["search"]["url"].startswith("/listings?q=")
    assert data["search"]["filters"] == {"make": "BMW", "max_price": 50000}
    assert "$50,000" in data["reply"]


def test_question_not_treated_as_search(client, monkeypatch):
    _allow_feature(monkeypatch)
    monkeypatch.setattr("backend.utils.query_parser.parse_natural_query", lambda m: {})
    monkeypatch.setattr("backend.utils.llm_client.complete", lambda *a, **k: "Financing works via a loan.")
    r = client.post("/api/ai/chat", json={"message": "how does financing work?"})
    data = r.get_json()
    assert data["ok"] and data["context"] == "general" and "search" not in data


def test_truncated_rewrite_falls_back_to_raw_query(client, monkeypatch):
    """A rewrite cut off at max_tokens is never used — the raw query is searched."""
    from backend.utils.local_llm import Completion

    _allow_feature(monkeypatch)
    parsed = []

    def fake_parse(q):
        parsed.append(q)
        return {"make": "BMW", "body_style": "convertible"}

    # Question-shaped message: the first parse yields filters but _looks_like_search
    # rejects it, so the route reaches the LLM rewrite step.
    monkeypatch.setattr("backend.utils.query_parser.parse_natural_query", fake_parse)
    cut = Completion("BMW convert", stop_reason="length", truncated=True, provider="local")
    monkeypatch.setattr("backend.utils.llm_client.complete", lambda *a, **k: cut)

    message = "which sporty bmws do you have in stock"
    r = client.post("/api/ai/chat", json={"message": message})
    data = r.get_json()
    assert data["ok"] and data["context"] == "search"
    assert data["search"]["q"] == message          # raw query, not the cut rewrite
    assert "BMW convert" not in data["search"]["url"]
    assert parsed[-1] == message                   # filters were parsed from the raw query


def test_complete_rewrite_still_used_when_not_truncated(client, monkeypatch):
    """An untruncated Completion keeps the pre-existing rewrite behavior."""
    from backend.utils.local_llm import Completion

    _allow_feature(monkeypatch)
    monkeypatch.setattr("backend.utils.query_parser.parse_natural_query",
                        lambda m: {"make": "BMW", "body_style": "convertible"})
    full = Completion('"BMW convertible"', stop_reason="stop", truncated=False, provider="local")
    monkeypatch.setattr("backend.utils.llm_client.complete", lambda *a, **k: full)

    r = client.post("/api/ai/chat", json={"message": "which sporty bmws do you have in stock"})
    data = r.get_json()
    assert data["ok"] and data["context"] == "search"
    assert data["search"]["q"] == "BMW convertible"


def test_anonymous_blocked_in_production_even_with_billing_off(client, monkeypatch):
    """2026-09-30: with billing off require_feature() passes everyone, so anonymous
    visitors could spend paid model calls. Production now needs an account."""
    _allow_feature(monkeypatch, ok=True)
    monkeypatch.setattr(bp, "is_production_env", lambda: True)
    r = client.post("/api/ai/chat", json={"message": "hi"})
    assert r.status_code == 401
    assert r.get_json()["error"] == "login_required"
