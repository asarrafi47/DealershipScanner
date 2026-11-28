"""Tests for the AI narration blueprint (backend.routes.ai_narrate_bp)."""
from __future__ import annotations

import pytest
from flask import Flask

import backend.routes.ai_narrate_bp as bp_mod
from backend.billing import access as paid_access
from backend.routes.ai_narrate_bp import ai_narrate_bp
from backend.utils.csrf import _SESSION_KEY

_TOKEN = "n" * 32


@pytest.fixture(autouse=True)
def _clear_narration_cache():
    bp_mod._cache.clear()
    yield
    bp_mod._cache.clear()


@pytest.fixture()
def client(monkeypatch):
    def fake_get_car_by_id(car_id, *, include_inactive=False):
        if car_id == 1:
            return {"id": 1, "year": 2022, "make": "BMW", "model": "M3"}
        return None

    monkeypatch.setattr(bp_mod, "get_car_by_id", fake_get_car_by_id)
    monkeypatch.setattr(
        bp_mod, "narrate_vehicle", lambda row: "A tidy 2022 BMW M3."
    )

    # The narrate endpoint requires sign-in and the CSRF header.
    monkeypatch.setattr(
        paid_access,
        "current_access",
        lambda: paid_access.AccessContext(logged_in=True, user_id=1, profile={"id": 1}),
    )

    app = Flask(__name__)
    app.secret_key = "narrate-test-secret"
    app.register_blueprint(ai_narrate_bp)
    app.testing = True
    c = app.test_client()
    with c.session_transaction() as sess:
        sess["user_id"] = 1
        sess[_SESSION_KEY] = _TOKEN
    c.environ_base["HTTP_X_CSRF_TOKEN"] = _TOKEN
    return c


def test_narrate_found(client):
    resp = client.get("/api/car/1/narrate")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["description"] == "A tidy 2022 BMW M3."


def test_narrate_missing_returns_404(client):
    resp = client.get("/api/car/999/narrate")
    assert resp.status_code == 404
    data = resp.get_json()
    assert data["ok"] is False
    assert "error" in data


def test_narrate_error_returns_500(client, monkeypatch):
    def boom(row):
        raise RuntimeError("provider down")

    monkeypatch.setattr(bp_mod, "narrate_vehicle", boom)
    resp = client.get("/api/car/1/narrate")
    assert resp.status_code == 500
    data = resp.get_json()
    assert data["ok"] is False
    # The raw exception detail must NOT leak to the client (it is logged instead).
    assert data["error"] == "narration_failed"
    assert "provider down" not in data["error"]


def test_narrate_caches_repeat_requests(client, monkeypatch):
    calls = []

    def counting_narrate(row):
        calls.append(1)
        return "A tidy 2022 BMW M3."

    monkeypatch.setattr(bp_mod, "narrate_vehicle", counting_narrate)
    r1 = client.get("/api/car/1/narrate")
    r2 = client.get("/api/car/1/narrate")
    assert r1.status_code == 200 and r2.status_code == 200
    assert r2.get_json().get("cached") is True
    assert len(calls) == 1  # generated once; second request served from cache
