"""Regression tests for May 2026 audit fixes (premium gates, chat limits, sticker preview)."""

from __future__ import annotations

import importlib

import pytest


def _fresh_app(monkeypatch: pytest.MonkeyPatch, tmp_path, **env):
    flask_env = env.pop("FLASK_ENV", "development")
    monkeypatch.setenv("FLASK_ENV", flask_env)
    monkeypatch.setenv("MFA_DELIVERY_MODE", "log")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_test.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users_test.db"))
    monkeypatch.setenv("ALLOW_DEFAULT_APP_USER", "0")
    monkeypatch.setenv("RATE_LIMIT_SQLITE_PATH", str(tmp_path / "rate_limits.db"))
    monkeypatch.delenv("USERS_DB_ENCRYPTION_KEY", raising=False)
    if flask_env == "production":
        monkeypatch.setenv("SECRET_KEY", "pytest-secret-key-do-not-use-in-deployment")
        monkeypatch.setenv("ADMIN_PASSWORD", "pytest-admin-bootstrap-do-not-use-in-deployment")
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    import backend.main as main

    importlib.reload(main)
    return main


def test_sticker_preview_requires_premium_when_billing_on(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    main = _fresh_app(
        monkeypatch,
        tmp_path,
        BILLING_STRIPE_ENABLED="1",
    )
    car = {
        "id": 7,
        "vin": "1C6RRFFG4NN401203",
        "make": "Ram",
        "inactive": 0,
        "gallery": [],
    }
    monkeypatch.setattr(main, "get_car_by_id", lambda *_a, **_k: car)

    from pathlib import Path

    fake_pdf = tmp_path / "sticker.pdf"
    fake_pdf.write_bytes(b"%PDF-1.4")
    fake_png = tmp_path / "sticker.png"
    fake_png.write_bytes(b"\x89PNG\r\n\x1a\n")

    def _local_path(**_k):
        return fake_pdf

    def _ensure_png(_path):
        return fake_png

    monkeypatch.setattr(
        "backend.enrichment.window_sticker_service.window_sticker_local_path",
        _local_path,
    )
    monkeypatch.setattr(
        "backend.enrichment.window_sticker_service.ensure_sticker_preview_png",
        _ensure_png,
    )

    client = main.app.test_client()
    rv = client.get("/car/7/window-sticker-preview.png")
    assert rv.status_code == 403


def test_car_chat_requires_login_in_production_when_billing_off(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    main = _fresh_app(
        monkeypatch,
        tmp_path,
        FLASK_ENV="production",
        BILLING_STRIPE_ENABLED="0",
    )
    monkeypatch.setattr(
        main,
        "get_car_by_id",
        lambda *_a, **_k: {"id": 1, "year": 2020, "make": "Test", "model": "Car", "inactive": 0},
    )
    from backend.utils.csrf import _SESSION_KEY

    tok = "t" * 32
    with main.app.test_client() as c:
        with c.session_transaction() as sess:
            sess[_SESSION_KEY] = tok
        rv = c.post(
            "/api/car/1/chat",
            json={"message": "hello"},
            headers={"X-CSRF-Token": tok, "Content-Type": "application/json"},
        )
        assert rv.status_code == 403
        body = rv.get_json()
        assert body is not None
        assert body.get("error") == "login_required"


def test_car_chat_daily_limit_is_per_user(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    main = _fresh_app(
        monkeypatch,
        tmp_path,
        BILLING_STRIPE_ENABLED="0",
        CAR_CHAT_MAX_PER_USER_DAILY="1",
        RATE_LIMIT_CAR_CHAT_GLOBAL_PER_MIN="500",
        RATE_LIMIT_CAR_CHAT_PER_IP_PER_MIN="500",
        RATE_LIMIT_CAR_CHAT_PER_MIN="500",
    )
    dummy_car = {"id": 1, "year": 2020, "make": "Test", "model": "Car", "vin": "X", "inactive": 0}
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
            sess["user_id"] = 42
        hdr = {"X-CSRF-Token": tok, "Content-Type": "application/json"}
        rv1 = c.post("/api/car/1/chat", json={"message": "a"}, headers=hdr)
        assert rv1.status_code == 200
        rv2 = c.post("/api/car/2/chat", json={"message": "b"}, headers=hdr)
        assert rv2.status_code == 429
        body = rv2.get_json()
        assert body is not None
        assert body.get("error") == "user_chat_limit_reached"
