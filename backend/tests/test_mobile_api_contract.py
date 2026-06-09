"""Flask app exposes every route in backend.mobile.contract (iOS API surface)."""

from __future__ import annotations

import importlib

import pytest

from backend.mobile.contract import MOBILE_API_ROUTES, MobileRoute


def _fresh_app(monkeypatch, tmp_path):
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_contract.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users_contract.db"))
    monkeypatch.delenv("USERS_DB_ENCRYPTION_KEY", raising=False)
    import backend.main as main

    importlib.reload(main)
    return main.app


def _rule_keys(app) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for rule in app.url_map.iter_rules():
        if rule.endpoint == "static":
            continue
        methods = {m for m in rule.methods if m not in ("HEAD", "OPTIONS")}
        for method in methods:
            out.add((method, rule.rule))
    return out


@pytest.mark.parametrize("route", MOBILE_API_ROUTES, ids=lambda r: f"{r.method} {r.path}")
def test_mobile_route_registered(route: MobileRoute, monkeypatch, tmp_path) -> None:
    app = _fresh_app(monkeypatch, tmp_path)
    keys = _rule_keys(app)
    assert (route.method, route.path) in keys, f"missing {route.method} {route.path}"


@pytest.mark.parametrize("route", MOBILE_API_ROUTES, ids=lambda r: r.endpoint)
def test_mobile_endpoint_name(route: MobileRoute, monkeypatch, tmp_path) -> None:
    app = _fresh_app(monkeypatch, tmp_path)
    endpoints = {rule.endpoint for rule in app.url_map.iter_rules()}
    assert route.endpoint in endpoints


def test_contract_matches_ios_doc_route_count() -> None:
    """Guardrail: ios/docs/API_CONTRACT.md table should list the same routes."""
    assert len(MOBILE_API_ROUTES) == 15
