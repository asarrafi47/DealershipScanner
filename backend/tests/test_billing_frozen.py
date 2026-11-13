"""F1: the site is free, billing is dormant (owner decision 2026-11-13)."""

from __future__ import annotations

import logging
import re

import pytest

from backend.utils.production_security import assert_production_security_config

_PRICE = re.compile(r"\$\d+\.\d\d")
_UPSELL = re.compile(r"(?i)upgrade|go premium|view plans|premium plans")
_PLAN_NAMES = ("Research plan", "Assistant plan", "Complete plan")
_PS_LOGGER = "backend.utils.production_security"


@pytest.fixture()
def free_app(monkeypatch, app_factory):
    monkeypatch.delenv("BILLING_STRIPE_ENABLED", raising=False)
    return app_factory().app


def test_shipped_default_is_billing_off(monkeypatch):
    from backend.config import Config

    monkeypatch.delenv("BILLING_STRIPE_ENABLED", raising=False)
    assert Config.billing_stripe_enabled() is False


@pytest.mark.parametrize("path", ["/", "/listings", "/compare", "/login", "/register"])
def test_no_price_plan_or_upgrade_text_with_billing_off(free_app, path):
    r = free_app.test_client().get(path)
    assert r.status_code == 200
    body = r.get_data(as_text=True)
    assert not _PRICE.search(body)
    assert not _UPSELL.search(body)
    assert not any(n in body for n in _PLAN_NAMES)


def test_premium_page_shows_no_price_with_billing_off(free_app):
    r = free_app.test_client().get("/premium")
    assert r.status_code == 200
    body = r.get_data(as_text=True)
    assert not _PRICE.search(body)
    assert "aren't on sale yet" in body
    assert "Upgrade to Premium" not in body


def test_billing_plans_json_is_404_with_billing_off(free_app):
    assert free_app.test_client().get("/billing/plans").status_code == 404


@pytest.mark.parametrize(
    "path,dest",
    [
        ("/billing/required", "/home"),
        ("/billing/checkout", "/home"),
        ("/billing/premium/checkout", "/premium"),
        ("/billing/plan/checkout", "/premium"),
    ],
)
def test_billing_pages_redirect_with_billing_off(free_app, path, dest):
    r = free_app.test_client().get(path, follow_redirects=False)
    assert r.status_code in (302, 303)
    assert r.headers["Location"].endswith(dest)


@pytest.mark.parametrize("path", ["/billing/webhook", "/billing/premium/webhook"])
def test_both_stripe_webhooks_404_with_billing_off(free_app, path):
    assert free_app.test_client().post(path, data=b"{}").status_code == 404


def _prod_env(monkeypatch):
    import backend.db.inventory_pg as inventory_pg
    import backend.utils.production_security as ps

    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.setattr(ps, "assert_credential_db_encryption_config", lambda: None)
    monkeypatch.setattr(inventory_pg, "assert_inventory_backend_configured", lambda: None)


def _warnings(caplog):
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=_PS_LOGGER):
        assert_production_security_config()
    return [r.getMessage() for r in caplog.records if r.name == _PS_LOGGER]


def test_boot_warning_when_production_starts_with_billing_on(monkeypatch, caplog):
    _prod_env(monkeypatch)
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "1")
    assert any("billing is dormant by owner decision" in m for m in _warnings(caplog))


def test_no_billing_warning_when_production_billing_off(monkeypatch, caplog):
    _prod_env(monkeypatch)
    monkeypatch.delenv("BILLING_STRIPE_ENABLED", raising=False)
    assert not any("BILLING_STRIPE_ENABLED" in m and "dormant" in m for m in _warnings(caplog))
