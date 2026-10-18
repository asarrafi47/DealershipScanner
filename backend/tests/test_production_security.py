"""SEC-081–SEC-084: production config guards and privileged-access policy."""

from __future__ import annotations

import logging

import pytest

from backend.utils.production_security import (
    app_admin_dev_pass_through_allowed,
    assert_production_security_config,
)


def test_app_admin_dev_pass_through_off_in_production(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.delenv("ALLOW_APP_ADMIN_DEV_PASS_THROUGH", raising=False)
    assert app_admin_dev_pass_through_allowed() is False


def test_app_admin_dev_pass_through_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.setenv("ALLOW_APP_ADMIN_DEV_PASS_THROUGH", "1")
    assert app_admin_dev_pass_through_allowed() is True


def test_production_rejects_dev_console_without_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.setenv("DEV_CONSOLE", "1")
    monkeypatch.delenv("DEV_CONSOLE_SECRET", raising=False)
    monkeypatch.delenv("ALLOW_UNENCRYPTED_USER_DB", raising=False)
    monkeypatch.setenv("USERS_DB_ENCRYPTION_KEY", "x" * 32)
    monkeypatch.setenv("DEV_USERS_DB_ENCRYPTION_KEY", "y" * 32)
    with pytest.raises(RuntimeError, match="DEV_CONSOLE_SECRET"):
        assert_production_security_config()


def test_production_import_with_dev_console_secret(
    monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory
) -> None:
    main = app_factory(production=True, ALLOW_DEFAULT_APP_USER="0", DEV_CONSOLE=None)
    assert main.app is not None


def test_security_headers_on_login(monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory) -> None:
    main = app_factory(ALLOW_DEFAULT_APP_USER="0")
    rv = main.app.test_client().get("/login")
    assert rv.headers.get("X-Content-Type-Options") == "nosniff"
    assert rv.headers.get("X-Frame-Options") == "DENY"


def test_app_admin_session_does_not_access_dev_api_in_production(
    monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory
) -> None:
    main = app_factory(
        production=True,
        ALLOW_DEFAULT_APP_USER="0",
        ALLOW_APP_ADMIN_DEV_PASS_THROUGH=None,
    )
    client = main.app.test_client()
    with client.session_transaction() as sess:
        sess["user_id"] = 1
        sess["user_role"] = "admin"
        sess["username"] = "ops"
    rv = client.get("/dev/api/status")
    assert rv.status_code == 401


def test_dev_console_secret_required_in_production(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.setenv("DEV_CONSOLE", "1")
    monkeypatch.delenv("DEV_CONSOLE_SECRET", raising=False)
    from backend.dev import console

    assert console.dev_console_secret_required_in_production() is True


def test_dealer_locator_google_requires_login_in_production(
    monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory
) -> None:
    main = app_factory(
        production=True,
        ALLOW_DEFAULT_APP_USER="0",
        GOOGLE_MAPS_API_KEY="fake-key-for-test",
        DEALER_LOCATOR_REQUIRE_LOGIN="1",
    )
    rv = main.app.test_client().get("/api/dealer-locator?zip=92618")
    assert rv.status_code == 403
    assert rv.get_json().get("error") == "login_required"


# --- P3.6: production config warnings -------------------------------------------------

_PS_LOGGER = "backend.utils.production_security"
_BASE_URL_WARNING = "PUBLIC_BASE_URL is unset"
_EMAIL_WARNING = "neither RESEND_API_KEY nor SMTP_HOST"
_PASS_THROUGH_WARNING = "ALLOW_APP_ADMIN_DEV_PASS_THROUGH=1 in production"

_P36_ENV_KEYS = (
    "PASSWORD_RESET_ENABLED",
    "EMAIL_VERIFICATION_ENABLED",
    "BILLING_STRIPE_ENABLED",
    "PUBLIC_BASE_URL",
    "MFA_QR_BASE_URL",
    "RESEND_API_KEY",
    "SMTP_HOST",
    "ALLOW_APP_ADMIN_DEV_PASS_THROUGH",
    "DEV_CONSOLE",
)


@pytest.fixture()
def prod_config_env(monkeypatch: pytest.MonkeyPatch):
    """Production env with every P3.6 input blank; unrelated fail-fast checks stubbed."""
    import backend.db.inventory_pg as inventory_pg
    import backend.utils.production_security as ps

    monkeypatch.setenv("FLASK_ENV", "production")
    for key in _P36_ENV_KEYS:
        monkeypatch.setenv(key, "")
    monkeypatch.setattr(ps, "assert_credential_db_encryption_config", lambda: None)
    monkeypatch.setattr(inventory_pg, "assert_inventory_backend_configured", lambda: None)
    return monkeypatch


def _warnings_after_check(caplog: pytest.LogCaptureFixture) -> list[str]:
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=_PS_LOGGER):
        assert_production_security_config()
    return [r.getMessage() for r in caplog.records if r.name == _PS_LOGGER]


def _count(messages: list[str], needle: str) -> int:
    return sum(1 for m in messages if needle in m)


_LINK_FLAG_SETS = [
    (),
    ("PASSWORD_RESET_ENABLED",),
    ("EMAIL_VERIFICATION_ENABLED",),
    ("BILLING_STRIPE_ENABLED",),
    ("PASSWORD_RESET_ENABLED", "EMAIL_VERIFICATION_ENABLED", "BILLING_STRIPE_ENABLED"),
]


@pytest.mark.parametrize("flags", _LINK_FLAG_SETS)
@pytest.mark.parametrize("public_base_url", ["", "https://sarraficars.com"])
def test_public_base_url_warning_fires_exactly_when_link_flow_on_without_base(
    prod_config_env, caplog, flags, public_base_url
) -> None:
    for flag in flags:
        prod_config_env.setenv(flag, "1")
    prod_config_env.setenv("PUBLIC_BASE_URL", public_base_url)
    # Keep the email-delivery warning out of the way of this matrix.
    prod_config_env.setenv("RESEND_API_KEY", "re_test")

    messages = _warnings_after_check(caplog)

    expected = bool(flags) and not public_base_url
    assert _count(messages, _BASE_URL_WARNING) == (1 if expected else 0)
    if expected:
        line = next(m for m in messages if _BASE_URL_WARNING in m)
        for flag in flags:
            assert flag in line


def test_public_base_url_warning_ignores_mfa_qr_base_url(prod_config_env, caplog) -> None:
    """The spec keys on PUBLIC_BASE_URL: Stripe return URLs never read MFA_QR_BASE_URL."""
    prod_config_env.setenv("BILLING_STRIPE_ENABLED", "1")
    prod_config_env.setenv("MFA_QR_BASE_URL", "https://sarraficars.com")
    assert _count(_warnings_after_check(caplog), _BASE_URL_WARNING) == 1


_EMAIL_FLAG_SETS = [
    (),
    ("PASSWORD_RESET_ENABLED",),
    ("EMAIL_VERIFICATION_ENABLED",),
    ("BILLING_STRIPE_ENABLED",),
    ("PASSWORD_RESET_ENABLED", "EMAIL_VERIFICATION_ENABLED"),
]


@pytest.mark.parametrize("flags", _EMAIL_FLAG_SETS)
@pytest.mark.parametrize("resend_key", ["", "re_test"])
@pytest.mark.parametrize("smtp_host", ["", "smtp.example.com"])
def test_email_delivery_warning_fires_exactly_when_email_flow_has_no_transport(
    prod_config_env, caplog, flags, resend_key, smtp_host
) -> None:
    for flag in flags:
        prod_config_env.setenv(flag, "1")
    prod_config_env.setenv("RESEND_API_KEY", resend_key)
    prod_config_env.setenv("SMTP_HOST", smtp_host)
    prod_config_env.setenv("PUBLIC_BASE_URL", "https://sarraficars.com")

    messages = _warnings_after_check(caplog)

    email_flows = [f for f in flags if f != "BILLING_STRIPE_ENABLED"]
    expected = bool(email_flows) and not resend_key and not smtp_host
    assert _count(messages, _EMAIL_WARNING) == (1 if expected else 0)
    if expected:
        line = next(m for m in messages if _EMAIL_WARNING in m)
        for flag in email_flows:
            assert flag in line


@pytest.mark.parametrize(
    ("value", "expected"),
    [("", False), ("0", False), ("no", False), ("1", True), ("true", True)],
)
def test_pass_through_guard_warns_exactly_when_enabled(prod_config_env, caplog, value, expected) -> None:
    prod_config_env.setenv("ALLOW_APP_ADMIN_DEV_PASS_THROUGH", value)
    messages = _warnings_after_check(caplog)
    assert _count(messages, _PASS_THROUGH_WARNING) == (1 if expected else 0)
    # The warning tracks what the app honors.
    assert app_admin_dev_pass_through_allowed() is expected


def test_p36_warnings_silent_outside_production(prod_config_env, caplog) -> None:
    prod_config_env.setenv("FLASK_ENV", "development")
    for flag in ("PASSWORD_RESET_ENABLED", "EMAIL_VERIFICATION_ENABLED", "BILLING_STRIPE_ENABLED"):
        prod_config_env.setenv(flag, "1")
    prod_config_env.setenv("ALLOW_APP_ADMIN_DEV_PASS_THROUGH", "1")
    messages = _warnings_after_check(caplog)
    assert _count(messages, _BASE_URL_WARNING) == 0
    assert _count(messages, _EMAIL_WARNING) == 0
    assert _count(messages, _PASS_THROUGH_WARNING) == 0
