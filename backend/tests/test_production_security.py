"""SEC-081–SEC-084: production config guards and privileged-access policy."""

from __future__ import annotations

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
