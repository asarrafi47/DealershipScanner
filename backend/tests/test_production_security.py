"""SEC-081–SEC-084: production config guards and privileged-access policy."""

from __future__ import annotations

import importlib

import pytest

from backend.db.password_hash import hash_password, verify_or_legacy
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
    with pytest.raises(RuntimeError, match="DEV_CONSOLE_SECRET"):
        assert_production_security_config()


def test_plaintext_password_rejected_in_production(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASK_ENV", "production")
    assert verify_or_legacy("secret", "secret") is False
    assert verify_or_legacy("secret", hash_password("secret")) is True


def test_production_import_with_dev_console_secret(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.setenv("SECRET_KEY", "pytest-secret-key-do-not-use-in-deployment")
    monkeypatch.setenv("ADMIN_PASSWORD", "pytest-admin-bootstrap-do-not-use-in-deployment")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users.db"))
    monkeypatch.setenv("ALLOW_DEFAULT_APP_USER", "0")
    monkeypatch.delenv("DEV_CONSOLE", raising=False)
    import backend.main as main

    importlib.reload(main)
    assert main.app is not None


def test_security_headers_on_login(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_h.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_h.db"))
    monkeypatch.setenv("ALLOW_DEFAULT_APP_USER", "0")
    import backend.main as main

    importlib.reload(main)
    rv = main.app.test_client().get("/login")
    assert rv.headers.get("X-Content-Type-Options") == "nosniff"
    assert rv.headers.get("X-Frame-Options") == "DENY"


def test_app_admin_session_does_not_access_dev_api_in_production(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.setenv("SECRET_KEY", "pytest-secret-key-do-not-use-in-deployment")
    monkeypatch.setenv("ADMIN_PASSWORD", "pytest-admin-bootstrap-do-not-use-in-deployment")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_dev.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users_dev.db"))
    monkeypatch.setenv("ALLOW_DEFAULT_APP_USER", "0")
    monkeypatch.delenv("ALLOW_APP_ADMIN_DEV_PASS_THROUGH", raising=False)
    import backend.main as main

    importlib.reload(main)
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
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.setenv("SECRET_KEY", "pytest-secret-key-do-not-use-in-deployment")
    monkeypatch.setenv("ADMIN_PASSWORD", "pytest-admin-bootstrap-do-not-use-in-deployment")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_dl.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_dl.db"))
    monkeypatch.setenv("ALLOW_DEFAULT_APP_USER", "0")
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "fake-key-for-test")
    import backend.main as main

    importlib.reload(main)
    rv = main.app.test_client().get("/api/dealer-locator?zip=92618")
    assert rv.status_code == 403
    assert rv.get_json().get("error") == "login_required"
