"""Password hashing and credential DB encryption (SEC-082, SEC-088)."""

from __future__ import annotations

import time

import bcrypt
import pytest

from backend.db.password_hash import (
    bcrypt_cost,
    bcrypt_rounds,
    hash_password,
    password_needs_rehash,
    verify_or_legacy,
    verify_password,
)
from backend.utils.credential_db_encryption import assert_credential_db_encryption_config


def test_bcrypt_default_rounds_is_13(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BCRYPT_ROUNDS", raising=False)
    assert bcrypt_rounds() == 13


def test_password_needs_rehash_for_legacy_plaintext() -> None:
    assert password_needs_rehash("plaintext") is True


def test_password_needs_rehash_when_cost_below_target(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BCRYPT_ROUNDS", "13")
    old_hash = bcrypt.hashpw(b"secret", bcrypt.gensalt(rounds=12)).decode("utf-8")
    assert bcrypt_cost(old_hash) == 12
    assert password_needs_rehash(old_hash) is True


def test_password_does_not_need_rehash_at_current_cost(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BCRYPT_ROUNDS", "13")
    current = hash_password("secret")
    assert password_needs_rehash(current) is False
    assert verify_password("secret", current) is True


def test_check_user_upgrades_weak_bcrypt_on_login(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_rehash.db"))
    monkeypatch.delenv("USERS_DB_ENCRYPTION_KEY", raising=False)
    monkeypatch.setenv("BCRYPT_ROUNDS", "13")

    from backend.db import users_db

    users_db.init_users_db()
    uid = users_db.save_user("rehash_user", "rehash@example.com", "UpgradeMe1!")
    weak = bcrypt.hashpw(b"UpgradeMe1!", bcrypt.gensalt(rounds=12)).decode("utf-8")
    conn = users_db.get_conn()
    conn.execute("UPDATE users SET password = ? WHERE id = ?", (weak, uid))
    conn.commit()
    conn.close()

    assert users_db.check_user("rehash_user", "UpgradeMe1!") is True

    deadline = time.time() + 3.0
    upgraded = False
    while time.time() < deadline:
        conn = users_db.get_conn()
        row = conn.execute("SELECT password FROM users WHERE id = ?", (uid,)).fetchone()
        conn.close()
        if row is not None and bcrypt_cost(row[0]) == 13:
            upgraded = True
            break
        time.sleep(0.05)
    assert upgraded, "expected async bcrypt rehash to finish within 3s"


def test_production_requires_credential_db_encryption_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.delenv("ALLOW_UNENCRYPTED_USER_DB", raising=False)
    monkeypatch.delenv("USERS_DB_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("DEV_USERS_DB_ENCRYPTION_KEY", raising=False)
    with pytest.raises(RuntimeError, match="USERS_DB_ENCRYPTION_KEY"):
        assert_credential_db_encryption_config()


def test_allow_unencrypted_forbidden_in_production(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.setenv("ALLOW_UNENCRYPTED_USER_DB", "1")
    monkeypatch.setenv("USERS_DB_ENCRYPTION_KEY", "x" * 32)
    monkeypatch.setenv("DEV_USERS_DB_ENCRYPTION_KEY", "y" * 32)
    with pytest.raises(RuntimeError, match="ALLOW_UNENCRYPTED_USER_DB"):
        assert_credential_db_encryption_config()


def test_plaintext_password_rejected_in_production(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASK_ENV", "production")
    assert verify_or_legacy("secret", "secret") is False
    assert verify_or_legacy("secret", hash_password("secret")) is True
