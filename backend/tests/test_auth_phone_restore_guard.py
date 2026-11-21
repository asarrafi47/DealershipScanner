"""S5 review (f): phone sign-in refuses to start when identities point past the users table."""

from __future__ import annotations

import pytest

from backend.auth.phone import restore_guard


def _raise():
    raise RuntimeError("database down")


def test_guard_passes_when_identities_stay_within_users():
    assert restore_guard.restore_guard_problem(identity_max=lambda: 40, users_max=lambda: 40) is None
    assert restore_guard.restore_guard_problem(identity_max=lambda: 0, users_max=lambda: 0) is None


def test_guard_fails_when_an_identity_names_an_id_above_the_users_table():
    problem = restore_guard.restore_guard_problem(identity_max=lambda: 41, users_max=lambda: 40)
    assert problem and "restored" in problem
    assert "41" not in problem and "40" not in problem  # no values in the message


def test_guard_fails_closed_when_either_side_cannot_be_read():
    assert restore_guard.restore_guard_problem(identity_max=_raise, users_max=lambda: 1)
    assert restore_guard.restore_guard_problem(identity_max=lambda: 0, users_max=_raise)


def test_boot_refuses_production_with_the_flag_on_and_a_restored_users_table(monkeypatch):
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.setenv("AUTH_PHONE_LOGIN_ENABLED", "1")
    with pytest.raises(RuntimeError, match="restore guard"):
        restore_guard.assert_identity_ids_within_users(identity_max=lambda: 9, users_max=lambda: 3)
    restore_guard.assert_identity_ids_within_users(identity_max=lambda: 3, users_max=lambda: 3)


def test_boot_check_does_nothing_with_the_flag_off_or_outside_production(monkeypatch):
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.delenv("AUTH_PHONE_LOGIN_ENABLED", raising=False)
    restore_guard.assert_identity_ids_within_users(identity_max=_raise, users_max=_raise)
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("AUTH_PHONE_LOGIN_ENABLED", "1")
    restore_guard.assert_identity_ids_within_users(identity_max=_raise, users_max=_raise)


def test_production_security_runs_the_restore_guard(monkeypatch):
    import backend.db.inventory_pg as inventory_pg
    import backend.utils.production_security as ps
    from backend.auth.phone import config

    monkeypatch.setattr(ps, "assert_credential_db_encryption_config", lambda: None)
    monkeypatch.setattr(inventory_pg, "assert_inventory_backend_configured", lambda: None)
    monkeypatch.setattr(config, "assert_phone_login_config_for_production", lambda: None)
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.setenv("AUTH_PHONE_LOGIN_ENABLED", "1")
    monkeypatch.setattr(restore_guard, "max_identity_user_id", lambda: 5)
    monkeypatch.setattr(restore_guard, "max_users_id", lambda: 4)
    with pytest.raises(RuntimeError, match="restore guard"):
        ps.assert_production_security_config()


def test_max_users_id_reads_the_users_table(tmp_path, monkeypatch):
    from backend.db.users_db import _common

    class _Conn:
        def execute(self, sql):
            assert "MAX(id)" in sql and "sqlite_sequence" in sql

            class _R:
                def fetchone(self):
                    return (12,)

            return _R()

        def close(self):
            pass

    monkeypatch.setattr(_common, "get_conn", lambda: _Conn())
    assert restore_guard.max_users_id() == 12


def _users_db(tmp_path, monkeypatch):
    """A real users database (the app's own schema, ids AUTOINCREMENT) in a temporary file."""
    from backend.db.users_db import _common, init_users_db

    from backend.tests.conftest import app_env_defaults

    for name, value in app_env_defaults(tmp_path).items():  # every user database under tmp_path
        monkeypatch.setenv(name, value)
    init_users_db()
    conn = _common.get_conn()
    conn.execute("DELETE FROM users")
    for n in (1, 2, 3):
        conn.execute(
            "INSERT INTO users (id, username, email, password) "
            f"VALUES ({n}, 'guard{n}', 'guard{n}@example.invalid', 'unusable')"
        )
    conn.commit()
    return conn


def test_deleting_the_newest_account_does_not_trip_the_guard(tmp_path, monkeypatch):
    conn = _users_db(tmp_path, monkeypatch)
    conn.execute("DELETE FROM users WHERE id = 3")
    conn.commit()
    conn.close()
    assert restore_guard.max_users_id() == 3  # the highest id ever issued, not MAX(id)
    assert restore_guard.restore_guard_problem(identity_max=lambda: 3) is None


def test_a_restored_older_users_copy_still_trips_the_guard(tmp_path, monkeypatch):
    conn = _users_db(tmp_path, monkeypatch)
    conn.execute("DELETE FROM users WHERE id = 3")
    conn.execute("UPDATE sqlite_sequence SET seq = 2 WHERE name = 'users'")  # as an older copy would have
    conn.commit()
    conn.close()
    assert restore_guard.max_users_id() == 2
    assert restore_guard.restore_guard_problem(identity_max=lambda: 3)
