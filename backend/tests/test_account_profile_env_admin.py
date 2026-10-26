"""SEC-A-001: profile edits may not move an account onto an APP_ADMIN_* identity."""

from __future__ import annotations


def test_update_profile_blocks_unclaimed_env_admin_identity(monkeypatch, app_factory):
    monkeypatch.setenv("APP_ADMIN_EMAILS", "boss@example.com")
    monkeypatch.setenv("APP_ADMIN_USERNAMES", "bossuser")
    app_factory()
    from backend.db.users_db import get_user_profile, save_user, update_user_profile
    from backend.utils.roles import ROLE_GENERAL

    uid = save_user("regular", "regular@example.com", "long-enough-password", role=ROLE_GENERAL)
    assert update_user_profile(uid, "regular", "boss@example.com")
    assert update_user_profile(uid, "BossUser", "regular@example.com")
    prof = get_user_profile(uid)
    assert prof["email"] == "regular@example.com" and prof["username"] == "regular"
    # Unrelated edits still work.
    assert update_user_profile(uid, "regular2", "regular@example.com") is None


def test_update_profile_keeps_held_env_admin_identity(monkeypatch, app_factory):
    monkeypatch.setenv("APP_ADMIN_EMAILS", "boss@example.com")
    app_factory()
    from backend.db.users_db import save_user, update_user_profile
    from backend.utils.roles import ROLE_GENERAL

    uid = save_user("boss", "boss@example.com", "long-enough-password", role=ROLE_GENERAL)
    assert update_user_profile(uid, "boss-renamed", "boss@example.com") is None
