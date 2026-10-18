"""P3.1: a username may not be shaped like, or equal to, an account's email.

Login and password reset match username OR email, so ``victim@example.com`` as a
username answered to the victim's login and (before P3.1) to the OAuth email
match. New usernames are refused at registration, dealer registration, admin
create/update and profile update; legacy rows keep working.
"""

from __future__ import annotations

_CSRF = "c" * 32
_PW = "long-enough-password"


def _post_form(client, url: str, data: dict):
    from backend.utils.csrf import _SESSION_KEY

    with client.session_transaction() as sess:
        sess[_SESSION_KEY] = _CSRF
    return client.post(url, data={"csrf_token": _CSRF, **data}, follow_redirects=False)


def test_register_rejects_username_with_at(app_factory) -> None:
    app = app_factory().app
    from backend.db.users_db import find_users_by_email

    with app.test_client() as c:
        r = _post_form(c, "/register", {
            "username": "victim@example.com", "email": "squatter@example.com",
            "password": _PW, "plan": "free",
        })
    assert r.status_code == 200
    assert b"cannot contain @" in r.data
    assert find_users_by_email("squatter@example.com") == []


def test_api_register_rejects_username_with_at(app_factory) -> None:
    app = app_factory().app
    with app.test_client() as c:
        token = c.get("/api/auth/csrf").get_json()["csrf_token"]
        r = c.post(
            "/api/auth/register",
            json={"username": "a@b.example", "email": "squatter@example.com", "password": _PW},
            headers={"X-CSRF-Token": token},
        )
    assert r.status_code == 400
    assert r.get_json()["error"] == "validation_error"


def test_username_equal_to_existing_email_counts_as_taken(app_factory) -> None:
    app_factory()
    from backend.db.users_db import save_user, user_exists_by_username

    save_user("victim", "victim@example.com", _PW, role="general_user")
    assert user_exists_by_username("victim")
    assert user_exists_by_username("VICTIM@example.com")
    assert not user_exists_by_username("someone-else")


def test_dealer_register_rejects_username_with_at(app_factory) -> None:
    app = app_factory().app
    from backend.db.users_db import find_users_by_email

    with app.test_client() as c:
        r = _post_form(c, "/dealer/register", {
            "username": "victim@example.com", "email": "dealer@example.com",
            "password": "dealer-password-long-1", "org_name": "Squat Motors",
        })
    assert r.status_code == 200
    assert b"cannot contain @" in r.data
    assert find_users_by_email("dealer@example.com") == []


def test_registration_form_error_still_accepts_legacy_at_username() -> None:
    """Password reset/change pass the stored username here; legacy '@' names must pass."""
    from backend.utils.registration_validation import registration_form_error, username_format_error

    assert registration_form_error("old@name.example", "old@name.example", _PW, min_password_len=8) is None
    assert username_format_error("old@name.example")
    assert username_format_error("plain_name") is None


def test_profile_update_rejects_new_username_with_at_or_equal_to_email(app_factory) -> None:
    app_factory()
    from backend.db.users_db import save_user, update_user_profile

    uid = save_user("someone", "someone@example.com", _PW, role="general_user")
    save_user("victim", "victim@example.com", _PW, role="general_user")
    assert "cannot contain @" in (update_user_profile(uid, "victim@example.com", "someone@example.com") or "")
    assert "cannot contain @" in (update_user_profile(uid, "x@y", "someone@example.com") or "")
    assert update_user_profile(uid, "someone2", "someone@example.com") is None


def test_profile_update_keeps_unchanged_legacy_at_username(app_factory) -> None:
    app_factory()
    from backend.db.users_db import get_user_profile, save_user, update_user_profile

    uid = save_user("legacy@name.example", "legacy@name.example", _PW, role="general_user")
    assert update_user_profile(uid, "legacy@name.example", "new@name.example") is None
    assert get_user_profile(uid)["email"] == "new@name.example"


def test_profile_email_change_clears_verification_state(app_factory) -> None:
    app_factory()
    from backend.db.users_db import (
        get_user_email_verification_state,
        mark_user_email_verified,
        save_user,
        set_user_email_verify_token,
        update_user_profile,
    )

    uid = save_user("someone", "someone@example.com", _PW, role="general_user")
    assert mark_user_email_verified(uid)
    # Unchanged email (case only): verification stays.
    assert update_user_profile(uid, "someone", "SOMEONE@example.com") is None
    assert get_user_email_verification_state(uid)["email_verified_at"]

    assert update_user_profile(uid, "someone", "other@example.com") is None
    assert not get_user_email_verification_state(uid)["email_verified_at"]

    # A verify link issued for the old address must not verify the new one.
    assert set_user_email_verify_token(uid, "hash-for-old-address", 4_000_000_000)
    assert update_user_profile(uid, "someone", "third@example.com") is None
    state = get_user_email_verification_state(uid)
    assert not state.get("email_verify_token_hash")
    assert not state.get("email_verified_at")


def test_admin_create_and_update_apply_username_rules(app_factory) -> None:
    app_factory()
    from backend.db.users_db import (
        admin_create_user,
        admin_update_user,
        get_user_email_verification_state,
        mark_user_email_verified,
        save_user,
    )

    save_user("victim", "victim@example.com", _PW, role="general_user")
    uid, err = admin_create_user(
        username="victim@example.com", email="new@example.com", password=_PW, role="general_user"
    )
    assert uid is None and "cannot contain @" in (err or "")

    uid, err = admin_create_user(username="newbie", email="new@example.com", password=_PW, role="general_user")
    assert err is None and uid
    assert mark_user_email_verified(uid)
    err = admin_update_user(
        uid, username="victim@example.com", email="new@example.com", role="general_user", actor_user_id=1
    )
    assert "cannot contain @" in (err or "")
    err = admin_update_user(uid, username="newbie", email="moved@example.com", role="general_user", actor_user_id=1)
    assert err is None
    assert not get_user_email_verification_state(uid)["email_verified_at"]
