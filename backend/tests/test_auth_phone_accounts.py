"""Card S7 (the part card S8 needs): accounts without an email.

The phone sign-in service stores a placeholder address (``p-<id>-<hmac8>@phone.invalid``) so the
users table keeps its NOT NULL unique email. These tests pin the rules around it: who may sign in by
phone, how the account is created and adopted, and that the placeholder never reaches a session
cookie, a JSON answer, a profile page or a mail.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from backend.auth.phone import accounts
from backend.auth.phone.accounts import (
    PhoneAccounts,
    is_phone_eligible,
    is_placeholder_email,
    public_email,
    purge_phone_identity,
)

PLACEHOLDER = "p-12-0a1b2c3d@phone.invalid"
USERNAME = "user-12-0a1b"


# ----------------------------------------------------------------- the placeholder predicate


@pytest.mark.parametrize(
    "value",
    [
        PLACEHOLDER,
        PLACEHOLDER.upper(),
        "  " + PLACEHOLDER + "  ",
        "anything@phone.invalid",
    ],
)
def test_placeholder_addresses_are_recognised(value):
    assert is_placeholder_email(value) is True
    assert public_email(value) == ""


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        "arman@example.com",
        "a@phone.invalid.example.com",
        "phone.invalid",
        "a@notphone.invalid",
        b"p-1-aa@phone.invalid",
        12,
    ],
)
def test_real_addresses_are_not_placeholders(value):
    assert is_placeholder_email(value) is False


def test_public_email_keeps_a_real_address_and_empties_everything_else():
    assert public_email(" Arman@Example.com ") == "Arman@Example.com"
    assert public_email(None) == ""
    assert public_email(7) == ""
    assert public_email(PLACEHOLDER) == ""


def test_the_service_builds_addresses_this_module_recognises():
    from backend.auth.phone import service

    class Ident:
        id = 12
        detached_at = None

    email, username = service.placeholder_for(Ident())
    assert is_placeholder_email(email)
    assert re.fullmatch(r"p-12-[0-9a-f]{8}@phone\.invalid", email)
    assert re.fullmatch(r"user-12-[0-9a-f]{4}", username)


# ----------------------------------------------------------------- who may sign in by phone


def _row(**over):
    base = {
        "id": 5,
        "username": "shopper",
        "email": PLACEHOLDER,
        "role": "general_user",
        "dealer_id": None,
        "dealership_registry_id": None,
        "org_id": None,
        "is_active": True,
    }
    base.update(over)
    return base


def test_a_plain_active_shopper_is_eligible():
    assert is_phone_eligible(_row()) is True


def test_an_empty_stored_role_is_a_shopper_only_without_dealer_or_org_ids():
    assert is_phone_eligible(_row(role="")) is True
    assert is_phone_eligible(_row(role=None)) is True
    assert is_phone_eligible(_row(role="", dealer_id="d-1")) is False
    assert is_phone_eligible(_row(role="", org_id=3)) is False
    assert is_phone_eligible(_row(role="", dealership_registry_id=9)) is False


@pytest.mark.parametrize(
    "role",
    ["admin", "dealership_owner", "dealership_admin", "dealership_member", "dealer_staff", "general", "nonsense"],
)
def test_other_roles_are_refused(role):
    assert is_phone_eligible(_row(role=role)) is False


def test_a_shopper_row_that_carries_a_dealer_or_org_is_refused():
    assert is_phone_eligible(_row(dealer_id="dealer-9")) is False
    assert is_phone_eligible(_row(dealership_registry_id=4)) is False
    assert is_phone_eligible(_row(org_id=2)) is False
    assert is_phone_eligible(_row(org_id="2")) is False
    assert is_phone_eligible(_row(org_id="not a number")) is False  # unreadable means not a plain shopper


def test_a_suspended_account_is_refused():
    assert is_phone_eligible(_row(is_active=False)) is False
    assert is_phone_eligible(_row(is_active=0)) is False


def test_an_environment_admin_identity_is_refused(monkeypatch):
    monkeypatch.setenv("APP_ADMIN_EMAILS", "boss@example.com")
    monkeypatch.setenv("APP_ADMIN_USERNAMES", "bossuser")
    assert is_phone_eligible(_row(email="boss@example.com")) is False
    assert is_phone_eligible(_row(username="BossUser")) is False
    assert is_phone_eligible(_row()) is True


def test_nothing_to_check_is_refused():
    assert is_phone_eligible(None) is False
    assert is_phone_eligible({}) is False


# ----------------------------------------------------------------- creating and adopting accounts


@pytest.fixture
def users(app_factory):
    app_factory()


def test_adopt_or_create_makes_a_shopper_with_an_unusable_password(users):
    from backend.db.users_db import authenticate_app_user, get_user_by_login

    uid = PhoneAccounts().adopt_or_create(email=PLACEHOLDER, username=USERNAME)
    assert isinstance(uid, int) and uid > 0
    row = get_user_by_login(PLACEHOLDER)
    assert row["id"] == uid and row["username"] == USERNAME
    assert row["role"] == "general_user"  # not the save_user default, which is dealer_staff
    assert not row.get("dealer_id") and not row.get("org_id")
    assert is_phone_eligible(row)
    # Nobody knows the password: not the placeholder, not the username, not an empty string.
    for guess in ("", USERNAME, PLACEHOLDER, "password", "long-enough-password-1"):
        assert authenticate_app_user(USERNAME, guess) is None
        assert authenticate_app_user(PLACEHOLDER, guess) is None


def test_adopt_or_create_is_idempotent_by_email(users):
    from backend.db.users_db import find_users_by_email

    first = PhoneAccounts().adopt_or_create(email=PLACEHOLDER, username=USERNAME)
    again = PhoneAccounts().adopt_or_create(email=PLACEHOLDER, username=USERNAME)
    other_name = PhoneAccounts().adopt_or_create(email=PLACEHOLDER.upper(), username="user-12-ffff")
    assert first == again == other_name
    assert len(find_users_by_email(PLACEHOLDER, limit=5)) == 1


def test_adopt_or_create_survives_a_lost_race(users, monkeypatch):
    """Two tabs, or a retry after a crash: the second creator finds the row the first one wrote."""
    from backend.db import users_db

    winner = users_db.save_user(USERNAME, PLACEHOLDER, "x" * 30, role="general_user", org_id=None)
    real_find = users_db.find_users_by_email
    calls = {"n": 0}

    def late_find(email, *, limit=2):
        calls["n"] += 1
        return [] if calls["n"] == 1 else real_find(email, limit=limit)  # the first look finds nothing

    monkeypatch.setattr(users_db, "find_users_by_email", late_find)
    got = PhoneAccounts().adopt_or_create(email=PLACEHOLDER, username=USERNAME)
    assert got == winner and calls["n"] == 2


def test_adopt_or_create_reports_a_real_failure_without_the_address(users, monkeypatch, caplog):
    from backend.db import users_db

    def boom(*a, **k):
        raise RuntimeError(f"disk full while saving {PLACEHOLDER}")

    monkeypatch.setattr(users_db, "save_user", boom)
    with caplog.at_level("DEBUG"):
        with pytest.raises(RuntimeError):
            PhoneAccounts().adopt_or_create(email=PLACEHOLDER, username=USERNAME)
    text = " ".join(r.getMessage() for r in caplog.records)
    assert "event=account_create_failed reason=RuntimeError" in text
    assert PLACEHOLDER not in text and "phone.invalid" not in text


# ----------------------------------------------------------------- the placeholder never leaves the server


def _phone_user(app, **over):
    from backend.db.users_db import save_user

    return int(save_user(over.get("username", USERNAME), over.get("email", PLACEHOLDER), "x" * 30,
                         role="general_user", org_id=None))


def _sign_in(main, uid, *, method="phone"):
    """A test client that holds the session finalize_app_session would write."""
    from flask import session

    from backend.auth.session import finalize_app_session

    app = main.app
    with app.test_request_context("/"):
        session.clear()
        assert finalize_app_session(uid, auth_method=method) is True
        data = dict(session)
    client = app.test_client()
    with client.session_transaction() as sess:
        sess.update(data)
    return client, data


def test_the_session_cookie_holds_no_placeholder(app_factory):
    main = app_factory()
    uid = _phone_user(main)
    _client, data = _sign_in(main, uid)
    assert data["user_email"] == ""
    assert data["auth_method"] == "phone" and data["auth_role"] == "shopper"
    assert data["user_role"] == "general_user"
    assert "phone.invalid" not in repr(data)


def test_a_normal_account_keeps_its_email_in_the_session(app_factory):
    main = app_factory()
    from backend.db.users_db import save_user

    uid = save_user("regular", "regular@example.com", "long-enough-password-1", role="general_user")
    _client, data = _sign_in(main, int(uid), method="password")
    assert data["user_email"] == "regular@example.com"


def test_the_json_user_payload_hides_the_placeholder(app_factory):
    main = app_factory()
    uid = _phone_user(main)
    client, _data = _sign_in(main, uid)
    body = client.get("/api/auth/me").get_json()
    assert body["ok"] is True
    assert body["user"]["email"] == ""
    assert body["user"]["username"] == USERNAME
    assert "phone.invalid" not in client.get("/api/auth/me").get_data(as_text=True)


def test_the_json_payload_still_returns_a_real_email(app_factory):
    main = app_factory()
    from backend.db.users_db import save_user

    uid = int(save_user("regular", "regular@example.com", "long-enough-password-1", role="general_user"))
    client, _ = _sign_in(main, uid, method="password")
    assert client.get("/api/auth/me").get_json()["user"]["email"] == "regular@example.com"


def test_the_profile_page_has_no_email_or_password_form_for_a_phone_account(app_factory):
    main = app_factory()
    uid = _phone_user(main)
    client, _ = _sign_in(main, uid)
    page = client.get("/account/profile")
    assert page.status_code == 200
    html = page.get_data(as_text=True)
    assert "phone.invalid" not in html
    assert 'name="email"' not in html
    assert 'name="current_password"' not in html
    assert 'name="username"' in html  # the person can still choose a name
    assert "sign_out_everywhere" in html  # and still sign out everywhere


def test_the_profile_page_of_a_normal_account_is_unchanged(app_factory):
    main = app_factory()
    from backend.db.users_db import save_user

    uid = int(save_user("regular", "regular@example.com", "long-enough-password-1", role="general_user"))
    client, _ = _sign_in(main, uid, method="password")
    html = client.get("/account/profile").get_data(as_text=True)
    assert 'name="email"' in html and 'value="regular@example.com"' in html
    assert 'name="current_password"' in html


def test_a_posted_email_never_replaces_the_placeholder(app_factory):
    main = app_factory()
    from backend.db.users_db import get_user_profile

    uid = _phone_user(main)
    client, _ = _sign_in(main, uid)
    with client.session_transaction() as sess:
        sess["_csrf_token"] = "t" * 32
    resp = client.post(
        "/account/profile",
        data={"csrf_token": "t" * 32, "form_action": "profile", "username": "renamed-shopper",
              "email": "attacker@example.com"},
    )
    assert resp.status_code == 200
    row = get_user_profile(uid)
    assert row["username"] == "renamed-shopper"
    assert row["email"] == PLACEHOLDER  # unchanged: no one can attach an unverified mailbox this way
    assert "phone.invalid" not in resp.get_data(as_text=True)
    with client.session_transaction() as sess:
        assert sess["user_email"] == ""


# ----------------------------------------------------------------- mail is never sent to a placeholder


def test_password_reset_skips_a_placeholder_account(app_factory, monkeypatch):
    monkeypatch.setenv("PASSWORD_RESET_ENABLED", "1")
    app_factory()
    from backend.auth import password_reset

    uid = PhoneAccounts().adopt_or_create(email=PLACEHOLDER, username=USERNAME)
    sent = []
    monkeypatch.setattr(password_reset, "send_transactional_email", lambda **kw: sent.append(kw))
    monkeypatch.setattr(password_reset, "set_user_password_reset_token", lambda *a, **k: sent.append("token") or True)
    # By username (the placeholder is never typed) and by the placeholder itself.
    assert password_reset.request_password_reset(login_input=USERNAME) is True
    assert password_reset.request_password_reset(login_input=PLACEHOLDER) is True
    assert sent == []
    assert uid > 0


def test_password_reset_still_mails_a_real_account(app_factory, monkeypatch):
    monkeypatch.setenv("PASSWORD_RESET_ENABLED", "1")
    app_factory()
    from backend.auth import password_reset
    from backend.db.users_db import save_user

    save_user("regular", "regular@example.com", "long-enough-password-1", role="general_user")
    sent = []
    monkeypatch.setattr(password_reset, "send_transactional_email", lambda **kw: sent.append(kw))
    monkeypatch.setattr(password_reset, "set_user_password_reset_token", lambda *a, **k: True)
    password_reset.request_password_reset(login_input="regular")
    assert [m["to_email"] for m in sent] == ["regular@example.com"]


def test_verification_mail_skips_a_placeholder(app_factory, monkeypatch):
    monkeypatch.setenv("EMAIL_VERIFICATION_ENABLED", "1")
    app_factory()
    from backend.auth import email_verification

    sent = []
    monkeypatch.setattr(email_verification, "send_transactional_email", lambda **kw: sent.append(kw))
    monkeypatch.setattr(email_verification, "set_user_email_verify_token", lambda *a, **k: True)
    assert email_verification.issue_and_send_verification_email(user_id=1, to_email=PLACEHOLDER) is False
    assert sent == []
    assert email_verification.issue_and_send_verification_email(user_id=1, to_email="a@example.com") is True
    assert [m["to_email"] for m in sent] == ["a@example.com"]


# ----------------------------------------------------------------- deleting an account frees the number


def test_purge_phone_identity_asks_the_repository(monkeypatch):
    from backend.db.repositories import auth_phone_repo

    seen = []
    monkeypatch.setattr(auth_phone_repo, "delete_identity_for_user", lambda uid: seen.append(uid) or True)
    purge_phone_identity(41)
    purge_phone_identity("42")
    purge_phone_identity(0)
    purge_phone_identity(-3)
    purge_phone_identity("junk")
    assert seen == [41, 42]


def test_purge_phone_identity_never_raises_and_never_logs_values(monkeypatch, caplog):
    from backend.db.repositories import auth_phone_repo

    def boom(uid):
        raise RuntimeError("connection to server at 10.1.2.3 failed, password=hunter2")

    monkeypatch.setattr(auth_phone_repo, "delete_identity_for_user", boom)
    with caplog.at_level("DEBUG"):
        purge_phone_identity(7)  # must not raise: the account is already gone
    text = " ".join(r.getMessage() for r in caplog.records)
    assert "event=identity_purge_failed reason=RuntimeError" in text
    assert "hunter2" not in text and "10.1.2.3" not in text


def test_purge_phone_identity_is_quiet_when_there_is_no_postgres(monkeypatch, caplog):
    monkeypatch.delenv("INVENTORY_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with caplog.at_level("DEBUG"):
        purge_phone_identity(7)
    assert not [r for r in caplog.records if "identity_purge_failed" in r.getMessage()]


def test_the_admin_delete_actions_purge_the_phone_identity():
    """Both delete branches of the users hub call the purge (a source check keeps the two in step)."""
    src = Path("backend/dealer/admin/users_hub.py").read_text(encoding="utf-8")
    assert src.count('_revoke_sessions(target_id, "delete")\n                    _purge_phone_identity(target_id)') == 1
    assert src.count('_revoke_sessions(user_id, "delete")\n                    _purge_phone_identity(user_id)') == 1


# ----------------------------------------------------------------- this module stays free of SQL


def test_accounts_module_has_no_sql_and_no_sqlite():
    """test_no_sqlite freezes the SQLite constructs per file: this module must add none."""
    src = Path(accounts.__file__).read_text(encoding="utf-8")
    for needle in ("sqlite3", "PRAGMA", "lastrowid", "INSERT ", "SELECT ", "UPDATE ", "DELETE "):
        assert needle not in src, needle
