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


def test_adopt_or_create_makes_a_shopper_with_an_unusable_password(users, monkeypatch):
    from backend.db import users_db
    from backend.db.users_db import authenticate_app_user, get_user_by_login

    real_save = users_db.save_user
    handed = []  # the password each account is created with, as save_user receives it

    def spy(*args, **kwargs):
        handed.append(kwargs["password"] if "password" in kwargs else args[2])
        return real_save(*args, **kwargs)

    monkeypatch.setattr(users_db, "save_user", spy)
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

    # Every adoption draws its own long random secret: no shared or fixed password to find out.
    other_email, other_name = "p-13-4e5f6a7b@phone.invalid", "user-13-4e5f"
    PhoneAccounts().adopt_or_create(email=other_email, username=other_name)
    assert len(handed) == 2
    assert handed[0] != handed[1]
    assert all(isinstance(pw, str) and len(pw) >= 32 for pw in handed)
    for guess in ("", other_name, other_email, "password", handed[0]):
        assert authenticate_app_user(other_name, guess) is None
        assert authenticate_app_user(other_email, guess) is None
    assert authenticate_app_user(USERNAME, handed[0]) is not None  # the control: the real secret does open it


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


_ADMIN_PW = "long-enough-password"
_CSRF = "t" * 32


@pytest.fixture
def admin_hub(app_factory, monkeypatch, tmp_path):
    """A client signed in as a site admin through the real /login route, plus the users hub module.

    Returns ``(client, hub)``; ``hub`` is the module object, so tests patch ``hub.delete_user_by_id``
    the way the routes look it up.
    """
    from backend.db import inventory_db as inv_db

    # inventory_db.DB_PATH is resolved at import time, so the env var alone does not move it.
    inventory_file = str(tmp_path / "inventory-for-admin-hub")
    monkeypatch.setattr(inv_db, "DB_PATH", inventory_file)
    main = app_factory(INVENTORY_DB_PATH=inventory_file)
    from backend.dealer.admin import users_hub
    from backend.db.users_db import save_user
    from backend.utils.roles import ROLE_ADMIN

    save_user("hubadmin", "hubadmin@example.com", _ADMIN_PW, role=ROLE_ADMIN)
    client = main.app.test_client()
    client.get("/login")
    with client.session_transaction() as sess:
        sess["_csrf_token"] = _CSRF
    client.post("/login", data={"csrf_token": _CSRF, "login": "hubadmin@example.com", "password": _ADMIN_PW})
    assert client.get("/admin/users").status_code == 200  # really an admin: the hub answers
    with client.session_transaction() as sess:
        sess["_csrf_token"] = _CSRF  # signing in rotates the session: put a known token back for the posts
    return client, users_hub


def _delete_from_list(client, uid):
    return client.post("/admin/users", data={"csrf_token": _CSRF, "action": "delete", "user_id": str(uid)})


def _delete_from_edit_page(client, uid):
    return client.post(f"/admin/users/{uid}/edit", data={"csrf_token": _CSRF, "action": "delete"})


_DELETE_BRANCHES = pytest.mark.parametrize(
    "delete_action", [_delete_from_list, _delete_from_edit_page], ids=["list_page", "edit_page"]
)


def _shopper(name="shopper-to-delete"):
    from backend.db.users_db import save_user

    return int(save_user(name, PLACEHOLDER, "x" * 30, role="general_user", org_id=None))


@pytest.fixture
def purge_calls(monkeypatch):
    """Records every phone-identity purge as (user_id, account_still_exists_at_that_moment)."""
    from backend.db.users_db import get_user_admin_record

    calls = []
    monkeypatch.setattr(
        accounts, "purge_phone_identity", lambda uid: calls.append((uid, get_user_admin_record(uid) is not None))
    )
    return calls


@_DELETE_BRANCHES
def test_deleting_an_account_from_the_admin_hub_purges_its_phone_identity_once_after_the_delete(
    admin_hub, purge_calls, delete_action
):
    from backend.db.users_db import get_user_admin_record

    client, _hub = admin_hub
    uid = _shopper()
    resp = delete_action(client, uid)
    assert resp.status_code in (302, 303)
    assert get_user_admin_record(uid) is None
    assert purge_calls == [(uid, False)]  # once, for this id, and the users row was already gone


@_DELETE_BRANCHES
def test_a_refused_delete_from_the_admin_hub_does_not_purge_the_phone_identity(
    admin_hub, purge_calls, monkeypatch, delete_action
):
    from backend.db.users_db import get_user_admin_record

    client, hub = admin_hub
    uid = _shopper()
    monkeypatch.setattr(hub, "delete_user_by_id", lambda user_id: (False, "blocked"))
    resp = delete_action(client, uid)
    assert resp.status_code in (302, 303)
    assert purge_calls == []  # the account is still there, so its number must stay tied to it
    assert get_user_admin_record(uid) is not None


@_DELETE_BRANCHES
def test_an_admin_cannot_delete_their_own_account_and_nothing_is_purged(admin_hub, purge_calls, delete_action):
    from backend.db.users_db import get_user_by_login

    client, _hub = admin_hub
    own_id = int(get_user_by_login("hubadmin@example.com")["id"])
    assert delete_action(client, own_id).status_code in (302, 303)
    assert purge_calls == []
    assert get_user_by_login("hubadmin@example.com") is not None


# ----------------------------------------------------------------- this module stays free of SQL


def test_accounts_module_has_no_sql_and_no_sqlite():
    """test_no_sqlite freezes the SQLite constructs per file: this module must add none."""
    src = Path(accounts.__file__).read_text(encoding="utf-8")
    for needle in ("sqlite3", "PRAGMA", "lastrowid", "INSERT ", "SELECT ", "UPDATE ", "DELETE "):
        assert needle not in src, needle


# ----------------------------------------------------------------- review R13: adopt only what phone sign-in made


@pytest.mark.parametrize(
    "over",
    [
        {"role": "admin"},
        {"role": "dealership_owner"},
        {"google_sub": "g-123"},
        {"apple_sub": "a-123"},
    ],
)
def test_adopt_or_create_never_adopts_an_account_phone_sign_in_did_not_make(users, over):
    # A row holding the placeholder email that is not a plain shopper (or is linked to Google or
    # Apple) cannot be the account an earlier try of this sign-up made: refuse, never adopt it.
    from backend.auth.phone.accounts import PlaceholderTaken
    from backend.db import users_db
    from backend.db.users_db._common import get_conn

    uid = users_db.save_user(USERNAME, PLACEHOLDER, "x" * 30, role="general_user", org_id=None)
    conn = get_conn()
    for col, value in over.items():
        conn.execute("UPDATE users SET %s = ? WHERE id = ?" % col, (value, uid))
    conn.commit()
    conn.close()
    with pytest.raises(PlaceholderTaken):
        PhoneAccounts().adopt_or_create(email=PLACEHOLDER, username=USERNAME)


def test_is_eligible_reads_the_stored_row_and_lets_a_deleted_account_heal(users):
    from backend.db import users_db

    shopper = PhoneAccounts().adopt_or_create(email=PLACEHOLDER, username=USERNAME)
    dealer = users_db.save_user("dealer-r8", "dealer-r8@example.com", "x" * 30, role="dealership_owner")
    assert PhoneAccounts().is_eligible(shopper) is True
    assert PhoneAccounts().is_eligible(dealer) is False
    # No row: the route releases the stale identity and makes a fresh account (identity_orphan_healed).
    assert PhoneAccounts().is_eligible(10**9) is True
