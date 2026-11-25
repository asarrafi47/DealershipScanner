"""The ``phone.invalid`` domain belongs to phone accounts and to nobody else.

A phone account is stored under a placeholder address on ``phone.invalid`` (RFC 2606: it can never
receive mail). If a person could register, or later change their email, to an address on that domain
they could squat another phone account's placeholder or get an account that is treated as "has no
email" (no mail, no password reset, hidden address). These tests pin the three doors: the shared
registration check, the registration routes, and the profile update in the users database. A phone
account that only renames itself must keep working: its own placeholder is not a change.
"""

from __future__ import annotations

import pytest

from backend.utils.registration_validation import registration_form_error

_CSRF = "c" * 32
_PW = "long-enough-password"
_REFUSED = "Enter a valid email address."
_PLACEHOLDER = "p-12-0a1b2c3d@phone.invalid"  # the shape AuthPhoneService stores


def _form_error(email: str):
    return registration_form_error("someone", email, _PW, min_password_len=8)


def _post_form(client, url: str, data: dict):
    from backend.utils.csrf import _SESSION_KEY

    with client.session_transaction() as sess:
        sess[_SESSION_KEY] = _CSRF
    return client.post(url, data={"csrf_token": _CSRF, **data}, follow_redirects=False)


# ------------------------------------------------------------------ one predicate, one home


def test_the_phone_accounts_module_re_exports_the_leaf_predicate_instead_of_keeping_its_own():
    from backend.auth.phone import accounts
    from backend.utils import phone_placeholder

    assert accounts.is_placeholder_email is phone_placeholder.is_placeholder_email
    assert accounts.PLACEHOLDER_DOMAIN == phone_placeholder.PLACEHOLDER_DOMAIN


# ------------------------------------------------------------------ the shared registration check


@pytest.mark.parametrize("email", ["x@phone.invalid", "X@Phone.Invalid", " x@PHONE.INVALID ", _PLACEHOLDER])
def test_registration_refuses_an_address_on_the_reserved_domain(email):
    assert _form_error(email) == _REFUSED


@pytest.mark.parametrize(
    "email",
    ["x@phone.invalid.example.com", "x@invalid.com", "x@notphone.invalid", "phone.invalid@example.com"],
)
def test_registration_accepts_addresses_that_only_look_like_the_reserved_domain(email):
    assert _form_error(email) is None


def test_an_admin_cannot_create_an_ordinary_account_on_the_reserved_domain(app_factory):
    app_factory()
    from backend.db.users_db import admin_create_user, find_users_by_email

    uid, err = admin_create_user(
        username="squatter", email="x@phone.invalid", password=_PW, role="general_user"
    )
    assert uid is None and err == _REFUSED
    assert find_users_by_email("x@phone.invalid") == []


def test_an_admin_cannot_edit_an_ordinary_account_onto_the_reserved_domain(app_factory):
    app_factory()
    from backend.db.users_db import admin_update_user, find_users_by_email, save_user

    uid = int(save_user("shopper", "shopper@example.com", _PW, role="general_user"))
    for spelling in ("x@phone.invalid", "X@Phone.Invalid"):
        err = admin_update_user(
            uid, username="shopper", email=spelling, role="general_user", actor_user_id=uid + 1000
        )
        assert err == _REFUSED, spelling
    assert find_users_by_email("x@phone.invalid") == []
    assert find_users_by_email("shopper@example.com")


def test_an_admin_can_still_rename_a_phone_account_without_touching_its_placeholder(app_factory):
    app_factory()
    from backend.db.users_db import admin_update_user, find_users_by_email, save_user

    placeholder = "p-7-abcd1234@phone.invalid"
    uid = int(save_user("user-7-abcd", placeholder, _PW, role="general_user"))
    err = admin_update_user(
        uid, username="renamed-phone-user", email=placeholder, role="general_user", actor_user_id=uid + 1000
    )
    assert err is None
    assert find_users_by_email(placeholder)[0]["username"] == "renamed-phone-user"


# ------------------------------------------------------------------ the registration routes


@pytest.mark.parametrize("email", ["x@phone.invalid", "X@Phone.Invalid", " x@PHONE.INVALID "])
def test_the_register_page_refuses_the_reserved_domain_and_creates_no_user(app_factory, email):
    app = app_factory().app
    from backend.db.users_db import find_users_by_email, user_exists_by_username

    with app.test_client() as c:
        r = _post_form(c, "/register", {"username": "squatter", "email": email, "password": _PW, "plan": "free"})
    assert r.status_code == 200  # the form again, not a redirect into a new session
    assert _REFUSED.encode() in r.data
    assert find_users_by_email("x@phone.invalid") == []
    assert user_exists_by_username("squatter") is False


def test_the_register_page_still_creates_an_account_for_a_lookalike_domain(app_factory):
    """The control: the refusal above comes from the domain, not from the form or the test setup."""
    app = app_factory().app
    from backend.db.users_db import find_users_by_email

    with app.test_client() as c:
        r = _post_form(
            c,
            "/register",
            {"username": "lookalike", "email": "x@phone.invalid.example.com", "password": _PW, "plan": "free"},
        )
    assert r.status_code in (302, 303)
    assert len(find_users_by_email("x@phone.invalid.example.com")) == 1


def test_the_json_register_endpoint_refuses_the_reserved_domain(app_factory):
    app = app_factory().app
    from backend.db.users_db import find_users_by_email

    with app.test_client() as c:
        token = c.get("/api/auth/csrf").get_json()["csrf_token"]
        r = c.post(
            "/api/auth/register",
            json={"username": "squatter", "email": "x@PHONE.invalid", "password": _PW},
            headers={"X-CSRF-Token": token},
        )
    assert r.status_code == 400
    assert r.get_json()["error"] == "validation_error"
    assert find_users_by_email("x@phone.invalid") == []


# ------------------------------------------------------------------ changing an email afterwards


def _save(username: str, email: str) -> int:
    from backend.db.users_db import save_user

    return int(save_user(username, email, "x" * 30, role="general_user", org_id=None))


def _stored_email(uid: int) -> str:
    from backend.db.users_db import get_user_profile

    return get_user_profile(uid)["email"]


@pytest.mark.parametrize("target", ["x@phone.invalid", "X@Phone.Invalid", " x@PHONE.INVALID "])
def test_a_normal_account_cannot_move_its_email_onto_the_reserved_domain(app_factory, target):
    app_factory()
    from backend.db.users_db import update_user_profile

    uid = _save("regular", "regular@example.com")
    assert update_user_profile(uid, "regular", target) == _REFUSED
    assert _stored_email(uid) == "regular@example.com"


def test_a_phone_account_cannot_swap_its_placeholder_for_another_one(app_factory):
    """Changing to a different placeholder is still a change, so it is refused like any other."""
    app_factory()
    from backend.db.users_db import update_user_profile

    uid = _save("user-12-0a1b", _PLACEHOLDER)
    assert update_user_profile(uid, "user-12-0a1b", "p-99-ffffffff@phone.invalid") == _REFUSED
    assert _stored_email(uid) == _PLACEHOLDER


def test_a_phone_account_can_still_change_only_its_username(app_factory):
    app_factory()
    from backend.db.users_db import get_user_profile, update_user_profile

    uid = _save("user-12-0a1b", _PLACEHOLDER)
    assert update_user_profile(uid, "my-new-name", _PLACEHOLDER) is None
    row = get_user_profile(uid)
    assert row["username"] == "my-new-name" and row["email"] == _PLACEHOLDER


def test_a_phone_account_resending_its_own_placeholder_in_other_letters_is_not_a_change(app_factory):
    app_factory()
    from backend.db.users_db import get_user_profile, update_user_profile

    uid = _save("user-12-0a1b", _PLACEHOLDER)
    assert update_user_profile(uid, "renamed-again", f" {_PLACEHOLDER.upper()} ") is None
    assert get_user_profile(uid)["username"] == "renamed-again"


def test_an_ordinary_email_change_is_still_allowed(app_factory):
    app_factory()
    from backend.db.users_db import update_user_profile

    uid = _save("regular", "regular@example.com")
    assert update_user_profile(uid, "regular", "moved@example.com") is None
    assert _stored_email(uid) == "moved@example.com"
    assert update_user_profile(uid, "regular", "x@phone.invalid.example.com") is None  # a lookalike is not reserved
    assert _stored_email(uid) == "x@phone.invalid.example.com"
