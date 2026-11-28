"""Phone sign-in end to end (cards S7, S8 and the /login switch of S9) on a real Postgres.

The route tests in ``test_auth_phone_routes.py`` stand a stub in for the service. These run the real
``AuthPhoneService`` over the real repository (a cloned migration-built database), the fake
provider (code ``000000``), the fake Turnstile verifier and a real users database, through the real
Flask routes. Test numbers are the fictional ``+1 202 555 01xx`` range.

Each test uses at most three sends from 127.0.0.1 (the address limit is 3 an hour); tests that need
more give their client another address.
"""

from __future__ import annotations

import itertools
import re
import threading

import pytest

from backend.auth.phone import numbers, registry, routes, turnstile
from backend.auth.phone.endpoints import PHONE_FLAG_PATHS
from backend.db.repositories import auth_phone_repo as repo
from backend.utils.ip_rate_limit import clear_rate_limit_state

pytestmark = pytest.mark.pg

NUMBER_A = "(202) 555-0101"
E164_A = "+12025550101"
NUMBER_B = "(202) 555-0102"
E164_B = "+12025550102"
SENTINEL_CODE = "000000"
CSRF_RE = re.compile(r'name="csrf_token" value="([^"]+)"')
_tokens = itertools.count(1)


def _token() -> str:
    """A fresh Turnstile token (the fake verifier accepts each one once)."""
    return f"ts-{next(_tokens)}"


@pytest.fixture
def phone_app(pg_db, app_factory, monkeypatch):
    main = app_factory(
        AUTH_PHONE_LOGIN_ENABLED="1",
        AUTH_PHONE_PROVIDER="fake",
        PHONE_HASH_PEPPER="e2e-pepper-" + "p" * 32,
        TURNSTILE_SITE_KEY="site-key-for-tests",
        SMS_DAILY_SEND_CAP=None,
        SERVER_TIMING="1",  # on, so the Server-Timing tests prove an absence the middleware could have filled
    )
    registry.reset()
    monkeypatch.setattr(routes, "_pad_sleep", lambda s: None)  # the R7 floor, without the wait
    routes.reset_service()
    turnstile.set_verifier(turnstile.FakeTurnstileVerifier(frozenset(f"ts-{n}" for n in range(1, 500))))
    yield main
    turnstile.set_verifier(None)
    routes.reset_service()
    registry.reset()


def _client(app, ip="127.0.0.1"):
    client = app.test_client()
    client.environ_base["REMOTE_ADDR"] = ip
    return client


def _csrf(client, path="/login/phone"):
    html = client.get(path).get_data(as_text=True)
    found = CSRF_RE.search(html)
    assert found, f"no csrf token on {path}"
    return found.group(1)


def _start(client, number=NUMBER_A, *, consent="1", token=None):
    csrf = _csrf(client)
    return client.post(
        "/login/phone",
        data={"csrf_token": csrf, "phone": number, "consent": consent, "cf-turnstile-response": token or _token()},
    )


def _verify(client, code=SENTINEL_CODE):
    csrf = _csrf(client, "/login/phone/code")
    return client.post("/login/phone/code", data={"csrf_token": csrf, "code": code})


def _sign_in(client, number=NUMBER_A):
    started = _start(client, number)
    assert started.status_code == 303 and started.headers["Location"].endswith("/login/phone/code")
    return _verify(client)


def _identities(pg_db):
    with pg_db.connect() as conn:
        return conn.execute(
            "SELECT id, user_id, phone_e164, state FROM public.auth_phone_identities ORDER BY id"
        ).fetchall()


def _send_count(pg_db) -> int:
    with pg_db.connect() as conn:
        return int(conn.execute("SELECT count(*) FROM public.auth_phone_sends").fetchone()[0])


def _forget_sends(pg_db) -> None:
    """Empty the send ledger, so the per-number limit (two sends in 15 minutes) does not decide a test
    that is about accounts. Nothing else of the limits is touched."""
    with pg_db.connect() as conn:
        conn.execute("DELETE FROM public.auth_phone_sends")
        conn.commit()


def _assert_log_is_clean(text: str) -> None:
    """The feature's logging rule: no number (typed or normalised), no code, no 7+ digit run, no 64-hex."""
    assert "2025550101" not in text and "202-555-0101" not in text and "+1202" not in text
    assert SENTINEL_CODE not in text
    assert not re.search(r"\d{7,}", text)
    assert not re.search(r"\b[0-9a-f]{64}\b", text)


def _users_with_placeholder_email():
    from backend.db.users_db import list_users_for_admin

    rows = list_users_for_admin(search="phone.invalid", limit=500)
    return [r for r in rows if str(r.get("email") or "").endswith("@phone.invalid")]


def _me(client):
    return client.get("/api/auth/me")


# ----------------------------------------------------------------- the happy path


def test_a_new_number_signs_up_and_the_second_visit_signs_in_as_the_same_account(phone_app, pg_db):
    first = _client(phone_app.app)
    done = _sign_in(first)
    assert done.status_code == 302 and done.headers["Location"].endswith("/home")
    provider = registry.get_provider()
    assert [m.to for m in provider.outbox] == [E164_A]

    me = _me(first).get_json()
    assert me["ok"] is True and me["user"]["email"] == "" and me["user"]["role"] == "general_user"
    first_id = me["user"]["id"]
    with first.session_transaction() as sess:
        assert sess["auth_method"] == "phone" and sess["auth_role"] == "shopper"
        assert sess["user_email"] == "" and sess["user_id"] == first_id
        assert "phone_cid" not in sess  # the challenge died with the session

    rows = _identities(pg_db)
    assert [(r[1], r[2], r[3]) for r in rows] == [(first_id, E164_A, "active")]

    # A second browser, the same number: the same account, no second user, no second identity.
    second = _client(phone_app.app)
    again = _sign_in(second)
    assert again.status_code == 302
    assert _me(second).get_json()["user"]["id"] == first_id
    assert len(_identities(pg_db)) == 1
    assert len(_users_with_placeholder_email()) == 1


def test_two_numbers_make_two_accounts(phone_app, pg_db):
    a, b = _client(phone_app.app), _client(phone_app.app, "127.0.0.2")
    _sign_in(a, NUMBER_A)
    _sign_in(b, NUMBER_B)
    assert _me(a).get_json()["user"]["id"] != _me(b).get_json()["user"]["id"]
    assert sorted(r[2] for r in _identities(pg_db)) == [E164_A, E164_B]


def test_the_session_and_every_page_hold_no_number_and_no_placeholder(phone_app):
    client = _client(phone_app.app)
    seen = []
    started = _start(client)
    seen.append(started.get_data(as_text=True))
    with client.session_transaction() as sess:
        assert "2025550101" not in repr(dict(sess)) and "phone.invalid" not in repr(dict(sess))
        assert sess["phone_sent_msg"] == "We sent a 6-digit code to the number ending 01."
    seen.append(client.get("/login/phone/code").get_data(as_text=True))
    done = _verify(client)
    seen.append(done.get_data(as_text=True))
    seen.append(client.get("/account/profile").get_data(as_text=True))
    seen.append(_me(client).get_data(as_text=True))
    blob = "\n".join(seen)
    assert "2025550101" not in blob and "202-555-0101" not in blob and "(202) 555-0101" not in blob
    assert "phone.invalid" not in blob
    for cookie in client._cookies.values() if hasattr(client, "_cookies") else []:
        assert "2025550101" not in str(cookie.value)


def test_logs_carry_no_number_no_code_and_no_long_digit_run(phone_app, caplog):
    client = _client(phone_app.app)
    with caplog.at_level("DEBUG"):
        _sign_in(client)
        _start(_client(phone_app.app, "127.0.0.9"), NUMBER_B, consent="")  # a refusal line too
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "2025550101" not in text and "+1202" not in text and SENTINEL_CODE not in text
    assert not re.search(r"\d{7,}", text)
    assert not re.search(r"\b[0-9a-f]{64}\b", text)


# ----------------------------------------------------------------- refusals


def test_no_consent_sends_nothing(phone_app, pg_db):
    client = _client(phone_app.app)
    res = _start(client, consent="")
    html = res.get_data(as_text=True)
    assert res.status_code == 200 and "Tick the box to continue." in html
    assert registry.get_provider().outbox == []
    assert _identities(pg_db) == []


def test_a_missing_turnstile_token_sends_nothing(phone_app):
    client = _client(phone_app.app)
    res = _start(client, token="never-issued")
    assert "We could not check that you are a person." in res.get_data(as_text=True)
    assert registry.get_provider().outbox == []


@pytest.mark.parametrize(
    "kwargs,sentence",
    [
        pytest.param({"consent": ""}, "Tick the box to continue.", id="consent_unticked"),
        pytest.param({"token": "never-issued"}, "We could not check that you are a person.", id="turnstile_failed"),
        pytest.param({"number": "12"}, "We could not send a code to that number.", id="not_a_number"),
    ],
)
def test_a_refused_start_shows_the_number_page_again_not_the_code_page(phone_app, kwargs, sentence):
    """No challenge exists after a refused start, so the answer is the number form with the reason on it."""
    client = _client(phone_app.app)
    res = _start(client, **kwargs)
    html = res.get_data(as_text=True)
    assert res.status_code == 200 and sentence in html
    assert 'id="ph-start-form"' in html and "Text me a code" in html and 'name="phone"' in html
    assert "/staff/login" in html  # the fallback link is on the number page
    assert "Enter your code" not in html and 'id="ph-code-form"' not in html
    assert client.get("/login/phone/code").status_code == 303  # no challenge was opened behind the page
    assert registry.get_provider().outbox == []


def test_a_wrong_code_fails_and_the_right_one_then_signs_in(phone_app):
    client = _client(phone_app.app)
    assert _start(client).status_code == 303
    wrong = _verify(client, "111111")
    assert wrong.status_code == 200
    assert "That code did not work. Check it and try again, or ask for a new code." in wrong.get_data(as_text=True)
    assert _me(client).status_code == 401
    assert _verify(client).status_code == 302
    assert _me(client).status_code == 200


def test_a_code_works_once(phone_app):
    app = phone_app.app
    provider = registry.get_provider()
    client = _client(app)
    assert _start(client).status_code == 303
    cookie_name = app.config.get("SESSION_COOKIE_NAME") or "session"
    pending = client.get_cookie(cookie_name).value  # the session while the code is pending: it holds the challenge
    assert _verify(client).status_code == 302
    assert _me(client).status_code == 200
    client.post("/logout", data={"csrf_token": _csrf(client)})
    assert _me(client).status_code == 401
    # the challenge is gone from the session; asking again lands on the number page
    assert client.get("/login/phone/code").status_code == 303

    # The same code, typed in a browser that still holds the old challenge: it was spent.
    replay = _client(app, "127.0.0.8")
    replay.set_cookie(cookie_name, pending)
    res = _verify(replay)
    html = res.get_data(as_text=True)
    assert "That code has expired or was already used. Please ask for a new code." in html
    assert 'id="ph-start-form"' in html  # the challenge is over: back to the number page
    assert _me(replay).status_code == 401
    with replay.session_transaction() as sess:
        assert "user_id" not in sess and "auth_method" not in sess
    assert provider.check_calls == 1  # the replay never reached the vendor
    assert len(provider.outbox) == 1


def test_a_code_typed_in_another_browser_fails_with_the_start_again_sentence(phone_app):
    owner, thief = _client(phone_app.app), _client(phone_app.app, "127.0.0.3")
    assert _start(owner).status_code == 303
    with owner.session_transaction() as sess:
        cid = sess["phone_cid"]
    _csrf(thief)  # the thief has a session and a CSRF token of their own
    with thief.session_transaction() as sess:
        sess["phone_cid"] = cid  # they somehow learned the challenge id
        sess["phone_nonce"] = "n" * 24  # but not the owner's browser nonce
    res = _verify(thief)
    assert "That code has expired or was already used. Please ask for a new code." in res.get_data(as_text=True)
    assert _me(thief).status_code == 401
    # and the owner's code still works
    assert _verify(owner).status_code == 302


def test_five_wrong_codes_lock_and_a_new_code_does_not_unlock(phone_app):
    client = _client(phone_app.app)
    assert _start(client).status_code == 303
    last = None
    for _ in range(5):
        last = _verify(client, "111111")
    assert "Too many wrong codes. Please wait 15 minutes and try again." in last.get_data(as_text=True)
    assert last.status_code == 429
    again = _start(client)  # a new code for the same number from the same browser
    assert "We could not send a code to that number." in again.get_data(as_text=True)


@pytest.mark.parametrize(
    "role,extra",
    [
        ("admin", {}),
        ("dealership_owner", {"dealer_id": "dealer-x"}),
        ("dealer_staff", {}),
    ],
)
def test_an_account_that_is_not_a_shopper_cannot_sign_in_by_phone(phone_app, pg_db, role, extra, caplog):
    """Even if such an account somehow holds a number, the code opens nothing and reads like a wrong code."""
    from datetime import datetime, timezone

    from backend.db.users_db import save_user

    uid = int(save_user("staffer", "staffer@example.com", "long-enough-password-1", role=role))
    repo.insert_identity(
        phone_e164=E164_A, phone_hash=numbers.hash_phone(E164_A), country="US", now=datetime.now(timezone.utc),
        consent_at=datetime.now(timezone.utc), consent_text_version="1", user_id=uid, state="active",
    )
    client = _client(phone_app.app)
    assert _start(client).status_code == 303
    with caplog.at_level("INFO"):
        res = _verify(client)
    assert res.status_code == 200
    assert "That code did not work. Check it and try again, or ask for a new code." in res.get_data(as_text=True)
    assert _me(client).status_code == 401
    with client.session_transaction() as sess:
        assert "user_id" not in sess and "auth_method" not in sess
    assert any("event=login_refused reason=ineligible" in r.getMessage() for r in caplog.records)


def test_a_suspended_shopper_cannot_sign_in_by_phone(phone_app, pg_db, caplog):
    from datetime import datetime, timezone

    from backend.db.users_db import save_user, set_user_is_active

    uid = int(save_user("sleeper", "p-9-aaaaaaaa@phone.invalid", "x" * 30, role="general_user"))
    repo.insert_identity(
        phone_e164=E164_A, phone_hash=numbers.hash_phone(E164_A), country="US", now=datetime.now(timezone.utc),
        consent_at=datetime.now(timezone.utc), consent_text_version="1", user_id=uid, state="active",
    )
    assert set_user_is_active(uid, active=False)
    client = _client(phone_app.app)
    _start(client)
    with caplog.at_level("INFO"):
        res = _verify(client)
    assert "That code did not work." in res.get_data(as_text=True)
    assert _me(client).status_code == 401
    assert any("event=login_refused" in r.getMessage() for r in caplog.records)


# ----------------------------------------------------------------- resend and change


def test_send_again_waits_a_minute_and_use_a_different_number_starts_over(phone_app):
    client = _client(phone_app.app)
    assert _start(client).status_code == 303
    csrf = _csrf(client, "/login/phone/code")
    soon = client.post("/login/phone/resend", data={"csrf_token": csrf, "cf-turnstile-response": _token()})
    assert "Please wait a minute before asking for another code." in soon.get_data(as_text=True)
    assert soon.status_code == 429
    assert len(registry.get_provider().outbox) == 1
    changed = client.post("/login/phone/change", data={"csrf_token": csrf})
    assert changed.status_code == 303 and changed.headers["Location"].endswith("/login/phone")
    assert client.get("/login/phone/code").status_code == 303  # nothing open any more


# ----------------------------------------------------------------- the /login switch


def test_login_is_the_phone_page_and_the_password_form_moves_to_staff_login(phone_app):
    from backend.db.users_db import save_user

    save_user("regular", "regular@example.com", "long-enough-password-1", role="general_user")
    client = _client(phone_app.app)
    page = client.get("/login").get_data(as_text=True)
    assert "Text me a code" in page and 'name="phone"' in page and 'name="password"' not in page
    assert "/staff/login" in page  # the fallback link
    staff = client.get("/staff/login").get_data(as_text=True)
    assert 'name="password"' in staff and 'action="/staff/login"' in staff

    # A password posted to /login is never read ...
    csrf = _csrf(client, "/login")
    posted = client.post("/login", data={"csrf_token": csrf, "login": "regular", "password": "long-enough-password-1"})
    assert posted.status_code == 303 and _me(client).status_code == 401
    # ... and the fallback still works for an existing email and password account.
    csrf = _csrf(client, "/staff/login")
    ok = client.post("/staff/login", data={"csrf_token": csrf, "login": "regular", "password": "long-enough-password-1"})
    assert ok.status_code == 302
    assert _me(client).get_json()["user"]["email"] == "regular@example.com"


def test_with_the_flag_off_the_routes_vanish_and_login_is_the_password_form(phone_app, monkeypatch):
    monkeypatch.delenv("AUTH_PHONE_LOGIN_ENABLED", raising=False)
    client = _client(phone_app.app)
    assert 'name="password"' in client.get("/login").get_data(as_text=True)
    for path in ("/login/phone", "/login/phone/code", "/staff/login"):
        assert client.get(path).status_code == 404, path
    for path in ("/login/phone", "/login/phone/code", "/login/phone/resend", "/login/phone/change",
                 "/staff/login", "/api/auth/phone/start", "/api/auth/phone/verify", "/api/auth/phone/resend"):
        assert client.post(path, data={}).status_code == 404, path


def test_with_the_flag_off_every_method_on_a_phone_path_is_a_404(phone_app, monkeypatch):
    """A wrong method (405) or OPTIONS (200) would tell a visitor that the route exists."""
    monkeypatch.delenv("AUTH_PHONE_LOGIN_ENABLED", raising=False)
    client = _client(phone_app.app)
    for path in sorted(PHONE_FLAG_PATHS):
        for method in ("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"):
            res = client.open(path, method=method)
            assert res.status_code == 404, (method, path, res.status_code)
    # the same paths with the flag on are real: a wrong method is a 405 there
    monkeypatch.setenv("AUTH_PHONE_LOGIN_ENABLED", "1")
    assert client.open("/login/phone/resend", method="PUT").status_code == 405


# ----------------------------------------------------------------- the JSON twins


def test_the_json_twins_sign_a_phone_in(phone_app):
    client = _client(phone_app.app)
    client.get("/login/phone")  # a cookie route: sets sc_dev
    token = client.get("/api/auth/csrf").get_json()["csrf_token"]
    headers = {"X-CSRF-Token": token}
    start = client.post(
        "/api/auth/phone/start",
        json={"phone": NUMBER_A, "consent": True, "turnstile_token": _token()},
        headers=headers,
    )
    assert start.status_code == 200
    assert start.get_json() == {"ok": True, "message": "We sent a 6-digit code to the number ending 01.", "resend_after_s": 60}
    bad = client.post("/api/auth/phone/verify", json={"code": "111111"}, headers=headers)
    assert bad.status_code == 400 and bad.get_json()["error"] == "code_failed"
    good = client.post("/api/auth/phone/verify", json={"code": SENTINEL_CODE}, headers=headers)
    assert good.status_code == 200
    user = good.get_json()["user"]
    assert user["email"] == "" and user["role"] == "general_user" and user["username"].startswith("user-")
    assert "phone.invalid" not in good.get_data(as_text=True)
    assert _me(client).get_json()["user"]["id"] == user["id"]


def test_the_json_start_without_consent_says_so(phone_app):
    client = _client(phone_app.app)
    client.get("/login/phone")
    token = client.get("/api/auth/csrf").get_json()["csrf_token"]
    res = client.post(
        "/api/auth/phone/start",
        json={"phone": NUMBER_A, "consent": "true", "turnstile_token": _token()},  # a string is not a yes
        headers={"X-CSRF-Token": token},
    )
    assert res.status_code == 400 and res.get_json()["error"] == "consent_required"
    assert registry.get_provider().outbox == []


# ----------------------------------------------------------------- an account that is deleted gives its number back


def test_deleting_the_account_frees_the_number_for_a_new_one(phone_app, pg_db):
    from backend.auth.phone.accounts import purge_phone_identity
    from backend.db.users_db import delete_user_by_id

    client = _client(phone_app.app)
    _sign_in(client)
    old_id = _me(client).get_json()["user"]["id"]
    ok, err = delete_user_by_id(old_id)
    assert ok, err
    purge_phone_identity(old_id)
    assert _identities(pg_db) == []

    again = _client(phone_app.app, "127.0.0.4")
    assert _sign_in(again).status_code == 302
    new_id = _me(again).get_json()["user"]["id"]
    assert new_id != old_id
    assert [(r[1], r[3]) for r in _identities(pg_db)] == [(new_id, "active")]


def test_a_number_whose_account_vanished_signs_in_as_a_new_account(phone_app, pg_db, caplog):
    """The purge after a delete can fail, or the process can die between the two steps: the number then
    points at a users row that is gone. The next sign-in releases that identity and makes a fresh account."""
    from backend.db.users_db import delete_user_by_id, get_user_profile

    first = _client(phone_app.app)
    _sign_in(first)
    old_id = _me(first).get_json()["user"]["id"]
    ok, err = delete_user_by_id(old_id)  # the users row only: no admin hub, so the identity stays
    assert ok, err
    assert get_user_profile(old_id) is None
    assert [(r[1], r[3]) for r in _identities(pg_db)] == [(old_id, "active")]  # the orphan

    with caplog.at_level("DEBUG"):
        again = _client(phone_app.app, "127.0.0.5")
        done = _sign_in(again)
        assert done.status_code == 302 and done.headers["Location"].endswith("/home")
        new_id = _me(again).get_json()["user"]["id"]
        assert new_id != old_id
        assert [(r[1], r[2], r[3]) for r in _identities(pg_db)] == [(new_id, E164_A, "active")]
        assert get_user_profile(old_id) is None and get_user_profile(new_id) is not None
        with again.session_transaction() as sess:
            assert sess["user_id"] == new_id and sess["auth_method"] == "phone"

        # the third visit is an ordinary sign-in: same new account, nothing healed this time
        _forget_sends(pg_db)
        third = _client(phone_app.app, "127.0.0.6")
        assert _sign_in(third).status_code == 302
        assert _me(third).get_json()["user"]["id"] == new_id
    assert len(_identities(pg_db)) == 1
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert text.count("event=identity_orphan_healed") == 1
    _assert_log_is_clean(text)


# ----------------------------------------------------------------- two tabs finishing the same sign-up


def test_two_browsers_finishing_the_same_new_number_make_one_account(phone_app, pg_db):
    """Both hold a valid code for a number that has no account yet; both approve at once."""
    a, b = _client(phone_app.app, "127.0.0.6"), _client(phone_app.app, "127.0.0.7")
    assert _start(a).status_code == 303
    assert _start(b).status_code == 303
    csrf_a, csrf_b = _csrf(a, "/login/phone/code"), _csrf(b, "/login/phone/code")
    gate = threading.Barrier(2)
    results: dict[str, int] = {}

    def run(name, client, csrf):
        gate.wait()
        results[name] = client.post("/login/phone/code", data={"csrf_token": csrf, "code": SENTINEL_CODE}).status_code

    threads = [threading.Thread(target=run, args=("a", a, csrf_a)), threading.Thread(target=run, args=("b", b, csrf_b))]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    assert set(results) == {"a", "b"}
    # A loser may be told to try again (429: its code is spent and the account was still being made); it
    # never makes a second account.
    assert all(code in (200, 302, 429) for code in results.values()), results
    assert 302 in results.values(), results  # at least one browser got in
    assert len(_identities(pg_db)) == 1
    assert len(_users_with_placeholder_email()) == 1
    winners = [c for c in (a, b) if _me(c).status_code == 200]
    assert winners  # at least one browser is signed in, and every signed-in one is the same account
    assert len({_me(c).get_json()["user"]["id"] for c in winners}) == 1


# ----------------------------------------------------------------- Server-Timing leaks nothing about numbers


def test_phone_answers_carry_no_server_timing_for_a_known_or_an_unknown_number(phone_app, pg_db):
    """The header's db_connect count differs between a number with an account and one without, so the
    phone routes keep it off every answer. An HTML page outside the auth routes (control) still has it; the
    staff form is an auth route and has none."""
    assert "Server-Timing" not in _client(phone_app.app).get("/staff/login").headers
    control = _client(phone_app.app).get("/terms")
    assert control.headers.get("Server-Timing"), "the middleware is off, so an absence below proves nothing"
    assert _client(phone_app.app).get("/api/auth/csrf").headers.get("Server-Timing")  # JSON carries it too

    answers = []
    for ip in ("127.0.0.1", "127.0.0.2"):  # the first visit: an unknown number; the second: a known one
        client = _client(phone_app.app, ip)
        answers.append(client.get("/login/phone"))
        answers.append(_start(client))  # 303 to the code page
        answers.append(client.get("/login/phone/code"))
        wrong = _verify(client, "111111")
        answers.append(wrong)
        done = _verify(client)
        assert done.status_code == 302
        answers.append(done)
        assert len(_identities(pg_db)) == 1  # the second round signed in to the account the first made
    # refusals on the number page and the JSON twins
    refused_client = _client(phone_app.app, "127.0.0.3")
    answers.append(_start(refused_client, consent=""))
    refused_client.get("/login/phone")
    token = refused_client.get("/api/auth/csrf").get_json()["csrf_token"]
    answers.append(
        refused_client.post(
            "/api/auth/phone/start",
            json={"phone": NUMBER_A, "consent": False, "turnstile_token": _token()},
            headers={"X-CSRF-Token": token},
        )
    )
    answers.append(
        refused_client.post("/api/auth/phone/verify", json={"code": SENTINEL_CODE}, headers={"X-CSRF-Token": token})
    )
    assert len(answers) == 13
    for res in answers:
        assert "Server-Timing" not in res.headers, (res.request.path, res.status_code)


# ----------------------------------------------------------------- the per-address throttle on the real stack


def test_a_flood_of_starts_from_one_address_is_turned_away_before_turnstile_and_before_a_send(
    phone_app, pg_db, monkeypatch
):
    monkeypatch.setattr(routes.Config, "RATE_LIMIT_LOGIN_PER_MIN", 3)
    verifier = turnstile.FakeTurnstileVerifier(frozenset(f"ts-{n}" for n in range(1, 500)))
    turnstile.set_verifier(verifier)  # the fixture puts the shared one back on teardown
    provider = registry.get_provider()
    flooder = _client(phone_app.app)
    for _ in range(3):  # cheap refusals count against the address too
        assert _start(flooder, consent="").status_code == 200
    asked, sends = len(verifier.calls), _send_count(pg_db)

    token = _token()
    fourth = _start(flooder, token=token)
    html = fourth.get_data(as_text=True)
    assert fourth.status_code == 429 and "Please try again in a minute." in html
    assert 'id="ph-start-form"' in html and 'id="ph-code-form"' not in html  # nothing was started: the number page
    assert len(verifier.calls) == asked  # no Cloudflare call
    assert _send_count(pg_db) == sends and provider.outbox == []  # no send row, no text
    assert flooder.get("/login/phone/code").status_code == 303

    # another address is not affected, and the token the throttle never looked at still works
    other = _client(phone_app.app, "127.0.0.2")
    assert _start(other, token=token).status_code == 303
    assert [m.to for m in provider.outbox] == [E164_A]


def test_a_flood_of_code_guesses_is_turned_away_before_the_vendor_and_spends_no_attempt(phone_app, monkeypatch):
    monkeypatch.setattr(routes.Config, "RATE_LIMIT_LOGIN_PER_MIN", 3)
    provider = registry.get_provider()
    client = _client(phone_app.app)
    assert _start(client).status_code == 303  # the first of the three
    for _ in range(2):
        assert _verify(client, "111111").status_code == 200
    assert provider.check_calls == 2

    blocked = _verify(client)  # the right code, but the address is over its minute
    html = blocked.get_data(as_text=True)
    assert blocked.status_code == 429 and "Please try again in a minute." in html
    assert 'id="ph-code-form"' in html  # the challenge is still open, so the code page stays
    assert provider.check_calls == 2 and _me(client).status_code == 401

    clear_rate_limit_state()  # a minute later
    assert _verify(client).status_code == 302  # the throttled request did not spend the code or an attempt
    assert _me(client).status_code == 200
