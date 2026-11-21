"""D1: the sc_dev device cookie and keyed hashes."""

from __future__ import annotations

import pytest

from backend.utils import device_id as dev


@pytest.fixture()
def app(monkeypatch, app_factory):
    monkeypatch.setenv("PHONE_HASH_PEPPER", "pepper-one-for-tests-0123456789-ab")
    return app_factory().app


def _cookie_header(resp):
    return [h for h in resp.headers.getlist("Set-Cookie") if h.startswith("sc_dev=")]


def test_cookie_set_on_chat_route_not_on_other_pages(app):
    c = app.test_client()
    assert not _cookie_header(c.get("/listings"))
    r = c.get("/api/car/1/narrate")
    assert len(_cookie_header(r)) == 1


def test_cookie_is_httponly_lax_and_400_days(app):
    h = _cookie_header(app.test_client().get("/api/car/1/narrate"))[0]
    assert "HttpOnly" in h
    assert "SameSite=Lax" in h
    assert f"Max-Age={400 * 24 * 3600}" in h


def test_cookie_secure_in_production(monkeypatch, app_factory):
    monkeypatch.setenv("PHONE_HASH_PEPPER", "pepper-one-for-tests-0123456789-ab")
    app = app_factory(production=True).app
    h = _cookie_header(app.test_client().get("/api/car/1/narrate", base_url="https://localhost"))
    assert h and "Secure" in h[0]


def test_cookie_is_signed_and_keeps_device(app):
    c = app.test_client()
    first = c.get("/api/car/1/narrate")
    raw = c.get_cookie("sc_dev").value
    assert "." in raw  # payload.signature
    again = c.get("/api/car/1/narrate")
    assert _cookie_header(first)
    assert not _cookie_header(again)  # a returning device is not re-issued
    with app.test_request_context("/", headers={"Cookie": f"sc_dev={raw}"}):
        d1 = dev.read_device_id(dev.request)
    assert d1 and len(d1) == 32


def test_tampered_cookie_gives_new_device(app):
    c = app.test_client()
    c.get("/api/car/1/narrate")
    raw = c.get_cookie("sc_dev").value
    with app.test_request_context("/", headers={"Cookie": f"sc_dev={raw}"}):
        good = dev.read_device_id(dev.request)
    bad = raw[:-2] + ("aa" if not raw.endswith("aa") else "bb")
    with app.test_request_context("/", headers={"Cookie": f"sc_dev={bad}"}):
        assert dev.read_device_id(dev.request) is None
    c2 = app.test_client()
    c2.set_cookie("sc_dev", bad)
    r = c2.get("/api/car/1/narrate")
    assert _cookie_header(r)  # a new device cookie replaces the bad one
    new_raw = c2.get_cookie("sc_dev").value
    with app.test_request_context("/", headers={"Cookie": f"sc_dev={new_raw}"}):
        assert dev.read_device_id(dev.request) not in (None, good)


def test_hashes_stable_for_one_pepper_and_differ_for_another(monkeypatch):
    monkeypatch.setenv("PHONE_HASH_PEPPER", "pepper-one-for-tests-0123456789-ab")
    a1 = dev.device_hash("a" * 32)
    assert a1 == dev.device_hash("a" * 32)
    assert a1 != dev.device_hash("b" * 32)
    monkeypatch.setenv("PHONE_HASH_PEPPER", "pepper-two-for-tests-0123456789-ab")
    assert dev.device_hash("a" * 32) != a1


def test_production_without_pepper_fails_only_when_feature_on(monkeypatch):
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.delenv("PHONE_HASH_PEPPER", raising=False)
    monkeypatch.delenv("AUTH_PHONE_LOGIN_ENABLED", raising=False)
    monkeypatch.delenv("ALLOW_DEV_PHONE_PEPPER", raising=False)
    dev.assert_pepper_if_needed()  # feature off: no failure
    with pytest.raises(dev.PepperMissing):
        dev.device_hash("a" * 32)
    monkeypatch.setenv("AUTH_PHONE_LOGIN_ENABLED", "1")
    with pytest.raises(dev.PepperMissing):
        dev.assert_pepper_if_needed()


def test_non_ascii_cookie_is_a_new_device_not_a_500(app):
    good = "a" * 32
    for bad in (good + ".é" + "b" * 63, good + "." + "b" * 63 + "²", good):
        with app.test_request_context("/", environ_overrides={"HTTP_COOKIE": "sc_dev=" + bad}):
            assert dev.read_device_id(dev.request) is None
    c = app.test_client()
    r = c.get("/api/car/1/narrate", environ_overrides={"HTTP_COOKIE": "sc_dev=" + good + ".é" + "b" * 63})
    assert r.status_code != 500
    assert _cookie_header(r)


# ---------------------------------------------------------------- wave 1 review follow-ups

import re
from pathlib import Path

from backend.auth.phone import numbers

_DESIGN = Path(__file__).resolve().parents[2] / "docs" / "auth_sms" / "DESIGN.md"
# In the section 15 table, but not a browser phone or chat page: the sign-out POST (a signed-in action),
# /logout (unchanged) and the vendor callback (a server, not a browser).
_NO_COOKIE_ROUTES = {"/account/sign-out-everywhere", "/logout", "/webhooks/twilio/verify"}


def _design_routes() -> list[tuple[str, str]]:
    text = _DESIGN.read_text(encoding="utf-8")
    section = text.split("## 15. Screens and endpoints", 1)[1].split("\n## 16.", 1)[0]
    out: list[tuple[str, str]] = []
    for row in section.splitlines():
        if not row.startswith("| `"):
            continue
        for chunk in re.findall(r"`([^`]+)`", row.split("|")[1]):
            m = re.match(r"^((?:GET|POST)(?:/(?:GET|POST))?)\s+(\S+)$", chunk.strip())
            if m:
                out.append((m.group(1), m.group(2)))
            elif chunk.startswith("/") and out:  # "`/verify`, `/resend`": siblings of the path before it
                out.append((out[-1][0], out[-1][1].rsplit("/", 1)[0] + chunk))
    return out


def test_the_design_table_was_parsed():
    routes = _design_routes()
    paths = {p for _m, p in routes}
    assert {"/login", "/login/phone", "/staff/login", "/account/phone", "/api/auth/phone/start"} <= paths
    assert len(routes) >= 14


def test_every_design_route_that_needs_the_cookie_gets_it(app):
    c = app.test_client()
    needed = [p for _m, p in _design_routes() if p not in _NO_COOKIE_ROUTES]
    assert needed
    for path in needed:
        concrete = re.sub(r"<[^>]+>", "tok", path)
        assert dev.wants_device_cookie(concrete), path
    for path in ("/login", "/staff/login", "/account/phone", "/login/phone/code", "/login/claim"):
        r = app.test_client().get(path)
        assert _cookie_header(r), path  # the cookie is set whatever the page answers (404 while the flag is off)


def test_the_routes_that_must_not_get_the_cookie_do_not():
    for path in _NO_COOKIE_ROUTES | {"/", "/listings", "/api/health"}:
        assert not dev.wants_device_cookie(path), path


def test_the_cookie_is_a_rate_limit_key_only_rule_is_written_down():
    text = (Path(dev.__file__)).read_text(encoding="utf-8")
    assert "rate-limit key ONLY" in text and "never marks a browser trusted" in text
    tasks = (Path(__file__).resolve().parents[2] / "docs" / "auth_sms" / "TASKS.md").read_text(encoding="utf-8")
    assert "`sc_dev` is a rate-limit key only" in tasks


def test_the_cookie_value_never_reaches_a_session_or_an_authorization_decision(app):
    c = app.test_client()
    c.get("/login")
    with c.session_transaction() as sess:
        assert "sc_dev" not in str(dict(sess)) and "device" not in " ".join(sess.keys()).lower()


def test_device_id_delegates_to_the_s1_helpers(monkeypatch):
    monkeypatch.setenv("PHONE_HASH_PEPPER", "pepper-one-for-tests-0123456789-ab")
    assert dev.device_hash("a" * 32) == numbers.hash_device("a" * 32)
    assert dev.hmac_hash("device", "a" * 32) == numbers.hash_value("device", "a" * 32) == dev.device_hash("a" * 32)
    assert dev.pepper() == numbers.pepper_bytes()
    assert dev.PepperMissing is numbers.PepperMissing  # one class, one dev pepper, one minimum length
    src = Path(dev.__file__).read_text(encoding="utf-8")
    assert "_DEV_PEPPER" not in src and "dev-only-pepper" not in src


def test_device_id_without_pepper_raises_the_shared_error_and_honours_the_shared_opt_in(monkeypatch):
    monkeypatch.delenv("PHONE_HASH_PEPPER", raising=False)
    monkeypatch.delenv("ALLOW_DEV_PHONE_PEPPER", raising=False)
    with pytest.raises(numbers.PepperMissing):
        dev.device_hash("a" * 32)
    monkeypatch.setenv("ALLOW_DEV_PHONE_PEPPER", "1")
    assert dev.device_hash("a" * 32) == numbers.hash_device("a" * 32)
    monkeypatch.setenv("PHONE_HASH_PEPPER", "too-short")
    with pytest.raises(numbers.PepperMissing):
        dev.pepper()
