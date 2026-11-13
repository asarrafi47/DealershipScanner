"""D1: the sc_dev device cookie and keyed hashes."""

from __future__ import annotations

import pytest

from backend.utils import device_id as dev


@pytest.fixture()
def app(monkeypatch, app_factory):
    monkeypatch.setenv("PHONE_HASH_PEPPER", "pepper-one-for-tests-0123456789")
    return app_factory().app


def _cookie_header(resp):
    return [h for h in resp.headers.getlist("Set-Cookie") if h.startswith("sc_dev=")]


def test_cookie_set_on_chat_route_not_on_other_pages(app):
    c = app.test_client()
    assert not _cookie_header(c.get("/login"))
    r = c.get("/api/car/1/narrate")
    assert len(_cookie_header(r)) == 1


def test_cookie_is_httponly_lax_and_400_days(app):
    h = _cookie_header(app.test_client().get("/api/car/1/narrate"))[0]
    assert "HttpOnly" in h
    assert "SameSite=Lax" in h
    assert f"Max-Age={400 * 24 * 3600}" in h


def test_cookie_secure_in_production(monkeypatch, app_factory):
    monkeypatch.setenv("PHONE_HASH_PEPPER", "pepper-one-for-tests-0123456789")
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
    monkeypatch.setenv("PHONE_HASH_PEPPER", "pepper-one-for-tests-0123456789")
    a1 = dev.device_hash("a" * 32)
    assert a1 == dev.device_hash("a" * 32)
    assert a1 != dev.device_hash("b" * 32)
    monkeypatch.setenv("PHONE_HASH_PEPPER", "pepper-two-for-tests-0123456789")
    assert dev.device_hash("a" * 32) != a1


def test_production_without_pepper_fails_only_when_feature_on(monkeypatch):
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.delenv("PHONE_HASH_PEPPER", raising=False)
    monkeypatch.delenv("AUTH_PHONE_LOGIN_ENABLED", raising=False)
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
