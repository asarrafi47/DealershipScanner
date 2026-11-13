"""S4: POST /webhooks/twilio/verify. Signed with the twilio library's own signer; no network."""

from __future__ import annotations

import json
import logging

import pytest
from twilio.request_validator import RequestValidator

from backend.auth.phone import webhook as wh

TOKEN = "auth-token-for-tests"
KEY_SECRET = "api-key-secret-for-tests"
BASE = "https://sarraficars.example"
PATH = "/webhooks/twilio/verify"
NUMBER = "+12025550123"
CODE = "482913"


@pytest.fixture
def client(app_factory, monkeypatch):
    for name in ("TWILIO_AUTH_TOKEN", "TWILIO_API_KEY_SECRET"):
        monkeypatch.delenv(name, raising=False)
    main = app_factory(
        DEV_IP_ALLOWLIST="", AUTH_PHONE_LOGIN_ENABLED="1", PUBLIC_BASE_URL=BASE, TWILIO_AUTH_TOKEN=TOKEN,
    )
    sink = wh.MemorySendEventSink()
    wh.set_sink(sink)
    wh.reset_state()
    c = main.app.test_client()
    c.sink = sink
    yield c
    wh.set_sink(None)
    wh.reset_state()


def sign(params, *, url=BASE + PATH, secret=TOKEN):
    return RequestValidator(secret).compute_signature(url, params)


def form(status="delivered", sid="SMabc123", event="EVevent1", **extra):
    # A real callback carries the number; ours must never log it.
    return {"MessageSid": sid, "MessageStatus": status, "EventSid": event, "To": NUMBER,
            "Body": "Sarrafi Cars: Your verification code is " + CODE, **extra}


def post(client, params, *, signature=None, url=PATH, headers=None):
    h = {"X-Twilio-Signature": signature if signature is not None else sign(params)}
    h.update(headers or {})
    return client.post(url, data=params, headers=h)


def test_good_signature_updates_the_send_row_through_the_sink(client):
    r = post(client, form("delivered", Price="-0.0079"))
    assert r.status_code == 200 and r.get_json() == {"ok": True}
    event = client.sink.events["EVevent1"]
    assert (event.provider_ref, event.status, event.cost_cents) == ("SMabc123", "delivered", 0.79)


def test_bad_signature_gets_403_and_changes_nothing(client):
    r = post(client, form(), signature="not-a-signature")
    assert r.status_code == 403
    assert client.sink.events == {}
    assert wh.bad_signature_count == 1


def test_signature_made_with_another_secret_is_refused(client):
    assert post(client, form(), signature=sign(form(), secret="someone-elses-token")).status_code == 403
    assert client.sink.events == {}


def test_missing_signature_gets_403(client):
    r = client.post(PATH, data=form())
    assert r.status_code == 403 and client.sink.events == {}


def test_a_tampered_body_is_refused(client):
    good = form("sent")
    sig = sign(good)
    assert post(client, form("delivered"), signature=sig).status_code == 403
    assert client.sink.events == {}


def test_signature_covers_the_public_url_not_the_proxy_url(client):
    # The proxy-facing host differs from the public URL; the signature is over the public one.
    r = post(client, form(), headers={"Host": "internal.railway.example", "X-Forwarded-Proto": "http",
                                      "X-Forwarded-Host": "internal.railway.example"})
    assert r.status_code == 200 and "EVevent1" in client.sink.events


def test_signature_over_the_request_url_instead_of_the_public_url_is_refused(client):
    params = form()
    r = post(client, params, signature=sign(params, url="http://localhost" + PATH))
    assert r.status_code == 403


def test_query_string_is_part_of_the_signed_url(client):
    params = form()
    url = BASE + PATH + "?v=2"
    assert post(client, params, url=PATH + "?v=2", signature=sign(params, url=url)).status_code == 200
    assert post(client, form(event="EVevent2"), url=PATH + "?v=2").status_code == 403  # signed without the query


def test_no_public_url_configured_fails_closed(client, monkeypatch):
    monkeypatch.delenv("PUBLIC_BASE_URL")
    assert post(client, form()).status_code == 403
    assert client.sink.events == {}


def test_route_is_404_while_the_flag_is_off(app_factory, monkeypatch):
    main = app_factory(DEV_IP_ALLOWLIST="", AUTH_PHONE_LOGIN_ENABLED=None, PUBLIC_BASE_URL=BASE, TWILIO_AUTH_TOKEN=TOKEN)
    wh.set_sink(wh.MemorySendEventSink())
    try:
        c = main.app.test_client()
        params = form()
        assert post(c, params).status_code == 404  # even with a good signature
        assert c.post(PATH, data=params).status_code == 404
        assert c.get(PATH).status_code in (404, 405)
    finally:
        wh.set_sink(None)


def test_flag_toggles_at_request_time(client, monkeypatch):
    monkeypatch.setenv("AUTH_PHONE_LOGIN_ENABLED", "0")
    assert post(client, form()).status_code == 404
    monkeypatch.setenv("AUTH_PHONE_LOGIN_ENABLED", "1")
    assert post(client, form()).status_code == 200


def test_same_event_id_applies_once(client):
    assert post(client, form()).status_code == 200
    assert post(client, form("failed")).status_code == 200  # same EventSid: a repeat
    assert list(client.sink.events) == ["EVevent1"]
    assert client.sink.events["EVevent1"].status == "delivered"


def test_without_an_event_id_one_status_per_message_is_one_event(client):
    params = {"MessageSid": "SMx1", "MessageStatus": "sent"}
    assert post(client, params).status_code == 200
    assert post(client, params).status_code == 200
    assert post(client, {"MessageSid": "SMx1", "MessageStatus": "delivered"}).status_code == 200
    assert sorted(client.sink.events) == ["SMx1:delivered", "SMx1:sent"]


def test_unknown_events_get_200_and_are_ignored(client):
    for params in ({"MessageSid": "SM1", "MessageStatus": "queued"}, {"Foo": "bar"}, {}, {"MessageStatus": "sent"}):
        assert post(client, params).status_code == 200
    assert client.sink.events == {}


def test_json_body_is_verified_over_the_raw_body_and_parsed(client):
    body = json.dumps({"type": "verification.delivered", "data": {"sid": "VE9", "status": "delivered"},
                       "id": "EVjson1"})
    url = BASE + PATH + "?bodySHA256=" + __import__("hashlib").sha256(body.encode()).hexdigest()
    sig = RequestValidator(TOKEN).compute_signature(url, {})  # body is bound through bodySHA256
    r = client.post(PATH + url[len(BASE + PATH):], data=body, headers={"Content-Type": "application/json",
                                                                       "X-Twilio-Signature": sig})
    assert r.status_code == 200
    assert client.sink.events["EVjson1"].provider_ref == "VE9"
    bad = client.post(PATH + url[len(BASE + PATH):], data=body + " ", headers={"Content-Type": "application/json",
                                                                            "X-Twilio-Signature": sig})
    assert bad.status_code == 403


def test_the_api_key_secret_never_validates_a_callback(client, monkeypatch):
    monkeypatch.setenv("TWILIO_API_KEY_SECRET", KEY_SECRET)
    params = form(event="EVkey1")
    assert post(client, params, signature=sign(params, secret=KEY_SECRET)).status_code == 403
    assert post(client, form(event="EVkey2")).status_code == 200  # the auth token still works


def test_route_is_404_without_the_auth_token_even_with_the_flag_on(client, monkeypatch):
    monkeypatch.delenv("TWILIO_AUTH_TOKEN", raising=False)
    monkeypatch.setenv("TWILIO_API_KEY_SECRET", KEY_SECRET)
    params = form(event="EVnone")
    assert post(client, params, signature=sign(params, secret=KEY_SECRET)).status_code == 404
    assert client.sink.events == {}


def test_sink_failure_returns_500_so_the_vendor_retries_and_the_retry_applies(client):
    class Flaky(wh.MemorySendEventSink):
        fail = True

        def apply(self, event):
            if self.fail:
                self.fail = False
                raise RuntimeError("db down " + NUMBER)
            return super().apply(event)

    flaky = Flaky()
    wh.set_sink(flaky)
    assert post(client, form()).status_code == 500
    assert post(client, form()).status_code == 200
    assert "EVevent1" in flaky.events


def test_default_sink_acknowledges_and_stores_nothing(client):
    wh.set_sink(None)
    assert post(client, form()).status_code == 200


def test_oversized_body_is_refused(client):
    r = client.post(PATH, data="x=" + "a" * (wh.MAX_BODY_BYTES + 10), headers={
        "Content-Type": "application/x-www-form-urlencoded", "X-Twilio-Signature": "x"})
    assert r.status_code == 413


def test_get_is_not_allowed(client):
    assert client.get(PATH).status_code in (404, 405)


def test_captured_logs_hold_no_number_code_body_or_signature(client, caplog):
    caplog.set_level(logging.DEBUG)
    good = form()
    post(client, good)
    post(client, good)  # repeat
    post(client, form(event="EVevent9"), signature="forged-signature-text")
    post(client, {"Foo": "bar", "To": NUMBER})
    wh.set_sink(type("Boom", (), {"apply": lambda self, e: (_ for _ in ()).throw(RuntimeError(NUMBER + CODE))})())
    post(client, form(event="EVevent10"))
    text = caplog.text
    for needle in (NUMBER, "2025550123", CODE, "verification code", "forged-signature-text", TOKEN, "SMabc123"):
        assert needle not in text, needle
    assert "event=webhook_bad_signature" in text and "event=webhook_event" in text


def test_the_webhook_is_listed_as_signature_verified_in_the_csrf_module():
    from backend.web import security

    assert "twilio_webhook.twilio_verify_webhook" in security._CSRF_SIGNATURE_VERIFIED_ENDPOINTS
    for ep in security._CSRF_SIGNATURE_VERIFIED_ENDPOINTS:
        assert ep not in security._CSRF_FORM_POST_ENDPOINTS and ep not in security._CSRF_HEADER_POST_ENDPOINTS
