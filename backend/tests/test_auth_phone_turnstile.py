"""S4: the Turnstile server check. A fake HTTP layer only; no network."""

from __future__ import annotations

import logging

import pytest
import requests

from backend.auth.phone import turnstile as t

SECRET = "turnstile-secret-test-value"
IP = "203.0.113.9"


class FakeHttp:
    def __init__(self, *answers):
        self.answers = list(answers)
        self.calls: list[tuple[str, dict, float]] = []

    def __call__(self, url, data, timeout):
        self.calls.append((url, dict(data), timeout))
        answer = self.answers.pop(0) if self.answers else (200, {"success": True})
        if isinstance(answer, BaseException):
            raise answer
        return answer


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    for name in ("TURNSTILE_SECRET_KEY", "FLASK_ENV", "ENV", "RAILWAY_PROJECT_ID", "RAILWAY_ENVIRONMENT_NAME",
                 "RAILWAY_ENVIRONMENT"):
        monkeypatch.delenv(name, raising=False)
    t.set_verifier(None)
    yield
    t.set_verifier(None)


def make(*answers, secret=SECRET, clock=None):
    http = FakeHttp(*answers)
    kwargs = {"clock": clock} if clock else {}
    return http, t.CloudflareTurnstile(secret, http_post=http, **kwargs)


def test_valid_token_passes_and_sends_secret_token_and_ip_with_a_3_second_timeout():
    http, v = make((200, {"success": True}))
    assert v.verify("tok-1", IP) == t.TurnstileResult(True, t.OK)
    url, data, timeout = http.calls[0]
    assert url == "https://challenges.cloudflare.com/turnstile/v0/siteverify"
    assert data == {"secret": SECRET, "response": "tok-1", "remoteip": IP}
    assert timeout == 3


def test_address_is_left_out_when_unknown():
    http, v = make()
    v.verify("tok-1", None)
    assert "remoteip" not in http.calls[0][1]


@pytest.mark.parametrize("token", [None, "", "   ", "x" * 2049, 123])
def test_missing_or_absurd_token_is_refused_without_calling_cloudflare(token):
    http, v = make()
    assert v.verify(token, IP) == t.TurnstileResult(False, t.MISSING_TOKEN)
    assert http.calls == []


def test_missing_secret_fails_closed_without_calling_cloudflare():
    http, v = make(secret="")
    assert v.verify("tok-1", IP) == t.TurnstileResult(False, t.NOT_CONFIGURED)
    assert http.calls == []


def test_secret_is_read_from_the_environment_when_not_given(monkeypatch):
    monkeypatch.setenv("TURNSTILE_SECRET_KEY", "env-secret")
    http = FakeHttp()
    v = t.CloudflareTurnstile(http_post=http)
    assert v.verify("tok-1", IP).ok
    assert http.calls[0][1]["secret"] == "env-secret"


def test_bad_token_is_rejected():
    _, v = make((200, {"success": False, "error-codes": ["invalid-input-response"]}))
    assert v.verify("tok-1", IP) == t.TurnstileResult(False, t.REJECTED)


def test_replayed_token_is_refused_locally_before_a_second_call():
    http, v = make((200, {"success": True}))
    assert v.verify("tok-1", IP).ok
    assert v.verify("tok-1", IP) == t.TurnstileResult(False, t.REPLAYED)
    assert len(http.calls) == 1


def test_replay_reported_by_cloudflare_is_refused():
    _, v = make((200, {"success": False, "error-codes": ["timeout-or-duplicate"]}))
    assert v.verify("tok-1", IP) == t.TurnstileResult(False, t.REPLAYED)


def test_replay_memory_expires_with_the_token_life():
    now = [1000.0]
    http, v = make((200, {"success": True}), (200, {"success": False, "error-codes": ["timeout-or-duplicate"]}),
                   clock=lambda: now[0])
    assert v.verify("tok-1", IP).ok
    now[0] += t.REPLAY_WINDOW_S + 1
    assert v.verify("tok-1", IP).reason == t.REPLAYED  # asked Cloudflare again, which refuses it
    assert len(http.calls) == 2


def test_timeout_fails_closed():
    _, v = make(requests.Timeout("slow"))
    assert v.verify("tok-1", IP) == t.TurnstileResult(False, t.TIMEOUT)


@pytest.mark.parametrize("error", [requests.ConnectionError("down"), OSError("x"), RuntimeError("boom")])
def test_unreachable_cloudflare_fails_closed(error):
    _, v = make(error)
    assert v.verify("tok-1", IP) == t.TurnstileResult(False, t.UNREACHABLE)


@pytest.mark.parametrize("answer", [(500, {"success": True}), (200, None), (200, ["x"]), (200, {"ok": 1}),
                                    (200, {"success": "true"}), (403, {"success": True})])
def test_odd_answers_fail_closed(answer):
    _, v = make(answer)
    assert v.verify("tok-1", IP) == t.TurnstileResult(False, t.BAD_RESPONSE)


def test_a_failed_check_still_uses_up_the_token():
    http, v = make((200, {"success": False}), (200, {"success": True}))
    assert v.verify("tok-1", IP).reason == t.REJECTED
    assert v.verify("tok-1", IP).reason == t.REPLAYED
    assert len(http.calls) == 1


def test_module_function_uses_the_installed_verifier():
    fake = t.FakeTurnstileVerifier({"good"})
    t.set_verifier(fake)
    assert t.verify_turnstile("good", IP).ok
    assert t.verify_turnstile("good", IP).reason == t.REPLAYED
    assert t.verify_turnstile("other", IP).reason == t.REJECTED
    assert t.verify_turnstile(None, IP).reason == t.MISSING_TOKEN
    assert fake.calls[0] == ("good", IP)


def test_default_verifier_fails_closed_without_a_secret():
    assert t.verify_turnstile("tok-1", IP) == t.TurnstileResult(False, t.NOT_CONFIGURED)


def test_fake_verifier_refuses_to_load_in_production(monkeypatch):
    monkeypatch.setenv("FLASK_ENV", "production")
    with pytest.raises(RuntimeError):
        t.FakeTurnstileVerifier()


def test_every_reason_is_a_short_code_and_logs_hold_no_token_secret_or_address(caplog):
    caplog.set_level(logging.DEBUG)
    _, v = make((200, {"success": False, "error-codes": ["invalid-input-response"]}), requests.Timeout("slow"))
    v.verify("tok-secretish", IP)
    v.verify("tok-other", IP)
    v.verify(None, IP)
    text = caplog.text
    for needle in ("tok-secretish", "tok-other", SECRET, IP, "invalid-input-response"):
        assert needle not in text
    assert "event=blocked_turnstile" in text
    for reason in t.REASONS:
        assert reason == reason.lower() and " " not in reason
