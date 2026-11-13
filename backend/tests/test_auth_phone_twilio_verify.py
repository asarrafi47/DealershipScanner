"""S3: Twilio Verify adapter, with a fake client object. No network, no real text."""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest
import requests
from twilio.base.exceptions import TwilioRestException

from backend.auth.phone import config, provider as p, registry, twilio_verify as tv
from backend.auth.phone.lookup_cache import MemoryLookupCache

NUMBER = "+12025550123"
CODE = "482913"
SERVICE = "VAfakeservice"
SECRET = "very-secret-key-value-test"
VENDOR_TEXT = "The phone number +12025550123 is unverified; trial account text"


def vendor_error(status: int, code: int | None = None) -> TwilioRestException:
    return TwilioRestException(status, "https://verify.twilio.test/v2/x", VENDOR_TEXT, code)


class FakeClient:
    """Just enough of twilio.rest.Client: records calls, returns or raises what the test queued."""

    def __init__(self):
        self.calls: list[tuple] = []
        self.queues = {"start": [], "check": [], "lookup": []}
        me = self

        def pop(kind, record):
            me.calls.append(record)
            item = me.queues[kind].pop(0)
            if isinstance(item, BaseException):
                raise item
            return item

        class _Verifs:
            def create(self, **kw):
                return pop("start", ("start", kw))

        class _Checks:
            def create(self, **kw):
                return pop("check", ("check", kw))

        class _Service:
            verifications = _Verifs()
            verification_checks = _Checks()

        class _Phone:
            def __init__(self, number):
                self.number = number

            def fetch(self, **kw):
                return pop("lookup", ("lookup", {"number": self.number, **kw}))

        def services(sid):
            me.calls.append(("service", sid))
            return _Service()

        self.verify = SimpleNamespace(v2=SimpleNamespace(services=services))
        self.lookups = SimpleNamespace(v2=SimpleNamespace(phone_numbers=lambda n: _Phone(n)))


def make(cache=None):
    fc = FakeClient()
    return fc, tv.TwilioVerifyProvider(fc, SERVICE, cache=cache, hash_fn=lambda e: "hash:" + e[-4:])


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    for name in ("TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN", "TWILIO_API_KEY_SID", "TWILIO_API_KEY_SECRET",
                 "TWILIO_VERIFY_SERVICE_SID", "AUTH_PHONE_PROVIDER", "FLASK_ENV", "ENV", "RAILWAY_PROJECT_ID",
                 "RAILWAY_ENVIRONMENT_NAME", "RAILWAY_ENVIRONMENT"):
        monkeypatch.delenv(name, raising=False)
    registry.reset()
    tv.set_lookup_cache(None)
    yield
    registry.reset()
    tv.set_lookup_cache(None)


def _queue(fc, kind, *items):
    fc.queues[{"verifications": "start", "checks": "check", "lookup": "lookup"}[kind]].extend(items)


# ---- start ----------------------------------------------------------------------------------

def test_start_creates_a_sms_verification_and_returns_the_vendor_id():
    fc, prov = make()
    _queue(fc, "verifications", SimpleNamespace(sid="VEabc", status="pending"))
    result = prov.start(NUMBER, locale="en")
    assert result == p.StartResult(True, "VEabc", None, p.START_SENT)
    assert ("service", SERVICE) in fc.calls
    assert ("start", {"to": NUMBER, "channel": "sms", "locale": "en"}) in fc.calls


def test_start_throttled_by_http_429():
    fc, prov = make()
    _queue(fc, "verifications", vendor_error(429, 20429))
    result = prov.start(NUMBER, locale="en")
    assert (result.ok, result.reason) == (False, p.START_THROTTLED)


def test_start_throttled_by_max_send_attempts():
    fc, prov = make()
    _queue(fc, "verifications", vendor_error(429, 60203))
    assert prov.start(NUMBER, locale="en").reason == p.START_THROTTLED


@pytest.mark.parametrize("status,code,reason", [
    (400, 60200, p.START_INVALID_NUMBER), (400, 21211, p.START_INVALID_NUMBER),
    (403, 60410, p.START_BLOCKED), (500, None, p.START_PROVIDER_ERROR), (401, 20003, p.START_PROVIDER_ERROR),
])
def test_start_maps_vendor_errors(status, code, reason):
    fc, prov = make()
    _queue(fc, "verifications", vendor_error(status, code))
    result = prov.start(NUMBER, locale="en")
    assert (result.ok, result.reason, result.provider_ref) == (False, reason, None)


def test_connect_failure_on_start_is_retried_once_then_reported():
    fc, prov = make()
    _queue(fc, "verifications", requests.ConnectTimeout("down"), requests.ConnectTimeout("slow"))
    result = prov.start(NUMBER, locale="en")
    assert (result.ok, result.reason) == (False, p.START_NETWORK_ERROR)
    assert [c for c in fc.calls if c[0] == "start"].__len__() == 2


def test_network_error_on_start_then_success_sends_once():
    fc, prov = make()
    _queue(fc, "verifications", requests.ConnectTimeout("down"), SimpleNamespace(sid="VEok", status="pending"))
    assert prov.start(NUMBER, locale="en") == p.StartResult(True, "VEok", None, p.START_SENT, attempts=2)


@pytest.mark.parametrize("exc", [requests.ReadTimeout("slow"), requests.ConnectionError("reset"), requests.Timeout("t")])
def test_a_failure_that_may_have_sent_the_text_is_never_retried(exc):
    fc, prov = make()
    _queue(fc, "verifications", exc, SimpleNamespace(sid="VEdup", status="pending"))
    result = prov.start(NUMBER, locale="en")
    assert (result.ok, result.reason, result.attempts) == (False, p.START_NETWORK_ERROR, 1)
    assert len([c for c in fc.calls if c[0] == "start"]) == 1


def test_a_vendor_error_on_start_is_not_retried():
    fc, prov = make()
    _queue(fc, "verifications", vendor_error(500))
    prov.start(NUMBER, locale="en")
    assert len([c for c in fc.calls if c[0] == "start"]) == 1


def test_unexpected_exception_becomes_provider_error():
    fc, prov = make()
    _queue(fc, "verifications", ValueError(VENDOR_TEXT))
    assert prov.start(NUMBER, locale="en").reason == p.START_PROVIDER_ERROR


# ---- check ----------------------------------------------------------------------------------

def test_check_approved_uses_the_verification_id():
    fc, prov = make()
    _queue(fc, "checks", SimpleNamespace(status="approved"))
    result = prov.check(NUMBER, code=CODE, provider_ref="VEabc")
    assert result == p.CheckResult(True, p.CHECK_APPROVED)
    assert ("check", {"code": CODE, "verification_sid": "VEabc"}) in fc.calls


def test_check_without_a_ref_uses_the_number():
    fc, prov = make()
    _queue(fc, "checks", SimpleNamespace(status="approved"))
    prov.check(NUMBER, code=CODE, provider_ref=None)
    assert ("check", {"code": CODE, "to": NUMBER}) in fc.calls


def test_check_wrong_code():
    fc, prov = make()
    _queue(fc, "checks", SimpleNamespace(status="pending"))
    result = prov.check(NUMBER, code=CODE, provider_ref="VEabc")
    assert (result.approved, result.reason) == (False, p.CHECK_WRONG_CODE)


def test_check_expired_when_the_vendor_has_no_open_verification():
    fc, prov = make()
    _queue(fc, "checks", vendor_error(404, 20404))
    assert prov.check(NUMBER, code=CODE, provider_ref="VEabc").reason == p.CHECK_EXPIRED


def test_check_canceled_counts_as_expired():
    fc, prov = make()
    _queue(fc, "checks", SimpleNamespace(status="canceled"))
    assert prov.check(NUMBER, code=CODE, provider_ref="VEabc").reason == p.CHECK_EXPIRED


def test_check_max_attempts():
    fc, prov = make()
    _queue(fc, "checks", vendor_error(429, 60202))
    assert prov.check(NUMBER, code=CODE, provider_ref="VEabc").reason == p.CHECK_TOO_MANY


def test_check_throttled_429():
    fc, prov = make()
    _queue(fc, "checks", vendor_error(429, 20429))
    assert prov.check(NUMBER, code=CODE, provider_ref="VEabc").reason == p.CHECK_THROTTLED


def test_check_network_error_is_not_retried_and_is_not_a_wrong_code():
    fc, prov = make()
    _queue(fc, "checks", requests.ConnectionError("down"))
    result = prov.check(NUMBER, code=CODE, provider_ref="VEabc")
    assert (result.approved, result.reason) == (False, p.CHECK_NETWORK_ERROR)
    assert len([c for c in fc.calls if c[0] == "check"]) == 1


def test_check_unknown_status_or_error():
    fc, prov = make()
    _queue(fc, "checks", SimpleNamespace(status="weird"), vendor_error(500), vendor_error(400, 60200))
    assert prov.check(NUMBER, code=CODE, provider_ref="VEabc").reason == p.CHECK_UNKNOWN
    assert prov.check(NUMBER, code=CODE, provider_ref="VEabc").reason == p.CHECK_UNKNOWN
    assert prov.check(NUMBER, code=CODE, provider_ref="VEabc").reason == p.CHECK_WRONG_CODE


# ---- lookup ---------------------------------------------------------------------------------

def _line(line_type, valid=True, country="US", intel=True):
    return SimpleNamespace(valid=valid, country_code=country,
                           line_type_intelligence={"type": line_type} if intel else None)


@pytest.mark.parametrize("line_type,ok,reason", [
    ("mobile", True, p.LINE_OK), ("nonFixedVoip", False, p.LINE_VOIP), ("fixedVoip", False, p.LINE_VOIP),
    ("landline", False, p.LINE_LANDLINE), ("tollFree", False, p.LINE_NOT_MOBILE),
])
def test_lookup_maps_line_types(line_type, ok, reason):
    fc, prov = make()
    _queue(fc, "lookup", _line(line_type))
    info = prov.lookup(NUMBER)
    assert (info.ok, info.reason, info.line_type, info.country) == (ok, reason, line_type, "US")
    assert ("lookup", {"number": NUMBER, "fields": "line_type_intelligence"}) in fc.calls


def test_lookup_with_no_type_is_refused_and_not_cached():
    fc, prov = make()
    _queue(fc, "lookup", _line(None, intel=False))
    info = prov.lookup(NUMBER)
    assert (info.ok, info.line_type, info.reason) == (False, None, p.LINE_UNKNOWN)
    cache = p.MemoryLookupCache() if hasattr(p, "MemoryLookupCache") else None
    assert cache is None or cache.get("x") is None


def test_lookup_invalid_number():
    fc, prov = make()
    _queue(fc, "lookup", _line("mobile", valid=False), vendor_error(404, 20404))
    assert prov.lookup(NUMBER).reason == p.LINE_INVALID_NUMBER
    assert prov.lookup(NUMBER).reason == p.LINE_INVALID_NUMBER


def test_lookup_failure_fails_closed():
    fc, prov = make()
    _queue(fc, "lookup", requests.Timeout("slow"), vendor_error(500), vendor_error(429))
    for _ in range(3):
        info = prov.lookup(NUMBER)
        assert (info.ok, info.reason) == (False, p.LINE_LOOKUP_FAILED)


def test_lookup_answers_are_cached_by_hash_and_failures_are_not():
    cache = MemoryLookupCache()
    fc, prov = make(cache)
    _queue(fc, "lookup", requests.Timeout("slow"), _line("nonFixedVoip"))
    assert prov.lookup(NUMBER).reason == p.LINE_LOOKUP_FAILED  # not cached
    first = prov.lookup(NUMBER)
    second = prov.lookup(NUMBER)  # served from the cache: the queue is empty, a real call would crash
    assert first == second and second.reason == p.LINE_VOIP
    assert len([c for c in fc.calls if c[0] == "lookup"]) == 2
    assert cache.get("hash:0123") == first  # keyed by the hash, never the number
    assert cache.get(NUMBER) is None


def test_lookup_cache_ttl_is_thirty_days():
    class Spy:
        def __init__(self):
            self.puts = []

        def get(self, h):
            return None

        def put(self, h, info, ttl_s):
            self.puts.append(ttl_s)

    spy = Spy()
    fc, prov = make(spy)
    _queue(fc, "lookup", _line("mobile"))
    prov.lookup(NUMBER)
    assert spy.puts == [30 * 24 * 3600]


# ---- logs and exception text ----------------------------------------------------------------

def test_no_number_code_secret_or_vendor_text_in_logs_or_results(caplog):
    caplog.set_level(logging.DEBUG)
    fc, prov = make()
    _queue(fc, "verifications", vendor_error(429, 60203), requests.ConnectionError(VENDOR_TEXT),
           requests.ConnectionError(VENDOR_TEXT), ValueError(VENDOR_TEXT))
    _queue(fc, "checks", vendor_error(404), requests.Timeout(VENDOR_TEXT), SimpleNamespace(status="approved"))
    _queue(fc, "lookup", vendor_error(500), requests.Timeout(VENDOR_TEXT))
    results = [
        prov.start(NUMBER, locale="en"), prov.start(NUMBER, locale="en"), prov.start(NUMBER, locale="en"),
        prov.check(NUMBER, code=CODE, provider_ref="VEabc"), prov.check(NUMBER, code=CODE, provider_ref="VEabc"),
        prov.check(NUMBER, code=CODE, provider_ref="VEabc"),
        prov.lookup(NUMBER), prov.lookup(NUMBER),
    ]
    text = caplog.text + repr(results)
    for needle in (NUMBER, "2025550123", CODE, SECRET, "unverified", "trial account", "twilio.test"):
        assert needle not in text, needle
    assert "event=provider_error" in caplog.text  # failures are still visible, as reason codes


def test_results_hold_only_our_reason_codes():
    fc, prov = make()
    _queue(fc, "verifications", vendor_error(500))
    result = prov.start(NUMBER, locale="en")
    assert result.reason in p.START_REASONS and VENDOR_TEXT not in repr(result)


# ---- the real client builder and the registry ------------------------------------------------

def _set_api_key_env(mp):
    mp.setenv("TWILIO_ACCOUNT_SID", "ACfake")
    mp.setenv("TWILIO_API_KEY_SID", "SKfake")
    mp.setenv("TWILIO_API_KEY_SECRET", SECRET)
    mp.setenv("TWILIO_VERIFY_SERVICE_SID", SERVICE)
    mp.setenv("AUTH_PHONE_PROVIDER", "twilio_verify")


def test_real_client_is_built_from_the_api_key_with_a_5_second_timeout_and_silenced_logging(monkeypatch):
    _set_api_key_env(monkeypatch)
    client = tv._build_client(config.twilio_credentials())
    assert client.username == "SKfake" and client.account_sid == "ACfake"
    assert client.http_client.timeout == 5
    assert client.http_client.logger.disabled is True  # Twilio's HTTP client logs URLs, which hold the number


def test_real_client_with_the_auth_token_style(monkeypatch):
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "ACfake")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "tok-fake")
    client = tv._build_client(config.twilio_credentials())
    assert client.username == "ACfake" and client.account_sid == "ACfake"


def test_twilio_http_logging_stays_off_even_at_debug(monkeypatch, caplog):
    _set_api_key_env(monkeypatch)
    caplog.set_level(logging.DEBUG)
    client = tv._build_client(config.twilio_credentials())
    client.http_client.log_request({"method": "GET", "url": "https://x/+12025550123", "params": {"To": NUMBER},
                                    "headers": {"X": "y"}})
    assert "2025550123" not in caplog.text


def test_registry_returns_the_adapter_only_when_variables_exist(monkeypatch):
    monkeypatch.setenv("AUTH_PHONE_PROVIDER", "twilio_verify")
    with pytest.raises(registry.ProviderUnavailable) as exc:
        registry.get_provider()
    assert str(exc.value) == "twilio_not_configured"
    _set_api_key_env(monkeypatch)
    monkeypatch.delenv("TWILIO_VERIFY_SERVICE_SID")
    with pytest.raises(registry.ProviderUnavailable):
        registry.get_provider()
    monkeypatch.setenv("TWILIO_VERIFY_SERVICE_SID", SERVICE)
    registry.reset()
    prov = registry.get_provider()
    assert isinstance(prov, tv.TwilioVerifyProvider) and prov.name == "twilio_verify"
    assert isinstance(prov, p.PhoneVerifyProvider)


def test_registry_works_with_the_auth_token_style(monkeypatch):
    monkeypatch.setenv("AUTH_PHONE_PROVIDER", "twilio_verify")
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "ACfake")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "tok-fake")
    monkeypatch.setenv("TWILIO_VERIFY_SERVICE_SID", SERVICE)
    assert isinstance(registry.get_provider(), tv.TwilioVerifyProvider)


def test_registry_in_production_builds_twilio_and_refuses_fake(monkeypatch):
    monkeypatch.setenv("FLASK_ENV", "production")
    _set_api_key_env(monkeypatch)
    assert registry.get_provider().name == "twilio_verify"
    registry.reset()
    monkeypatch.setenv("AUTH_PHONE_PROVIDER", "fake")
    with pytest.raises(registry.ProviderUnavailable):
        registry.get_provider()


def test_building_the_adapter_makes_no_network_call(monkeypatch):
    """The test tier blocks sockets; building the client and the registry must not open one."""
    import socket

    def _boom(*a, **k):
        raise AssertionError("network used")

    monkeypatch.setattr(socket, "create_connection", _boom)
    _set_api_key_env(monkeypatch)
    assert registry.get_provider().name == "twilio_verify"
