"""S2: provider interface, fake provider and registry."""

from __future__ import annotations

import ast
import logging
from pathlib import Path

import pytest

from backend.auth.phone import fake_provider, provider as p, registry
from backend.auth.phone.fake_provider import FAKE_CODE, FakePhoneProvider, FakeProviderForbidden
from backend.auth.phone.lookup_cache import MemoryLookupCache

NUMBER = "+12025550123"
OTHER = "+12025550124"
PKG = Path(__file__).resolve().parents[1] / "auth" / "phone"


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    for name in ("FLASK_ENV", "ENV", "RAILWAY_PROJECT_ID", "RAILWAY_ENVIRONMENT_NAME", "RAILWAY_ENVIRONMENT",
                 "AUTH_PHONE_PROVIDER"):
        monkeypatch.delenv(name, raising=False)
    registry.reset()
    yield
    registry.reset()


def _started(fake: FakePhoneProvider, number: str = NUMBER) -> str:
    result = fake.start(number, locale="en")
    assert result.ok and result.provider_ref
    return result.provider_ref


def test_right_code_approves():
    fake = FakePhoneProvider()
    ref = _started(fake)
    result = fake.check(NUMBER, code=FAKE_CODE, provider_ref=ref)
    assert result == p.CheckResult(True, p.CHECK_APPROVED)


def test_wrong_code_does_not_approve():
    fake = FakePhoneProvider()
    ref = _started(fake)
    result = fake.check(NUMBER, code="123456", provider_ref=ref)
    assert result.approved is False and result.reason == p.CHECK_WRONG_CODE


def test_a_code_works_once():
    fake = FakePhoneProvider()
    ref = _started(fake)
    assert fake.check(NUMBER, code=FAKE_CODE, provider_ref=ref).approved
    assert fake.check(NUMBER, code=FAKE_CODE, provider_ref=ref).approved is False


def test_check_without_a_started_verification_is_expired_not_approved():
    fake = FakePhoneProvider()
    assert fake.check(NUMBER, code=FAKE_CODE, provider_ref=None).reason == p.CHECK_EXPIRED
    ref = _started(fake)
    assert fake.check(OTHER, code=FAKE_CODE, provider_ref=ref).approved is False  # ref belongs to another number


def test_check_without_ref_uses_the_latest_for_the_number():
    fake = FakePhoneProvider()
    _started(fake)
    assert fake.check(NUMBER, code=FAKE_CODE, provider_ref=None).approved


def test_outbox_shows_one_message_per_start():
    fake = FakePhoneProvider()
    assert fake.outbox == []
    ref = _started(fake)
    assert len(fake.outbox) == 1
    assert fake.outbox[0].to == NUMBER and fake.outbox[0].provider_ref == ref
    assert FAKE_CODE not in repr(fake.outbox[0])


def test_failure_returns_a_short_reason_and_sends_nothing():
    fake = FakePhoneProvider()
    fake.fail_next_start(p.START_PROVIDER_ERROR)
    result = fake.start(NUMBER, locale="en")
    assert (result.ok, result.reason, result.provider_ref) == (False, p.START_PROVIDER_ERROR, None)
    assert fake.outbox == []
    assert fake.start(NUMBER, locale="en").ok  # only the next one failed


def test_throttle_carries_retry_after():
    fake = FakePhoneProvider()
    fake.throttle_next_start(retry_after_s=42)
    result = fake.start(NUMBER, locale="en")
    assert (result.ok, result.reason, result.retry_after_s) == (False, p.START_THROTTLED, 42)
    assert fake.outbox == []


def test_check_failure_reason_is_returned():
    fake = FakePhoneProvider()
    ref = _started(fake)
    fake.fail_next_check(p.CHECK_NETWORK_ERROR)
    assert fake.check(NUMBER, code=FAKE_CODE, provider_ref=ref).reason == p.CHECK_NETWORK_ERROR
    assert fake.check(NUMBER, code=FAKE_CODE, provider_ref=ref).approved  # the code was not used up


def test_expired_verification_does_not_approve():
    fake = FakePhoneProvider()
    ref = _started(fake)
    fake.expire(ref)
    result = fake.check(NUMBER, code=FAKE_CODE, provider_ref=ref)
    assert (result.approved, result.reason) == (False, p.CHECK_EXPIRED)


def test_voip_and_landline_lines_are_reported_and_ordinary_numbers_pass():
    fake = FakePhoneProvider()
    assert fake.lookup(NUMBER) == p.LineInfo(True, "mobile", "US", p.LINE_OK)
    fake.mark_voip(NUMBER)
    fake.mark_landline(OTHER)
    voip = fake.lookup(NUMBER)
    assert (voip.ok, voip.reason, voip.line_type) == (False, p.LINE_VOIP, "nonFixedVoip")
    assert fake.lookup(OTHER).reason == p.LINE_LANDLINE
    fake.fail_next_lookup()
    assert fake.lookup("+12025550125").reason == p.LINE_LOOKUP_FAILED


def test_stall_waits_through_the_injected_sleeper():
    slept: list[float] = []
    fake = FakePhoneProvider(sleeper=slept.append, stall_s=2.5)
    fake.start(NUMBER, locale="en")
    fake.lookup(NUMBER)
    assert slept == [2.5, 2.5]


def test_fake_provider_refuses_to_load_in_production(monkeypatch):
    monkeypatch.setenv("FLASK_ENV", "production")
    with pytest.raises(FakeProviderForbidden):
        FakePhoneProvider()


def test_registry_returns_the_fake_outside_production_and_caches_it():
    first = registry.get_provider()
    assert isinstance(first, FakePhoneProvider)
    assert registry.get_provider() is first
    registry.reset()
    assert registry.get_provider() is not first


def test_registry_refuses_the_fake_in_production(monkeypatch):
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.setenv("AUTH_PHONE_PROVIDER", "fake")
    with pytest.raises(registry.ProviderUnavailable) as exc:
        registry.get_provider()
    assert str(exc.value) == "fake_provider_forbidden_in_production"


def test_registry_refuses_an_unknown_provider(monkeypatch):
    monkeypatch.setenv("AUTH_PHONE_PROVIDER", "carrier-pigeon")
    with pytest.raises(registry.ProviderUnavailable):
        registry.get_provider()


def test_fake_satisfies_the_interface():
    assert isinstance(FakePhoneProvider(), p.PhoneVerifyProvider)
    assert isinstance(MemoryLookupCache(), p.LookupCache)


@pytest.mark.parametrize(
    "line_type,allowed,reason",
    [
        ("mobile", True, p.LINE_OK), ("personal", True, p.LINE_OK), (None, False, p.LINE_UNKNOWN),
        ("unknown", False, p.LINE_UNKNOWN), ("fixedVoip", False, p.LINE_VOIP), ("nonFixedVoip", False, p.LINE_VOIP),
        ("landline", False, p.LINE_LANDLINE), ("tollFree", False, p.LINE_NOT_MOBILE),
        ("premium", False, p.LINE_NOT_MOBILE), ("somethingNew", False, p.LINE_NOT_MOBILE),
    ],
)
def test_line_type_classification(line_type, allowed, reason):
    assert p.classify_line_type(line_type) == (allowed, reason)


def test_every_reason_is_a_short_code_not_text():
    for group in (p.START_REASONS, p.CHECK_REASONS, p.LINE_REASONS):
        for reason in group:
            assert reason.replace("_", "").isalpha() and reason == reason.lower() and len(reason) <= 16


def test_memory_lookup_cache_expires():
    now = [1000.0]
    cache = MemoryLookupCache(clock=lambda: now[0])
    info = p.LineInfo(True, "mobile", "US", p.LINE_OK)
    assert cache.get("h") is None
    cache.put("h", info, 100)
    assert cache.get("h") == info
    now[0] = 1100.0
    assert cache.get("h") is None


def test_interface_modules_import_no_vendor_and_no_flask():
    """provider.py, fake_provider.py, lookup_cache.py and registry.py stay vendor- and Flask-free."""
    banned = {"flask", "twilio", "requests", "werkzeug", "urllib3", "httpx"}
    for name in ("provider.py", "fake_provider.py", "lookup_cache.py", "registry.py"):
        tree = ast.parse((PKG / name).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            mods = []
            if isinstance(node, ast.Import):
                mods = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
                mods = [node.module]
            for mod in mods:
                assert mod.split(".")[0] not in banned, (name, mod)


def test_fake_flow_writes_nothing_to_logs(caplog):
    caplog.set_level(logging.DEBUG)
    fake = FakePhoneProvider()
    ref = _started(fake)
    fake.fail_next_start()
    fake.start(NUMBER, locale="en")
    fake.check(NUMBER, code="123456", provider_ref=ref)
    fake.check(NUMBER, code=FAKE_CODE, provider_ref=ref)
    fake.lookup(NUMBER)
    assert caplog.records == []


def test_fake_module_constant_is_not_a_real_secret():
    assert fake_provider.FAKE_CODE == "000000"
