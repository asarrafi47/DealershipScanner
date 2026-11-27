"""Review R5: the production boot check for phone sign-in never crashes the site on a soft problem.

Hard refusal (RuntimeError at boot) only for the fake provider in production and for a missing or short
PHONE_HASH_PEPPER. Every other problem logs ONE critical line (names, never values) and turns phone
sign-in off for the process: ``config.boot_disabled_reason()`` is set and ``config.enabled()`` is False.
With the flag off the check does nothing at all. Offline: no database, no network.
"""

from __future__ import annotations

import logging
import sys

import pytest

import backend.auth.phone as phone_pkg
from backend.auth.phone import config, legal, restore_guard
from backend.utils.production_security import assert_production_security_config

SECRETS = ("keysecret-test", "site-x", "secret-x", "p" * 40, "ACtest", "SKtest", "VAtest")
FULL = {
    "FLASK_ENV": "production",
    "AUTH_PHONE_LOGIN_ENABLED": "1",
    "AUTH_PHONE_PROVIDER": "twilio_verify",
    "PHONE_HASH_PEPPER": "p" * 40,
    "LEGAL_PAGES_APPROVED_VERSION": config.CONSENT_TEXT_VERSION,  # test value only; nothing is approved in the repo
    "TURNSTILE_SITE_KEY": "site-x",
    "SUPPORT_CONTACT": "help@example.com",
    "TURNSTILE_SECRET_KEY": "secret-x",
    "TWILIO_ACCOUNT_SID": "ACtest",
    "TWILIO_API_KEY_SID": "SKtest",
    "TWILIO_API_KEY_SECRET": "keysecret-test",
    "TWILIO_VERIFY_SERVICE_SID": "VAtest",
}
OTHER = (
    "AUTH_PHONE_ALLOWED_NUMBERS", "TWILIO_AUTH_TOKEN", "TRUST_PROXY_HEADERS", "TRUSTED_PROXY_HOPS",
    "TRUSTED_PROXY_CIDRS", "DEV_CONSOLE", "DEV_CONSOLE_SECRET", "ENV", "RAILWAY_ENVIRONMENT_NAME",
    "RAILWAY_ENVIRONMENT", "RAILWAY_PROJECT_ID", "SMS_ALLOWED_COUNTRIES",
)


class Calls:
    def __init__(self) -> None:
        self.legal = 0
        self.guard = 0


@pytest.fixture
def boot(monkeypatch):
    """Production with the unrelated boot checks stubbed and every phone value good."""
    import backend.db.inventory_pg as inventory_pg
    import backend.utils.production_security as ps

    for name in tuple(FULL) + OTHER:
        monkeypatch.delenv(name, raising=False)
    for k, v in FULL.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setattr(ps, "assert_credential_db_encryption_config", lambda: None)
    monkeypatch.setattr(inventory_pg, "assert_inventory_backend_configured", lambda: None)
    monkeypatch.setattr(config, "_boot_disabled", None)
    config.reset_warnings()
    calls = Calls()

    def ident_max():
        calls.guard += 1
        return 0

    def no_missing(*a, **k):
        calls.legal += 1
        return []

    monkeypatch.setattr(restore_guard, "max_identity_user_id", ident_max)
    monkeypatch.setattr(restore_guard, "max_users_id", lambda: 0)
    monkeypatch.setattr(legal, "missing_legal_pages", no_missing)
    monkeypatch.calls = calls
    return monkeypatch


def _set(mp, **values):
    for k, v in values.items():
        if v is None:
            mp.delenv(k, raising=False)
        else:
            mp.setenv(k, v)


def _soft(caplog) -> str:
    """Boot must succeed, with phone sign-in off, after exactly one critical line without values."""
    with caplog.at_level(logging.DEBUG, logger="auth_phone"):
        assert_production_security_config()
    reason = config.boot_disabled_reason()
    assert reason
    assert config.enabled() is False
    critical = [r.getMessage() for r in caplog.records if r.levelno >= logging.CRITICAL]
    assert len(critical) == 1 and "event=phone_login_boot_disabled" in critical[0]
    for secret in SECRETS:
        assert secret not in critical[0]
    return reason


# ---------------------------------------------------------------- hard refusals


def test_the_fake_provider_in_production_still_refuses_the_start(boot):
    _set(boot, AUTH_PHONE_PROVIDER="fake")
    with pytest.raises(RuntimeError, match="fake provider"):
        assert_production_security_config()


def test_a_short_pepper_still_refuses_the_start(boot):
    _set(boot, PHONE_HASH_PEPPER="p" * (config.PEPPER_MIN_LENGTH - 1))
    with pytest.raises(RuntimeError, match="PHONE_HASH_PEPPER") as exc:
        assert_production_security_config()
    assert "p" * (config.PEPPER_MIN_LENGTH - 1) not in str(exc.value)


def test_a_missing_pepper_still_refuses_the_start(boot):
    _set(boot, PHONE_HASH_PEPPER=None)
    with pytest.raises(RuntimeError, match="PHONE_HASH_PEPPER"):
        assert_production_security_config()


def test_a_hard_refusal_names_only_the_hard_problems(boot):
    _set(boot, AUTH_PHONE_PROVIDER="fake", TURNSTILE_SECRET_KEY=None)
    with pytest.raises(RuntimeError) as exc:
        assert_production_security_config()
    assert "TURNSTILE" not in str(exc.value)


# ---------------------------------------------------------------- soft: phone off, site up


@pytest.mark.parametrize(
    "values,name",
    [
        ({"TWILIO_ACCOUNT_SID": None}, "TWILIO_ACCOUNT_SID"),
        ({"TWILIO_API_KEY_SECRET": None}, "TWILIO_API_KEY_SECRET"),
        ({"TWILIO_VERIFY_SERVICE_SID": None}, "TWILIO_VERIFY_SERVICE_SID"),
        ({"TURNSTILE_SECRET_KEY": None}, "TURNSTILE_SECRET_KEY"),
        ({"TURNSTILE_SITE_KEY": None}, "TURNSTILE_SITE_KEY"),
        ({"AUTH_PHONE_PROVIDER": "carrier-pigeon"}, "unknown provider"),
        ({"LEGAL_PAGES_APPROVED_VERSION": None}, "LEGAL_PAGES_APPROVED_VERSION"),
        ({"LEGAL_PAGES_APPROVED_VERSION": "999"}, "LEGAL_PAGES_APPROVED_VERSION"),
        ({"AUTH_PHONE_ALLOWED_NUMBERS": "+12025550142, notanumber"}, "AUTH_PHONE_ALLOWED_NUMBERS"),
        ({"TRUST_PROXY_HEADERS": "1", "TRUSTED_PROXY_HOPS": "2"}, "TRUSTED_PROXY_CIDRS"),
    ],
)
def test_a_configuration_problem_turns_phone_off_and_the_site_still_starts(boot, caplog, values, name):
    _set(boot, **values)
    reason = _soft(caplog)
    assert name in reason
    assert "notanumber" not in reason and "2025550142" not in reason and "999" not in reason


def test_several_problems_still_make_one_line(boot, caplog):
    _set(boot, TURNSTILE_SECRET_KEY=None, TWILIO_VERIFY_SERVICE_SID=None, LEGAL_PAGES_APPROVED_VERSION=None)
    reason = _soft(caplog)
    for name in ("TURNSTILE_SECRET_KEY", "TWILIO_VERIFY_SERVICE_SID", "LEGAL_PAGES_APPROVED_VERSION"):
        assert name in reason


def test_missing_legal_pages_turn_phone_off(boot, caplog):
    boot.setattr(legal, "missing_legal_pages", lambda *a, **k: ["template terms.html", "SUPPORT_CONTACT"])
    reason = _soft(caplog)
    assert "terms.html" in reason and "SUPPORT_CONTACT" in reason


def test_a_legal_check_that_raises_turns_phone_off(boot, caplog):
    def broken(*a, **k):
        raise OSError("disk")

    boot.setattr(legal, "missing_legal_pages", broken)
    assert "legal pages" in _soft(caplog)


def test_a_missing_legal_module_turns_phone_off(boot, caplog):
    boot.delattr(phone_pkg, "legal", raising=False)
    boot.setitem(sys.modules, "backend.auth.phone.legal", None)  # the import now fails
    assert "legal module could not be loaded" in _soft(caplog)


def test_a_check_that_raises_turns_phone_off(boot, caplog):
    def broken():
        raise ValueError("boom")

    boot.setattr(config, "allow_list_unparsable_count", broken)
    _set(boot, AUTH_PHONE_ALLOWED_NUMBERS="+12025550142")
    assert "AUTH_PHONE_ALLOWED_NUMBERS (the check failed" in _soft(caplog)


def test_unreadable_phone_tables_turn_phone_off(boot, caplog):
    def no_table():
        raise RuntimeError("relation auth_phone_identities does not exist")  # V031 not applied

    boot.setattr(restore_guard, "max_identity_user_id", no_table)
    reason = _soft(caplog)
    assert "could not be read" in reason and "auth_phone_identities does not exist" not in reason


def test_a_postgres_blip_turns_phone_off(boot, caplog):
    def blip():
        raise ConnectionError("server closed the connection")

    boot.setattr(restore_guard, "max_identity_user_id", blip)
    assert "ConnectionError" in _soft(caplog)


def test_a_failed_restore_guard_turns_phone_off(boot, caplog):
    boot.setattr(restore_guard, "max_identity_user_id", lambda: 9)
    boot.setattr(restore_guard, "max_users_id", lambda: 3)
    assert "restore guard" in _soft(caplog)


def test_the_restore_guard_is_skipped_when_the_configuration_already_turned_phone_off(boot, caplog):
    _set(boot, TURNSTILE_SITE_KEY=None)
    _soft(caplog)  # exactly one critical line
    assert boot.calls.guard == 0


def test_a_soft_disable_leaves_the_other_refusals_unchanged(boot):
    # Admin, dealer and password sign-in are unaffected: the unrelated boot refusals still stop the start.
    _set(boot, TURNSTILE_SITE_KEY=None, DEV_CONSOLE="1")
    with pytest.raises(RuntimeError, match="DEV_CONSOLE_SECRET"):
        assert_production_security_config()
    assert config.boot_disabled_reason() is not None


def test_the_phone_service_reads_the_soft_disable(boot, caplog):
    from backend.auth.phone.service import ServiceConfig

    _set(boot, TURNSTILE_SITE_KEY=None)
    _soft(caplog)
    assert ServiceConfig.from_env().enabled is False
    assert config.flag_set() is True  # the variable itself is untouched


# ---------------------------------------------------------------- all good / flag off


def test_all_good_enables_phone_sign_in(boot, caplog):
    with caplog.at_level(logging.DEBUG, logger="auth_phone"):
        assert_production_security_config()
    assert config.boot_disabled_reason() is None and config.enabled() is True
    assert not [r for r in caplog.records if r.levelno >= logging.CRITICAL]
    assert boot.calls.legal == 1 and boot.calls.guard == 1


def test_a_second_boot_check_after_a_fix_clears_the_disable(boot, caplog):
    _set(boot, TURNSTILE_SITE_KEY=None)
    _soft(caplog)
    _set(boot, TURNSTILE_SITE_KEY="site-x")
    assert_production_security_config()
    assert config.enabled() is True


@pytest.mark.parametrize("bad", [{}, {"AUTH_PHONE_PROVIDER": "fake"}, {"PHONE_HASH_PEPPER": None},
                                 {"TURNSTILE_SITE_KEY": None}])
def test_flag_off_does_nothing_at_all(boot, caplog, bad):
    _set(boot, AUTH_PHONE_LOGIN_ENABLED=None, **bad)
    with caplog.at_level(logging.DEBUG, logger="auth_phone"):
        assert_production_security_config()
    assert config.boot_disabled_reason() is None and config.enabled() is False
    assert [r for r in caplog.records if r.name == "auth_phone"] == []
    assert (boot.calls.legal, boot.calls.guard) == (0, 0)
    assert config.missing_for_production() == []


def test_outside_production_nothing_is_checked(boot, caplog):
    _set(boot, FLASK_ENV="development", TURNSTILE_SITE_KEY=None)
    with caplog.at_level(logging.DEBUG, logger="auth_phone"):
        config.assert_phone_login_config_for_production()
        restore_guard.assert_identity_ids_within_users()
    assert config.boot_disabled_reason() is None and config.enabled() is True
    assert (boot.calls.legal, boot.calls.guard) == (0, 0)
