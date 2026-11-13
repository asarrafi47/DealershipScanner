"""S1: phone settings and the production boot check."""

from __future__ import annotations

import pytest

from backend.auth.phone import config
from backend.utils.production_security import assert_production_security_config

_NAMES = (
    "AUTH_PHONE_LOGIN_ENABLED", "AUTH_PHONE_PROVIDER", "AUTH_PHONE_ALLOWED_NUMBERS", "AUTH_PHONE_CODE_TTL_S",
    "AUTH_PHONE_MAX_WRONG_TRIES", "AUTH_PHONE_LOCK_MINUTES", "AUTH_PHONE_RESEND_WAIT_S", "SMS_DAILY_SEND_CAP",
    "SMS_DAILY_SPEND_CAP_CENTS", "SMS_ALERT_SEND_THRESHOLD", "SMS_EST_COST_CENTS", "SMS_ALLOWED_COUNTRIES",
    "PHONE_HASH_PEPPER", "TURNSTILE_SITE_KEY", "TURNSTILE_SECRET_KEY", "TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN",
    "TWILIO_API_KEY_SID", "TWILIO_API_KEY_SECRET", "TWILIO_VERIFY_SERVICE_SID", "FLASK_ENV", "ENV",
    "RAILWAY_PROJECT_ID", "RAILWAY_ENVIRONMENT_NAME", "RAILWAY_ENVIRONMENT", "LEGAL_PAGES_APPROVED_VERSION",
    "ALLOW_DEV_PHONE_PEPPER", "TWILIO_WEBHOOK_ENABLED",
)


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    for name in _NAMES:
        monkeypatch.delenv(name, raising=False)
    config.reset_warnings()


def _full_production(mp, **override):
    values = {
        "FLASK_ENV": "production",
        "AUTH_PHONE_LOGIN_ENABLED": "1",
        "AUTH_PHONE_PROVIDER": "twilio_verify",
        "PHONE_HASH_PEPPER": "p" * 40,
        "LEGAL_PAGES_APPROVED_VERSION": config.CONSENT_TEXT_VERSION,
        "TURNSTILE_SITE_KEY": "site-x",
        "TURNSTILE_SECRET_KEY": "secret-x",
        "TWILIO_ACCOUNT_SID": "ACtest",
        "TWILIO_API_KEY_SID": "SKtest",
        "TWILIO_API_KEY_SECRET": "keysecret-test",
        "TWILIO_VERIFY_SERVICE_SID": "VAtest",
    }
    values.update(override)
    for k, v in values.items():
        if v is None:
            mp.delenv(k, raising=False)
        else:
            mp.setenv(k, v)


@pytest.fixture
def prod(monkeypatch):
    """Production with the unrelated boot checks stubbed, so only the phone check is exercised."""
    import backend.db.inventory_pg as inventory_pg
    import backend.utils.production_security as ps

    monkeypatch.setattr(ps, "assert_credential_db_encryption_config", lambda: None)
    monkeypatch.setattr(inventory_pg, "assert_inventory_backend_configured", lambda: None)
    return monkeypatch


def test_enabled_is_false_when_unset():
    assert config.enabled() is False


@pytest.mark.parametrize("value,expected", [("1", True), ("true", True), ("ON", True), ("0", False), ("", False), ("no", False)])
def test_enabled_reads_the_flag(monkeypatch, value, expected):
    monkeypatch.setenv("AUTH_PHONE_LOGIN_ENABLED", value)
    assert config.enabled() is expected


def test_defaults_match_the_design():
    assert config.daily_send_cap() == 300
    assert config.daily_spend_cap_cents() == 1500
    assert config.alert_send_threshold() == 150
    assert config.est_cost_cents() == 6
    assert config.code_ttl_s() == 300
    assert config.max_wrong_tries() == 5
    assert config.lock_minutes() == 15
    assert config.resend_wait_s() == 60
    assert config.allowed_countries() == frozenset({"US"})
    assert config.allowed_numbers() == frozenset()
    assert config.provider_name() == "twilio_verify"  # the fake is never the default


def test_values_are_read_and_bad_values_fall_back(monkeypatch):
    monkeypatch.setenv("SMS_DAILY_SEND_CAP", "50")
    monkeypatch.setenv("AUTH_PHONE_CODE_TTL_S", "not-a-number")
    monkeypatch.setenv("AUTH_PHONE_LOCK_MINUTES", "-3")
    monkeypatch.setenv("SMS_ALLOWED_COUNTRIES", " us , ca ")
    monkeypatch.setenv("AUTH_PHONE_ALLOWED_NUMBERS", "+12025550123, +12025550124")
    assert config.daily_send_cap() == 50
    assert config.code_ttl_s() == 300
    assert config.lock_minutes() == 15
    assert config.allowed_countries() == frozenset({"US", "CA"})
    assert config.allowed_numbers() == frozenset({"+12025550123", "+12025550124"})


def test_provider_default_is_twilio_in_production_and_outside_it(monkeypatch):
    assert config.provider_name() == "twilio_verify"  # development: unset is NOT the fake
    monkeypatch.setenv("FLASK_ENV", "production")
    assert config.provider_name() == "twilio_verify"
    monkeypatch.setenv("AUTH_PHONE_PROVIDER", "Fake")
    assert config.provider_name() == "fake"  # only on purpose


def test_twilio_credentials_api_key_style(monkeypatch):
    assert config.twilio_credentials() is None
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "ACtest")
    monkeypatch.setenv("TWILIO_API_KEY_SID", "SKtest")
    assert config.twilio_credentials() is None  # secret missing
    monkeypatch.setenv("TWILIO_API_KEY_SECRET", "keysecret-test")
    creds = config.twilio_credentials()
    assert creds is not None and creds.style == "api_key"
    assert (creds.account_sid, creds.api_key_sid, creds.secret) == ("ACtest", "SKtest", "keysecret-test")
    assert "keysecret-test" not in repr(creds) and "ACtest" not in repr(creds)


def test_twilio_credentials_auth_token_style(monkeypatch):
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "ACtest")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "tok-test")
    creds = config.twilio_credentials()
    assert creds is not None and creds.style == "auth_token" and creds.secret == "tok-test"
    monkeypatch.setenv("TWILIO_API_KEY_SID", "SKtest")
    monkeypatch.setenv("TWILIO_API_KEY_SECRET", "keysecret-test")
    assert config.twilio_credentials().style == "api_key"  # the key wins when both exist


def test_boot_check_does_nothing_when_flag_is_off(prod):
    prod.setenv("FLASK_ENV", "production")
    assert_production_security_config()


def test_boot_check_does_nothing_outside_production(monkeypatch):
    monkeypatch.setenv("AUTH_PHONE_LOGIN_ENABLED", "1")
    assert_production_security_config()
    config.assert_phone_login_config_for_production()


def test_boot_check_accepts_a_complete_production_setup(prod):
    _full_production(prod)
    assert_production_security_config()
    assert config.missing_for_production() == []


def test_boot_check_refuses_flag_on_with_the_fake_provider(prod):
    _full_production(prod, AUTH_PHONE_PROVIDER="fake")
    with pytest.raises(RuntimeError, match="fake provider"):
        assert_production_security_config()


@pytest.mark.parametrize(
    "name,override",
    [
        ("PHONE_HASH_PEPPER", {"PHONE_HASH_PEPPER": None}),
        ("PHONE_HASH_PEPPER", {"PHONE_HASH_PEPPER": "short"}),
        ("TURNSTILE_SECRET_KEY", {"TURNSTILE_SECRET_KEY": None}),
        ("TURNSTILE_SITE_KEY", {"TURNSTILE_SITE_KEY": None}),
        ("TWILIO_VERIFY_SERVICE_SID", {"TWILIO_VERIFY_SERVICE_SID": None}),
        ("TWILIO_ACCOUNT_SID", {"TWILIO_ACCOUNT_SID": None}),
        ("TWILIO_API_KEY_SECRET", {"TWILIO_API_KEY_SECRET": None}),
    ],
)
def test_boot_check_refuses_a_missing_value_and_names_it(prod, name, override):
    _full_production(prod, **override)
    with pytest.raises(RuntimeError) as exc:
        assert_production_security_config()
    assert name in str(exc.value)


def test_boot_check_message_never_holds_a_value(prod):
    _full_production(prod, TURNSTILE_SECRET_KEY=None)
    with pytest.raises(RuntimeError) as exc:
        assert_production_security_config()
    for secret in ("keysecret-test", "site-x", "p" * 40):
        assert secret not in str(exc.value)


def test_boot_check_refuses_an_unknown_provider(prod):
    _full_production(prod, AUTH_PHONE_PROVIDER="carrier-pigeon")
    with pytest.raises(RuntimeError, match="unknown provider"):
        assert_production_security_config()


def test_boot_check_accepts_the_auth_token_style(prod):
    _full_production(prod, TWILIO_API_KEY_SID=None, TWILIO_API_KEY_SECRET=None, TWILIO_AUTH_TOKEN="tok-test")
    assert_production_security_config()


def test_the_auth_token_is_never_required_at_boot(prod):
    _full_production(prod, TWILIO_AUTH_TOKEN=None)
    assert_production_security_config()  # no error: callbacks are optional
    assert config.webhook_active() is False
    assert config.twilio_webhook_token() == ""


# ---------------------------------------------------------------- wave 1 review follow-ups


def test_the_fake_provider_is_opt_in_the_registry_refuses_when_unset(monkeypatch):
    from backend.auth.phone import registry

    registry.reset()
    with pytest.raises(registry.ProviderUnavailable):
        registry.get_provider()  # unset in development: Twilio is not configured, nothing falls back to the fake
    monkeypatch.setenv("AUTH_PHONE_PROVIDER", "fake")
    registry.reset()
    assert registry.get_provider().name == "fake"
    registry.reset()


def test_boot_check_requires_the_legal_approval_version(prod):
    _full_production(prod, LEGAL_PAGES_APPROVED_VERSION=None)
    with pytest.raises(RuntimeError, match="LEGAL_PAGES_APPROVED_VERSION"):
        assert_production_security_config()
    _full_production(prod, LEGAL_PAGES_APPROVED_VERSION="999")
    with pytest.raises(RuntimeError, match="LEGAL_PAGES_APPROVED_VERSION"):
        assert_production_security_config()
    _full_production(prod)
    assert_production_security_config()


def test_the_legal_check_names_the_variable_and_never_a_value(prod):
    _full_production(prod, LEGAL_PAGES_APPROVED_VERSION="secret-looking-9")
    with pytest.raises(RuntimeError) as exc:
        assert_production_security_config()
    assert "secret-looking-9" not in str(exc.value)


def test_a_malformed_send_cap_logs_one_warning_without_the_value_and_falls_back(monkeypatch, caplog):
    monkeypatch.setenv("SMS_DAILY_SEND_CAP", "3x1")
    with caplog.at_level("WARNING", logger="auth_phone"):
        assert [config.daily_send_cap() for _ in range(3)] == [300, 300, 300]
    lines = [r.getMessage() for r in caplog.records if "SMS_DAILY_SEND_CAP" in r.getMessage()]
    assert len(lines) == 1 and "3x1" not in lines[0]


@pytest.mark.parametrize("bad", ["USA", "U1", ",,", "U S", "\u00dcS", "US;CA"])
def test_a_malformed_country_list_logs_one_warning_and_falls_back_to_us(monkeypatch, caplog, bad):
    monkeypatch.setenv("SMS_ALLOWED_COUNTRIES", bad)
    with caplog.at_level("WARNING", logger="auth_phone"):
        assert config.allowed_countries() == frozenset({"US"})
        assert config.allowed_countries() == frozenset({"US"})
    lines = [r.getMessage() for r in caplog.records if "SMS_ALLOWED_COUNTRIES" in r.getMessage()]
    assert len(lines) == 1 and bad not in lines[0]


def test_a_blank_country_list_means_not_set_and_says_nothing(monkeypatch, caplog):
    monkeypatch.setenv("SMS_ALLOWED_COUNTRIES", "  ")
    with caplog.at_level("WARNING", logger="auth_phone"):
        assert config.allowed_countries() == frozenset({"US"})
    assert caplog.records == []


def test_a_non_ascii_digit_is_malformed_not_a_number(monkeypatch, caplog):
    monkeypatch.setenv("SMS_DAILY_SEND_CAP", "\u0663\u0660")  # Arabic-Indic 30
    with caplog.at_level("WARNING", logger="auth_phone"):
        assert config.daily_send_cap() == 300


def test_the_webhook_is_inert_unless_the_flag_and_the_token_are_both_set(monkeypatch):
    assert config.webhook_active() is False
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "tok-test")
    assert config.webhook_active() is False  # token alone
    monkeypatch.delenv("TWILIO_AUTH_TOKEN")
    monkeypatch.setenv("TWILIO_WEBHOOK_ENABLED", "1")
    assert config.webhook_active() is False  # flag alone
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "tok-test")
    assert config.webhook_active() is True


def test_dev_pepper_is_never_allowed_in_production(monkeypatch):
    monkeypatch.setenv("ALLOW_DEV_PHONE_PEPPER", "1")
    assert config.dev_pepper_allowed() is True
    monkeypatch.setenv("FLASK_ENV", "production")
    assert config.dev_pepper_allowed() is False


def test_phone_libraries_are_pinned_with_upper_bounds_and_the_lite_phone_build():
    from pathlib import Path

    lines = (Path(__file__).resolve().parents[2] / "requirements-web.txt").read_text(encoding="utf-8").splitlines()
    pins = {ln.split(">=")[0].strip(): ln for ln in lines if ln.strip() and not ln.startswith("#") and ">=" in ln}
    assert "phonenumbers" not in pins and "phonenumberslite" in pins  # same import name, no geocoder or carrier data
    for name in ("phonenumberslite", "twilio"):
        assert "<" in pins[name], f"{name} has no upper bound"
    # the code only needs parsing, validity, number type and region: nothing from the geocoder, carrier or timezone modules
    src = Path(__file__).resolve().parents[2].joinpath("backend/auth/phone/numbers.py").read_text(encoding="utf-8")
    for heavy in ("phonenumbers.geocoder", "phonenumbers.carrier", "phonenumbers.timezone"):
        assert heavy not in src
