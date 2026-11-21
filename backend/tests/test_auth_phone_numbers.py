"""S1: number handling (docs/auth_sms/TASKS.md). Test numbers use the fictional +1 202 555 01xx range."""

from __future__ import annotations

import logging

import pytest

from backend.auth.phone import numbers
from backend.auth.phone.numbers import PhoneNumberRefused

PEPPER_A = "a" * 40
PEPPER_B = "b" * 40
TEST_NUMBER = "+12025550123"


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    for name in ("PHONE_HASH_PEPPER", "SMS_ALLOWED_COUNTRIES", "FLASK_ENV", "ENV", "RAILWAY_PROJECT_ID",
                 "RAILWAY_ENVIRONMENT_NAME", "RAILWAY_ENVIRONMENT", "ALLOW_DEV_PHONE_PEPPER"):
        monkeypatch.delenv(name, raising=False)


def test_three_spellings_give_one_value():
    values = {numbers.normalize(s) for s in ("(202) 555-0123", "202-555-0123", "+12025550123", " 1 202 555 0123 ")}
    assert values == {TEST_NUMBER}


def test_uk_number_is_refused_by_default():
    with pytest.raises(PhoneNumberRefused) as exc:
        numbers.normalize("+44 20 7183 8750")
    assert exc.value.reason == numbers.COUNTRY_NOT_ALLOWED


@pytest.mark.parametrize("raw", ["call me", "1-800-FLOWERS", "202-555-0123 ext 5", "202-555-0123x5", "abc"])
def test_letters_are_refused(raw):
    with pytest.raises(PhoneNumberRefused) as exc:
        numbers.normalize(raw)
    assert exc.value.reason == numbers.INVALID


@pytest.mark.parametrize("raw", [None, "", "   ", 12025550123, b"+12025550123"])
def test_empty_and_non_text_are_refused(raw):
    with pytest.raises(PhoneNumberRefused) as exc:
        numbers.normalize(raw)
    assert exc.value.reason == numbers.EMPTY


@pytest.mark.parametrize("raw", ["123", "+1 202 555", "000-000-0000", "1" * 41, "+12025550123;ext=4"])
def test_invalid_numbers_are_refused(raw):
    with pytest.raises(PhoneNumberRefused):
        numbers.normalize(raw)


def test_premium_rate_is_refused():
    with pytest.raises(PhoneNumberRefused) as exc:
        numbers.normalize("+1 900 555 1234")
    assert exc.value.reason == numbers.PREMIUM


def test_shared_cost_is_refused():
    # UK shared-cost 0845 numbers: allowed country, refused type.
    with pytest.raises(PhoneNumberRefused) as exc:
        numbers.normalize("+44 845 46 47", allowed_countries={"GB"})
    assert exc.value.reason == numbers.PREMIUM


def test_country_decided_by_library_not_prefix():
    # +1 416 is Canada: same prefix as the US but a different country.
    with pytest.raises(PhoneNumberRefused) as exc:
        numbers.normalize("+1 416 555 0123")
    assert exc.value.reason == numbers.COUNTRY_NOT_ALLOWED
    assert numbers.normalize("+1 416 555 0123", allowed_countries={"US", "CA"}) == "+14165550123"
    assert numbers.country_of("+14165550123") == "CA"
    assert numbers.country_of(TEST_NUMBER) == "US"


def test_allowed_countries_follow_the_setting(monkeypatch):
    monkeypatch.setenv("SMS_ALLOWED_COUNTRIES", "US, gb")
    assert numbers.normalize("+44 20 7183 8750") == "+442071838750"
    monkeypatch.setenv("SMS_ALLOWED_COUNTRIES", "GB")
    with pytest.raises(PhoneNumberRefused):
        numbers.normalize("202-555-0123")


def test_try_normalize_returns_none_instead_of_raising():
    assert numbers.try_normalize("nope") is None
    assert numbers.try_normalize("202-555-0123") == TEST_NUMBER


def test_mask_shows_two_digits_only():
    assert numbers.mask(TEST_NUMBER) == "ending 23"
    assert numbers.mask("(202) 555-0199") == "ending 99"
    assert numbers.mask("5") == "ending **"
    assert numbers.mask(None) == "ending **"
    assert "2025550" not in numbers.mask(TEST_NUMBER)


def test_phone_hash_is_stable_for_one_pepper_and_differs_for_another(monkeypatch):
    monkeypatch.setenv("PHONE_HASH_PEPPER", PEPPER_A)
    first = numbers.hash_phone(TEST_NUMBER)
    assert first == numbers.hash_phone(TEST_NUMBER)
    assert len(first) == 64 and TEST_NUMBER[1:] not in first
    assert numbers.hash_phone("+12025550124") != first
    monkeypatch.setenv("PHONE_HASH_PEPPER", PEPPER_B)
    assert numbers.hash_phone(TEST_NUMBER) != first


def test_phone_and_ip_hashes_do_not_collide_across_domains(monkeypatch):
    monkeypatch.setenv("PHONE_HASH_PEPPER", PEPPER_A)
    assert numbers.hash_phone("203.0.113.9") != numbers.hash_ip("203.0.113.9")


def test_ip_hash_counts_ipv6_by_64(monkeypatch):
    monkeypatch.setenv("PHONE_HASH_PEPPER", PEPPER_A)
    a = numbers.hash_ip("2001:db8:1:2:aaaa:bbbb:cccc:dddd")
    b = numbers.hash_ip("2001:db8:1:2::1")
    c = numbers.hash_ip("2001:db8:1:3::1")
    assert a == b != c
    assert numbers.hash_ip("::ffff:203.0.113.9") == numbers.hash_ip("203.0.113.9")
    assert numbers.hash_ip("203.0.113.9") != numbers.hash_ip("203.0.113.10")
    assert numbers.hash_ip(None) == numbers.hash_ip("")


def test_hashing_without_pepper_fails_closed_everywhere_unless_the_dev_opt_in_is_set(monkeypatch):
    with pytest.raises(numbers.PepperMissing):
        numbers.hash_phone(TEST_NUMBER)  # development, no opt-in: a missing variable is never a default
    monkeypatch.setenv("ALLOW_DEV_PHONE_PEPPER", "1")
    assert numbers.hash_phone(TEST_NUMBER)  # opted in on purpose
    monkeypatch.setenv("FLASK_ENV", "production")
    with pytest.raises(numbers.PepperMissing):
        numbers.hash_phone(TEST_NUMBER)  # production never uses it, opt-in or not
    with pytest.raises(numbers.PepperMissing):
        numbers.hash_ip("203.0.113.9")


def test_a_pepper_shorter_than_32_characters_is_refused_in_every_environment(monkeypatch):
    monkeypatch.setenv("PHONE_HASH_PEPPER", "x" * 31)
    with pytest.raises(numbers.PepperMissing):
        numbers.hash_phone(TEST_NUMBER)
    monkeypatch.setenv("PHONE_HASH_PEPPER", "x" * 32)
    assert numbers.hash_phone(TEST_NUMBER)


def test_the_dev_pepper_meets_the_minimum_length():
    assert len(numbers.DEV_PEPPER) >= numbers.PEPPER_MIN_LENGTH == 32


def test_pepper_errors_name_the_variable_and_never_a_value(monkeypatch):
    monkeypatch.setenv("PHONE_HASH_PEPPER", "short-secret-value")
    with pytest.raises(numbers.PepperMissing) as exc:
        numbers.pepper_bytes()
    assert "PHONE_HASH_PEPPER" in str(exc.value) and "short-secret-value" not in str(exc.value)


def test_each_hash_domain_has_its_own_key_derived_from_the_pepper(monkeypatch):
    monkeypatch.setenv("PHONE_HASH_PEPPER", PEPPER_A)
    same = "x"
    assert len({numbers.hash_phone(same), numbers.hash_ip(same), numbers.hash_device(same)}) == 3
    import hashlib
    import hmac

    key = hmac.new(PEPPER_A.encode(), b"phone", hashlib.sha256).digest()
    assert numbers.hash_phone(TEST_NUMBER) == hmac.new(key, b"phone:" + TEST_NUMBER.encode(), hashlib.sha256).hexdigest()


def test_mask_counts_ascii_digits_only():
    assert numbers.mask("+1 (202) 555-0123") == "ending 23"
    assert numbers.mask("\u0661\u0662\u0663\u0664") == "ending **"  # Arabic-Indic digits are not digits here
    assert numbers.mask("\u0663") == "ending **"
    assert numbers.mask("55\u0663") == "ending 55"
    assert numbers.mask("\u00b2\u00b3 5 7") == "ending 57"  # superscripts are ignored too


def test_canonical_ip_is_one_form_and_accepts_its_own_output(monkeypatch):
    from backend.utils.client_ip import canonical_ip

    monkeypatch.setenv("PHONE_HASH_PEPPER", PEPPER_A)
    assert canonical_ip("2001:db8:1:2:aaaa::1") == "2001:db8:1:2::"
    assert canonical_ip(canonical_ip("2001:db8:1:2:aaaa::1")) == "2001:db8:1:2::"  # no re-masking drift
    assert numbers.canonical_ip is canonical_ip  # numbers and the client address share one function
    assert numbers.hash_ip("2001:db8:1:2:aaaa::1") == numbers.hash_ip(canonical_ip("2001:db8:1:2:aaaa::1"))


def test_short_hash_is_eight_characters():
    assert numbers.short_hash("0123456789abcdef") == "01234567"


def test_no_function_logs_its_input(monkeypatch, caplog):
    monkeypatch.setenv("PHONE_HASH_PEPPER", PEPPER_A)
    caplog.set_level(logging.DEBUG)
    for raw in ("(202) 555-0123", "+44 20 7183 8750", "letters", "", None, "+1 900 555 1234"):
        numbers.try_normalize(raw)
        numbers.mask(raw)
    numbers.hash_phone(TEST_NUMBER)
    numbers.hash_ip("203.0.113.9")
    try:
        numbers.normalize("5550123 secret-looking-text")
    except PhoneNumberRefused as exc:
        assert "secret" not in str(exc) and "5550123" not in str(exc)
    assert caplog.records == []
    assert "555" not in caplog.text


def test_refusal_text_is_only_a_reason_code():
    with pytest.raises(PhoneNumberRefused) as exc:
        numbers.normalize("+44 20 7183 8750")
    assert str(exc.value) == numbers.COUNTRY_NOT_ALLOWED
    assert "7183" not in repr(exc.value)
