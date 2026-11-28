"""backend/auth/phone/legal.py (card S14, contract 4): consent text, approval, support contact, boot helper."""

from __future__ import annotations

import re

import pytest

from backend.auth.phone import config as phone_config
from backend.auth.phone import legal


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LEGAL_PAGES_APPROVED_VERSION", raising=False)
    monkeypatch.delenv("SUPPORT_CONTACT", raising=False)


def _text(html: str) -> str:
    return re.sub(r"<[^>]+>", "", html)


def test_consent_text_carries_every_required_item() -> None:
    t = legal.CONSENT_TEXT
    assert t.startswith("I agree to receive one-time login codes by text message from Sarrafi Cars")
    for piece in (
        "Message and data rates may apply.",
        "Message frequency: only when I request a code.",
        "Reply STOP to opt out or HELP for help.",
        "Carriers are not liable for delayed or undelivered messages.",
        "See our Terms and Privacy Policy.",
        "We do not sell or share your phone number or opt-in data with third parties for marketing.",
        "You must be 13 or older to use Sarrafi Cars.",
    ):
        assert piece in t


def test_consent_version_is_two() -> None:
    # The linked Terms and Privacy Policy were rewritten, so returning people are asked again.
    assert phone_config.CONSENT_TEXT_VERSION == "2"


def test_consent_html_same_words_with_real_links() -> None:
    html = str(legal.consent_html())
    assert _text(html) == legal.CONSENT_TEXT
    assert '<a href="/terms">Terms</a>' in html
    assert '<a href="/privacy">Privacy Policy</a>' in html
    from backend.auth.phone import consent

    assert legal.CONSENT_TEXT == consent.CONSENT_TEXT  # one wording for the phone page and /sms-consent


@pytest.mark.parametrize(
    ("value", "href"),
    [
        ("help@example.com", "mailto:help@example.com"),
        ("https://example.com/help", "https://example.com/help"),
        ("Call the office", "/sms-consent#help"),
        ("javascript:alert(1)", "/sms-consent#help"),
    ],
)
def test_support_link_target(monkeypatch: pytest.MonkeyPatch, value: str, href: str) -> None:
    monkeypatch.setenv("SUPPORT_CONTACT", value)
    assert legal.support_contact() == value
    assert legal.support_href() == href


def test_support_contact_placeholder_when_unset() -> None:
    assert legal.support_contact() is None
    assert legal.support_contact_display() == legal.SUPPORT_PLACEHOLDER
    assert "@" not in legal.SUPPORT_PLACEHOLDER


def test_legal_approved_only_on_exact_version(monkeypatch: pytest.MonkeyPatch) -> None:
    assert legal.legal_approved() is False
    monkeypatch.setenv("LEGAL_PAGES_APPROVED_VERSION", "1")
    assert legal.legal_approved() is False
    monkeypatch.setenv("LEGAL_PAGES_APPROVED_VERSION", phone_config.CONSENT_TEXT_VERSION)
    assert legal.legal_approved() is True


def test_missing_legal_pages_without_app_checks_templates_only() -> None:
    # The support contact is the config check's job (SUPPORT_CONTACT in missing_for_production).
    assert legal.missing_legal_pages() == []


def test_missing_legal_pages_with_app(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.main import app

    monkeypatch.setenv("SUPPORT_CONTACT", "help@example.com")
    assert legal.missing_legal_pages(app) == []


def test_missing_legal_pages_names_missing_endpoints_and_templates(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    from flask import Flask

    monkeypatch.setenv("SUPPORT_CONTACT", "help@example.com")
    bare = Flask("legal_probe", template_folder=str(tmp_path))
    missing = legal.missing_legal_pages(bare)
    for template, endpoint in legal.LEGAL_PAGES:
        assert "template " + template in missing
        assert "endpoint " + endpoint in missing


def test_missing_legal_pages_never_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_a, **_k):
        raise RuntimeError("x")

    monkeypatch.setattr(legal, "_template_exists", boom)
    assert legal.missing_legal_pages() == ["legal pages (check failed)"]
    monkeypatch.undo()
    got = legal.missing_legal_pages(object())  # no jinja_env, no view_functions: still a list
    assert "endpoint terms" in got
