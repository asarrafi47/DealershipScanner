"""Draft /terms, /privacy and /sms-consent pages (card S14)."""

from __future__ import annotations

import re

import pytest

from backend.auth.phone import config as phone_config
from backend.auth.phone import legal

PAGES = ("/terms", "/privacy", "/sms-consent")
DRAFT = "This page is a draft pending the owner's approval."


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("LEGAL_PAGES_APPROVED_VERSION", "SUPPORT_CONTACT", "AUTH_PHONE_LOGIN_ENABLED"):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def client():
    from backend.main import app

    with app.test_client() as c:
        yield c


def _html(client, path: str) -> str:
    rv = client.get(path)
    assert rv.status_code == 200, path
    return rv.get_data(as_text=True)


def _text(html: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", html))


@pytest.mark.parametrize("path", PAGES)
@pytest.mark.parametrize("flag", ["", "1"])
def test_pages_are_public_with_flag_on_or_off(client, monkeypatch, path: str, flag: str) -> None:
    monkeypatch.setenv("AUTH_PHONE_LOGIN_ENABLED", flag)
    html = _html(client, path)
    assert "Version %s." % phone_config.CONSENT_TEXT_VERSION in html
    assert "Last updated %s." % legal.LAST_UPDATED in html


@pytest.mark.parametrize("path", PAGES)
def test_draft_sentence_until_approved(client, monkeypatch, path: str) -> None:
    html = _html(client, path)
    assert DRAFT in html
    # Plain sentence first in the main column: no box, banner or pill class.
    assert re.search(r'<main[^>]*>\s*<p id="legal-draft"><strong>DRAFT\.</strong>', html)
    monkeypatch.setenv("LEGAL_PAGES_APPROVED_VERSION", phone_config.CONSENT_TEXT_VERSION)
    assert DRAFT not in _html(client, path)


@pytest.mark.parametrize("path", PAGES)
def test_support_placeholder_or_contact(client, monkeypatch, path: str) -> None:
    assert legal.SUPPORT_PLACEHOLDER in _html(client, path)
    monkeypatch.setenv("SUPPORT_CONTACT", "help@example.com")
    html = _html(client, path)
    assert legal.SUPPORT_PLACEHOLDER not in html
    assert '<a href="mailto:help@example.com">help@example.com</a>' in html


def test_sms_consent_shows_exactly_the_consent_text(client) -> None:
    html = _html(client, "/sms-consent")
    m = re.search(r'<p id="consent-text">(.*?)</p>', html, re.S)
    assert m
    assert re.sub(r"<[^>]+>", "", m.group(1)) == legal.CONSENT_TEXT
    assert m.group(1) == str(legal.consent_html())
    assert 'id="help"' in html


def test_sms_consent_program_items(client) -> None:
    text = _text(_html(client, "/sms-consent"))
    for piece in ("Sarrafi Cars", "Reply STOP", "Reply HELP", "Carriers are not liable for delayed or undelivered messages",
                  "Message and data rates may apply", "You must be 13 or older to use Sarrafi Cars"):
        assert piece in text


def test_terms_content(client) -> None:
    html = _html(client, "/terms")
    assert "<h2>Selling a car on Sarrafi Cars</h2>" in html
    text = _text(html)
    for piece in ("You must be 13 or older to use Sarrafi Cars", "curbstoning", "title in hand",
                  "never handles money between buyer and seller", "licence", "within 30 days of a request",
                  "car assistant"):
        assert piece in text


def test_privacy_content(client) -> None:
    text = _text(_html(client, "/privacy"))
    for piece in (
        "We treat it as personal data",
        "Only to send you sign-in codes",
        "We do not sell or share your phone number or opt-in data with third parties for marketing",
        "Twilio", "line type", "Cloudflare", "Turnstile", "Railway", "Resend", "Anthropic",
        "7 days after they end", "30 days", "24 hours after", "35 days", "until your account is deleted",
        "Account data is deleted within 30 days of a request",
        "Rate-limit counters keyed by a one-way hash are kept up to 35 days",
        "Backups are overwritten on the provider's schedule",
        "ZIP code area", "13 or older",
    ):
        assert piece in text, piece


@pytest.mark.parametrize("path", PAGES)
def test_no_cards_boxes_or_pills(client, path: str) -> None:
    html = _html(client, path)
    main = re.search(r"<main.*?</main>", html, re.S).group(0)
    assert not re.search(r'class="[^"]*(card|pill|badge|banner|box)', main)


def test_no_approved_value_in_repository_text() -> None:
    src = (legal._TEMPLATES_DIR.parent.parent / "backend" / "auth" / "phone" / "legal.py").read_text()
    assert not re.search(r"LEGAL_PAGES_APPROVED_VERSION\s*=", src)


def _footer(client) -> str:
    html = _html(client, "/terms")
    return re.search(r'<div class="app-version-footer">.*?</div>', html, re.S).group(0)


def test_footer_links_terms_and_privacy_while_flag_off(client) -> None:
    from backend.routes.site_misc import _app_version

    assert _footer(client) == (
        '<div class="app-version-footer">v%s &middot; <a href="/terms">Terms</a> &middot; '
        '<a href="/privacy">Privacy</a></div>' % _app_version()
    )


def test_footer_links_when_phone_login_enabled(client, monkeypatch) -> None:
    from backend.main import app

    monkeypatch.setitem(app.jinja_env.globals, "phone_login_enabled", lambda: True)
    footer = _footer(client)
    for path in PAGES:
        assert 'href="%s"' % path in footer
    monkeypatch.setitem(app.jinja_env.globals, "phone_login_enabled", lambda: False)
    footer = _footer(client)
    assert 'href="/sms-consent"' not in footer
    assert 'href="/terms"' in footer and 'href="/privacy"' in footer
