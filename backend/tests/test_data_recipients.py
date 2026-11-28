"""The Privacy Policy names every outside party the web app sends visitor data to.

The list lives in ``backend/web/data_recipients.py``. These tests scan the web app's request-path code and
browser pages for outbound hosts and SDK imports and fail when one is not on that list (or on
``NOT_USER_DATA`` with a reason), when a listed host or SDK no longer appears in the code, or when the
rendered policy leaves a recipient out. They also check the legal pages render for a visitor who is not
signed in and that the sign-up pages and the footer link them.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from backend.web import data_recipients as dr

ROOT = Path(__file__).resolve().parents[2]

# The web app's request path and what the browser loads. Scanner, discovery, enrichment jobs and offline
# scripts fetch dealer and catalog sites only; the files below are the ones a visitor's request reaches.
SCAN_DIRS = (
    "backend/auth",
    "backend/billing",
    "backend/chat_budget",
    "backend/dealer",
    "backend/intelligence",
    "backend/listings",
    "backend/llm",
    "backend/mobile",
    "backend/routes",
    "backend/utils",
    "backend/vision",
    "backend/web",
    "frontend/static",
    "frontend/templates",
)
SCAN_FILES = (
    "backend/main.py",
    "backend/discovery/google_places.py",
    "backend/enrichment/nhtsa_vpic.py",
    "backend/enrichment/recalls_lookup.py",
    "backend/enrichment/vehicle_history_intelligence.py",
)
SKIP_PARTS = ("vendor", "__pycache__", "tests")
SUFFIXES = (".py", ".js", ".html")

# Modules that send data to an outside service when imported and used.
OUTBOUND_MODULES = (
    "anthropic", "openai", "stripe", "resend", "twilio", "smtplib", "boto3", "sentry_sdk",
    "google.cloud", "googleapiclient", "mailgun", "sendgrid", "posthog", "segment", "mixpanel",
)

_HOST_RE = re.compile(r"https?://((?:\{[a-z]\}\.)?[A-Za-z0-9.-]+\.[A-Za-z]{2,})")
_PLACEHOLDER_HOST = re.compile(r"(^|\.)(example(\.[a-z]+)?|test|invalid|local|localhost)$|example\.")
_SELF = ("sarraficars.com",)


def _files() -> list[Path]:
    out: list[Path] = []
    for d in SCAN_DIRS:
        for p in (ROOT / d).rglob("*"):
            if p.suffix in SUFFIXES and p.is_file() and not any(part in SKIP_PARTS for part in p.parts):
                out.append(p)
    out.extend(ROOT / f for f in SCAN_FILES)
    return out


def _hosts_in_code() -> dict[str, str]:
    found: dict[str, str] = {}
    for p in _files():
        for m in _HOST_RE.finditer(p.read_text(encoding="utf-8", errors="replace")):
            host = re.sub(r"^\{[a-z]\}\.", "", m.group(1).lower()).rstrip(".")
            if _PLACEHOLDER_HOST.search(host) or host.endswith(_SELF) or "{" in host:
                continue
            found.setdefault(host, str(p.relative_to(ROOT)))
    return found


def _modules_in_code() -> dict[str, str]:
    found: dict[str, str] = {}
    for p in _files():
        if p.suffix != ".py":
            continue
        text = p.read_text(encoding="utf-8", errors="replace")
        for mod in OUTBOUND_MODULES:
            if re.search(r"^\s*(import|from)\s+%s\b" % re.escape(mod), text, re.M):
                found.setdefault(mod, str(p.relative_to(ROOT)))
    return found


def _listed_hosts() -> set[str]:
    return {h for r in dr.RECIPIENTS for h in r.hosts}


def _covered(host: str) -> bool:
    return any(host == h or host.endswith("." + h) for h in _listed_hosts()) or host in dr.NOT_USER_DATA


def test_every_outbound_host_in_the_code_is_a_listed_recipient() -> None:
    missing = {h: f for h, f in _hosts_in_code().items() if not _covered(h)}
    assert not missing, (
        "add these hosts to backend/web/data_recipients.py (RECIPIENTS, or NOT_USER_DATA with the reason "
        "they carry no visitor data): %r" % missing
    )


def test_every_outbound_sdk_in_the_code_is_a_listed_recipient() -> None:
    listed = {m for r in dr.RECIPIENTS for m in r.modules}
    missing = {m: f for m, f in _modules_in_code().items() if m not in listed}
    assert not missing, "add a recipient for these modules in backend/web/data_recipients.py: %r" % missing


def test_listed_hosts_and_modules_still_appear_in_the_code() -> None:
    hosts = _hosts_in_code()
    modules = _modules_in_code()
    stale_hosts = [h for h in _listed_hosts() if not any(f == h or f.endswith("." + h) for f in hosts)]
    stale_modules = [m for r in dr.RECIPIENTS for m in r.modules if m not in modules]
    stale_exempt = [h for h in dr.NOT_USER_DATA if h not in hosts]
    assert not stale_hosts and not stale_modules and not stale_exempt, (stale_hosts, stale_modules, stale_exempt)


def test_recipient_names_are_unique_and_complete() -> None:
    names = dr.recipient_names()
    assert len(names) == len(set(names))
    for r in dr.RECIPIENTS:
        assert r.name and r.purpose and r.data and r.when, r.name


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch):
    for name in ("LEGAL_PAGES_APPROVED_VERSION", "SUPPORT_CONTACT", "AUTH_PHONE_LOGIN_ENABLED"):
        monkeypatch.delenv(name, raising=False)
    from backend.main import app

    with app.test_client() as c:
        yield c


def _text(html: str) -> str:
    import html as html_lib

    return re.sub(r"\s+", " ", html_lib.unescape(re.sub(r"<[^>]+>", " ", html)))


@pytest.mark.parametrize("path", ["/privacy", "/terms"])
def test_legal_pages_render_without_a_signed_in_user(client, path: str) -> None:
    with client.session_transaction() as sess:
        assert "user_id" not in sess
    rv = client.get(path)
    assert rv.status_code == 200
    assert rv.headers.get("Location") is None


def test_privacy_page_names_every_recipient(client) -> None:
    text = _text(client.get("/privacy").get_data(as_text=True))
    for r in dr.RECIPIENTS:
        assert r.name in text, r.name
        assert _text(r.data) in text, r.name


def test_privacy_page_makes_no_blanket_encryption_claim(client) -> None:
    text = _text(client.get("/privacy").get_data(as_text=True))
    assert "The account database is encrypted" not in text
    assert "does not encrypt itself" in text


@pytest.mark.parametrize("path", ["/register", "/dealer/register"])
def test_sign_up_pages_link_terms_and_privacy(client, path: str) -> None:
    rv = client.get(path)
    if rv.status_code == 404:
        pytest.skip("%s is not registered in this app" % path)
    html = rv.get_data(as_text=True)
    assert rv.status_code == 200, path
    notice = re.search(r'<p class="auth-footer signup-legal-notice">(.*?)</p>', html, re.S)
    assert notice, path
    assert 'href="/terms"' in notice.group(1) and 'href="/privacy"' in notice.group(1)
    assert "13 or older" in notice.group(1)


def test_footer_links_terms_and_privacy_on_every_page_that_has_it(client) -> None:
    html = client.get("/register").get_data(as_text=True)
    footer = re.search(r'<div class="app-version-footer">.*?</div>', html, re.S).group(0)
    assert 'href="/terms"' in footer and 'href="/privacy"' in footer
