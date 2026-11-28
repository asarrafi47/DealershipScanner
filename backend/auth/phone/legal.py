"""Draft Terms, Privacy Policy and text-message consent wording (card S14, DESIGN.md sections 15 and 16).

The three pages (``/terms``, ``/privacy``, ``/sms-consent``) are DRAFTS until the owner approves them by
setting ``LEGAL_PAGES_APPROVED_VERSION`` to ``CONSENT_TEXT_VERSION`` (config.py). Nothing in the repository
sets or documents an approved value; while they differ every page shows a plain DRAFT sentence and the
production boot check keeps phone sign-in off. This is not legal advice.

``CONSENT_TEXT`` is the exact consent wording shown beside the checkbox on the phone page; it lives in
``consent.py`` (card S8, pinned there by a hash test) and ``/sms-consent`` shows the same words. The HELP
contact is ``SUPPORT_CONTACT`` (``config.support_contact``; names only, never logged). Unset, the pages show a
visible placeholder and the production boot check already names ``SUPPORT_CONTACT``.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from markupsafe import Markup, escape

from backend.auth.phone import config as phone_config
from backend.auth.phone import consent
from backend.web import data_recipients

LAST_UPDATED = "2026-11-28"
PROGRAM_NAME = "Sarrafi Cars"
SUPPORT_PLACEHOLDER = "[support contact to be added by the owner]"

# Templates and the endpoint each one is served by (backend/routes/site_misc.py).
LEGAL_PAGES: tuple[tuple[str, str], ...] = (
    ("terms.html", "terms"),
    ("privacy.html", "privacy"),
    ("sms_consent.html", "sms_consent"),
)

CONSENT_TEXT = consent.CONSENT_TEXT

_TEMPLATES_DIR = Path(__file__).resolve().parents[3] / "frontend" / "templates"
_EMAIL_RE = re.compile(r"[^@\s<>\"']+@[^@\s<>\"']+\.[A-Za-z]{2,}")


def support_contact() -> str | None:
    """``SUPPORT_CONTACT`` (through ``consent.support_contact``), stripped; None when unset."""
    return (consent.support_contact() or "").strip() or None


def support_href() -> str:
    """Where the HELP link points: mailto for an email, the page for an https URL, else the help section."""
    contact = support_contact()
    if contact and _EMAIL_RE.fullmatch(contact):
        return "mailto:" + contact
    if contact and contact.lower().startswith("https://") and not any(c in contact for c in " <>\"'"):
        return contact
    return "/sms-consent#help"


def support_contact_display() -> str:
    """The contact as shown on the pages, or the visible placeholder."""
    return support_contact() or SUPPORT_PLACEHOLDER


def legal_approved() -> bool:
    """True only when the owner set ``LEGAL_PAGES_APPROVED_VERSION`` to the current consent text version."""
    version = phone_config.CONSENT_TEXT_VERSION
    return bool(version) and phone_config.legal_pages_approved_version() == version


def _link(text: str, href: str) -> str:
    return '<a href="%s">%s</a>' % (escape(href), escape(text))


def consent_html() -> Markup:
    """``CONSENT_TEXT`` with real links on "Terms" and "Privacy Policy" (the same words)."""
    return Markup(
        escape(consent.CONSENT_LEAD)
        + Markup(_link(consent.CONSENT_TERMS, consent.TERMS_PATH))
        + escape(consent.CONSENT_AND)
        + Markup(_link(consent.CONSENT_PRIVACY, consent.PRIVACY_PATH))
        + escape(consent.CONSENT_TAIL)
    )


def page_context() -> dict[str, Any]:
    """Values every legal template reads."""
    return {
        "legal_draft": not legal_approved(),
        "legal_version": phone_config.CONSENT_TEXT_VERSION,
        "legal_last_updated": LAST_UPDATED,
        "legal_support_contact": support_contact_display(),
        "legal_support_missing": support_contact() is None,
        "legal_support_href": support_href(),
        "consent_text": CONSENT_TEXT,
        "consent_html": consent_html(),
        "data_recipients": data_recipients.RECIPIENTS,
    }


def missing_legal_pages(app: Any = None) -> list[str]:
    """Names of what the legal pages lack: templates and endpoints (when ``app`` is given). The support
    contact is checked by ``config.missing_for_production`` (``SUPPORT_CONTACT``). Never raises; an
    unexpected error is reported as a name too."""
    missing: list[str] = []
    try:
        for template, endpoint in LEGAL_PAGES:
            if not _template_exists(app, template):
                missing.append("template " + template)
            if app is not None and endpoint not in getattr(app, "view_functions", {}):
                missing.append("endpoint " + endpoint)
    except Exception:  # noqa: BLE001 - the boot check must get a list, never an exception
        missing.append("legal pages (check failed)")
    return missing


def _template_exists(app: Any, name: str) -> bool:
    if app is not None:
        loader = getattr(getattr(app, "jinja_env", None), "loader", None)
        if loader is not None:
            try:
                loader.get_source(app.jinja_env, name)
                return True
            except Exception:  # noqa: BLE001 - TemplateNotFound or a loader error both mean "missing"
                return False
    return (_TEMPLATES_DIR / name).is_file()
