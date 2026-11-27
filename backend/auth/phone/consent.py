"""The consent wording shown beside the unticked box on the phone sign-in page (card S8).

The owner's text of DESIGN.md section 15 plus the additions the S8 review asks for: the program name
("Sarrafi Cars"), real links to the Terms and the Privacy Policy, the carrier sentence and the age
line. It is split around the two links so the template can render real anchors inside the label.

``config.CONSENT_TEXT_VERSION`` says which wording a person agreed to. ANY change to the words below
must bump that version (and the owner then approves the new version in ``LEGAL_PAGES_APPROVED_VERSION``);
``backend/tests/test_auth_phone_routes.py`` pins a hash of :data:`CONSENT_TEXT` to the version so a
silent edit fails a test. The support contact is shown after the text when ``SUPPORT_CONTACT`` is set; it
is an address, not wording, and is not part of the pinned text.
"""

from __future__ import annotations

from backend.auth.phone import config

CONSENT_LEAD = (
    "I agree to receive one-time login codes by text message from Sarrafi Cars at the number above. "
    "Message and data rates may apply. Message frequency: only when I request a code. "
    "Reply STOP to opt out or HELP for help. See our "
)
CONSENT_TERMS = "Terms"
CONSENT_AND = " and "
CONSENT_PRIVACY = "Privacy Policy"
CONSENT_TAIL = (
    ". We do not sell or share your phone number or opt-in data with third parties for marketing. "
    "Carriers are not liable for delayed or undelivered messages. "
    "You must be 13 or older to use Sarrafi Cars."
)

# The same words as one plain string (what the hash in the test covers).
CONSENT_TEXT = CONSENT_LEAD + CONSENT_TERMS + CONSENT_AND + CONSENT_PRIVACY + CONSENT_TAIL

TERMS_PATH = "/terms"
PRIVACY_PATH = "/privacy"


def support_contact() -> str:
    """``SUPPORT_CONTACT``: where a person can get help (an address or a link). Empty when unset: the
    page then shows no contact line and never invents one (production refuses the flag without it,
    see ``config.missing_for_production``)."""
    return config.support_contact()
