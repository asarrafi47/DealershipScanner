from __future__ import annotations

import os

# Post-2FA redirect (session['mfa_intent']) — used by app login vs dealer login flows
MFA_INTENT_GENERAL = "general"
MFA_INTENT_DEALER = "dealer"

ROLE_ADMIN = "admin"
ROLE_DEALERSHIP_OWNER = "dealership_owner"
ROLE_DEALERSHIP_ADMIN = "dealership_admin"
ROLE_DEALERSHIP_MEMBER = "dealership_member"
ROLE_GENERAL = "general_user"

ALL_ROLES = frozenset(
    {
        ROLE_ADMIN,
        ROLE_DEALERSHIP_OWNER,
        ROLE_DEALERSHIP_ADMIN,
        ROLE_DEALERSHIP_MEMBER,
        ROLE_GENERAL,
    }
)


def normalize_role(role: str | None) -> str:
    r = (role or "").strip().lower()
    if r in ALL_ROLES:
        return r
    return ROLE_DEALERSHIP_MEMBER


def admin_emails() -> set[str]:
    raw = (os.environ.get("APP_ADMIN_EMAILS") or "").strip()
    if not raw:
        return set()
    parts = [p.strip().lower() for p in raw.split(",")]
    return {p for p in parts if p and "@" in p and len(p) <= 254}


def email_is_admin(email: str | None) -> bool:
    e = (email or "").strip().lower()
    if not e:
        return False
    return e in admin_emails()


def is_admin_role(role: str | None) -> bool:
    return normalize_role(role) == ROLE_ADMIN

