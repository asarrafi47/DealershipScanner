"""Link an OAuth sign-in to an existing account by email (P3.1, D-SEC4 (a)).

Pre-registration takeover: someone registers a password account with another
person's email (email verification is optional), then that person signs in with
Google or Apple and is linked into the squatter's account. So:

- the email match is strict (``lower(email)``), never ``get_user_by_login``,
  which also matches usernames;
- an existing account is auto-linked only when its email is already verified
  (accounts created through OAuth are verified at creation; a changed email
  clears the flag, see ``update_user_profile``);
- otherwise the link waits in the session for ``PENDING_OAUTH_LINK_TTL_S`` and
  completes only when the same browser logs in with that account's password
  (``pages.login_page`` / ``pages.api_auth_login``). Only then is the sub linked
  and the email marked verified: the provider proved the mailbox, the password
  proved the account.

The spec's narrower rule ("no provider sub, a usable password, no
email_verified_at") is reduced to the verified check: OAuth-created accounts
carry a random but valid bcrypt hash, so "usable password" cannot be read from
the row, and a sub from the *other* provider does not vouch for an email that
was changed after that sub was linked.
"""

from __future__ import annotations

import logging
import time
from typing import NamedTuple

from flask import session

_log = logging.getLogger(__name__)

PENDING_OAUTH_LINK_KEY = "pending_oauth_link"
PENDING_OAUTH_LINK_TTL_S = 600

PROVIDER_LABELS = {"google": "Google", "apple": "Apple"}


class Resolution(NamedTuple):
    """Outcome of resolving an OAuth identity to an app user."""

    user_id: int | None = None
    error: str | None = None
    is_new_user: bool = False
    deferred: bool = False


def _link_sub(provider: str, user_id: int, sub: str) -> bool:
    from backend.db.users_db import link_user_apple_sub, link_user_google_sub

    if provider == "google":
        return link_user_google_sub(user_id, sub)
    if provider == "apple":
        return link_user_apple_sub(user_id, sub)
    return False


def resolve_by_email(provider: str, sub: str, email: str) -> Resolution | None:
    """Match ``email`` to an existing account. ``None`` means no account uses it.

    Call only with an email the provider says is verified.
    """
    from backend.db.users_db import find_users_by_email, mark_user_email_verified

    label = PROVIDER_LABELS[provider]
    matches = find_users_by_email(email)
    if not matches:
        return None
    if len(matches) > 1:
        _log.warning("%s sign-in: %d accounts share one email; not linking", label, len(matches))
        return Resolution(error="More than one account uses this email. Log in with your password.")
    user = matches[0]
    uid = int(user["id"])
    stored_sub = (user.get(f"{provider}_sub") or "").strip()
    if stored_sub and stored_sub != sub:
        return Resolution(error=f"This email is linked to a different {label} account.")
    if not (user.get("email_verified_at") or "").strip():
        stash_pending_link(provider, sub, email, uid)
        return Resolution(deferred=True)
    if not _link_sub(provider, uid, sub):
        return Resolution(error=f"Could not link {label} to your account.")
    mark_user_email_verified(uid)
    return Resolution(user_id=uid)


def stash_pending_link(provider: str, sub: str, email: str, user_id: int) -> None:
    session[PENDING_OAUTH_LINK_KEY] = {
        "provider": provider,
        "sub": sub,
        "email": (email or "").strip().lower(),
        "user_id": int(user_id),
        "exp": int(time.time()) + PENDING_OAUTH_LINK_TTL_S,
    }


def _valid_pending(raw: object) -> dict | None:
    if not isinstance(raw, dict):
        return None
    provider = raw.get("provider")
    if provider not in PROVIDER_LABELS:
        return None
    try:
        exp = int(raw.get("exp") or 0)
        uid = int(raw.get("user_id") or 0)
    except (TypeError, ValueError):
        return None
    sub = str(raw.get("sub") or "").strip()
    email = str(raw.get("email") or "").strip().lower()
    if exp <= int(time.time()) or uid <= 0 or not sub or "@" not in email:
        return None
    return {"provider": provider, "sub": sub, "email": email, "user_id": uid, "exp": exp}


def pending_link_notice() -> str | None:
    """Login-page line while a link waits; drops an expired or malformed entry."""
    raw = session.get(PENDING_OAUTH_LINK_KEY)
    if raw is None:
        return None
    pending = _valid_pending(raw)
    if not pending:
        session.pop(PENDING_OAUTH_LINK_KEY, None)
        return None
    label = PROVIDER_LABELS[pending["provider"]]
    return (
        f"An account with {pending['email']} already exists. "
        f"Log in with its password to connect {label} sign-in to it."
    )


def pop_pending_link() -> dict | None:
    """Take the pending link out of the session (call before ``session.clear()``)."""
    return _valid_pending(session.pop(PENDING_OAUTH_LINK_KEY, None))


def complete_pending_link(pending: dict | None, user_id: int) -> bool:
    """After a password login as ``user_id``: link the waiting sub and verify the email.

    Links only when the login is the same account the OAuth email matched, that
    account's email is still the provider's email, and its sub slot is empty.
    Never fails the login: any refusal or DB error leaves the account unlinked.
    """
    pending = _valid_pending(pending)
    if not pending:
        return False
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    if uid != pending["user_id"]:
        return False
    from backend.db.users_db import get_user_email_verification_state, mark_user_email_verified

    provider = pending["provider"]
    try:
        state = get_user_email_verification_state(uid) or {}
        if (state.get("email") or "").strip().lower() != pending["email"]:
            return False
        if (state.get(f"{provider}_sub") or "").strip():
            return False
        if not _link_sub(provider, uid, pending["sub"]):
            return False
        mark_user_email_verified(uid)
    except Exception as ex:  # noqa: BLE001 - a failed link must not fail the login
        _log.warning("pending %s link failed for user_id=%s: %s", provider, uid, type(ex).__name__)
        return False
    return True
