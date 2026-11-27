"""Accounts for phone sign-in (card S7, the part card S8 needs).

A phone account has no email of its own. ``AuthPhoneService.sign_in`` hands the port a
placeholder (``p-<identity id>-<hmac8>@phone.invalid`` and ``user-<identity id>-<4 hex>``, see
``service.placeholder_for``); the users table needs a unique, non-null email, so the placeholder is
stored. It is never shown, never mailed and never put in a session or a JSON answer: use
:func:`public_email` wherever an email would leave the server.

No SQL lives here. Accounts are created through the existing ``save_user`` (the users database is
SQLite and ``test_no_sqlite`` freezes the SQLite constructs per file), and the phone tables are
reached only through ``auth_phone_repo``.
"""

from __future__ import annotations

import logging
import secrets
from collections.abc import Mapping
from typing import Any

from backend.utils.phone_placeholder import PLACEHOLDER_DOMAIN, is_placeholder_email  # noqa: F401 (re-exported)
from backend.utils.roles import ROLE_GENERAL, account_has_env_admin_privilege

_log = logging.getLogger("auth_phone")

# ``PLACEHOLDER_DOMAIN`` and ``is_placeholder_email`` live in ``backend/utils/phone_placeholder.py`` (a leaf
# module), so the registration form and the users database can reserve the domain without importing this.


def public_email(email: Any) -> str:
    """The email that may be shown, mailed or returned: empty for a placeholder or a non-string."""
    if not isinstance(email, str) or is_placeholder_email(email):
        return ""
    return email.strip()


def is_phone_eligible(user_row: Mapping[str, Any] | None) -> bool:
    """May this account sign in by phone? (DESIGN.md section 10, TASKS card S7 review (a)).

    True only for an active shopper: the **stored** role is ``general_user`` (or empty, but only when
    the row has no dealer id, no dealership registry id and no organisation), the account is active,
    and it is not an environment admin (``APP_ADMIN_EMAILS`` / ``APP_ADMIN_USERNAMES``). Admins,
    dealers and staff keep their password sign-in; a phone code never opens them.

    The role is read as a raw string. ``normalize_role`` maps every unknown value to ``general_user``,
    so it must not be used here, and ``get_user_profile`` turns a NULL role into ``dealer_staff``
    (refused, which is the safe direction).
    """
    if not user_row:
        return False
    if "is_active" in user_row and not bool(user_row.get("is_active")):
        return False
    role = str(user_row.get("role") or "").strip().lower()
    has_dealer = bool(str(user_row.get("dealer_id") or "").strip())
    has_registry = user_row.get("dealership_registry_id") not in (None, "", 0, "0")
    try:
        has_org = int(user_row.get("org_id") or 0) > 0
    except (TypeError, ValueError):
        has_org = True  # unreadable means "not a plain shopper": refuse
    if has_dealer or has_registry or has_org:
        return False
    if role not in (ROLE_GENERAL, ""):
        return False
    if account_has_env_admin_privilege(user_row.get("email"), user_row.get("username")):
        return False
    return True


class PhoneAccounts:
    """The ``Accounts`` port of ``AuthPhoneService`` (``adopt_or_create``), over the users database."""

    def adopt_or_create(self, *, email: str, username: str) -> int:
        """Return the account that already holds ``email``, else create it. Idempotent by email.

        The account is a plain shopper (``general_user``, no organisation) with a random password
        nobody knows, so a phone account has no password sign-in at all. A lost race (two tabs, or a
        retry after a crash between creating the account and activating the identity) is resolved
        by looking the email up again.
        """
        from backend.db.users_db import find_users_by_email, save_user

        found = find_users_by_email(email, limit=2)
        if found:
            return _adoptable(found, email, username)
        try:
            return int(save_user(username, email, secrets.token_urlsafe(48), role=ROLE_GENERAL, org_id=None))
        except Exception as exc:
            again = find_users_by_email(email, limit=2)
            if again:
                return _adoptable(again, email, username)
            # Never the email, the username or the driver text: only the exception class.
            _log.warning("event=account_create_failed reason=%s", type(exc).__name__)
            raise


    def is_eligible(self, user_id: int) -> bool:
        """The service's eligibility check before a login stamp or a dormant detach (review R8).

        A missing row answers True on purpose: the number then points at a deleted account, and the
        route releases that identity and makes a fresh account (``identity_orphan_healed``); users ids
        are never reused, so nobody else can be reached that way. A row that exists must pass
        :func:`is_phone_eligible`. A read error raises, and the route answers with its generic failure.
        """
        from backend.db.users_db import get_user_profile

        row = get_user_profile(int(user_id))
        return True if row is None else is_phone_eligible(row)


class PlaceholderTaken(Exception):
    """An account already holds this placeholder email but was not made by phone sign-in (review R13)."""


def _adoptable(rows: list[dict], email: str, username: str) -> int:
    """The id of the account to adopt for a retry, only if phone sign-in itself made it (review R13).

    The placeholder email and username both carry an HMAC tag from the pepper and the identity id, and
    the ``.invalid`` domain is refused on every other way to set an email, so a match should only ever be
    the account an earlier try of this same sign-up created. Defence in depth: the row must also be a
    plain shopper with no Google or Apple link; anything else is refused and
    never adopted.
    """
    row = rows[0]
    if len(rows) != 1 or not is_placeholder_email(row.get("email")):
        raise PlaceholderTaken()
    if str(row.get("role") or "").strip().lower() not in (ROLE_GENERAL, ""):
        raise PlaceholderTaken()
    if str(row.get("google_sub") or "").strip() or str(row.get("apple_sub") or "").strip():
        raise PlaceholderTaken()
    if str(row.get("email") or "").strip().lower() != (email or "").strip().lower():
        raise PlaceholderTaken()
    return int(row["id"])


def purge_phone_identity(user_id: int) -> None:
    """Delete the phone identity of an account that is being deleted.

    Without this the number would stay tied to an account id that no longer exists, and nobody could
    sign in or sign up with it again. Best effort by design: a database error is logged by class and
    does not undo the account deletion that already happened.
    """
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return
    if uid <= 0:
        return
    try:
        from backend.db.connect import InventoryDsnMissing
        from backend.db.repositories import auth_phone_repo

        try:
            auth_phone_repo.delete_identity_for_user(uid)
        except InventoryDsnMissing:
            return  # no Postgres here (a plain development box): no identity can exist
    except Exception as exc:  # the account is gone already; report, never raise
        _log.warning("event=identity_purge_failed reason=%s", type(exc).__name__)


def touch_phone_last_login(user_id: int, *, now: Any = None) -> bool:
    """Stamp the phone identity's last sign-in (review R4). The session hook calls it at most once a day
    for a phone session, so a person who stays signed in is never detached as dormant after 180 days.
    Raises on a database error; the caller swallows it."""
    from datetime import datetime, timezone

    from backend.db.repositories import auth_phone_repo

    return auth_phone_repo.touch_login_for_user(int(user_id), now=now or datetime.now(timezone.utc))
