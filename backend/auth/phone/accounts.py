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
            return int(found[0]["id"])
        try:
            return int(save_user(username, email, secrets.token_urlsafe(48), role=ROLE_GENERAL, org_id=None))
        except Exception as exc:
            again = find_users_by_email(email, limit=2)
            if again:
                return int(again[0]["id"])
            # Never the email, the username or the driver text: only the exception class.
            _log.warning("event=account_create_failed reason=%s", type(exc).__name__)
            raise


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
