"""The reserved address a phone account is stored under (it has no email of its own).

A leaf module with no imports, so the users database layer, the registration check and
``backend/auth/phone/accounts.py`` can all use it without a cycle. ``.invalid`` is reserved by
RFC 2606 and can never receive mail, so no real person owns an address on it.
"""

from __future__ import annotations

from typing import Any

PLACEHOLDER_DOMAIN = "phone.invalid"
_PLACEHOLDER_SUFFIX = "@" + PLACEHOLDER_DOMAIN


def is_placeholder_email(email: Any) -> bool:
    """True for the address ``AuthPhoneService`` stores for an account that has no email.

    A suffix test, so the shape the service builds today and any later shape on the same reserved
    domain are both covered.
    """
    return isinstance(email, str) and email.strip().lower().endswith(_PLACEHOLDER_SUFFIX)
