"""Anti-spam / validation guards for user-submitted dealership reviews.

Kept deliberately simple and dependency-free: a synchronous ``validate_review``
that returns ``(ok, error_message)``, a lightweight per-user rate limit, and an
``ip_hash`` helper for de-anonymized abuse tracking.
"""
from __future__ import annotations

import hashlib
import os
import re
from typing import Any

BODY_MIN = 10
BODY_MAX = 4000
RATING_MIN = 1
RATING_MAX = 5

# At most this many review submits per user per rolling hour.
RATE_LIMIT_PER_HOUR = 5

# Reject bodies containing links — dealers/spammers drop URLs. Catches
# ``http://`` / ``https://``, a leading ``www.``, and bare domains like
# ``foo.com`` / ``deals.example.io/path``.
_URL_RE = re.compile(
    r"(https?://|www\.|\b[a-z0-9][a-z0-9\-]*\.(?:com|net|org|io|co|us|biz|info|dealer|cars|auto)\b)",
    re.IGNORECASE,
)

# Small, deliberately conservative wordlist. Reject on match (keep it simple).
_PROFANITY = {
    "fuck", "shit", "bitch", "asshole", "cunt", "bastard", "dick",
    "nigger", "faggot", "slut", "whore", "retard",
}
_WORD_RE = re.compile(r"[a-z]+")


def hash_ip(ip: str | None) -> str | None:
    """SHA-1 of ``ip + salt`` for storing an opaque ``ip_hash`` (never the raw IP)."""
    if not ip:
        return None
    salt = os.environ.get("REVIEWS_IP_HASH_SALT") or "dealer-reviews-salt-v1"
    return hashlib.sha1((str(ip) + salt).encode("utf-8")).hexdigest()


def _contains_url(text: str) -> bool:
    return bool(_URL_RE.search(text or ""))


def _contains_profanity(text: str) -> bool:
    words = set(_WORD_RE.findall((text or "").lower()))
    return bool(words & _PROFANITY)


def _to_number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def validate_review(payload: dict) -> tuple[bool, str | None]:
    """Validate a review submission. Returns ``(ok, error_message)``.

    Enforces: rating int 1..5; body length 10..4000 after strip; no URLs in the
    body; no profanity; and — when an add-on fee is reported — a non-empty
    description plus a non-negative amount if one is supplied.
    """
    # Rating: int 1..5.
    raw_rating = payload.get("rating")
    try:
        rating = int(raw_rating)
    except (TypeError, ValueError):
        return False, "Please choose a star rating from 1 to 5."
    if rating < RATING_MIN or rating > RATING_MAX:
        return False, "Rating must be between 1 and 5 stars."

    # Body length + content checks.
    body = (payload.get("body") or "").strip()
    if len(body) < BODY_MIN:
        return False, f"Your review is too short — please write at least {BODY_MIN} characters."
    if len(body) > BODY_MAX:
        return False, f"Your review is too long — please keep it under {BODY_MAX} characters."
    if _contains_url(body):
        return False, "Links aren't allowed in reviews. Please remove any URLs or website addresses."
    if _contains_profanity(body):
        return False, "Please keep your review free of profanity."

    # Add-on fee reporting.
    fee_reported = _truthy(payload.get("addon_fee_reported"))
    if fee_reported:
        desc = (payload.get("addon_fee_desc") or "").strip()
        if not desc:
            return False, "Please describe the add-on fee you were charged."
        amount = _to_number(payload.get("addon_fee_amount"))
        if amount is not None and amount < 0:
            return False, "Add-on fee amount can't be negative."

    return True, None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in ("1", "true", "yes", "on", "checked")


def check_rate_limit(conn: Any, user_id: int) -> tuple[bool, str | None]:
    """Enforce at most ``RATE_LIMIT_PER_HOUR`` submits per user per rolling hour."""
    from backend.reviews.store import count_recent_reviews_by_user

    recent = count_recent_reviews_by_user(conn, user_id, within_hours=1)
    if recent >= RATE_LIMIT_PER_HOUR:
        return False, "You're posting reviews too quickly. Please try again later."
    return True, None
