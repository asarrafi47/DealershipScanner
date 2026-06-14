"""Site-admin: enqueue dealer onboard jobs from locator / discovery data."""

from __future__ import annotations

import re
from typing import Any

from backend.dev.dealers import slug_from_url
from backend.db.inventory_pg import is_inventory_postgres
from backend.scanner.job_queue import enqueue_job
from backend.utils.safe_listing_url import normalize_safe_http_url

_DEALER_ID_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


def _normalize_dealer_id(raw: str) -> str | None:
    did = (raw or "").strip().lower()
    if not did or not _DEALER_ID_RE.match(did):
        return None
    return did


def request_dealer_onboard(
    *,
    url: str,
    dealer_id: str | None = None,
    name: str | None = None,
) -> tuple[bool, str, dict[str, Any]]:
    """
    Validate inputs and enqueue an ``onboard`` job.

    Returns ``(ok, error_code, payload)`` where payload may include ``job_id`` and ``dealer_id``.
    """
    if not is_inventory_postgres():
        return False, "postgres_required", {}

    inv_url = normalize_safe_http_url((url or "").strip())
    if not inv_url:
        return False, "invalid_url", {}

    did = _normalize_dealer_id(dealer_id) if dealer_id else None
    if not did:
        did = _normalize_dealer_id(slug_from_url(inv_url))
    if not did:
        return False, "invalid_dealer_id", {}

    payload: dict[str, Any] = {"url": inv_url}
    dealer_name = (name or "").strip()
    if dealer_name:
        payload["name"] = dealer_name

    job_id = enqueue_job(dealer_id=did, job_type="onboard", payload=payload)
    if not job_id:
        return False, "enqueue_failed", {}
    return True, "", {"job_id": job_id, "dealer_id": did}
