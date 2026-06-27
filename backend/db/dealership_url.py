"""Canonical dealership website URL helpers."""

from __future__ import annotations

from typing import Any

from backend.db.dealer_geo import normalize_dealer_host


def effective_website_url(row: dict[str, Any] | None) -> str:
    """Prefer ``website_url``; fall back to legacy ``dealer_website_url``."""
    if not row:
        return ""
    return (str(row.get("website_url") or row.get("dealer_website_url") or "")).strip()


def normalized_dealership_host(*, url: str = "", row: dict[str, Any] | None = None) -> str:
    """Normalized host key for registry matching (``www.`` stripped, lowercase)."""
    if row is not None:
        url = effective_website_url(row)
    return normalize_dealer_host(url)
