"""Browser-free dealer *specials / offers* acquisition (additive research surface).

Dealer LEASE specials, MANAGER specials, and finance/APR offers live on the
dealer's own website (``/specials``, ``/new-vehicle-specials``, ``/lease-specials``,
``/manager-specials`` …) — NOT in the inventory feed. This package fetches those
pages over plain HTTP (browser-navigation headers, paced, proxy-aware) and
extracts offer cards into normalized dicts, then stores them in the
``dealer_specials`` table keyed by ``dealer_id``.

This surface is intentionally separate from the car SEARCH / listings flow and
must not affect it. It is additive only.

Public API
----------
* :func:`extract_offers_from_html` — parse one page's HTML into offer dicts.
* :func:`fetch_specials_pages` — fetch a dealer's likely specials URLs.
* :func:`ensure_dealer_specials_table` / :func:`upsert_specials` /
  :func:`get_specials_for_dealer` — persistence.
* :func:`scan_dealer_specials` — end-to-end: fetch + extract + store for one dealer.
"""
from __future__ import annotations

from backend.scanner.specials.extract import Offer, extract_offers_from_html
from backend.scanner.specials.fetch import SPECIALS_PATHS, fetch_specials_pages
from backend.scanner.specials.scan import scan_dealer_specials
from backend.scanner.specials.store import (
    ensure_dealer_specials_table,
    get_specials_for_dealer,
    upsert_specials,
)

__all__ = [
    "Offer",
    "extract_offers_from_html",
    "SPECIALS_PATHS",
    "fetch_specials_pages",
    "scan_dealer_specials",
    "ensure_dealer_specials_table",
    "get_specials_for_dealer",
    "upsert_specials",
]
