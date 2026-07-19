"""End-to-end specials scan for one dealer: fetch → extract → store."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from backend.scanner.specials.extract import Offer, extract_offers_from_html
from backend.scanner.specials.fetch import fetch_specials_pages
from backend.scanner.specials.store import upsert_specials


@dataclass
class SpecialsScanResult:
    dealer_id: str
    base_url: str
    pages_fetched: int = 0
    pages_ok: int = 0
    offers: list[Offer] = field(default_factory=list)
    stored: int = 0
    hit_urls: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def had_specials(self) -> bool:
        return bool(self.offers)


def scan_dealer_specials(
    dealer_id: str,
    base_url: str,
    *,
    conn: Any = None,
    store: bool = True,
    timeout: int = 25,
    paths: list[str] | None = None,
) -> SpecialsScanResult:
    """Fetch a dealer's specials pages, extract offers, and (optionally) store.

    When *conn* is None and *store* is True, a shared inventory connection is
    opened and closed here. Pass *conn* to reuse an existing connection.
    """
    result = SpecialsScanResult(dealer_id=dealer_id, base_url=base_url)
    try:
        fetched = fetch_specials_pages(base_url, dealer_id, timeout=timeout, paths=paths)
    except Exception as e:  # noqa: BLE001 - never let one dealer abort a sweep
        result.error = f"{type(e).__name__}: {e}"
        return result

    result.pages_fetched = len(fetched.pages)
    seen_hashes: set[str] = set()
    for page in fetched.ok_pages:
        result.pages_ok += 1
        offers = extract_offers_from_html(page.html or "", source_url=page.url)
        page_new = 0
        for off in offers:
            h = off.get("offer_hash")
            if h and h in seen_hashes:
                continue
            if h:
                seen_hashes.add(h)
            result.offers.append(off)
            page_new += 1
        if page_new:
            result.hit_urls.append(page.url)

    if store and result.offers:
        own_conn = conn is None
        if own_conn:
            from backend.db.inventory_db import get_conn

            conn = get_conn()
        try:
            result.stored = upsert_specials(conn, dealer_id, result.offers)
        finally:
            if own_conn and conn is not None:
                conn.close()
    return result
