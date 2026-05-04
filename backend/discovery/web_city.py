"""
City-level dealership discovery via web search (stub/optional tier).

This tier is currently a stub that returns empty results. It's designed as an
optional supplement to DMV records. In a full implementation, it could:
- Use Yelp Fusion API (free tier: 500 calls/day)
- Download and query a local OSM PBF database
- Scrape dealer directories with proper anti-bot headers

For now, discovery relies on:
1. DMV records (state-specific, truly unlimited)
2. DDG URL gap-fill (existing tier with no hard quotas)

This is still effective for the top 100 cities approach because:
- DMV records cover official dealerships
- DDG URL fill finds websites for known dealer names
- No Overpass API rate limits
"""
from __future__ import annotations

import logging
from typing import Any

import requests

from backend.discovery.candidate import DealerCandidate

logger = logging.getLogger(__name__)


def search_city_dealerships(
    city: str,
    state: str,
    *,
    timeout_s: float = 15.0,
    session: requests.Session | None = None,
) -> list[DealerCandidate]:
    """
    Discover dealerships in a city via web search.

    Currently returns empty list (stub implementation).
    In production, this could use Yelp API, local OSM database, or
    dealer directory scraping.

    Args:
        city: City name (e.g., "Charlotte")
        state: 2-letter state code (e.g., "NC")
        timeout_s: HTTP timeout in seconds
        session: Optional requests.Session for connection reuse

    Returns:
        List of DealerCandidate objects (currently empty stub)
    """
    logger.debug("Web city search stub for %s, %s (no-op)", city, state)
    return []
