"""
Helpers shared by the platform templates: the dealer origin and the reference
recipe a template borrows its request shape from. Moved verbatim from
``backend/scanner/recipe_synth.py`` (audit F-6).
"""
from __future__ import annotations

from urllib.parse import urlparse

from backend.scanner.recipes import EndpointRecipe, load_recipes


def _origin(dealer_url: str) -> str:
    """``https://host`` for *dealer_url* (scheme defaulted to https)."""
    p = urlparse(dealer_url if "://" in dealer_url else "https://" + dealer_url)
    scheme = p.scheme or "https"
    host = p.netloc or p.path
    return f"{scheme}://{host}".rstrip("/")


# ── Reference templates (borrowed from real captured recipes) ─────────────────


def _load_reference_recipe(dealer_id: str, url_contains: str) -> EndpointRecipe | None:
    """First non-stale recipe of *dealer_id* whose URL contains *url_contains*."""
    for r in load_recipes(dealer_id):
        if url_contains in r.url and not r.stale:
            return r
    # fall back to stale (template body is still valid even if that dealer's
    # capture went stale — we only borrow the POST shape / shared key)
    for r in load_recipes(dealer_id):
        if url_contains in r.url:
            return r
    return None
