"""
Normalize dealer manifest URLs for Dealer.com-style inventory scraping.

When ``dealers.json`` stores a marketing or fixed document path (e.g. ``schedule-service.htm``),
inventory paths such as ``/new-inventory/index.htm`` must be rooted at the site origin, not
appended under that document's directory.
"""
from __future__ import annotations

import re
from pathlib import PurePosixPath
from urllib.parse import urlparse


def dealer_inventory_base_url(manifest_url: str) -> tuple[str, bool]:
    """
    Return ``(base_url, did_normalize_to_origin)``.

    If the URL path ends with ``.htm`` or ``.html``, returns ``scheme://netloc`` (no trailing
    slash). Otherwise returns the input normalized with ``https://`` added when missing and
    trailing slashes trimmed for consistent ``base + '/new-inventory/...'`` joins elsewhere.
    """
    raw_in = (manifest_url or "").strip()
    if not raw_in:
        return "", False

    candidate = raw_in
    if not re.match(r"https?://", candidate, re.I):
        candidate = "https://" + candidate.lstrip("/")

    try:
        parts = urlparse(candidate)
    except ValueError:
        return raw_in.rstrip("/"), False

    if parts.scheme not in ("http", "https") or not parts.netloc:
        return candidate.rstrip("/"), False

    # Strip query string and fragment — they must never appear in the base URL
    # that inventory paths are appended to (e.g. ?utm_medium=social/new-inventory/).
    candidate = f"{parts.scheme}://{parts.netloc}{parts.path or ''}"
    parts = urlparse(candidate)

    path_clean = (parts.path or "").strip().rstrip("/")
    if path_clean:
        suf = PurePosixPath(path_clean).suffix.lower()
        if suf in (".htm", ".html"):
            root = f"{parts.scheme}://{parts.netloc}".rstrip("/")
            return root, True

        _NON_INVENTORY_SEGMENTS = frozenset({
            "service", "schedule-service", "service-department", "parts", "about",
            "about-us", "contact", "contact-us", "financing", "careers", "specials",
            "coupons", "reviews", "staff", "team", "why-us", "hours", "directions",
            "privacy", "sitemap",
        })
        first_seg = path_clean.lstrip("/").split("/")[0].lower()
        if first_seg in _NON_INVENTORY_SEGMENTS:
            return f"{parts.scheme}://{parts.netloc}", True

    return candidate.rstrip("/"), False
