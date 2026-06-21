"""
Recover gallery URLs (and optional description) from a VDP URL without Playwright.

Used when intercept/VDP harvest left a thin gallery — fetches listing HTML (requests,
then Playwright fallback via ``listing_gap_fill.fetch_listing_html``) and deep-harvests
image URLs from JSON-LD, inline JSON, and script payloads.
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any
from urllib.parse import urlparse

from backend.parsers.base import (
    dedupe_urls_order_prefer_large,
    harvest_image_urls_from_json,
    inventory_gallery_max,
    normalize_image_url_https,
)
from backend.scanner.post_scan.gap_fill import fetch_listing_html
from backend.scanner.utils.vdp_spec_parse import parse_html_for_vehicle_specs

logger = logging.getLogger(__name__)

_IMG_URL_RE = re.compile(
    r"https?://[^\s\"'<>]+?\.(?:jpe?g|png|webp|gif|avif)(?:\?[^\s\"'<>]*)?",
    re.I,
)


def thin_gallery_threshold() -> int:
    """Gallery counts at or below this trigger HTML recovery (default 14)."""
    try:
        return max(3, int((os.environ.get("SCANNER_GALLERY_RECOVERY_MIN") or "14").strip()))
    except ValueError:
        return 14


def vdp_html_recovery_enabled() -> bool:
    raw = (os.environ.get("SCANNER_VDP_HTML_RECOVERY") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def count_https_gallery_urls(urls: list[str] | None) -> int:
    if not urls:
        return 0
    seen: set[str] = set()
    n = 0
    for u in urls:
        if not isinstance(u, str):
            continue
        nu = normalize_image_url_https(u.strip())
        if nu.startswith("https://") and nu not in seen:
            seen.add(nu)
            n += 1
    return n


def _origin_from_url(url: str) -> str:
    try:
        p = urlparse(url)
        if p.scheme and p.netloc:
            return f"{p.scheme}://{p.netloc}"
    except Exception:
        pass
    return ""


def _json_ld_objects(html: str) -> list[Any]:
    from bs4 import BeautifulSoup

    out: list[Any] = []
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = tag.string or tag.get_text() or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, list):
            out.extend(data)
        else:
            out.append(data)
    return out


def _inline_json_blobs(html: str, *, max_blobs: int = 24) -> list[Any]:
    """Parse ``__NEXT_DATA__`` and similar application/json script tags."""
    from bs4 import BeautifulSoup

    out: list[Any] = []
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("script"):
        if len(out) >= max_blobs:
            break
        stype = (tag.get("type") or "").lower()
        sid = (tag.get("id") or "").lower()
        if stype == "application/ld+json":
            continue
        if stype not in ("application/json", "") and "json" not in sid and "__next_data__" not in sid:
            if tag.get("id") != "__NEXT_DATA__":
                continue
        raw = tag.string or tag.get_text() or ""
        if len(raw) < 80 or len(raw) > 4_000_000:
            continue
        if not any(
            tok in raw.lower()
            for tok in ("image", "photo", "gallery", "vehicle", "inventory", "media")
        ):
            continue
        try:
            out.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return out


def harvest_gallery_urls_from_html(html: str, page_url: str, *, max_urls: int | None = None) -> list[str]:
    """Best-effort HTTPS gallery URLs from static/HTML/embedded JSON."""
    if not html or len(html) < 400:
        return []
    mx = max_urls if max_urls is not None else max(inventory_gallery_max(), 96)
    origin = _origin_from_url(page_url)
    seen: set[str] = set()
    urls: list[str] = []

    def _add(raw: str) -> None:
        nu = normalize_image_url_https((raw or "").strip())
        if not nu.startswith("https://") or nu in seen:
            return
        if any(x in nu.lower() for x in ("logo", "icon", "badge", "carfax", "kbb", "placeholder")):
            return
        seen.add(nu)
        urls.append(nu)

    for obj in _json_ld_objects(html):
        for u in harvest_image_urls_from_json(obj, origin, max_urls=mx):
            _add(u)

    for blob in _inline_json_blobs(html):
        for u in harvest_image_urls_from_json(blob, origin, max_urls=mx):
            _add(u)

    for m in _IMG_URL_RE.finditer(html):
        _add(m.group(0))
        if len(urls) >= mx:
            break

    return dedupe_urls_order_prefer_large(urls, max_len=mx)


_DESCRIPTION_MAX_LEN = 2000


def _normalize_description_text(text: str, *, max_len: int = _DESCRIPTION_MAX_LEN) -> str | None:
    t = " ".join((text or "").split()).strip()
    if len(t) < 60:
        return None
    if "inventory" in t.lower()[:40] and len(t) < 120:
        return None
    return t[:max_len]


def extract_description_from_html(html: str) -> str | None:
    """Dealer marketing paragraph from meta, Description headings, or common VDP blocks."""
    from bs4 import BeautifulSoup

    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")

    best: str | None = None

    def _consider(candidate: str | None) -> None:
        nonlocal best
        norm = _normalize_description_text(candidate or "")
        if not norm:
            return
        if not best or len(norm) > len(best):
            best = norm

    for sel, attr in (
        ('meta[property="og:description"]', "content"),
        ('meta[name="description"]', "content"),
    ):
        el = soup.select_one(sel)
        if el and el.get(attr):
            _consider(str(el[attr]))

    import re

    for heading in soup.find_all(re.compile(r"^h[1-6]$", re.I)):
        label = heading.get_text(" ", strip=True)
        if not re.match(r"^description\b", label, re.I):
            continue
        sibling = heading.find_next_sibling()
        for _ in range(4):
            if not sibling:
                break
            if getattr(sibling, "name", None) in ("script", "style"):
                sibling = sibling.find_next_sibling()
                continue
            _consider(sibling.get_text(" ", strip=True))
            sibling = sibling.find_next_sibling()
        section = heading.find_parent(["section", "article", "div"])
        if section:
            blob = section.get_text(" ", strip=True)
            blob = re.sub(r"^description\s*", "", blob, flags=re.I)
            _consider(blob)

    for sel in (
        ".vehicle-description",
        ".vdp-description",
        "[class*='vehicle-description']",
        "[class*='vdp-description']",
        "#vehicle-description",
        ".description-content",
        "[data-testid*='description']",
    ):
        for el in soup.select(sel):
            _consider(el.get_text(" ", strip=True))

    return best


def recover_from_detail_page(
    detail_url: str,
    *,
    existing_gallery: list[str] | None = None,
    fetch_html: Any = None,
) -> dict[str, Any]:
    """
    Fetch VDP HTML and return recovered gallery URLs plus optional spec/description hints.

    Returns dict with keys: ``gallery_urls``, ``description``, ``specs``, ``html_fetched``.
    """
    stats: dict[str, Any] = {
        "gallery_urls": [],
        "description": None,
        "specs": {},
        "html_fetched": False,
        "skipped": None,
    }
    if not vdp_html_recovery_enabled():
        stats["skipped"] = "disabled"
        return stats
    url = (detail_url or "").strip()
    if not url.lower().startswith("http"):
        stats["skipped"] = "no_url"
        return stats

    before = count_https_gallery_urls(existing_gallery or [])
    if before > thin_gallery_threshold():
        stats["skipped"] = "gallery_sufficient"
        return stats

    fetch_fn = fetch_html or fetch_listing_html
    html = fetch_fn(url)
    if not html:
        stats["skipped"] = "fetch_failed"
        return stats
    stats["html_fetched"] = True

    recovered = harvest_gallery_urls_from_html(html, url)
    stats["gallery_urls"] = recovered
    stats["specs"] = parse_html_for_vehicle_specs(html)
    from backend.scanner.utils.vdp_spec_parse import parse_color_from_listing_html
    stats["colors"] = parse_color_from_listing_html(html)
    desc = extract_description_from_html(html)
    if desc:
        stats["description"] = desc

    after = count_https_gallery_urls((existing_gallery or []) + recovered)
    stats["before_count"] = before
    stats["after_count"] = after
    stats["added"] = max(0, after - before)
    if recovered:
        logger.info(
            "VDP HTML recovery: %s — %d urls harvested (gallery %d→%d)",
            url[:80],
            len(recovered),
            before,
            after,
        )
    return stats
