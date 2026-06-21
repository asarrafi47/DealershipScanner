"""
Lightweight site structure profiler: runs after warmup, before parallel inventory scraping.

Opens its own page within the shared browser context (leaving the warmup page untouched),
navigates the base URL and optionally one inventory path, then returns a SiteProfile used
by the main scanner to choose paths and skip unnecessary wait loops.

Budget: max 2 page navigations, ~15-25 s wall time, no deep pagination, no Claude calls.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urljoin, urlparse

from backend.scanner.scan_efficiency import (
    INVENTORY_PATHS_CORE,
    INVENTORY_PATHS_DEALER_INSPIRE,
    inventory_paths_for_dealer,
)

logger = logging.getLogger("scanner")

# ── Provider signature tables ────────────────────────────────────────────────
# Each entry is (provider_name, [lowercased_substrings]).
# Listed most-specific first so the first full match wins.

# Substrings looked for in raw page HTML.
_HTML_SIGS: list[tuple[str, list[str]]] = [
    ("pixel_motion",    ["vlpm3vehiclerow", "pm-motors-plugin", "pixelmotiondemo"]),
    ("dealer_on",       ["vhcliaa", "prsnbaa.dealeron"]),
    ("dealer_inspire",  ["dealerinspire_inventory_vars", "mvnalgoliaconfig", "diinventoryconfig"]),
    ("dealer_eprocess", ["dealereprocess", "vehicle-facts.json"]),
    ("fox_dealer",      ["foxdealer.com", "foxdealer-"]),
    ("dealer_fire",     ["dealerfire.com", "df-inventory"]),
    ("sincro",          ["sincrodigital.com", "sincro.net"]),
    ("dealer_dot_com",  ["dealer.com/widget", "cdk.com", "ws-inv-data", "getinventoryandfacets"]),
    # autoWALL (Long Automotive Group CMS): React SPA, vehicle list via AJAX
    ("autowall",        ["autowall-vehicle-list", "powered_by_autowall", "/gs-vehicle/"]),
    # ShopperExpress: WordPress + Serti DMS, SSR inventory pages
    ("shopperexpress",  ["/themes/shopperexpress/", "themes/shopperexpress"]),
]

# Substrings looked for in <script src="…"> URLs.
_SCRIPT_SIGS: list[tuple[str, list[str]]] = [
    ("pixel_motion",    ["pixelmotion", "pm-motors"]),
    ("dealer_on",       ["dealeron.com", "vhcliaa"]),
    ("dealer_inspire",  ["dealerinspire.com"]),
    ("dealer_eprocess", ["dealereprocess.com", "eprocesstech"]),
    ("fox_dealer",      ["foxdealer.com"]),
    ("dealer_fire",     ["dealerfire.com"]),
    ("sincro",          ["sincrodigital.com"]),
    ("dealer_dot_com",  ["assets.dealer.com", "static.dealer.com", "www.dealer.com"]),
    # autoWALL CDN fingerprints (CloudFront hosted)
    ("autowall",        ["autowall-vehicle-list", "autowall-vehicle-external",
                         "dkbcpcob6xxyt.cloudfront", "d3dn269ayoh5p6.cloudfront"]),
    # Roadster storefront embed (often paired with another platform)
    ("roadster",        ["cdn.roadster.com", "cdn1.roadster.com",
                         "roadster_frame_embed", "roadster_dealer_analytics"]),
]

# Substrings looked for in intercepted JSON response URLs.
_API_SIGS: list[tuple[str, list[str]]] = [
    ("dealer_on",       ["vhcliaa", "prsnbaa.dealeron"]),
    ("dealer_inspire",  ["algolianet.com"]),
    ("dealer_eprocess", ["vehicle-facts.json", "canonicallexicon.json"]),
    ("dealer_dot_com",  ["getinventory", "getinventoryandfacets", "ws-inv-data"]),
    # autoWALL / Long Automotive Group: vehicle images via CAI media CDN signal
    ("autowall",        ["cai-media-management.com/resize"]),
]

# Vehicle card DOM selectors probed for presence (order: most specific first).
_VEHICLE_SELECTORS: list[str] = [
    "[data-vin]",
    "[data-vehicle-id]",
    "[data-vehicle]",
    ".vehicle-card",
    ".inventory-vehicle",
    ".vehicle-item",
    ".srp-vehicle-card",
    "a[href*='/inventory/']",
    "[class*='vehiclerow']",
    "[class*='vehicle-row']",
    "[class*='inventory-card']",
    # ShopperExpress / WordPress + Serti selectors
    ".se-vehicle-card",
    ".vehicle-listing",
    "[class*='gs-vehicle']",
    # autoWALL selectors
    ".autowall-vehicle",
    "[class*='autowall']",
]

# Href fragments that suggest an inventory link.
_INV_HREF_FRAGMENTS: frozenset[str] = frozenset({
    "/inventory", "/vehicles", "/new-inventory", "/used-inventory",
    "/certified-inventory", "/certified-pre-owned", "/new-vehicles",
    "/used-vehicles", "/vehicle-inventory",
    # autoWALL inventory paths
    "/gs-vehicle",
})

# JS patterns that suggest infinite-scroll (lazy-load observer) in HTML source.
_INFINITE_SCROLL_SIGS: frozenset[str] = frozenset({
    "intersectionobserver", "lazyload", "lazy-load",
    "infinite-scroll", "virtual-list", "virtualize",
})

# Regex to pull a lot-size number from visible text (e.g. "147 vehicles").
_TOTAL_COUNT_RE = re.compile(
    r"(\d{2,5})\s+(?:vehicles|listings|results|cars|trucks|suvs)\b",
    re.I,
)

# ── SiteProfile ──────────────────────────────────────────────────────────────


@dataclass
class SiteProfile:
    """Structural snapshot of a dealer website collected before inventory scraping."""

    dealer_url: str

    # Provider detection
    detected_provider: str = "unknown"
    # "unknown" | "dealer_dot_com" | "dealer_on" | "dealer_inspire"
    # | "pixel_motion" | "dealer_eprocess" | "fox_dealer" | "dealer_fire"
    # | "sincro" | "custom"

    # Path / URL discovery
    inventory_paths_found: list[str] = field(default_factory=list)
    likely_inventory_urls: list[str] = field(default_factory=list)

    # Script sources that matched a known provider
    script_signatures: list[str] = field(default_factory=list)

    # JSON API response URLs that look inventory-related
    api_endpoint_candidates: list[str] = field(default_factory=list)

    # Pagination
    pagination_type: str = "unknown"
    # "unknown" | "api" | "next_button" | "load_more" | "infinite_scroll" | "ssr"

    # DOM signals
    vehicle_dom_selectors_found: list[str] = field(default_factory=list)
    vin_signals_found: bool = False

    # Lot / filter signals
    total_count_signal: int | None = None
    location_filter_signal: bool = False

    # Quality
    confidence_score: float = 0.0
    # "low" | "algolia_auth" | "eprocess_path"
    scrape_risk: str = "low"
    notes: list[str] = field(default_factory=list)
    profiled_at: float = field(default_factory=time.time)


# ── Pure detection helpers ───────────────────────────────────────────────────


def _score_html(html: str) -> tuple[str, float]:
    """Return (provider, confidence) from raw HTML body."""
    low = html.lower()
    for provider, sigs in _HTML_SIGS:
        hits = sum(1 for s in sigs if s in low)
        if hits >= 2:
            return provider, 0.90
        if hits == 1:
            return provider, 0.60
    return "unknown", 0.0


def _score_scripts(script_urls: list[str]) -> tuple[str, float]:
    """Return (provider, confidence) from <script src> URLs."""
    for provider, sigs in _SCRIPT_SIGS:
        for url in script_urls:
            low = url.lower()
            if any(s in low for s in sigs):
                return provider, 0.85
    return "unknown", 0.0


def _score_api(api_urls: list[str]) -> tuple[str, float]:
    """Return (provider, confidence) from intercepted JSON response URLs."""
    for provider, sigs in _API_SIGS:
        for url in api_urls:
            low = url.lower()
            if any(s in low for s in sigs):
                return provider, 0.95
    return "unknown", 0.0


def _merge_provider(
    html_r: tuple[str, float],
    script_r: tuple[str, float],
    api_r: tuple[str, float],
) -> tuple[str, float]:
    """
    Pick the highest-confidence provider; boost when multiple signals agree.
    API signals are most trustworthy but not always captured in a 1-2 s window.
    """
    candidates = [api_r, html_r, script_r]  # priority order for tie-break
    best = max(candidates, key=lambda x: x[1])
    if best[1] == 0.0:
        return "unknown", 0.0
    # Agreement bonus: if two or more non-unknown results name the same provider, boost
    non_unknown = [c for c in candidates if c[0] != "unknown"]
    if len(non_unknown) >= 2 and len({c[0] for c in non_unknown}) == 1:
        return best[0], min(1.0, best[1] + 0.05)
    return best


def _extract_script_urls(html: str, base_url: str) -> list[str]:
    out: list[str] = []
    for m in re.finditer(r'<script[^>]+src=["\']([^"\']+)["\']', html, re.I):
        src = m.group(1).strip()
        if src:
            out.append(urljoin(base_url, src))
    return out[:50]


def _extract_inventory_links(html: str) -> list[str]:
    """Return unique path strings from hrefs that look like inventory pages."""
    found: list[str] = []
    seen: set[str] = set()
    for m in re.finditer(r'href=["\']([^"\'#?]+)["\']', html, re.I):
        raw = m.group(1).strip()
        if not raw:
            continue
        low = raw.lower()
        for frag in _INV_HREF_FRAGMENTS:
            if frag in low:
                # Normalise to path form
                parsed = urlparse(raw)
                path = parsed.path.rstrip("/") + "/"
                if path and path not in seen:
                    seen.add(path)
                    found.append(path)
                break
    return found[:20]


def _extract_total_count(html: str) -> int | None:
    best: int | None = None
    for m in _TOTAL_COUNT_RE.finditer(html):
        n = int(m.group(1))
        if 3 <= n <= 99_999 and (best is None or n > best):
            best = n
    return best


def _has_location_filter_html(html: str) -> bool:
    low = html.lower()
    return "location" in low and any(
        k in low for k in ("filter", "facet", "checkbox", "location-filter")
    )


def _html_pagination_hint(html: str) -> str:
    """Fast heuristic from HTML source; DOM check will override when possible."""
    low = html.lower()
    if any(s in low for s in _INFINITE_SCROLL_SIGS):
        return "infinite_scroll"
    if re.search(r'load\s*more', html, re.I):
        return "load_more"
    if "next" in low and any(k in low for k in ("pagination", "page-nav", "srp-pagination")):
        return "next_button"
    return "unknown"


def _looks_like_inventory_api(url: str) -> bool:
    low = url.lower()
    deny = ("payment", "/calc/", "checkout", "trade-in", "credit-app", "tradein")
    if any(d in low for d in deny):
        return False
    allow = (
        "inventory", "vehicle", "getinventory", "algolia", "vhcliaa",
        "vehicle-facts", "ws-inv-data", "/srp/", "srp/vehicles",
    )
    return any(a in low for a in allow)


def _compute_confidence(profile: SiteProfile, provider_conf: float) -> float:
    score = provider_conf * 0.50       # provider detection: up to 0.50
    if profile.pagination_type != "unknown":
        score += 0.15
    if profile.vehicle_dom_selectors_found:
        score += 0.20
    if profile.vin_signals_found:
        score += 0.10
    if profile.total_count_signal is not None:
        score += 0.05
    return round(min(1.0, score), 3)


# ── Async DOM probes ─────────────────────────────────────────────────────────


async def _probe_vehicle_selectors(page: Any) -> list[str]:
    """Return selectors that have ≥ 1 matching element (each capped at 1.5 s)."""
    found: list[str] = []
    for sel in _VEHICLE_SELECTORS:
        try:
            n = await asyncio.wait_for(page.locator(sel).count(), timeout=1.5)
            if n > 0:
                found.append(sel)
        except Exception:
            continue
    return found


async def _probe_pagination_dom(page: Any) -> str:
    """Check DOM for visible pagination controls; returns pagination_type string."""
    try:
        loc = page.locator(
            'button:has-text("Load More"), a:has-text("Load More"), '
            '[class*="load-more"], .load-more-btn'
        )
        if await asyncio.wait_for(loc.count(), timeout=1.5) > 0:
            return "load_more"
    except Exception:
        pass
    try:
        loc = page.locator(
            'button:has-text("Next"), a:has-text("Next"), '
            '[aria-label="Next page"], [aria-label="Go to next page"], '
            '.pagination-next, li.next > a, a[rel="next"], '
            '[data-testid*="next"], button[data-action="page-next"]'
        )
        if await asyncio.wait_for(loc.count(), timeout=1.5) > 0:
            return "next_button"
    except Exception:
        pass
    return "unknown"


async def _probe_location_filter_dom(page: Any) -> bool:
    try:
        loc = page.locator(
            "button:has-text('Location'), [aria-label*='Location'], "
            ".filter-header:has-text('Location'), [data-filter-name='location'], "
            "[data-facet='location'], .location-filter"
        )
        return await asyncio.wait_for(loc.count(), timeout=1.5) > 0
    except Exception:
        return False


# ── Main profiler ────────────────────────────────────────────────────────────


def _profiler_timeout_s() -> float:
    raw = (os.environ.get("SCANNER_PROFILER_TIMEOUT_S") or "22").strip()
    try:
        return max(8.0, float(raw))
    except ValueError:
        return 22.0


async def _run_profiler(
    context: Any,
    base_url: str,
    known_paths: list[str],
) -> SiteProfile:
    """Inner implementation — wrapped with a timeout by profile_dealer_site()."""
    profile = SiteProfile(dealer_url=base_url)
    api_urls: list[str] = []
    html_prov, html_conf = "unknown", 0.0
    script_prov, script_conf = "unknown", 0.0

    def _on_response(resp: Any) -> None:
        try:
            ct = (resp.headers.get("content-type") or "").lower()
            if "json" not in ct:
                return
            url = str(getattr(resp, "url", "") or "")
            if url and url.startswith("http"):
                api_urls.append(url)
        except Exception:
            pass

    page = await context.new_page()
    try:
        page.on("response", _on_response)

        # ── Phase 1: navigate base URL ────────────────────────────────────
        try:
            await page.goto(base_url, wait_until="domcontentloaded", timeout=12_000)
        except Exception as exc:
            profile.notes.append(f"base_nav_failed: {exc}")
            return profile

        # Allow a short window for lazy initial API calls to fire.
        await asyncio.sleep(1.5)

        # ── Phase 2: HTML analysis ────────────────────────────────────────
        try:
            html = await page.content()
        except Exception:
            html = ""

        if html:
            script_urls = _extract_script_urls(html, base_url)
            profile.script_signatures = [
                u for u in script_urls
                if any(
                    any(s in u.lower() for s in sigs)
                    for _, sigs in _SCRIPT_SIGS
                )
            ]

            inv_links = _extract_inventory_links(html)
            profile.inventory_paths_found = inv_links
            profile.likely_inventory_urls = [
                base_url.rstrip("/") + p for p in inv_links[:5]
            ]

            profile.total_count_signal = _extract_total_count(html)
            profile.location_filter_signal = _has_location_filter_html(html)

            html_prov, html_conf = _score_html(html)
            script_prov, script_conf = _score_scripts(script_urls)

        # ── Phase 3: DOM checks on the base page ─────────────────────────
        dom_sels = await _probe_vehicle_selectors(page)
        profile.vehicle_dom_selectors_found = dom_sels
        profile.vin_signals_found = bool(dom_sels)

        if await _probe_location_filter_dom(page):
            profile.location_filter_signal = True

        # ── Phase 4: optional inventory path visit ────────────────────────
        # Visit one inventory path when base page is ambiguous.
        need_inv_visit = not dom_sels or html_conf < 0.6
        candidate_path: str | None = None
        if need_inv_visit:
            if inv_links:
                candidate_path = inv_links[0]
            elif known_paths:
                candidate_path = known_paths[0]

        if candidate_path:
            inv_url = base_url.rstrip("/") + candidate_path
            try:
                await page.goto(inv_url, wait_until="domcontentloaded", timeout=12_000)
                await asyncio.sleep(2.0)
                inv_html = await page.content()

                inv_sels = await _probe_vehicle_selectors(page)
                for s in inv_sels:
                    if s not in profile.vehicle_dom_selectors_found:
                        profile.vehicle_dom_selectors_found.append(s)
                if inv_sels:
                    profile.vin_signals_found = True

                pag = await _probe_pagination_dom(page)
                if pag != "unknown":
                    profile.pagination_type = pag

                inv_prov, inv_conf = _score_html(inv_html)
                if inv_conf > html_conf:
                    html_prov, html_conf = inv_prov, inv_conf

                tc = _extract_total_count(inv_html)
                if tc and (profile.total_count_signal is None or tc > profile.total_count_signal):
                    profile.total_count_signal = tc

                profile.notes.append(f"profiled_inv_path:{candidate_path}")
            except Exception as exc:
                profile.notes.append(f"inv_nav_failed({candidate_path}):{exc}")
        else:
            # Try pagination DOM check on the base page
            if profile.pagination_type == "unknown":
                pag = await _probe_pagination_dom(page)
                if pag != "unknown":
                    profile.pagination_type = pag

        # ── Phase 5: API signal resolution ───────────────────────────────
        profile.api_endpoint_candidates = [
            u for u in api_urls if _looks_like_inventory_api(u)
        ][:10]

        api_prov, api_conf = _score_api(profile.api_endpoint_candidates)

        # Any captured inventory-looking JSON response implies API pagination.
        if profile.api_endpoint_candidates and profile.pagination_type == "unknown":
            profile.pagination_type = "api"

        # ── Phase 6: HTML heuristic pagination fallback ───────────────────
        if profile.pagination_type == "unknown" and html:
            profile.pagination_type = _html_pagination_hint(html)

        # ── Phase 7: PixelMotion always means SSR pagination ──────────────
        final_provider, prov_conf = _merge_provider(
            (html_prov, html_conf),
            (script_prov, script_conf),
            (api_prov, api_conf),
        )
        if final_provider == "pixel_motion":
            profile.pagination_type = "ssr"

        profile.detected_provider = final_provider
        profile.confidence_score = _compute_confidence(profile, prov_conf)
        if final_provider == "dealer_inspire":
            profile.scrape_risk = "algolia_auth"
        elif final_provider == "dealer_eprocess":
            profile.scrape_risk = "eprocess_path"

    finally:
        try:
            await page.close()
        except Exception:
            pass

    return profile


async def profile_dealer_site(
    context: Any,
    base_url: str,
    known_paths: list[str],
) -> SiteProfile:
    """
    Profile a dealer site for provider and pagination structure.

    Opens its own page within *context* (warmup page stays untouched).
    Hard-bounded by ``SCANNER_PROFILER_TIMEOUT_S`` (default 22 s).
    Always returns a SiteProfile — caller must tolerate partial/empty results.
    """
    timeout = _profiler_timeout_s()
    try:
        return await asyncio.wait_for(
            _run_profiler(context, base_url, known_paths),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        logger.warning(
            "Site profiler timed out after %.0f s for %s — using defaults",
            timeout,
            base_url,
        )
        p = SiteProfile(dealer_url=base_url)
        p.notes.append(f"profiler_timeout:{timeout}s")
        return p
    except Exception as exc:
        logger.warning("Site profiler error for %s: %s", base_url, exc)
        p = SiteProfile(dealer_url=base_url)
        p.notes.append(f"profiler_error:{exc}")
        return p


# ── Strategy helpers ─────────────────────────────────────────────────────────


def choose_inventory_paths(profile: SiteProfile | None, dealer: dict) -> list[str]:
    """
    Combine SiteProfile signals with manifest/env settings.

    Falls back entirely to existing ``inventory_paths_for_dealer()`` when:
    - profile is None (profiler failed or was skipped)
    - confidence is below threshold
    - no useful path signals were found
    """
    if profile is None:
        return inventory_paths_for_dealer(dealer)

    provider = profile.detected_provider
    conf = profile.confidence_score

    # High-confidence provider overrides: these platforms use non-standard paths
    if conf >= 0.7:
        if provider == "pixel_motion":
            from backend.scanner.scrapers.pixel_motion import _PIXEL_PATHS
            return list(_PIXEL_PATHS)
        if provider == "dealer_inspire":
            return list(INVENTORY_PATHS_DEALER_INSPIRE)
        if provider == "autowall":
            # autoWALL (Long Automotive Group CMS): inventory is served from /gs-vehicle/
            # and falls back to /inventory/ for JSON intercept discovery.
            return ["/gs-vehicle/shopFromHome", "/inventory/", "/new-inventory/index.htm"]
        if provider == "shopperexpress":
            # ShopperExpress (WordPress + Serti DMS): SSR HTML pages
            return ["/inventory/", "/new-vehicles/", "/used-vehicles/", "/certified-pre-owned/"]

    # Base set from manifest/env (respects dealer.provider + SCANNER_INVENTORY_PATHS)
    defaults = inventory_paths_for_dealer(dealer)

    # Merge in nav-discovered paths not already covered
    if profile.inventory_paths_found and conf >= 0.5:
        seen = set(defaults)
        extras = [p for p in profile.inventory_paths_found if p not in seen]
        # Prepend discovered paths when high confidence so they're tried first
        if conf >= 0.7 and extras:
            return extras[:3] + defaults
        return defaults + extras[:3]

    return defaults


__all__ = [
    "SiteProfile",
    "profile_dealer_site",
    "choose_inventory_paths",
]
