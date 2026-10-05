"""
Platform registry: the ordered :data:`PLATFORM_TEMPLATES`, fingerprinting,
synthesis entry points, and the generic schema.org Vehicle JSON-LD fallback
(:func:`detect_html_harvest`). Moved verbatim from ``recipe_synth.py``
(audit F-6).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable

from backend.scanner.recipes import EndpointRecipe
from backend.scanner.synth.common import _origin
from backend.scanner.synth.http import fetch_dealer_html
from backend.scanner.synth.platforms.autowall import _detect_autowall, _synth_autowall
from backend.scanner.synth.platforms.carscommerce import _detect_carscommerce, _synth_carscommerce
from backend.scanner.synth.platforms.chapman import _detect_chapman, _synth_chapman
from backend.scanner.synth.platforms.dealer_com import _detect_dealer_com, _synth_dealer_com
from backend.scanner.synth.platforms.dealer_eprocess import (
    _detect_dealer_eprocess,
    _synth_dealer_eprocess,
)
from backend.scanner.synth.platforms.dealermasters import (
    _detect_dealermasters,
    _synth_dealermasters,
)
from backend.scanner.synth.platforms.dealeron_cosmos import (
    _detect_dealer_on_cosmos,
    _synth_dealer_on_cosmos,
)
from backend.scanner.synth.platforms.html_cards import _detect_html_cards, _synth_html_cards
from backend.scanner.synth.platforms.jazel import _detect_jazel, _synth_jazel
from backend.scanner.synth.platforms.motive import _detect_motive, _synth_motive
from backend.scanner.synth.platforms.nabthat import _detect_nabthat, _synth_nabthat
from backend.scanner.synth.platforms.oneaudi import _detect_oneaudi, _synth_oneaudi
from backend.scanner.synth.platforms.overfuel import _detect_overfuel, _synth_overfuel
from backend.scanner.synth.platforms.sister_tv import _detect_sister_tv
from backend.scanner.synth.platforms.team_velocity import (
    _detect_team_velocity,
    _synth_team_velocity,
)
from backend.scanner.synth.platforms.typesense import _detect_typesense, _synth_typesense
from backend.scanner.synth.platforms.wp_vehicles import (
    _detect_wp_vehicles_index,
    _synth_wp_vehicles_index,
)

logger = logging.getLogger("scanner")


# ── Platform registry ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class PlatformTemplate:
    name: str
    detect: Callable[[str, str], bool]
    # ``None`` synth => platform is recognized but not synthesizable yet
    # (browser still required); still useful to report *why*. A synth may return
    # a single recipe, ``None``, or a list (a platform that needs several
    # endpoints for full coverage, e.g. Team Velocity's used + new feeds).
    synth: Callable[[str, str, str], "EndpointRecipe | list[EndpointRecipe] | None"] | None


# Ordered most-specific-first; :func:`fingerprint_platform` returns the first hit.
PLATFORM_TEMPLATES: list[PlatformTemplate] = [
    PlatformTemplate("carscommerce", _detect_carscommerce, _synth_carscommerce),
    # typesense also fingerprints dealer_alchemist (dv-framework theme): same
    # shared Typesense host/key, same multi_search shape + parser, only the
    # per-dealer collection differs — extracted by the extended _TS_* regexes.
    PlatformTemplate("typesense", _detect_typesense, _synth_typesense),
    PlatformTemplate("dealer_on_cosmos", _detect_dealer_on_cosmos, _synth_dealer_on_cosmos),
    PlatformTemplate("dealer_dot_com", _detect_dealer_com, _synth_dealer_com),
    PlatformTemplate("sister_tv", _detect_sister_tv, None),
    PlatformTemplate("team_velocity", _detect_team_velocity, _synth_team_velocity),
    PlatformTemplate("dealer_eprocess", _detect_dealer_eprocess, _synth_dealer_eprocess),
    PlatformTemplate("motive_ridemotive", _detect_motive, _synth_motive),
    PlatformTemplate("overfuel", _detect_overfuel, _synth_overfuel),
    PlatformTemplate("nabthat", _detect_nabthat, _synth_nabthat),
    PlatformTemplate("chapman", _detect_chapman, _synth_chapman),
    PlatformTemplate("jazel", _detect_jazel, _synth_jazel),
    PlatformTemplate("dealermasters", _detect_dealermasters, _synth_dealermasters),
    PlatformTemplate("wp_vehicles_index", _detect_wp_vehicles_index, _synth_wp_vehicles_index),
    PlatformTemplate("autowall", _detect_autowall, _synth_autowall),
    PlatformTemplate("oneaudi", _detect_oneaudi, _synth_oneaudi),
    PlatformTemplate("html_cards", _detect_html_cards, _synth_html_cards),
]

_TEMPLATES_BY_NAME = {t.name: t for t in PLATFORM_TEMPLATES}


def fingerprint_platform(html: str, dealer_url: str) -> str | None:
    """Return the platform name detected in *html*, or ``None`` if unrecognized."""
    if not html:
        return None
    for tmpl in PLATFORM_TEMPLATES:
        try:
            if tmpl.detect(html, dealer_url):
                return tmpl.name
        except Exception:  # a bad regex on weird HTML must not abort the sweep
            continue
    return None


def is_synthesizable(platform: str | None) -> bool:
    """True if we have a template that can build a recipe for *platform*."""
    t = _TEMPLATES_BY_NAME.get(platform or "")
    return bool(t and t.synth)


def synthesize_recipes(
    dealer_id: str, dealer_url: str, html: str, platform: str | None = None
) -> list[EndpointRecipe]:
    """Build ALL candidate recipes for *dealer_id* from *html*.

    Most platforms yield one recipe; some (Team Velocity) yield several to cover
    the full lot. Returns ``[]`` when the platform is unrecognized, has no
    template, or the per-dealer params can't be extracted. Each recipe is a
    *candidate*: validate it (:func:`validate_recipe`) before saving.
    """
    platform = platform or fingerprint_platform(html, dealer_url)
    tmpl = _TEMPLATES_BY_NAME.get(platform or "")
    if not tmpl or not tmpl.synth:
        return []
    try:
        result = tmpl.synth(dealer_id, dealer_url, html)
    except Exception as e:
        logger.debug("recipe_synth synth failed [%s/%s]: %s", dealer_id, platform, str(e)[:150])
        return []
    if result is None:
        return []
    return list(result) if isinstance(result, list) else [result]


def synthesize_recipe(
    dealer_id: str, dealer_url: str, html: str, platform: str | None = None
) -> EndpointRecipe | None:
    """Build the primary candidate recipe (first of :func:`synthesize_recipes`).

    Kept for single-recipe callers; returns ``None`` when nothing is synthesized.
    """
    recipes = synthesize_recipes(dealer_id, dealer_url, html, platform)
    return recipes[0] if recipes else None


# ── Universal browser-free fallback: generic schema.org Vehicle JSON-LD ────────
#
# The template path above is preferred: a replayable API/SRP recipe covers the
# WHOLE lot and paginates cleanly. But it only fires for platforms we have a
# template for. For the untemplated long tail — bespoke ``custom_standalone`` /
# luxury rooftops where a per-platform template has no ROI — there is still an
# elegant browser-free path IF the site server-renders schema.org Vehicle
# JSON-LD: the generic harvester (:func:`harvest_vehicles_from_html`) lists the
# lot straight from the HTML. This fallback detects that and confirms it yields
# real VINs, so such dealers are reported browser-free (strategy html_harvest)
# instead of "needs a browser". It is strictly additive — only consulted when no
# API template fingerprints/synthesizes.

# Inventory / SRP paths probed for embedded Vehicle JSON-LD when the page we were
# handed (usually the homepage) carries none. Ordered most-common-first. Both
# slashed and unslashed forms appear in the wild (dealer_eprocess/nabthat use a
# trailing slash; some CMSes 404 the other form rather than redirecting).
_HTML_HARVEST_SRP_PATHS = (
    "/used-inventory/",
    "/inventory",
    "/inventory/used",
    "/used-vehicles/",
    "/new-inventory/",
    "/vehicles/",
    "/all-inventory/",
    "/pre-owned/",
)


def harvest_html_vehicles(html: str) -> list[dict[str, Any]]:
    """Thin wrapper over :func:`html_jsonld_harvest.harvest_vehicles_from_html`
    (imported lazily to avoid a heavy import at module load)."""
    from backend.scanner.html_jsonld_harvest import harvest_vehicles_from_html

    return harvest_vehicles_from_html(html or "")


def detect_html_harvest(
    dealer_url: str,
    html: str | None,
    *,
    min_vins: int = 1,
    probe_srp: bool = True,
    max_srp_paths: int = 6,
) -> tuple[int, str | None]:
    """Universal browser-free fallback for untemplated-but-reachable dealers.

    Returns ``(vin_count, source_url)`` when *html* — or a server-rendered
    inventory / SRP page reachable over plain HTTP — embeds schema.org Vehicle
    JSON-LD carrying real VINs, else ``(0, None)``.

    First checks the HTML we already have (the caller's homepage/inventory
    fetch). If that carries fewer than *min_vins* distinct VINs and *probe_srp*
    is set, it fetches a short, ordered list of common inventory paths
    SEQUENTIALLY (paced by :func:`_pace`, with browser-navigation headers) and
    stops at the first page that clears the bar. Never launches a browser.
    """
    best_vins: set[str] = {v["vin"] for v in harvest_html_vehicles(html or "") if v.get("vin")}
    best_url: str | None = dealer_url if best_vins else None
    if len(best_vins) >= min_vins or not probe_srp:
        return len(best_vins), best_url

    origin = _origin(dealer_url)
    for path in _HTML_HARVEST_SRP_PATHS[:max_srp_paths]:
        page = fetch_dealer_html(origin + path)
        if not page:
            continue
        page_vins = {v["vin"] for v in harvest_html_vehicles(page) if v.get("vin")}
        if len(page_vins) > len(best_vins):
            best_vins, best_url = page_vins, origin + path
        if len(best_vins) >= min_vins:
            break
    return (len(best_vins), best_url) if best_vins else (0, None)
