"""Provider parser registry; :func:`parse` hands every payload to the ROOFTOP
ATTRIBUTION gate (``backend.attribution``).

Every scan path (delta replay, browser scan, recipe validation, recipe heal)
reaches a parser through :func:`parse`, which is therefore the one place a
vehicle can be tied to a storefront. Dealer-group platforms serve EVERY rooftop
in the group from the account the queried store sits on, so the queried host is
NOT evidence of who sells the car. :func:`resolve_rooftop_attribution` reads the
per-vehicle rooftop identity the feed itself carries and refuses any row it
cannot positively tie to the store being scanned.
"""
import logging
from functools import lru_cache

from backend.parsers.carscommerce import detect as detect_carscommerce
from backend.parsers.carscommerce import parse as parse_carscommerce
from backend.parsers.chapman import detect as detect_chapman
from backend.parsers.chapman import parse as parse_chapman
from backend.parsers.dealer_dot_com import parse as parse_dealer_dot_com
from backend.parsers.dealer_eprocess import parse as parse_dealer_eprocess
from backend.parsers.dealermasters import parse as parse_dealermasters
from backend.parsers.generic_json import parse as parse_generic_json
from backend.parsers.html_cards import parse as parse_html_cards
from backend.parsers.oneaudi import parse as parse_oneaudi
from backend.parsers.wp_vehicles_index import parse as parse_wp_vehicles_index
from backend.parsers.dealer_on import parse as parse_dealer_on
from backend.parsers.jazel import parse as parse_jazel
from backend.parsers.motive_ridemotive import parse as parse_motive_ridemotive
from backend.parsers.overfuel import parse as parse_overfuel
from backend.parsers.rooftop_aliases import roster_name_aliases  # noqa: F401 - re-export; the gate resolves aliases through this name
from backend.parsers.sister_tv import parse as parse_sister_tv
from backend.parsers.team_velocity import detect as detect_team_velocity
from backend.parsers.team_velocity import parse as parse_team_velocity
from backend.parsers.typesense import parse as parse_typesense


def _parse_autowall(raw_data, *, base_url="", dealer_id="", dealer_name="", dealer_url=""):
    """autoWALL vehicles come either pre-parsed (browser path: ``{"inventory": [...]}``)
    or as one raw SRP page of HTML (HTTP recipe replay of ``/gs-vehicle/list?page=N``,
    Long of Athens 2026-09-24: 309 cars, 25 per page, ``data-vin`` on every card)."""
    if isinstance(raw_data, str):
        from backend.scanner.scrapers.autowall import parse_autowall_inventory_html

        return parse_autowall_inventory_html(raw_data, base_url=base_url or dealer_url, dealer_id=dealer_id,
                                             dealer_name=dealer_name or dealer_id, dealer_url=dealer_url or base_url)
    if isinstance(raw_data, dict) and isinstance(raw_data.get("inventory"), list):
        return [v for v in raw_data["inventory"] if isinstance(v, dict) and v.get("vin")]
    return []


def _parse_cosmos_cards(raw_data, *, base_url="", dealer_id="", dealer_name="", dealer_url=""):
    """DealerOn cosmos SRP bodies ({"DisplayCards": [{"VehicleCard": ...}]}).

    Dealers on this platform are often provider-hinted "dealer_dot_com", so this
    shape must be reachable through auto-detect, not just the declared parser.
    """
    if not (isinstance(raw_data, dict) and isinstance(raw_data.get("DisplayCards"), list)):
        return []
    from backend.scanner.scrapers.dealer_on import _extract_vehicles_from_srp_body

    return _extract_vehicles_from_srp_body(
        raw_data, base_url, dealer_id, dealer_name or dealer_id, dealer_url or base_url
    )


PARSERS = {
    "carscommerce": parse_carscommerce,
    "dealer_dot_com": parse_dealer_dot_com,
    "dealer_on": parse_dealer_on,
    "dealer_on_cosmos": _parse_cosmos_cards,
    "typesense": parse_typesense,
    "sister_tv": parse_sister_tv,
    "team_velocity": parse_team_velocity,
    "dealer_eprocess": parse_dealer_eprocess,
    "autowall": _parse_autowall,
    "motive_ridemotive": parse_motive_ridemotive,
    "overfuel": parse_overfuel,
    "chapman": parse_chapman,
    "jazel": parse_jazel,
    "dealermasters": parse_dealermasters,
    "wp_vehicles_index": parse_wp_vehicles_index,
    "oneaudi": parse_oneaudi,
    "html_cards": parse_html_cards,
    "generic_json": parse_generic_json,
}

_log = logging.getLogger(__name__)
_warned_unknown_providers: set[str] = set()


# ─────────────────────────── rooftop attribution ────────────────────────────
#
# Parsers do not decide attribution: a parser that can see a per-vehicle rooftop
# identifier attaches it verbatim as ``row["_rooftop"]`` and the gate decides.
# The gate moved to ``backend/attribution/`` on 2026-10-01 (rooftop.py: the
# tiers and resolver; gate.py: the per-page and all-rows entry points). Every
# name is re-exported here so ``from backend.parsers import ...`` keeps working;
# ``resolve_rooftop_attribution`` and ``roster_name_aliases`` on THIS module are
# what the package calls through, so stubbing them here still takes effect.
from backend.attribution.gate import gate_page as _gate_page  # noqa: E402
from backend.attribution.rooftop import (  # noqa: E402,F401
    _ROOFTOP_NAME_STOPWORDS,
    _LOCALITY_ONLY_TIERS,
    _WEAK_NAME_TIERS,
    _DEPARTMENT_SUFFIXES,
    _STREET_TIERS,
    _WEAK_STREET_SOURCES,
    _ADDRESS_SHAPED,
    _nrm,
    _tokens,
    _STELLANTIS_INITIALS,
    _STELLANTIS_CORE,
    _STELLANTIS_CLUSTER_TOKEN,
    _expand_brand_initials,
    _brand_canonical_tokens,
    _host,
    _HTML_TAG,
    _LINE_BREAK,
    _TRAILING_SPLIT,
    _label_lines,
    _looks_like_address,
    _looks_like_store_name,
    _SLUG_SHAPED,
    _rooftop_slug,
    _rooftop_name,
    _rooftop_identity,
    _rooftop_of,
    _STREET_WORDS,
    _street_key,
    _rooftop_place,
    _Rooftop,
    _unique,
    _pick_target,
    resolve_rooftop_attribution,
    _department_base,
    _merge_department_rooftops,
    _resolve_rooftop_attribution_inner,
    _scorer_enabled,
    _resolve_via_scorer,
)


def _parse_rows(provider: str, raw_data, _kwargs: dict, dealer_id: str) -> list[dict]:
    # CarsCommerce search payloads are provider-hinted "dealer_dot_com" /
    # "dealer_inspire", but the generic parser drops colors, carfax, features,
    # packages and warranty. Route by shape BEFORE the declared provider so the
    # rich fields the feed returns are actually captured.
    if provider != "carscommerce" and detect_carscommerce(raw_data):
        # Once the CarsCommerce shape is detected, its parser OWNS the payload —
        # even an empty result must not fall through to generic parsers, or the
        # rooftop-attribution gate would be silently bypassed (the generic parser
        # does not read ``source_id``, so its rows carry no rooftop evidence).
        return list(parse_carscommerce(raw_data, **_kwargs))

    # Team Velocity feeds are provider-hinted "dealer_dot_com", but the generic
    # parser drops the feed's distinct field names (sellingPrice, driveTrain,
    # engineCylinders, city/highwayMpg) and never seeds the image-completion
    # placeholder. Route by shape BEFORE the declared provider so the dedicated
    # handler owns TV rows (its VDP image/carfax completion runs post-scan).
    if provider != "team_velocity" and detect_team_velocity(raw_data):
        tv_rows = list(parse_team_velocity(raw_data, **_kwargs))
        if tv_rows:
            return tv_rows

    # Chapman Auto Group dealers (chapmanbmwchandler-com, chapmanfordaz-com) have
    # been captured with the recipe mis-tagged "dealer_dot_com" in production —
    # the generic parser guesses via loose key matching and returns rows, so
    # nothing flags it as broken, but it doesn't know colorExt/drive/fuel/body
    # and silently drops color/drivetrain/fuel/body_style for every row. Route
    # by shape BEFORE the declared provider so a mis-tagged recipe can never
    # again silently degrade this dealer's data.
    if provider != "chapman" and detect_chapman(raw_data):
        chapman_rows = list(parse_chapman(raw_data, **_kwargs))
        if chapman_rows:
            return chapman_rows

    fn = PARSERS.get(provider)
    if fn:
        result = list(fn(raw_data, **_kwargs))
        if result:
            return result
        # Declared parser returned nothing — fall through to others
    elif provider not in _warned_unknown_providers:
        _warned_unknown_providers.add(provider)
        _log.debug("Unknown provider %r for dealer_id=%s — trying auto-detect", provider, dealer_id)

    # Flat snake_case vehicle lists (one-off sites: honestcardeal-com's Supabase
    # feed, 2026-09-26) go to the generic parser BEFORE the platform parsers: the
    # dealer.com fallback claims them and maps price 0 / placeholder image.
    if provider != "generic_json":
        generic = list(parse_generic_json(raw_data, **_kwargs))
        if generic:
            _log.debug("Auto-detected provider 'generic_json' for dealer_id=%s (declared: %r)", dealer_id, provider)
            return generic
    # Try remaining parsers
    for p_name, p_fn in PARSERS.items():
        if p_name in (provider, "generic_json"):
            continue  # already tried
        try:
            result = list(p_fn(raw_data, **_kwargs))
            if result:
                _log.debug("Auto-detected provider %r for dealer_id=%s (declared: %r)", p_name, dealer_id, provider)
                return result
        except Exception:
            continue
    return []


def parse(
    provider: str,
    raw_data,
    base_url: str,
    dealer_id: str,
    dealer_name: str = "",
    dealer_url: str = "",
    rejected_out: list | None = None,
    dealer_address: str = "",
    dealer_city: str = "",
    dealer_state: str = "",
    dealer_zip: str = "",
    dealer_address_source: str = "",
    trust_feed_scope: bool = False,
):
    """Parse *raw_data* into vehicle rows, resolving which store each belongs to.

    ``trust_feed_scope=True`` skips the rooftop gate: the caller replayed a
    recipe whose request is already filtered to this store's own feed ids
    (CarsCommerce ``facetFilters.source_id``, verified at synthesis by replaying
    the filter and seeing one rooftop in the store's city). Mall of Georgia
    Mazda files under two feed ids that stamp the same store two ways — a bare
    "Buford, GA" and a "3546 Highway 20" address block — and the gate, which can
    only ever pick ONE rooftop, refused 309 of 424 verified rows (2026-09-25).

    Pass ``rejected_out`` and you get back ONLY this store's rows, with the
    sibling rooftops' rows in that list — what the delta scanner wants, since it
    also has to disown VINs it stamped onto this store on earlier runs.

    Without ``rejected_out`` the sibling rows come back too, each carrying
    ``_rooftop_reject``. They are deliberately not swallowed here: the recipe
    replay counts VINs from this return value to decide whether a page was the
    last one and whether the endpoint is worth using at all, so silently
    emptying it truncates the page walk and retires the recipe of exactly the
    group-feed dealers this gate exists for. Storage is where the refusal is
    enforced — ``backend.scanner.database.upsert_vehicles`` drops every marked
    row, so a marked row can never reach ``cars``.

    The ``dealer_address``/``city``/``state``/``zip`` arguments are this store's
    postal address from the registry. They are the ONLY way to identify the
    store in a group feed whose rooftops are address blocks carrying no store
    name; omit them on such a feed and every page refuses every row. Callers
    that have a roster should pass them — see
    ``backend.scanner.delta_scan._roster_place``. ``dealer_address_source`` is
    that street's provenance (``dealerships.street_address_source``, V003):
    weak scrape tiers demote a street match so it cannot un-list on its own.
    """
    _kwargs = dict(base_url=base_url, dealer_id=dealer_id, dealer_name=dealer_name, dealer_url=dealer_url)
    rows = _parse_rows(provider, raw_data, _kwargs, dealer_id)
    return _gate_page(
        rows, raw_data, dealer_id=dealer_id, dealer_name=dealer_name, dealer_url=dealer_url,
        rejected_out=rejected_out, trust_feed_scope=trust_feed_scope,
        dealer_address=dealer_address, dealer_city=dealer_city,
        dealer_state=dealer_state, dealer_zip=dealer_zip,
        dealer_address_source=dealer_address_source,
    )


@lru_cache(maxsize=256)
def _cached_roster_place_items(dealer_url: str) -> tuple[tuple[str, str], ...]:
    # roster_place already swallows lookup errors and returns {} (the registry
    # is an optimisation, never a parse blocker); cached because recipe
    # validation calls parse_kept once per page of the same dealer.
    from backend.scanner.rooftop_disown import roster_place

    return tuple(sorted((roster_place(dealer_url) or {}).items()))


def parse_kept(
    provider: str,
    raw_data,
    *,
    base_url: str,
    dealer_id: str,
    dealer_name: str = "",
    dealer_url: str = "",
    **place_override,
):
    """``parse()`` returning ONLY this store's rows, with the store identified.

    ``dealer_city`` / ``dealer_state`` / ``dealer_zip`` / ``dealer_address`` passed
    explicitly win over the registry lookup (a place learned from the site's own
    <title> when the store is not in the registry — dealer_place.learn_place).

    Discarding the rooftop gate's refusals is only safe when the gate can tell
    which rooftop this store IS — on a group feed whose rooftops are address
    blocks with no store name, ``parse(..., rejected_out=[])`` without the
    ``dealer_address``/``city``/``state``/``zip`` kwargs refuses every row and
    silently returns nothing (the terrylabontechevy.com failure). This wrapper
    looks the roster place up itself, so a kept-rows caller cannot forget it.
    Best-effort: a dealer missing from the registry falls back to the name and
    host tiers, exactly as before.
    """
    place = dict(_cached_roster_place_items(dealer_url or base_url))
    place.update({k: v for k, v in place_override.items() if k in ("dealer_address", "dealer_city", "dealer_state", "dealer_zip") and v})
    if place_override.get("trust_feed_scope"):
        place["trust_feed_scope"] = True
    return parse(
        provider, raw_data,
        base_url=base_url, dealer_id=dealer_id,
        dealer_name=dealer_name, dealer_url=dealer_url or base_url,
        rejected_out=[],
        **place,
    )
