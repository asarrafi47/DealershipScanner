"""
HTTP-first recipe synthesis: build a replayable inventory-API recipe for a
dealer WITHOUT launching a browser, for platforms we already understand.

The scanner normally learns each dealer's inventory API by watching Playwright
network traffic and promoting it to a recipe (see ``backend.scanner.recipes``).
The key insight this module delivers is that the browser is needed once per
*platform*, not per *dealer*: once we know a platform's endpoint shape, any
dealer on that platform can have its recipe SYNTHESIZED over plain HTTP by

  1. fetching the dealer's page (:func:`fetch_dealer_html`),
  2. fingerprinting the platform from HTML markers (:func:`fingerprint_platform`),
  3. extracting the per-dealer params (domain / account id / api key), and
  4. filling a platform template borrowed from a real captured recipe
     (:func:`synthesize_recipe`).

This module NEVER launches a browser; all fetches go through the proxy-aware
``backend.scanner.http_fetch.open_url``. A synthesized recipe is only a
*candidate* — callers MUST validate it by replaying it over HTTP
(:func:`validate_recipe`) and counting real VINs before trusting or saving it.
A synthesized recipe that yields no VINs means the platform template no longer
fits this dealer and the browser is still required.

Adding a platform later is one entry in :data:`PLATFORM_TEMPLATES`.

Layout (audit F-6, 2026-10): this module is now a FACADE over the
``backend.scanner.synth`` package, which owns the code:

  synth/http.py                 headers, pacing, challenge detection, fetches
  synth/common.py               _origin, reference-recipe lookup
  synth/platforms/<name>.py     one module per platform template (detect + synth)
  synth/registry.py             PLATFORM_TEMPLATES, fingerprint/synthesize, HTML harvest
  synth/validate.py             shim; validate_recipe + replay walkers live in
                                backend/scanner/recipe_validation.py

Every name callers used from here is re-exported, so imports keep working. A
test that monkeypatches a helper must patch the synth module that LOOKS IT UP
(e.g. ``synth.http._fetch_impersonated`` for fetch_dealer_html,
``synth.platforms.overfuel._dep_fetch_html`` for _synth_overfuel): patching this
facade does not reach code that moved. The pacing clock (``_last_fetch_at``)
lives in ``synth.http`` and is not mirrored here.
"""
from __future__ import annotations

import logging
from pathlib import Path

from backend.scanner.http_fetch import open_url  # noqa: F401
from backend.scanner.recipes import (  # noqa: F401
    recipe_is_store_scoped,
    PAGINATION_ALGOLIA,
    PAGINATION_CARSCOMMERCE,
    PAGINATION_DEALER_COM,
    PAGINATION_DEP_SRP,
    PAGINATION_GRAPHQL_OFFSET,
    PAGINATION_HTML_PAGE,
    PAGINATION_JAZEL_SRP,
    PAGINATION_NONE,
    PAGINATION_COSMOS_PT,
    PAGINATION_PAGE_QUERY,
    PAGINATION_TYPESENSE,
    EndpointRecipe,
    _mutate_for_page,
    _replay_request,
    _unique_vins,
    _url_for_page,
    load_recipes,
)
from backend.scanner.synth.http import (  # noqa: F401
    _BROWSER_UA,
    _browser_headers,
    _MIN_FETCH_INTERVAL,
    _pace_lock,
    _pace,
    _CHALLENGE_MARKERS,
    _CHALLENGE_BEACON_MARKERS,
    _MIN_REAL_HTML_BYTES,
    _MAX_CHALLENGE_SHELL_BYTES,
    looks_like_challenge,
    _RETRYABLE_STATUS,
    _fetch_impersonated,
    fetch_dealer_html,
    _dep_fetch_page,
    _DEP_ESCALATE_STATUSES,
    _dep_fetch_html,
    _cosmos_get_json,
)
from backend.scanner.synth.common import (  # noqa: F401
    _origin,
    _load_reference_recipe,
)
from backend.scanner.synth.platforms.dealer_com import (  # noqa: F401
    _DEALER_COM_REF_ID,
    _DEALER_COM_REF_SITEID,
    _DEALER_COM_INVENTORY_PATH,
    _SITEID_RE,
    _detect_dealer_com,
    _extract_dealer_com_siteid,
    _synth_dealer_com,
)
from backend.scanner.synth.platforms.carscommerce import (  # noqa: F401
    _CARSCOMMERCE_REF_ID,
    _CARSCOMMERCE_HOST,
    _CCID_RES,
    _APIKEY_RE,
    _detect_carscommerce,
    _extract_ccid,
    _extract_carscommerce_key,
    _synth_carscommerce,
)
from backend.scanner.synth.platforms.carscommerce_scope import (  # noqa: F401
    _CC_CUSTOM_FACETS,
    _CC_LOCATION_MAP_RE,
    _cc_location_facets,
    _CC_PLACE_ONLY_RE,
    _CC_SINGLE_STORE_MAX_UNSTAMPED,
    _CC_SINGLE_STORE_MAX,
    _CC_FACET_CHUNK,
    _cc_facet_census,
    _cc_rooftop_sig,
    _cc_rooftop_place_of,
    _cc_rooftop_place,
    _cc_verify_store_filter,
    _nrm_facet,
    _carscommerce_store_filter,
)
from backend.scanner.synth.platforms.dealeron_cosmos import (  # noqa: F401
    _COSMOS_PATH,
    _COSMOS_PAGE_SIZE,
    _COSMOS_SRP_PATHS,
    _COSMOS_SRP_PATHS_NEW,
    _COSMOS_ACCOUNT_RES,
    _COSMOS_ITEMLIST_RE,
    _detect_dealer_on_cosmos,
    _extract_cosmos_account,
    _extract_cosmos_pagecfg,
    _cosmos_pagecfg_from_paths,
    _synth_dealer_on_cosmos,
)
from backend.scanner.synth.platforms.typesense import (  # noqa: F401
    _TYPESENSE_REF_ID,
    _TYPESENSE_REF_COLLECTION,
    _TS_HOST_RES,
    _TS_KEY_RES,
    _TS_COLLECTION_RES,
    _extract_ts_host,
    _extract_ts_key,
    _extract_ts_collection,
    _TS_SRP_PATHS,
    _TS_DEFAULT_QUERY_BY,
    _detect_typesense,
    _ts_ref_key,
    _ts_default_body,
    _synth_typesense,
)
from backend.scanner.synth.platforms.sister_tv import (  # noqa: F401
    _detect_sister_tv,
)
from backend.scanner.synth.platforms.team_velocity import (  # noqa: F401
    _TEAM_VELOCITY_FEED,
    _TEAM_VELOCITY_FEEDS,
    _detect_team_velocity,
    _tv_feed_recipe,
    _synth_team_velocity,
)
from backend.scanner.synth.platforms.dealer_eprocess import (  # noqa: F401
    _DEP_SRP_PATHS,
    _DEP_COUNT_RE,
    _DEP_PAGE_SIZE_SELECT_RE,
    _DEP_OPTION_VALUE_RE,
    _DEP_CT_RE,
    _DEP_PAGE_SIZE_PARAM,
    _DEP_PAGE_SIZE_CAP,
    _detect_dealer_eprocess,
    _dep_page_size,
    _with_query_param,
    _dep_page_vins,
    _DEP_LC_LINK_RE,
    _dep_store_lc,
    _dep_srp_recipe,
    _DEP_NAV_SRP_RE,
    _dep_nav_srp_paths,
    _dep_condition_bucket,
    _dep_pick_per_condition,
    _synth_dealer_eprocess,
)
from backend.scanner.synth.platforms.motive import (  # noqa: F401
    _MOTIVE_APP_ID,
    _MOTIVE_API_KEY,
    _MOTIVE_INDEX_PREFIX,
    _MOTIVE_SORT,
    _MOTIVE_HITS_PER_PAGE,
    _MOTIVE_APPID_RE,
    _MOTIVE_APIKEY_RE,
    _MOTIVE_INDEX_RE,
    _MOTIVE_DEALER_RES,
    _detect_motive,
    _extract_motive_dealer_id,
    _synth_motive,
)
from backend.scanner.synth.platforms.overfuel import (  # noqa: F401
    _OVERFUEL_SRP_PATH,
    _detect_overfuel,
    _synth_overfuel,
)
from backend.scanner.synth.platforms.nabthat import (  # noqa: F401
    _NABTHAT_SRP_PATHS,
    _detect_nabthat,
    _nabthat_page_vins,
    _nabthat_recipe,
    _synth_nabthat,
)
from backend.scanner.synth.platforms.chapman import (  # noqa: F401
    _CHAPMAN_API_HOST,
    _CHAPMAN_ARKONA_RES,
    _CHAPMAN_CONDITIONS,
    _detect_chapman,
    _extract_chapman_arkona,
    _chapman_recipe,
    _synth_chapman,
)
from backend.scanner.synth.platforms.jazel import (  # noqa: F401
    _JAZEL_SRP_PATH,
    _detect_jazel,
    _synth_jazel,
)
from backend.scanner.synth.platforms.dealermasters import (  # noqa: F401
    _DEALERMASTERS_INDEX_ROUTES,
    _detect_dealermasters,
    _dealermasters_static_query_hashes,
    _synth_dealermasters,
)
from backend.scanner.synth.platforms.wp_vehicles import (  # noqa: F401
    _WP_VEHICLES_PATH,
    _detect_wp_vehicles_index,
    _fetch_wp_vehicles,
    _synth_wp_vehicles_index,
)
from backend.scanner.synth.platforms.autowall import (  # noqa: F401
    _AUTOWALL_LIST_PATH,
    _AUTOWALL_TOTAL_RE,
    _detect_autowall,
    _synth_autowall,
)
from backend.scanner.synth.platforms.oneaudi import (  # noqa: F401
    _ONEAUDI_GRAPHQL,
    _ONEAUDI_SRP_PATHS,
    _ONEAUDI_DEALER_RE,
    _ONEAUDI_STATIMPORT_RE,
    _ONEAUDI_MARKET_RE,
    _ONEAUDI_PAGE_SIZE,
    _ONEAUDI_QUERY,
    _detect_oneaudi,
    _oneaudi_decoded,
    _oneaudi_inputs,
    _oneaudi_body,
    _synth_oneaudi,
)
from backend.scanner.synth.platforms.html_cards import (  # noqa: F401
    _HTML_CARDS_PATHS,
    _HTML_CARDS_MIN,
    _detect_html_cards,
    _synth_html_cards,
)
from backend.scanner.synth.registry import (  # noqa: F401
    PlatformTemplate,
    PLATFORM_TEMPLATES,
    _TEMPLATES_BY_NAME,
    fingerprint_platform,
    is_synthesizable,
    synthesize_recipes,
    synthesize_recipe,
    _HTML_HARVEST_SRP_PATHS,
    harvest_html_vehicles,
    detect_html_harvest,
)
from backend.scanner.recipe_validation import (  # noqa: F401
    _VALIDATE_MAX_PAGES,
    validate_recipe,
    _validate_json_feed,
    _validate_dep,
    _validate_html_walk,
    _validate_cosmos,
)

logger = logging.getLogger("scanner")

RECIPES_DIR = Path("workspace") / "recipes"
