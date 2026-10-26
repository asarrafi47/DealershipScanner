"""Where is this store? The ONE place lookup the attribution gate is fed with.

Before 2026-10-01 there were two: ``rooftop_disown.roster_place`` (registry
only) and ``dealer_place.roster_place_with_hints`` (registry + the page-learned
street from scan hints), and callers picked inconsistently — the delta path
once missed the learned street the full scan used (audit scanner.md F-2).

* :func:`store_place` — what every scan path hands the gate. Registry town and
  street, completed with the street (or, for a dealer missing from the
  registry, the whole place) learned from the dealer's own page and saved to
  scan hints by ``backend.scanner.dealer_place.learn_place``.
* :func:`registry_place` — the registry row alone (the SQL). An input to
  :func:`store_place`, and the default ``parse_kept`` base (recipe synthesis /
  validation pass the learned place explicitly on top of it).

Both are best-effort: a lookup failure is ``{}``, never an exception — the
registry is an optimisation, never a scan blocker.

The old names stay importable: ``rooftop_disown.roster_place`` is
:func:`registry_place` and ``dealer_place.roster_place_with_hints`` is
:func:`store_place`.
"""
from __future__ import annotations

import logging
import sys
import time
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger("scanner")

# The keys the gate accepts (``resolve_rooftop_attribution`` / ``parse``).
GATE_PLACE_KEYS = ("dealer_address", "dealer_city", "dealer_state", "dealer_zip", "dealer_address_source")


def registry_place(dealer_url: str) -> dict[str, str]:
    """This store's postal address from the dealership registry, or empty dict.

    Group feeds whose rooftops are address blocks rather than store names can
    only be told apart by address, so without this the rooftop gate cannot
    identify the store being scanned and refuses the whole payload. Best-effort
    by design: a dealer missing from the registry just falls back to the name
    and host tiers, which is where it was before.
    """
    if not dealer_url:
        return {}
    try:
        from backend.db.inventory_db import db_conn
        from backend.listings.dealer_registry_match import resolve_car_dealership_registry_id

        reg_id = resolve_car_dealership_registry_id({"dealer_url": dealer_url})
        if not reg_id:
            return {}
        with db_conn() as conn:
            row = conn.execute(
                "SELECT street_address, city, state, zip_code, street_address_source"
                " FROM dealerships WHERE id = ?",
                (reg_id,),
            ).fetchone()
        if not row:
            return {}
        return {
            "dealer_address": row[0] or "",
            "dealer_city": row[1] or "",
            "dealer_state": row[2] or "",
            "dealer_zip": row[3] or "",
            # WHERE the street came from (migrations/V003): the attribution gate
            # demotes a street match to locality weight when the source is a
            # weak scrape tier, so a bad site_text address can refuse writes but
            # can never be the sole evidence that un-lists a car.
            "dealer_address_source": row[4] or "",
        }
    except Exception as exc:  # registry is an optimisation, never a scan blocker
        logger.debug("roster place lookup failed for %s: %s", dealer_url, exc)
        return {}


def _registry_lookup(dealer_url: str) -> dict[str, str]:
    # Through ``backend.scanner.rooftop_disown.roster_place`` when that module is
    # loaded: it is the long-standing name callers and tests stub the registry by.
    mod = sys.modules.get("backend.scanner.rooftop_disown")
    fn = getattr(mod, "roster_place", None) if mod is not None else None
    return (fn or registry_place)(dealer_url)


def store_place(dealer_url: str, dealer_id: str) -> dict[str, Any]:
    """The registry place, completed with the page-learned street from scan hints
    when the roster has the town but no street (the gate matches street-block
    stamps only against a known street; Honda of Huntersville 2026-09-26).

    A dealer missing from the registry (no town) gets the hinted place alone.
    """
    try:
        place = dict(_registry_lookup(dealer_url) or {})
    except Exception:  # noqa: BLE001
        place = {}
    try:
        # scan-hint reading and the gate-key filter live with the place learner
        from backend.scanner import dealer_place as _dp

        hinted = _dp.place_kwargs(_dp.place_from_hints(dealer_id))
    except Exception:  # noqa: BLE001
        hinted = {}
    if not place.get("dealer_city"):
        return hinted
    if not place.get("dealer_address") and hinted.get("dealer_address"):
        place["dealer_address"] = hinted["dealer_address"]
        place["dealer_address_source"] = "site_jsonld"
    return place


# The scan-hints key holding a cosmos store's own DealerOn site id.
OWN_SITE_ID_HINT = "cosmos_site_id"


def own_site_id(dealer_id: str) -> str:
    """This store's own DealerOn cosmos site id from its scan hints, or "".

    A cosmos card normally carries the serving site's ``DealerId`` itself; the
    hint covers a payload without it. Best-effort like every lookup here: "" means
    unknown, and the mapper then keeps the rows unstamped and logs it.
    """
    try:
        from backend.scanner.recipe_store import get_scan_hints

        return str((get_scan_hints(dealer_id) or {}).get(OWN_SITE_ID_HINT) or "").strip()
    except Exception:  # noqa: BLE001
        return ""


def learn_own_site_id(dealer_id: str, site_id: str) -> bool:
    """Save a store's own cosmos site id to its scan hints (profiling tools call this)."""
    site_id = str(site_id or "").strip()
    if not dealer_id or not site_id:
        return False
    try:
        from backend.scanner.recipe_store import set_scan_hints

        return bool(set_scan_hints(dealer_id, {OWN_SITE_ID_HINT: site_id}, merge=True))
    except Exception as exc:  # noqa: BLE001
        logger.debug("own site id hint save failed [%s]: %s", dealer_id, exc)
        return False


# The scan-hints key holding a Dealer eProcess store's own location id (N8.11).
OWN_LOCATION_ID_HINT = "dep_location_id"


def own_location_id(dealer_id: str) -> str:
    """This store's own DEP ``location_id`` from its scan hints, or "".

    A DEP SRP names its own location in ``site_locations``; the hint covers a page
    without it. "" means unknown and the parser then keeps rows unstamped (logged).
    """
    try:
        from backend.scanner.recipe_store import get_scan_hints

        return str((get_scan_hints(dealer_id) or {}).get(OWN_LOCATION_ID_HINT) or "").strip()
    except Exception:  # noqa: BLE001
        return ""


def learn_own_location_id(dealer_id: str, location_id: str) -> bool:
    """Save a DEP store's own location id to its scan hints (profiling tools call this)."""
    location_id = str(location_id or "").strip()
    if not dealer_id or not location_id:
        return False
    try:
        from backend.scanner.recipe_store import set_scan_hints

        return bool(set_scan_hints(dealer_id, {OWN_LOCATION_ID_HINT: location_id}, merge=True))
    except Exception as exc:  # noqa: BLE001
        logger.debug("own location id hint save failed [%s]: %s", dealer_id, exc)
        return False


# --- who owns a detail-page host (N8.5) ---------------------------------------------
#
# The VDP host is never ownership evidence: group sites serve sister cars on their own
# domain. It is used the other way round, to REFUSE a row that carries no store evidence
# when its (non-guessed) detail page lives on a host that belongs to another roster
# store (mbbeverlyhills-com's 1,284 rows came from www.mbontario.com).

# Hosts a store also serves its own cars from, beyond its registry website: dealer_id ->
# hosts. Data, like rooftop_aliases; dealers also learn them into scan hints (below).
HOST_ALIASES: dict[str, tuple[str, ...]] = {}
HOST_ALIAS_HINT = "site_host_aliases"

_HOST_INDEX_TTL_FAIL = 60.0
_host_index_cache: dict[str, Any] = {"index": None, "at": 0.0, "ok": False}


def host_key(value: Any) -> str:
    """Lowercase host without ``www.`` of a URL or bare host, or ""."""
    raw = str(value or "").strip()
    if not raw:
        return ""
    if "//" not in raw:
        raw = "https://" + raw.lstrip("/")
    try:
        host = (urlparse(raw).hostname or "").lower()
    except ValueError:
        return ""
    return host[4:] if host.startswith("www.") else host


def _hinted_host_aliases(dealer_id: str) -> tuple[str, ...]:
    try:
        from backend.scanner.recipe_store import get_scan_hints

        return tuple(h for h in (host_key(x) for x in ((get_scan_hints(dealer_id) or {}).get(HOST_ALIAS_HINT) or [])) if h)
    except Exception:  # noqa: BLE001
        return ()


def host_aliases(dealer_id: str) -> frozenset[str]:
    """Extra hosts that are this store's own: the :data:`HOST_ALIASES` table plus the hint."""
    out = {h for h in (host_key(x) for x in HOST_ALIASES.get(dealer_id, ())) if h}
    return frozenset(out | set(_hinted_host_aliases(dealer_id)))


def _load_host_index() -> dict[str, frozenset[str]]:
    from backend.db.inventory_db import db_conn
    from backend.dev.dealers import slug_from_url

    owners: dict[str, set[str]] = {}
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT website_url, dealer_website_url FROM dealerships"
            " WHERE is_active = 1 AND duplicate_of_id IS NULL"
        ).fetchall()
    for row in rows:
        for url in (row[0], row[1]):
            host = host_key(url)
            if host:
                owners.setdefault(host, set()).add(slug_from_url(host))
    for dealer_id, hosts in HOST_ALIASES.items():
        for h in hosts:
            if host_key(h):
                owners.setdefault(host_key(h), set()).add(dealer_id)
    return {h: frozenset(ids) for h, ids in owners.items()}


def host_owner_index(*, refresh: bool = False) -> dict[str, frozenset[str]]:
    """``host -> dealer_ids`` over the active roster (website hosts plus aliases).

    Cached for the process; a lookup failure is an empty index (so nothing is refused)
    retried after a minute. Tests patch this name or call :func:`reset_host_owner_index`.
    """
    now = time.monotonic()
    c = _host_index_cache
    if c["index"] is not None and not refresh and (c["ok"] or now - c["at"] < _HOST_INDEX_TTL_FAIL):
        return c["index"]
    try:
        c.update(index=_load_host_index(), ok=True)
    except Exception as exc:  # noqa: BLE001 - the roster is an optimisation, never a scan blocker
        logger.debug("host owner index failed: %s", exc)
        c.update(index={}, ok=False)
    c["at"] = now
    return c["index"]


def reset_host_owner_index() -> None:
    _host_index_cache.update(index=None, at=0.0, ok=False)


def vdp_host_owned_elsewhere(row: dict, dealer_id: str, index: dict[str, frozenset[str]] | None = None) -> str:
    """The foreign host when *row*'s non-guessed detail page is on a host that belongs to
    another roster store (and not to *dealer_id*, nor an alias of it); else "".

    Guessed URLs (``_source_url_guessed``), dealer-site homepages and hosts the roster does
    not know are never a finding.
    """
    if not dealer_id or row.get("_source_url_guessed"):
        return ""
    idx = host_owner_index() if index is None else index
    if not idx:
        return ""
    own = host_aliases(dealer_id)
    for key in ("source_url", "_detail_url", "detail_url"):
        url = str(row.get(key) or "").strip()
        host = host_key(url)
        if not host or not urlparse(url if "//" in url else "https://" + url).path.strip("/"):
            continue  # no host, or only a site root
        owners = idx.get(host)
        if owners and dealer_id not in owners and host not in own:
            return host
    return ""


def gate_place(place: dict[str, Any] | None) -> dict[str, str]:
    """Only the keys ``parse`` / ``resolve_rooftop_attribution`` accept."""
    return {k: str(v) for k, v in (place or {}).items() if k in GATE_PLACE_KEYS and v is not None}


__all__ = [
    "GATE_PLACE_KEYS",
    "HOST_ALIASES",
    "HOST_ALIAS_HINT",
    "host_aliases",
    "host_key",
    "host_owner_index",
    "reset_host_owner_index",
    "vdp_host_owned_elsewhere",
    "OWN_LOCATION_ID_HINT",
    "OWN_SITE_ID_HINT",
    "gate_place",
    "learn_own_location_id",
    "learn_own_site_id",
    "own_location_id",
    "own_site_id",
    "registry_place",
    "store_place",
]
