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
from typing import Any

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


def gate_place(place: dict[str, Any] | None) -> dict[str, str]:
    """Only the keys ``parse`` / ``resolve_rooftop_attribution`` accept."""
    return {k: str(v) for k, v in (place or {}).items() if k in GATE_PLACE_KEYS and v is not None}


__all__ = ["GATE_PLACE_KEYS", "gate_place", "registry_place", "store_place"]
