"""Rooftop attribution: "is this car this store's?", decided in one package.

Monolith audit 2026-10-01 (scanner.md F-19, enrich.md P1 #4) counted seven
places deciding it. This package holds the scan-time ones:

* :mod:`.rooftop` — the per-payload gate (``resolve_rooftop_attribution``,
  ``_pick_target`` and its tiers), moved out of ``backend/parsers/__init__.py``
* :mod:`.match`   — the same decision as a scored matcher, behind
  ``SCANNER_ROOFTOP_SCORER`` (moved from ``backend/scanner/rooftop_match.py``)
* :mod:`.gate`    — the entry points: ``gate_page`` (one payload),
  ``parse_page`` (parse + gate one payload for a store), ``decide`` (the ONE
  all-rows pass), the store-scope (``_feed_scoped``) rule, ``DealerCtx`` and
  ``Decision``
* :mod:`.place`   — the ONE store-place lookup (``store_place``) and the
  registry row it starts from (``registry_place``)
* :mod:`.disown`  — which refusals may un-list a car, and the un-listing

Typical scan path::

    ctx = DealerCtx.for_store(dealer_id, name, url)      # blocking: run in a thread
    refused: list[dict] = []
    rows = [r for body in pages for r in parse_page(provider, body, ctx, refused)]
    decision = decide(rows + refused, ctx)               # kept / refused / reasons

Old import paths keep working: ``backend.parsers`` re-exports the gate's names,
``backend.scanner.rooftop_match`` is an alias of :mod:`.match`,
``backend.scanner.rooftop_disown`` and ``backend.scanner.dealer_place``
re-export the policy and place functions.

Not here (yet): the sister-store location filter
(``backend.scanner.dealer.location.filter_sister_store_vehicles``, a separate
fuzzy model over ``_lot_location`` that defers to this gate for every row
carrying a rooftop), the browser-era card-location stamp
(``inventory_card_location``, dead under HTTP-only) and the write-time VIN-owner
guard in ``backend.scanner.database.upsert_vehicles``.
"""
from backend.attribution.disown import EVIDENCE_BACKED_REJECTS, disown_foreign_rooftop_vins, split_refusals
from backend.attribution.gate import (
    FEED_SCOPED,
    REJECT_KEY,
    DealerCtx,
    Decision,
    decide,
    gate_page,
    mark_payload_feed_scoped,
    parse_page,
    payload_is_feed_scoped,
)
from backend.attribution.place import gate_place, registry_place, store_place
from backend.attribution.rooftop import resolve_rooftop_attribution

__all__ = [
    "EVIDENCE_BACKED_REJECTS",
    "FEED_SCOPED",
    "REJECT_KEY",
    "DealerCtx",
    "Decision",
    "decide",
    "disown_foreign_rooftop_vins",
    "gate_page",
    "gate_place",
    "mark_payload_feed_scoped",
    "parse_page",
    "payload_is_feed_scoped",
    "registry_place",
    "resolve_rooftop_attribution",
    "split_refusals",
    "store_place",
]
