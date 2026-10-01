"""Parse the captured payloads and settle rooftop attribution.

The per-page gate runs inside ``parse()`` (it is handed ``rejected_out`` and the
store's roster place); the one all-rows pass is ``backend.attribution.decide``.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable

# Rooftop attribution (per-page gate + the one all-rows pass) lives in backend.attribution.
from backend.attribution import DealerCtx, decide
from backend.parsers import parse
from backend.scanner.phases.dealer_run_steps.state import DealerRun

logger = logging.getLogger("scanner")


async def lookup_store_place(run: DealerRun) -> None:
    """This store's place (registry town + the page-learned street from scan
    hints), looked up ONCE and handed to every parse below and to the
    all-rows pass. A group feed whose rooftops are address blocks carrying
    no store name can only be told apart by address; omit it and the gate
    refuses every page of such a feed."""
    run.attr_ctx = await asyncio.to_thread(DealerCtx.for_store, run.dealer_id, run.name, run.url)
    run.roster_place = run.attr_ctx.place


def inventory_parser(run: DealerRun) -> Callable[[Any], list[dict[str, Any]]]:
    """The one inventory ``parse()`` call of the full scan: every payload goes
    through the per-page rooftop gate with this store's place, and the rows it
    refuses land in ``run.rooftop_refused``."""
    provider, url, dealer_id, name = run.provider, run.url, run.dealer_id, run.name
    rooftop_refused, roster_place = run.rooftop_refused, run.roster_place

    def _parse_inventory_raw(raw: Any) -> list[dict[str, Any]]:
        rows = list(
            parse(
                provider, raw, base_url=url, dealer_id=dealer_id, dealer_name=name,
                dealer_url=url, rejected_out=rooftop_refused, **roster_place,
            )
        )
        for v in rows:
            v.setdefault("dealer_name", name)
            v.setdefault("dealer_url", url)
        return rows

    return _parse_inventory_raw


def parse_intercepts(run: DealerRun, parse_raw: Callable[[Any], list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Parse all accumulated payloads (merge by VIN happens downstream). A payload
    object listed twice is parsed once and its rows repeated.

    Parser sets carfax_url from explicit feed links first, then vhr.carfax.com."""
    body_parse_cache: dict[int, list[dict[str, Any]]] = {}
    all_vehicles: list[dict[str, Any]] = []
    for _resp_url, body in run.intercept_records:
        bid = id(body)
        cached = body_parse_cache.get(bid)
        if cached is None:
            cached = parse_raw(body)
            body_parse_cache[bid] = cached
        all_vehicles.extend(cached)
    return all_vehicles


def settle_attribution(run: DealerRun, vehicles: list[dict[str, Any]]) -> None:
    """Rooftop attribution is settled ONCE, here, over the final row set
    (backend.attribution.decide — the same all-rows pass the delta scan
    runs): recovery may have replaced every intercepted row with output
    from a scraper strategy that never went through parse(), and a page
    holding nothing but one sibling's cars only reads as a sibling next to
    this store's own rooftop across the whole capture. Store-scoped recipe
    rows (_feed_scoped) are kept without the re-gate there."""
    attribution = decide(vehicles, run.attr_ctx)
    run.all_vehicles = attribution.kept
    run.rooftop_refused = attribution.refused
    if run.rooftop_refused:
        run.result["rooftop_refused_rows"] = len(run.rooftop_refused)
    run.result["inventory_rows"] = len(run.all_vehicles)
