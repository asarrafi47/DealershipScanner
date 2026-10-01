"""Run state shared by the dealer-scan steps, and the result-dict schema."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from backend.scanner.dealer_site_url import dealer_inventory_base_url

logger = logging.getLogger("scanner")


@dataclass
class DealerRun:
    """Everything one ``run_dealer`` call carries from step to step.

    ``result`` is the dict ``run_dealer`` returns (its keys are read by the
    orchestrator, scan_runs persistence and scan_log); the steps fill it in place.
    """

    dealer: dict
    name: str
    url: str
    provider: str
    dealer_id: str
    result: dict[str, Any]
    t0: float
    write_coordinator: Any = None
    context: Any = None
    page: Any = None
    intercept_records: list[tuple[str, Any]] = field(default_factory=list)
    attr_ctx: Any = None
    roster_place: dict[str, str] = field(default_factory=dict)
    # Rows this store's own site served that the feed assigns to a DIFFERENT
    # storefront. Keeping them out of ``all_vehicles`` is what keeps them out
    # of ``result["vins"]`` and out of the reconcile pass's "still seen" VIN
    # set — a mis-attributed VIN in that set is what has been holding
    # pre-existing mis-attributions at listing_active = 1 on this path.
    rooftop_refused: list[dict[str, Any]] = field(default_factory=list)
    all_vehicles: list[dict[str, Any]] = field(default_factory=list)
    reg_id: Any = None

    def elapsed(self) -> float:
        return time.perf_counter() - self.t0


def new_result(dealer_id: str, name: str, provider: str) -> dict[str, Any]:
    return {
        "dealer_id": dealer_id,
        "dealer_name": name,
        "provider": provider,
        "upserted": 0,
        "inventory_rows": 0,
        "deduped_rows": 0,
        "vdps_visited": 0,
        "vehicles_vdp_enriched": 0,
        "gallery_vdp_urls_added": 0,
        "intercept_count": 0,
        "filtered_count": 0,
        "gallery_bins": None,
        "seconds": 0.0,
        "error": None,
        "vins": [],
        "reconcile": None,
        "gallery_vision": None,
        "monroney_vision": None,
        "vdp_phase_timed_out": False,
        "phase_secs": {},
    }


def start_run(dealer: dict, write_coordinator: Any) -> DealerRun:
    """Normalise the manifest URL and lay out the result dict."""
    name = dealer.get("name", "")
    raw_manifest_url = (dealer.get("url") or "").strip()
    url, inv_doc_normalized = dealer_inventory_base_url(raw_manifest_url)
    url = url.rstrip("/")
    if inv_doc_normalized:
        logger.info(
            "Inventory base URL: %s — using site origin (manifest pointed at document: %s)",
            url,
            raw_manifest_url,
        )
    provider = dealer.get("provider", "dealer_dot_com")
    dealer_id = dealer.get("dealer_id", "")
    result = new_result(dealer_id, name, provider)
    return DealerRun(
        dealer=dealer,
        name=name,
        url=url,
        provider=provider,
        dealer_id=dealer_id,
        result=result,
        t0=time.perf_counter(),
        write_coordinator=write_coordinator,
    )
