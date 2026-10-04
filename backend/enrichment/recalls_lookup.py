"""NHTSA recall lookup shared by the car-page recall API and the /recalls page.

Moved out of ``backend/main.py`` (monolith audit 2026-10-01, W1). Validates the
VIN, applies the per-client rate limit and calls the NHTSA recalls API.
"""

from __future__ import annotations

from backend.config import Config
from backend.utils.ip_rate_limit import allow_request


def nhtsa_recalls_lookup_payload(
    *,
    vin_raw: str,
    make: str | None = None,
    model: str | None = None,
    year: str | None = None,
    rate_key: str,
) -> tuple[dict, int]:
    """Shared NHTSA recall lookup for HTML page and JSON API."""
    from backend.enrichment.vehicle_history_intelligence import fetch_nhtsa_recalls
    from backend.utils.hybrid_search import _normalize_listings_vin_query

    if not allow_request(
        rate_key,
        max_events=Config.RATE_LIMIT_NHTSA_RECALLS_PER_MIN,
        window_seconds=60.0,
    ):
        return {"ok": False, "error": "rate_limited", "recalls": []}, 429

    vin_norm = _normalize_listings_vin_query(vin_raw) if vin_raw else None
    ymm_make = (make or "").strip() or None
    ymm_model = (model or "").strip() or None
    ymm_year = (year or "").strip() or None
    vehicle_label = (
        f"{ymm_year} {ymm_make} {ymm_model}".strip()
        if ymm_make and ymm_model and ymm_year
        else None
    )

    if vin_raw and not vin_norm:
        return {
            "ok": False,
            "error": "invalid_vin",
            "vin": None,
            "vin_raw": vin_raw,
            "recalls": [],
            "vehicle_label": vehicle_label,
        }, 400
    if not vin_norm:
        return {
            "ok": False,
            "error": "missing_vin",
            "recalls": [],
            "vehicle_label": vehicle_label,
        }, 400

    recalls, api_err = fetch_nhtsa_recalls(
        vin_norm,
        make=ymm_make,
        model=ymm_model,
        year=ymm_year,
    )
    payload: dict = {
        "ok": api_err is None,
        "vin": vin_norm,
        "recalls": recalls,
        "vehicle_label": vehicle_label,
        "error": api_err,
    }
    if api_err:
        return payload, 502
    return payload, 200
