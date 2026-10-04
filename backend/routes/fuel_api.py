"""Fuel/TCO lookup API: cached EIA regional fuel + residential electricity rates.

The live-gas-prices cache state (``_LIVE_GAS_PRICES_PATH``,
``_live_gas_prices_cache``, ``_live_gas_prices_cache_mtime``) lives here, in the
only module that reads and writes it; tests monkeypatch it on this module.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

from flask import jsonify, request, session

from backend.listings.geo_session import listings_geo_kwargs_from_session

_logger = logging.getLogger(__name__)

_LIVE_GAS_PRICES_PATH = (
    Path(__file__).resolve().parent.parent / "dictionary" / "derived" / "live_gas_prices.json"
)
_live_gas_prices_cache: dict[str, Any] | None = None
_live_gas_prices_cache_mtime: float | None = None

_FUEL_TIER_ALIASES: dict[str, str] = {
    "regular": "regular",
    "mid": "mid",
    "midgrade": "mid",
    "mid-grade": "mid",
    "mid_grade": "mid",
    "premium": "premium",
    "diesel": "diesel",
}
_FUEL_TIER_JSON_KEYS: dict[str, tuple[str, ...]] = {
    "regular": ("regular", "Regular"),
    "mid": ("mid", "midgrade", "midGrade", "mid-grade", "Mid-Grade", "Mid Grade"),
    "premium": ("premium", "Premium"),
    "diesel": ("diesel", "Diesel"),
}


def _normalize_fuel_tier_param(raw: str | None) -> str:
    key = (raw or "regular").strip().lower().replace(" ", "-")
    if key in _FUEL_TIER_ALIASES:
        return _FUEL_TIER_ALIASES[key]
    if "premium" in key:
        return "premium"
    if "diesel" in key:
        return "diesel"
    if "mid" in key:
        return "mid"
    return "regular"


def _load_live_gas_prices_payload() -> dict[str, Any] | None:
    global _live_gas_prices_cache, _live_gas_prices_cache_mtime
    path = _LIVE_GAS_PRICES_PATH
    if not path.is_file():
        return None
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None
    if _live_gas_prices_cache is not None and _live_gas_prices_cache_mtime == mtime:
        return _live_gas_prices_cache
    try:
        with path.open(encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        _logger.warning("live_gas_prices.json unreadable: %s", exc)
        return None
    if not isinstance(payload, dict):
        return None
    _live_gas_prices_cache = payload
    _live_gas_prices_cache_mtime = mtime
    return payload


def _fuel_market_payload() -> dict[str, Any]:
    """Load cached EIA market data, falling back to 2026 anchors when missing."""
    payload = _load_live_gas_prices_payload()
    if payload:
        return payload
    from backend.cron.sync_gas_prices import fallback_payload

    return fallback_payload()


def _live_gas_region_block(payload: dict[str, Any], state_code: str) -> dict[str, Any] | None:
    raw = (state_code or "").strip()
    if not raw:
        return None
    code = raw.lower() if raw.lower() == "national" else raw.upper()
    states = payload.get("states")
    if isinstance(states, dict):
        for key in (code, raw, raw.upper(), raw.lower()):
            block = states.get(key)
            if isinstance(block, dict):
                return block
    for key in (code, raw, raw.upper(), raw.lower()):
        block = payload.get(key)
        if isinstance(block, dict):
            return block
    return None


def _live_gas_region_name(block: dict[str, Any], state_code: str, *, used_national: bool) -> str:
    for key in ("region_name", "regionName", "name", "region", "label"):
        val = block.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    if used_national:
        return "National Average"
    return state_code


def _live_gas_rate_from_block(block: dict[str, Any], fuel_tier: str) -> float | None:
    keys = _FUEL_TIER_JSON_KEYS.get(fuel_tier, (fuel_tier,))
    for key in keys:
        raw = block.get(key)
        if raw is None:
            continue
        try:
            rate = float(raw)
        except (TypeError, ValueError):
            continue
        if rate > 0:
            return rate
    prices = block.get("prices")
    if isinstance(prices, dict):
        for key in keys:
            raw = prices.get(key)
            if raw is None:
                continue
            try:
                rate = float(raw)
            except (TypeError, ValueError):
                continue
            if rate > 0:
                return rate
    return None


def _live_electricity_rate_from_block(block: dict[str, Any]) -> float | None:
    for key in ("electricity_rate", "electricityRate", "residential_electricity_rate"):
        raw = block.get(key)
        if raw is None:
            continue
        try:
            rate = float(raw)
        except (TypeError, ValueError):
            continue
        if rate > 0:
            return rate
    return None


def _resolve_live_gas_lookup(state_code: str, fuel_tier: str) -> tuple[float, str] | None:
    payload = _fuel_market_payload()
    state_block = _live_gas_region_block(payload, state_code)
    used_national = False
    if state_block is None:
        state_block = _live_gas_region_block(payload, "national")
        used_national = True
    if state_block is None:
        return None
    rate = _live_gas_rate_from_block(state_block, fuel_tier)
    if rate is None and not used_national:
        national_block = _live_gas_region_block(payload, "national")
        if national_block is not None:
            rate = _live_gas_rate_from_block(national_block, fuel_tier)
            if rate is not None:
                state_block = national_block
                used_national = True
    if rate is None:
        return None
    region_name = _live_gas_region_name(state_block, state_code, used_national=used_national)
    return rate, region_name


def _resolve_live_electricity_lookup(state_code: str) -> tuple[float, str]:
    payload = _fuel_market_payload()
    state_block = _live_gas_region_block(payload, state_code)
    used_national = False
    if state_block is None:
        state_block = _live_gas_region_block(payload, "national")
        used_national = True
    if state_block is None:
        national = payload.get("national")
        if isinstance(national, dict):
            state_block = national
            used_national = True
    rate = _live_electricity_rate_from_block(state_block or {})
    if rate is None and not used_national:
        national_block = _live_gas_region_block(payload, "national")
        if national_block is not None:
            rate = _live_electricity_rate_from_block(national_block)
            if rate is not None:
                state_block = national_block
                used_national = True
    if rate is None:
        national = payload.get("national")
        if isinstance(national, dict):
            rate = _live_electricity_rate_from_block(national)
            if rate is not None:
                state_block = national
                used_national = True
    if rate is None:
        from backend.cron.sync_gas_prices import FALLBACK_NATIONAL_ELECTRICITY

        rate = FALLBACK_NATIONAL_ELECTRICITY
        state_block = state_block or {"region_name": "National Average"}
        used_national = True
    region_name = _live_gas_region_name(state_block, state_code, used_national=used_national)
    return rate, region_name


def _resolve_fuel_lookup_state(request, session_obj: object) -> tuple[str, str | None]:
    """Resolve a two-letter state for fuel lookup; optional ZIP used for resolution."""
    zip_raw = (request.args.get("zip_code") or request.args.get("zip") or "").strip()
    if not zip_raw:
        geo = listings_geo_kwargs_from_session(session_obj)
        zip_raw = str(geo.get("zip_code") or "").strip()
    if zip_raw:
        zip_code = zip_raw[:5]
        if re.fullmatch(r"\d{5}", zip_code):
            try:
                from backend.db.geo import us_postal_meta_for_zip

                meta = us_postal_meta_for_zip(zip_code)
                if meta and meta.get("state_code"):
                    return str(meta["state_code"]).strip().upper(), zip_code
            except Exception:
                pass
    state_raw = (request.args.get("state") or "NC").strip().upper()
    state_code = state_raw[:2] if re.fullmatch(r"[A-Z]{2}", state_raw[:2] or "") else "NC"
    return state_code, None


def api_fuel_lookup():
    """Cached EIA regional fuel and residential electricity rates for TCO."""
    fuel_tier = _normalize_fuel_tier_param(request.args.get("fuel_tier"))
    state_code, zip_code = _resolve_fuel_lookup_state(request, session)
    resolved = _resolve_live_gas_lookup(state_code, fuel_tier)
    electricity_rate, electricity_region = _resolve_live_electricity_lookup(state_code)
    payload_data = _fuel_market_payload()
    if resolved is None:
        national = payload_data.get("national")
        if isinstance(national, dict):
            rate = _live_gas_rate_from_block(national, fuel_tier)
            region_name = str(national.get("region_name") or "National Average")
        else:
            from backend.cron.sync_gas_prices import fallback_payload

            fb = fallback_payload()
            national_fb = fb.get("national") or {}
            rate = _live_gas_rate_from_block(national_fb, fuel_tier) or 4.29
            region_name = str(national_fb.get("region_name") or "National Average")
    else:
        rate, region_name = resolved
    payload: dict[str, Any] = {
        "rate": rate,
        "electricity_rate": electricity_rate,
        "region_name": region_name,
        "electricity_region_name": electricity_region,
        "state": state_code,
        "source": payload_data.get("source"),
    }
    if zip_code:
        payload["zip_code"] = zip_code
    return jsonify(payload)


#: Approximate average COMBINED state + local sales tax rate per state, as a
#: decimal (0.0725 = 7.25%). These are rough, published-order-of-magnitude
#: figures (not pulled from a live feed — the repo has no such data source;
#: see project notes on the VDP finance calculator), meant only to seed a
#: sane default for the payment estimator below. The calculator always shows
#: this as editable and never as an authoritative rate — local county/city
#: add-ons and vehicle-specific rules (trade-in credits, EV surcharges, etc.)
#: are NOT modeled. States with no general sales tax are 0.
_STATE_AVG_SALES_TAX_RATE: dict[str, float] = {
    "AL": 0.0929, "AK": 0.0176, "AZ": 0.0840, "AR": 0.0946, "CA": 0.0882,
    "CO": 0.0781, "CT": 0.0635, "DE": 0.0, "FL": 0.0702, "GA": 0.0738,
    "HI": 0.0444, "ID": 0.0602, "IL": 0.0886, "IN": 0.0700, "IA": 0.0694,
    "KS": 0.0870, "KY": 0.0600, "LA": 0.0956, "ME": 0.0550, "MD": 0.0600,
    "MA": 0.0625, "MI": 0.0600, "MN": 0.0749, "MS": 0.0707, "MO": 0.0829,
    "MT": 0.0, "NE": 0.0694, "NV": 0.0823, "NH": 0.0, "NJ": 0.0660,
    "NM": 0.0772, "NY": 0.0853, "NC": 0.0698, "ND": 0.0696, "OH": 0.0724,
    "OK": 0.0898, "OR": 0.0, "PA": 0.0634, "RI": 0.0700, "SC": 0.0746,
    "SD": 0.0611, "TN": 0.0955, "TX": 0.0820, "UT": 0.0719, "VT": 0.0624,
    "VA": 0.0575, "WA": 0.0886, "WV": 0.0657, "WI": 0.0543, "WY": 0.0536,
    "DC": 0.0600,
}
_NATIONAL_AVG_SALES_TAX_RATE = 0.0715


def api_sales_tax_rate_lookup():
    """Best-effort default sales-tax rate for the VDP finance calculator.

    Resolves a ZIP (or session location) to a state the same way
    ``/api/fuel/lookup`` does, then returns that state's approximate average
    COMBINED state+local rate from ``_STATE_AVG_SALES_TAX_RATE``. This is a
    starting point only — every field it fills stays user-editable on the
    page, and the payload says so explicitly rather than implying an
    authoritative number.
    """
    state_code, zip_code = _resolve_fuel_lookup_state(request, session)
    rate = _STATE_AVG_SALES_TAX_RATE.get(state_code)
    is_state_match = rate is not None
    if rate is None:
        rate = _NATIONAL_AVG_SALES_TAX_RATE
    payload: dict[str, Any] = {
        "state": state_code,
        "rate": rate,
        "rate_pct": round(rate * 100, 2),
        "is_state_match": is_state_match,
        "label": (
            f"Estimated {state_code} average rate"
            if is_state_match
            else "Estimated national average rate"
        ),
        "disclaimer": (
            "Rough average combined state + local rate, not an official quote. "
            "Local county/city add-ons and vehicle-specific rules are not "
            "included — edit this field with your actual rate for an accurate estimate."
        ),
    }
    if zip_code:
        payload["zip_code"] = zip_code
    return jsonify(payload)


def register(app) -> None:
    """Attach routes to ``app`` keeping the original bare endpoint names."""
    app.add_url_rule("/api/fuel/lookup", view_func=api_fuel_lookup, methods=["GET"])
    app.add_url_rule(
        "/api/tax-rate/lookup", view_func=api_sales_tax_rate_lookup, methods=["GET"]
    )
