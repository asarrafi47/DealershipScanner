"""Fuel/TCO lookup API: cached EIA regional fuel + residential electricity rates.

The live-gas-prices cache state (``_LIVE_GAS_PRICES_PATH``,
``_live_gas_prices_cache``, ``_live_gas_prices_cache_mtime``) intentionally
stays on ``backend.main`` — tests monkeypatch those attributes there and
``importlib.reload(backend.main)`` resets them. See ``_shared`` docstring.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from flask import jsonify, request, session

from backend.listings.geo_session import listings_geo_kwargs_from_session
from backend.routes._shared import main_module

_logger = logging.getLogger(__name__)

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
    main = main_module()
    path = main._LIVE_GAS_PRICES_PATH
    if not path.is_file():
        return None
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None
    if main._live_gas_prices_cache is not None and main._live_gas_prices_cache_mtime == mtime:
        return main._live_gas_prices_cache
    try:
        with path.open(encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        _logger.warning("live_gas_prices.json unreadable: %s", exc)
        return None
    if not isinstance(payload, dict):
        return None
    main._live_gas_prices_cache = payload
    main._live_gas_prices_cache_mtime = mtime
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


def register(app) -> None:
    """Attach routes to ``app`` keeping the original bare endpoint names."""
    app.add_url_rule("/api/fuel/lookup", view_func=api_fuel_lookup, methods=["GET"])
