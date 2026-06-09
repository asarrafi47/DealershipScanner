#!/usr/bin/env python3
"""
Sync regional gasoline and residential electricity rates from EIA API v2.

Writes ``backend/dictionary/derived/live_gas_prices.json`` for ``/api/fuel/lookup``.

Usage:
  python backend/cron/sync_gas_prices.py
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[2]
OUTPUT_PATH = ROOT / "backend" / "dictionary" / "derived" / "live_gas_prices.json"
EIA_API_BASE = "https://api.eia.gov/v2/"
REQUEST_TIMEOUT_SEC = 30
MAX_REQUEST_ATTEMPTS = 3
RETRY_BACKOFF_SEC = 2.0

US_STATE_CODES: tuple[str, ...] = (
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "DC",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
)

STATE_NAMES: dict[str, str] = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District of Columbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}

FALLBACK_NATIONAL_REGULAR = 4.29
FALLBACK_CA_REGULAR = 5.84
FALLBACK_NATIONAL_ELECTRICITY = 0.176

logger = logging.getLogger("sync_gas_prices")


def _eia_api_key() -> str:
    return (os.environ.get("EIA_API_KEY") or "").strip().strip('"')


def _parse_eia_float(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def fetch_eia_weekly_rate(
    route_path: str,
    series_facets: dict[str, list[str] | str],
    *,
    data_field: str = "value",
    frequency: str = "weekly",
) -> float | None:
    """
    Fetch the most recent EIA v2 data point for ``route_path``.

    ``series_facets`` maps facet names (e.g. ``series``, ``sectorid``) to one or
    more facet values. Sorts by ``period`` descending and returns the latest
    numeric value as a float.
    """
    api_key = _eia_api_key()
    if not api_key:
        logger.warning("EIA_API_KEY is not configured")
        return None

    route = route_path.strip("/")
    url = f"{EIA_API_BASE}{route}/data/"
    params: dict[str, Any] = {
        "api_key": api_key,
        "frequency": frequency,
        "data[]": data_field,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": 1,
    }
    for facet_name, facet_values in series_facets.items():
        values = facet_values if isinstance(facet_values, list) else [facet_values]
        for value in values:
            params[f"facets[{facet_name}][]"] = value

    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            if attempt < MAX_REQUEST_ATTEMPTS:
                sleep_for = RETRY_BACKOFF_SEC * attempt
                logger.warning(
                    "EIA request failed for %s (attempt %s/%s): %s",
                    route,
                    attempt,
                    MAX_REQUEST_ATTEMPTS,
                    exc,
                )
                time.sleep(sleep_for)
                continue
            logger.warning("EIA request failed for %s: %s", route, exc)
            return None
        except ValueError as exc:
            logger.warning("EIA JSON decode failed for %s: %s", route, exc)
            return None

        rows = (payload.get("response") or {}).get("data") or []
        if not rows:
            logger.warning("EIA returned no rows for %s", route)
            return None
        parsed = _parse_eia_float(rows[0].get(data_field))
        if parsed is None:
            logger.warning("EIA row missing numeric %s for %s", data_field, route)
        return parsed
    return None


def _fetch_eia_bulk_rows(
    route_path: str,
    *,
    facets: dict[str, list[str] | str],
    data_field: str,
    frequency: str,
    length: int = 5000,
) -> list[dict[str, Any]]:
    api_key = _eia_api_key()
    if not api_key:
        return []

    route = route_path.strip("/")
    url = f"{EIA_API_BASE}{route}/data/"
    params: dict[str, Any] = {
        "api_key": api_key,
        "frequency": frequency,
        "data[]": data_field,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": length,
    }
    for facet_name, facet_values in facets.items():
        values = facet_values if isinstance(facet_values, list) else [facet_values]
        for value in values:
            params[f"facets[{facet_name}][]"] = value

    try:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("EIA bulk request failed for %s: %s", route, exc)
        return []
    rows = (payload.get("response") or {}).get("data") or []
    return rows if isinstance(rows, list) else []


def _tier_ratios_from_national(national: dict[str, float]) -> dict[str, float]:
    regular = national.get("regular") or FALLBACK_NATIONAL_REGULAR
    return {
        "mid": round((national.get("mid") or 4.79) / regular, 4),
        "premium": round((national.get("premium") or 5.17) / regular, 4),
        "diesel": round((national.get("diesel") or 5.43) / regular, 4),
    }


def _state_block_from_regular(
    region_name: str,
    regular: float,
    *,
    ratios: dict[str, float],
    electricity_rate: float,
) -> dict[str, Any]:
    return {
        "region_name": region_name,
        "regular": round(regular, 3),
        "mid": round(regular * ratios["mid"], 3),
        "premium": round(regular * ratios["premium"], 3),
        "diesel": round(regular * ratios["diesel"], 3),
        "electricity_rate": round(electricity_rate, 4),
    }


def fallback_payload() -> dict[str, Any]:
    """2026 market fallback when EIA calls fail."""
    national = {
        "region_name": "National Average",
        "regular": FALLBACK_NATIONAL_REGULAR,
        "mid": 4.79,
        "premium": 5.17,
        "diesel": 5.43,
        "electricity_rate": FALLBACK_NATIONAL_ELECTRICITY,
    }
    ratios = _tier_ratios_from_national(national)

    state_regular_overrides: dict[str, float] = {
        "CA": FALLBACK_CA_REGULAR,
        "NC": 3.94,
        "TX": 3.35,
        "MS": 3.45,
        "LA": 3.48,
        "OK": 3.52,
        "AR": 3.55,
        "MO": 3.58,
        "SC": 3.60,
        "TN": 3.62,
        "AL": 3.63,
        "KY": 3.65,
        "KS": 3.66,
        "IA": 3.68,
        "OH": 3.70,
        "IN": 3.72,
        "WI": 3.74,
        "MI": 3.76,
        "GA": 3.78,
        "FL": 3.80,
        "VA": 3.82,
        "PA": 3.85,
        "CO": 3.88,
        "AZ": 3.92,
        "NM": 3.95,
        "NV": 4.05,
        "IL": 4.10,
        "OR": 4.15,
        "WA": 4.25,
        "UT": 4.28,
        "ID": 4.30,
        "MT": 4.32,
        "WY": 4.34,
        "ND": 4.36,
        "SD": 4.38,
        "NE": 4.40,
        "MN": 4.42,
        "AK": 4.55,
        "HI": 5.10,
        "NY": 4.45,
        "NJ": 4.48,
        "CT": 4.52,
        "MA": 4.55,
        "RI": 4.58,
        "NH": 4.60,
        "VT": 4.62,
        "ME": 4.65,
        "MD": 4.68,
        "DE": 4.70,
        "WV": 4.72,
        "DC": 4.75,
    }

    states: dict[str, dict[str, Any]] = {}
    for code in US_STATE_CODES:
        regular = state_regular_overrides.get(code, national["regular"])
        states[code] = _state_block_from_regular(
            STATE_NAMES.get(code, code),
            regular,
            ratios=ratios,
            electricity_rate=national["electricity_rate"],
        )

    return {
        "source": "eia_fallback_2026",
        "synced_at": datetime.now(timezone.utc).isoformat(),
        "national": national,
        "states": states,
    }


def _latest_rows_by_key(
    rows: list[dict[str, Any]],
    key_field: str,
    value_field: str,
) -> dict[str, float]:
    latest: dict[str, tuple[str, float]] = {}
    for row in rows:
        key = str(row.get(key_field) or "").strip().upper()
        if not key:
            continue
        period = str(row.get("period") or "")
        value = _parse_eia_float(row.get(value_field))
        if value is None:
            continue
        prev = latest.get(key)
        if prev is None or period > prev[0]:
            latest[key] = (period, value)
    return {key: value for key, (_period, value) in latest.items()}


def _duoarea_to_state_code(duoarea: str) -> str | None:
    code = str(duoarea or "").strip().upper()
    if len(code) == 3 and code.startswith("S"):
        st = code[1:]
        if st in US_STATE_CODES:
            return st
    return None


def _fetch_state_gas_regular() -> dict[str, float]:
    rows = _fetch_eia_bulk_rows(
        "petroleum/pri/gnd",
        facets={"product": ["EPM0"], "process": ["PTE"]},
        data_field="value",
        frequency="weekly",
    )
    latest: dict[str, float] = {}
    for duoarea, value in _latest_rows_by_key(rows, "duoarea", "value").items():
        state_code = _duoarea_to_state_code(duoarea)
        if state_code:
            latest[state_code] = value
    return latest


def _fetch_state_electricity_rates() -> dict[str, float]:
    rows = _fetch_eia_bulk_rows(
        "electricity/retail-sales",
        facets={"sectorid": ["RES"]},
        data_field="price",
        frequency="monthly",
    )
    latest_cents = _latest_rows_by_key(rows, "stateid", "price")
    return {
        state_code: round(cents / 100.0, 4)
        for state_code, cents in latest_cents.items()
        if state_code in US_STATE_CODES or state_code == "US"
    }


def build_payload() -> dict[str, Any]:
    national_regular = fetch_eia_weekly_rate(
        "petroleum/pri/gnd",
        {"series": ["EMM_EPM0_PTE_NUS_DPG"]},
    )
    national_premium = fetch_eia_weekly_rate(
        "petroleum/pri/gnd",
        {"series": ["EMM_EPMP_PTE_NUS_DPG"]},
    )
    national_electricity_cents = fetch_eia_weekly_rate(
        "electricity/retail-sales",
        {"sectorid": ["RES"], "stateid": ["US"]},
        data_field="price",
        frequency="monthly",
    )

    if national_regular is None:
        logger.error("National EIA gasoline scrape failed; using fallback payload")
        return fallback_payload()

    national_mid = round(national_regular * (4.79 / FALLBACK_NATIONAL_REGULAR), 3)
    national_diesel = round(national_regular * (5.43 / FALLBACK_NATIONAL_REGULAR), 3)
    national_premium_val = national_premium or round(
        national_regular * (5.17 / FALLBACK_NATIONAL_REGULAR),
        3,
    )
    national_electricity = (
        round(national_electricity_cents / 100.0, 4)
        if national_electricity_cents is not None
        else FALLBACK_NATIONAL_ELECTRICITY
    )

    national = {
        "region_name": "National Average",
        "regular": round(national_regular, 3),
        "mid": national_mid,
        "premium": round(national_premium_val, 3),
        "diesel": national_diesel,
        "electricity_rate": national_electricity,
    }
    ratios = _tier_ratios_from_national(national)

    state_gas = _fetch_state_gas_regular()
    electricity_by_state = _fetch_state_electricity_rates()
    fallback_states = fallback_payload()["states"]

    states: dict[str, dict[str, Any]] = {}
    for code in US_STATE_CODES:
        if code in state_gas:
            regular = state_gas[code]
        else:
            fb = fallback_states.get(code) or {}
            fb_regular = _parse_eia_float(fb.get("regular"))
            if fb_regular is not None:
                regular = fb_regular
            else:
                regular = national["regular"]

        elec = electricity_by_state.get(code)
        if elec is None:
            elec = electricity_by_state.get("US", national_electricity)

        states[code] = _state_block_from_regular(
            STATE_NAMES.get(code, code),
            regular,
            ratios=ratios,
            electricity_rate=elec,
        )

    return {
        "source": "eia_api_v2",
        "synced_at": datetime.now(timezone.utc).isoformat(),
        "national": national,
        "states": states,
    }


def write_payload(payload: dict[str, Any]) -> Path:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = OUTPUT_PATH.with_suffix(".json.tmp")
    tmp_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(OUTPUT_PATH)
    return OUTPUT_PATH


def sync_gas_prices_cache(*, use_fallback_on_error: bool = True) -> dict[str, Any]:
    """
    Pull EIA gasoline and electricity rates and write ``live_gas_prices.json``.

    Returns a summary dict with ``ok``, ``path``, ``source``, ``states``, and
    ``national_regular``. When EIA calls fail and ``use_fallback_on_error`` is
    true, writes the hardcoded fallback payload instead of raising.
    """
    try:
        payload = build_payload()
        output = write_payload(payload)
        used_fallback = payload.get("source") == "eia_fallback_2026"
        return {
            "ok": not used_fallback,
            "path": str(output),
            "source": payload.get("source"),
            "states": len(payload.get("states") or {}),
            "national_regular": (payload.get("national") or {}).get("regular"),
            "national_electricity": (payload.get("national") or {}).get("electricity_rate"),
            "synced_at": payload.get("synced_at"),
            "used_fallback": used_fallback,
        }
    except Exception:
        logger.exception("Unexpected failure while syncing EIA market data")
        if not use_fallback_on_error:
            raise
        payload = fallback_payload()
        output = write_payload(payload)
        logger.warning("Wrote fallback market cache to %s", output)
        return {
            "ok": False,
            "path": str(output),
            "source": payload.get("source"),
            "states": len(payload.get("states") or {}),
            "national_regular": (payload.get("national") or {}).get("regular"),
            "national_electricity": (payload.get("national") or {}).get("electricity_rate"),
            "synced_at": payload.get("synced_at"),
            "used_fallback": True,
        }


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from backend.utils.project_env import load_project_dotenv

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    load_project_dotenv()

    summary = sync_gas_prices_cache(use_fallback_on_error=True)
    logger.info(
        "Wrote %s (%s states, national regular=%s, national electricity=%s, source=%s)",
        summary.get("path"),
        summary.get("states"),
        summary.get("national_regular"),
        summary.get("national_electricity"),
        summary.get("source"),
    )
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
