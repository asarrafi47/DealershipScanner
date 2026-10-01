"""Per-VIN vehicle specs from the cached NHTSA vPIC decode.

WHY THIS EXISTS
---------------
``epa_extended_specs`` is contaminated at the MODEL level: one scraped value is
stamped across every trim and model year of a nameplate. Measured 2026-08-04 on
the 49,912-row table — BMW 3 Series carries a single 0-60 of 3.7s across 579
rows spanning 1984-2026; Honda Civic a single 6.1s across 414 rows; Porsche
Boxster bottoms out at 2.8s, a 911 Turbo figure. 20% of all 0-60 values are
under 4.0s.

``knowledge_engine._extended_family_suspicious`` already refuses horsepower and
torque when every trim of a (year, make, model) family scraped to one value, and
its docstring is explicit that "nothing fills in behind them — the field goes
blank". That is why cars show no horsepower: the wrong number is correctly
withheld and there is no right one behind it.

vPIC is that right one. It is the manufacturer's own filing keyed on the
INDIVIDUAL VIN, so it cannot exhibit model-level contamination by construction —
a nameplate-wide value is impossible when every row is decoded from one car's
VIN. It is free, needs no API key, and 57,561 decodes are already cached locally
in ``nhtsa_vpic_cache``.

COVERAGE (active listings, measured 2026-08-04)
----------------------------------------------
30,388 of 55,246 active cars have a cached decode. Within those:

    BodyClass           99.8%      DriveType            88.9%
    FuelTypePrimary     99.7%      EngineHP             76.0%
    DisplacementL       92.8%      Trim                 74.4%
    EngineCylinders     88.8%      TransmissionStyle    58.6%

Spot-checked: WBA5B3C5XED539263 (2014 BMW 535i xDrive) decodes to EngineHP 300,
which is the correct N55 output — the value ``epa_extended_specs`` would have
supplied for any 5 Series of any year is a single nameplate-wide figure.

WHAT THIS DELIBERATELY DOES NOT SUPPLY
--------------------------------------
* ``CurbWeightLB`` — present on 0.2% of decodes. Not enough to be worth reading,
  and curb weight stays suppressed for the reasons in ``serialize.py``.
* 0-60 — vPIC does not carry it, and no free authoritative per-trim source does.
  It stays suppressed rather than being estimated from power-to-weight, because
  the horsepower and curb-weight inputs such an estimate needs are themselves
  the contaminated fields.

The cache is read-only here. Populating it is ``backfill_vpic_cache.py``'s job.
"""
from __future__ import annotations

import json
import re
import logging
from functools import lru_cache
from typing import Any

_log = logging.getLogger(__name__)

# Same bounds the knowledge-engine guard applies to epa_extended_specs values, so
# a decode cannot introduce a figure that path would have rejected.
_HP_MIN, _HP_MAX = 60, 1600
_CYL_MIN, _CYL_MAX = 2, 16
_DISP_MIN, _DISP_MAX = 0.4, 9.0

_EMPTY: dict[str, Any] = {}


def _clean(value: Any) -> str:
    s = str(value or "").strip()
    return "" if s.lower() in ("", "not applicable", "0", "none", "null") else s


def _num(value: Any) -> float | None:
    s = _clean(value)
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _decode_row(response_json: Any) -> dict[str, Any]:
    """The single flat Results[0] dict from a stored vPIC response.

    The cache stores vPIC's ``DecodeVinValues`` shape — ONE flat object of
    ``{"EngineHP": "300", ...}`` under ``Results`` — not the ``DecodeVin``
    shape, which is a list of ``{"Variable": ..., "Value": ...}`` pairs. Reading
    it as the latter silently yields nothing for every field.
    """
    if not response_json:
        return _EMPTY
    try:
        data = response_json if isinstance(response_json, dict) else json.loads(response_json)
    except (TypeError, ValueError):
        return _EMPTY
    results = data.get("Results") if isinstance(data, dict) else None
    if isinstance(results, list) and results and isinstance(results[0], dict):
        return results[0]
    return _EMPTY


def specs_from_decode(response_json: Any) -> dict[str, Any]:
    """Normalized specs from one stored decode. Only confidently-typed values."""
    row = _decode_row(response_json)
    if not row:
        return {}

    out: dict[str, Any] = {}

    hp = _num(row.get("EngineHP"))
    if hp is not None and _HP_MIN <= hp <= _HP_MAX:
        out["horsepower"] = int(round(hp))

    cyl = _num(row.get("EngineCylinders"))
    if cyl is not None and _CYL_MIN <= cyl <= _CYL_MAX:
        out["cylinders"] = int(round(cyl))

    disp = _num(row.get("DisplacementL"))
    if disp is not None and _DISP_MIN <= disp <= _DISP_MAX:
        out["displacement_l"] = round(disp, 1)

    for key, field in (
        ("body_class", "BodyClass"),
        ("fuel_type_primary", "FuelTypePrimary"),
        ("drive_type", "DriveType"),
        ("transmission_style", "TransmissionStyle"),
        ("engine_configuration", "EngineConfiguration"),
        ("vpic_trim", "Trim"),
        ("vpic_model", "Model"),
        # ``EngineHP`` on an electrified car is the combustion engine alone
        # (vPIC files no system output), so the reader needs these two to
        # label the figure honestly.
        ("electrification_level", "ElectrificationLevel"),
        ("fuel_type_secondary", "FuelTypeSecondary"),
    ):
        val = _clean(row.get(field))
        if val:
            out[key] = val

    speeds = _num(row.get("TransmissionSpeeds"))
    if speeds is not None and 1 <= speeds <= 12:
        out["transmission_speeds"] = int(round(speeds))

    return out


@lru_cache(maxsize=8192)
def _cached_specs_for_vin(vin: str) -> frozenset:
    """``frozenset`` of items so the LRU cache holds a hashable value."""
    try:
        from backend.db.repositories.base_repo import db_conn

        with db_conn() as conn:
            row = conn.execute(
                "SELECT response_json FROM nhtsa_vpic_cache WHERE vin = ?", (vin,)
            ).fetchone()
    except Exception as exc:  # a decode is an enrichment, never a page blocker
        _log.debug("vpic cache lookup failed for %s: %s", vin, exc)
        return frozenset()
    if not row:
        return frozenset()
    return frozenset(specs_from_decode(row[0]).items())


def vpic_specs_for_vin(vin: str | None) -> dict[str, Any]:
    """Per-VIN specs for *vin* from the local cache. ``{}`` when not decoded."""
    v = str(vin or "").strip().upper()
    if len(v) != 17:
        return {}
    return dict(_cached_specs_for_vin(v))


def vpic_horsepower(vin: str | None) -> int | None:
    """Manufacturer-filed horsepower for this exact VIN, or None."""
    return vpic_specs_for_vin(vin).get("horsepower")


_HYBRID_RE = re.compile(r"\b(?:P|M)?HEV\b|hybrid", re.IGNORECASE)


def hybrid_text(value: Any) -> bool:
    """True when a vPIC electrification level (or any fuel/engine text) names a hybrid."""
    return bool(value) and bool(_HYBRID_RE.search(str(value)))


def vpic_is_hybrid(specs: dict[str, Any]) -> bool:
    """True when the decode says this VIN is an HEV/PHEV/MHEV or carries a secondary electric fuel.

    ``EngineHP`` on such a car is the gasoline engine alone: a 2027 CR-V Hybrid
    decodes to 145 hp where the system makes 204 (visual review 2026-09-28,
    DC-1: 36,711 of 39,761 HEV/PHEV decodes on active listings carry one).
    """
    from backend.vehicle_facts.electrification import HEV, PHEV, _MILD, _vpic_verdict

    return _vpic_verdict(
        {
            "electrification_level": specs.get("electrification_level"),
            "fuel_type_primary": specs.get("fuel_type_primary"),
            "fuel_type_secondary": specs.get("fuel_type_secondary"),
        }
    ) in (HEV, PHEV, _MILD)


def reset_cache() -> None:
    """Tests / tooling: drop the in-process memo."""
    _cached_specs_for_vin.cache_clear()


def vpic_transmission_label(specs: dict[str, Any]) -> str | None:
    """Shopper label for the decode's ``TransmissionStyle`` (plus speeds), or None.

    DC-7 (visual review 2026-09-28): a 2027 CR-V Hybrid's feed code ``DDU``
    bucketed to "Automatic" while vPIC files "Electronic Continuously Variable
    (e-CVT)"; side by side with a CVT CR-V the compare page flagged a false
    difference. The decode is the per-VIN filing, so it names the gearbox.
    """
    style = str(specs.get("transmission_style") or "").strip()
    if not style:
        return None
    s = style.lower()
    speeds = specs.get("transmission_speeds")
    prefix = f"{int(speeds)}-Speed " if isinstance(speeds, int) and speeds > 1 else ""
    if "e-cvt" in s or "electronic continuously variable" in s:
        return "e-CVT"
    if "continuously variable" in s or re.search(r"\bcvt\b", s):
        return "CVT"
    if "dual-clutch" in s or "dual clutch" in s or re.search(r"\bdct\b", s):
        return f"{prefix}Dual-Clutch Automatic"
    if "automated manual" in s:
        return f"{prefix}Automated Manual"
    if "direct drive" in s:
        return "Single-Speed (Direct Drive)"
    if "manual" in s or "standard" in s:
        return f"{prefix}Manual"
    if "automatic" in s:
        return f"{prefix}Automatic"
    return None


def transmission_family(text: Any) -> str | None:
    """Coarse gearbox family of a display string: cvt / automatic / manual / direct.

    e-CVT and CVT are one family (a power-split hybrid and a belt CVT are both
    continuously variable to the shopper); dual-clutch and automated manual count
    as automatic. ``None`` when the text names no gearbox (feed codes like "DDU").
    """
    s = str(text or "").strip().lower()
    if not s or s in ("—", "-"):
        return None
    if re.search(r"\b(?:e-?)?cvt\b|continuously variable", s):
        return "cvt"
    if "direct drive" in s or re.search(r"\b(?:single|1)[-\s]speed\b", s):
        return "direct"
    if "automated manual" in s:
        return "automatic"
    if re.search(r"\bmanual\b|\bstandard\b", s):
        return "manual"
    if re.search(r"\bauto(?:matic)?\b|dual[-\s]clutch|\bdct\b|\d+[-\s]?speed", s):
        return "automatic"
    return None
