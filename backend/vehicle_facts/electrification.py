"""
The one electrification classifier.

Before 2026-10-01 nine-plus detectors each read the fuel label their own way
(``knowledge_engine_specs._is_battery_electric``, ``ev_range_estimates.
_is_electrified_car``, ``spec_backfill._is_ev_row``, ``spec_structured_backfill.
_is_ev_fuel_hint``, ``engine_consistency.is_bev_fuel``, ``catalog.resolver.
_electrification``, the vPIC level mapping in ``knowledge_engine`` ...). Only one
of them consulted the VIN decode. They now all answer from here.

:func:`electrification` returns one of ``ICE`` / ``HEV`` / ``PHEV`` / ``BEV`` /
``FCEV`` or ``None`` (unknown), deciding in this order (owner rules):

1. NHTSA vPIC. A decisive level (BEV / PHEV / FCEV / strong or unknown-level
   HEV, or an electric primary fuel with no level) wins outright. A decode that
   names only a combustion fuel (or a MILD hybrid) is not decisive between ICE
   and HEV -- vPIC leaves ElectrificationLevel blank on many hybrids -- but it
   does veto a plug: a dealer "Electric" on a car vPIC decodes as gasoline is
   the bad label (GX 550 x25), so the answer is ICE (HEV if the decode is mild).
2. The dealer's own fuel label.
3. The EPA catalog row (``atv_type`` / catalog fuel), only when the dealer said
   nothing. The catalog never outranks the dealer or the decode.

:func:`electrification_from_text` is the free-text classifier the catalog
resolver scores with (titles, trims, engine copy, EPA fuel strings).
"""
from __future__ import annotations

import re
from typing import Any, Mapping

ICE, HEV, PHEV, BEV, FCEV = "ICE", "HEV", "PHEV", "BEV", "FCEV"
ELECTRIFICATION_VALUES: tuple[str, ...] = (ICE, HEV, PHEV, BEV, FCEV)
PLUG_IN = frozenset({PHEV, BEV})
NO_ENGINE = frozenset({BEV, FCEV})

#: Legacy lower-case codes (``knowledge_engine`` vPIC dict, ``catalog.resolver``).
LEGACY_CODE = {BEV: "ev", PHEV: "phev", HEV: "hybrid", FCEV: "fcev"}
_FROM_LEGACY = {"ev": BEV, "bev": BEV, "phev": PHEV, "hybrid": HEV, "hev": HEV, "fcev": FCEV}

_COMBUSTION = "combustion"  # internal: vPIC says an engine, not whether it is hybrid
_MILD = "mild"              # internal: vPIC says 48V mild hybrid

_GAS_WORDS = ("gas", "petrol", "diesel", "flex", "e85", "ethanol", "unleaded", "unl",
              "premium", "regular", "midgrade", "cng", "natural", "propane", "lpg")


def fuel_label_electrification(fuel: Any) -> str | None:
    """Enum for a fuel LABEL (dealer ``fuel_type``, vPIC/EPA fuel string), else None."""
    if fuel is None:
        return None
    t = re.sub(r"\s+", " ", str(fuel).strip().lower())
    if not t:
        return None
    if "hydrogen" in t or "fuel cell" in t or t in ("fcev", "fcv"):
        return FCEV
    if "plug" in t or "phev" in t or "range extender" in t or "gas generator" in t:
        return PHEV
    if "electricity" in t and any(g in t for g in ("gasoline", "premium", "regular", "midgrade", "e85")):
        return PHEV  # EPA "Premium Gasoline / Electricity" is a plug-in
    if "hybrid" in t or re.search(r"\b(?:m|s)?hev\b|\bhyb", t):
        return HEV
    has_gas = any(w in t for w in _GAS_WORDS) or t in ("g", "d")
    if "electric" in t or t in ("ev", "bev", "electricity") or re.search(r"\bbattery[\s-]*electric\b", t):
        return HEV if has_gas else BEV  # "Gas/Electric", "Gasoline / Electric" = hybrid
    if has_gas:
        return ICE
    return None


def _vpic_verdict(vpic: Any) -> str | None:
    """BEV/PHEV/HEV/FCEV, or _COMBUSTION / _MILD, or None (no decode)."""
    if not vpic:
        return None
    if isinstance(vpic, str):
        return _FROM_LEGACY.get(vpic.strip().lower()) or _vpic_level(vpic)
    if not isinstance(vpic, Mapping):
        return None
    level = (vpic.get("ElectrificationLevel") or vpic.get("electrification_level") or "")
    primary = (vpic.get("FuelTypePrimary") or vpic.get("fuel_type_primary") or "")
    secondary = (vpic.get("FuelTypeSecondary") or vpic.get("fuel_type_secondary") or "")
    if not (level or primary or secondary) and "electrification" in vpic:
        # knowledge_engine normalized shape: {"electrification": ev|phev|hybrid|None, "fuel_type": ...}
        code = str(vpic.get("electrification") or "").strip().lower()
        if code in _FROM_LEGACY:
            return _FROM_LEGACY[code]
        ft = str(vpic.get("fuel_type") or "").strip().lower()
        if ft in ("gasoline", "diesel"):
            return _COMBUSTION
        return None
    v = _vpic_level(str(level))
    if v:
        return v
    p = str(primary).strip().lower()
    s = str(secondary).strip().lower()
    if p in ("", "not applicable") and s in ("", "not applicable"):
        return None
    if "hydrogen" in p or "fuel cell" in p:
        return FCEV
    if "electric" in p:
        return PHEV if s and "electric" not in s and s != "not applicable" else BEV
    if "electric" in s:
        return HEV
    return _COMBUSTION


def _vpic_level(level: str) -> str | None:
    el = level.strip().lower()
    if not el or el == "not applicable":
        return None
    if "fcev" in el or "fuel cell" in el:
        return FCEV
    if "bev" in el:
        return BEV
    if "phev" in el or "plug-in" in el:
        return PHEV
    if "mild" in el:
        return _MILD
    if "hev" in el:
        return HEV
    return None


def vpic_electrification(vpic: Any) -> str | None:
    """What the decode alone says: an enum value, or None when it is not decisive."""
    v = _vpic_verdict(vpic)
    return v if v in ELECTRIFICATION_VALUES else None


def electrification(fields: Mapping[str, Any] | None, vpic: Any = None, catalog: Any = None) -> str | None:
    """ICE / HEV / PHEV / BEV / FCEV, or None. See module docstring for the order.

    *fields* is a car row or any dict with ``fuel_type``; a ``vpic_electrification``
    key (``merge_verified_specs`` output) is read as the decode when *vpic* is
    not passed. *vpic* may be a raw vPIC ``Results[0]`` dict, a
    ``knowledge_engine.lookup_vpic_from_cache`` dict, a ``vpic_specs`` dict or a
    legacy code string. *catalog* is an ``epa_master`` row dict
    (``atv_type`` / ``fuel_type``).
    """
    fields = fields or {}
    if vpic is None:
        vpic = fields.get("vpic_electrification")
    v = _vpic_verdict(vpic)
    if v in ELECTRIFICATION_VALUES:
        return v
    dealer = fuel_label_electrification(fields.get("fuel_type"))
    if v in (_COMBUSTION, _MILD):
        if dealer in (ICE, HEV):
            return dealer
        return HEV if v == _MILD else ICE  # decode vetoes a plug / no-engine claim
    if dealer:
        return dealer
    if catalog:
        atv = str(catalog.get("atv_type") or "").strip().upper()
        if atv == "EV":
            return BEV
        if atv in ("FCV", "FCEV"):
            return FCEV
        if atv in ("PLUG-IN HYBRID", "PHEV"):
            return PHEV
        if atv == "HYBRID":
            return HEV
        return fuel_label_electrification(catalog.get("fuel_type"))
    return None


def is_battery_electric(fields: Mapping[str, Any] | None, vpic: Any = None) -> bool:
    return electrification(fields, vpic) == BEV


def can_plug_in(fields: Mapping[str, Any] | None, vpic: Any = None) -> bool:
    """BEV or PHEV: the car has a battery-only range."""
    return electrification(fields, vpic) in PLUG_IN


def electrification_from_text(text: str, fuel_text: str | None = None) -> str | None:
    """Enum from free text (title / trim / engine copy / EPA strings), else None.

    *fuel_text*: when given, the gas-AND-electricity PHEV test runs against it
    alone -- a trim like "Premium AWD" on a pure EV must not read as the
    "Premium" gasoline grade. (Moved verbatim from ``catalog.resolver``.)
    """
    t = (text or "").lower()
    ft = t if fuel_text is None else fuel_text.lower()
    if "plug-in" in t or "plugin" in t or "phev" in t or "prime" in t:
        return PHEV
    if "electricity" in ft and any(g in ft for g in ("gasoline", "premium", "regular", "midgrade", "e85")):
        return PHEV
    if "hybrid" in t or "i-force max" in t or "powerboost" in t or "etorque hybrid" in t:
        return HEV
    if ("electric" in t and "hybrid" not in t) or " bev" in t or t.strip() == "ev":
        return BEV
    return None
