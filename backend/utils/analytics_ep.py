"""
Dealer VDP / analytics payload (ep.*) — normalize and merge as fallback only.

Used when scanner attaches ``_ep_analytics`` (or ``analytics_ep``) to a vehicle dict.
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

from backend.utils.field_clean import is_effectively_empty, is_spec_overlay_junk, normalize_optional_str

logger = logging.getLogger(__name__)

# Single-token or very broad color words — do not replace a specific OEM color with these alone
_BROAD_COLOR_WORDS = frozenset(
    {
        "gray",
        "grey",
        "silver",
        "black",
        "white",
        "red",
        "blue",
        "green",
        "brown",
        "beige",
        "tan",
        "gold",
        "orange",
        "yellow",
        "charcoal",
    }
)


def _title_case_phrase(s: str) -> str:
    t = s.strip()
    if not t:
        return t
    return " ".join(w.capitalize() if w.islower() else w for w in t.split())


def _norm_transmission(s: str | None) -> str | None:
    if s is None or is_effectively_empty(s):
        return None
    low = str(s).strip().lower()
    if low in ("automatic", "auto"):
        return "Automatic"
    if low in ("manual", "stick", "mt"):
        return "Manual"
    if "cvt" in low:
        return "CVT"
    return _title_case_phrase(str(s))


def _norm_drivetrain(s: str | None) -> str | None:
    if s is None or is_effectively_empty(s):
        return None
    low = str(s).strip().lower()
    if "all-wheel" in low or low == "awd" or "xdrive" in low:
        return "AWD"
    if "four-wheel" in low or "4-wheel" in low or low in ("4wd", "4x4") or "4x4" in low or "4 wd" in low:
        return "4WD"
    if "front-wheel" in low or low in ("fwd", "2wd"):
        return "FWD"
    if "rear-wheel" in low or low in ("rwd",):
        return "RWD"
    return _title_case_phrase(str(s))


def _norm_body_style(s: str | None) -> str | None:
    if s is None or is_effectively_empty(s):
        return None
    low = str(s).strip().lower()
    if low == "sav":
        return "SUV"
    if low in ("sedan", "coupe", "convertible", "hatchback", "wagon", "suv", "truck", "van"):
        return low.upper() if len(low) <= 4 else _title_case_phrase(low)
    return _title_case_phrase(str(s))


def _norm_fuel_type(s: str | None) -> str | None:
    if s is None or is_effectively_empty(s):
        return None
    low = str(s).strip().lower()
    if "premium" in low and "unlead" in low:
        return "Premium Unleaded"
    if "unlead" in low and "regular" in low:
        return "Regular Unleaded"
    if "diesel" in low:
        return "Diesel"
    if "electric" in low:
        return "Electric"
    return _title_case_phrase(str(s))


def _norm_color(s: str | None) -> str | None:
    if s is None or is_effectively_empty(s):
        return None
    return _title_case_phrase(str(s).strip())


def _is_broad_only_color(phrase: str) -> bool:
    parts = phrase.strip().lower().split()
    if len(parts) > 1:
        return False
    return parts[0] in _BROAD_COLOR_WORDS if parts else False


def _should_skip_exterior_overwrite(existing: Any, proposed: str | None) -> bool:
    """Do not replace a rich OEM color with a single generic word (e.g. gray)."""
    if not proposed:
        return True
    ex = normalize_optional_str(existing)
    if not ex:
        return False
    if _is_broad_only_color(proposed) and not _is_broad_only_color(ex) and len(ex.split()) > 1:
        return True
    return False


def log_exterior_downgrade_skip(
    vehicle: dict[str, Any],
    ep_flat: dict[str, Any],
    log: logging.Logger,
    source: str,
) -> None:
    """If ep would propose a generic exterior color over a richer existing value, log once."""
    raw = ep_flat.get("exterior_color") or ep_flat.get("exteriorColor")
    ec = _norm_color(raw) if raw else None
    if ec and _should_skip_exterior_overwrite(vehicle.get("exterior_color"), ec):
        log.info(
            "VDP: skipped generic exterior_color=%s because existing color is more specific (source=%s)",
            ec,
            source,
        )


def parse_cylinders_from_engine(engine: str | None) -> int | None:
    if not engine or not str(engine).strip():
        return None
    u = str(engine).upper()
    m = re.search(r"(\d)\s*[-]?\s*CYL", u)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    m2 = re.search(r"V(\d)\b", u)
    if m2:
        try:
            return int(m2.group(1))
        except ValueError:
            return None
    return None


_BMW_TRIM_GENERIC_ONLY_TOKENS = frozenset(
    {
        "coupe",
        "sedan",
        "suv",
        "convertible",
        "hatchback",
        "wagon",
        "van",
        "truck",
        "crossover",
        "gasoline",
        "gas",
        "electric",
        "hybrid",
        "diesel",
        "plug",
        "in",
        "flex",
        "used",
        "new",
        "certified",
        "cpo",
        "preowned",
        "owned",
        "sport",
        "vehicle",
        "automatic",
        "manual",
        "awd",
        "fwd",
        "rwd",
        "4wd",
        "4x4",
        "2wd",
        "unleaded",
        "premium",
        "regular",
    }
)


def _bmw_trim_is_only_generic_labels(tail: str | None) -> bool:
    """
    True when *tail* is only generic body-style / fuel / condition words (e.g. "Coupe Gasoline").
    """
    if not tail or not str(tail).strip():
        return True
    parts = re.findall(r"[A-Za-z0-9]+", str(tail))
    if not parts:
        return True
    for p in parts:
        pl = p.lower()
        if any(ch.isdigit() for ch in p):
            return False
        if pl.startswith("xdrive") or pl.startswith("sdrive"):
            return False
        if re.match(r"^m\d", pl):
            return False
        if pl not in _BMW_TRIM_GENERIC_ONLY_TOKENS:
            return False
    return True


def _bmw_trim_token(tu: str) -> str:
    """e.g. ``xdrive35i`` → ``xDrive35i``, ``m40i`` → ``M40i``."""
    low = tu.lower()
    if low.startswith("xdrive"):
        return "xDrive" + tu[len("xdrive") :]
    if low.startswith("sdrive"):
        return "sDrive" + tu[len("sdrive") :]
    if len(tu) > 1 and tu[0].lower() == "m" and tu[1].isdigit():
        return tu[0].upper() + tu[1:].lower()
    return tu[0].upper() + tu[1:].lower() if len(tu) > 1 else tu.upper()


def parse_bmw_model_trim_from_vehicle_model(vehicle_model: str | None) -> tuple[str | None, str | None]:
    """
    Conservative split for BMW only, e.g. ``x3 xdrive35i`` -> (``X3``, ``xDrive35i``).
    """
    if not vehicle_model or not str(vehicle_model).strip():
        return None, None
    raw = " ".join(str(vehicle_model).strip().split())
    parts = raw.split()
    if len(parts) >= 2:
        base, tail = parts[0], " ".join(parts[1:])
        bu, tu = base.upper(), tail
        # x3 + xDrive35i / M40i / sDrive30i
        if re.match(r"^(X\d|M\d|I\d|Z\d|\d{3}[EI]?)$", bu) or re.match(r"^\d\s+SERIES$", bu):
            model = bu.replace(" ", "") if "SERIES" in bu else bu
            if re.match(r"^(xDrive|sDrive|M\d)", tu, re.I):
                trim = _bmw_trim_token(tu)
            else:
                trim = _title_case_phrase(tu)
            if trim and _bmw_trim_is_only_generic_labels(trim):
                trim = None
            return model, (trim[:120] if trim else None)
    # Glued token e.g. x3xdrive35i (rare)
    compact = raw.replace(" ", "")
    m = re.match(
        r"^([ixzm]?\d+[a-z]?)(xdrive\d+[a-z]*|sdrive\d+[a-z]*|m\d+[a-z0-9]*)$",
        compact,
        re.I,
    )
    if m:
        mod = m.group(1).upper()
        tr = m.group(2)
        trim = _bmw_trim_token(tr) if tr else None
        if trim and _bmw_trim_is_only_generic_labels(trim):
            trim = None
        return mod, trim
    if len(parts) == 1:
        return parts[0][:1].upper() + parts[0][1:].lower(), None
    return None, None


def flatten_ep_dict(ep: Any) -> dict[str, Any]:
    """Accept nested ``{"ep": {...}}`` or flat analytics keys."""
    if ep is None:
        return {}
    if isinstance(ep, str):
        try:
            ep = json.loads(ep)
        except (json.JSONDecodeError, TypeError):
            return {}
    if not isinstance(ep, dict):
        return {}
    inner = ep.get("ep")
    if isinstance(inner, dict):
        base = {k: v for k, v in ep.items() if k != "ep"}
        base.update(inner)
        return base
    return dict(ep)


def normalize_ep_field_aliases(ep: dict[str, Any]) -> dict[str, Any]:
    """
    GA / Adobe payloads often use camelCase; merge expects snake_case ep keys.
    Copy alias values into canonical keys when the canonical value is missing or empty.
    """
    out = dict(ep)
    pairs: list[tuple[str, str]] = [
        ("driveTrain", "drive_train"),
        ("driveType", "drive_train"),
        ("driveLine", "drive_train"),
        ("driveline", "drive_train"),
        ("drive_line", "drive_train"),
        ("drivetrain", "drive_train"),
        ("transmissionType", "transmission"),
        ("fuelType", "fuel_type"),
        ("interiorColor", "interior_color"),
        ("vehicleInteriorColor", "interior_color"),
        ("VehicleInteriorColor", "interior_color"),
        ("interiorTrimColor", "interior_color"),
        ("interior_trim_color", "interior_color"),
        ("interiorUpholstery", "interior_color"),
        ("interior_upholstery", "interior_color"),
        ("upholsteryColor", "interior_color"),
        ("upholstery_color", "interior_color"),
        ("seatColor", "interior_color"),
        ("seat_color", "interior_color"),
        ("trimColor", "interior_color"),
        ("trim_color", "interior_color"),
        ("cabinColor", "interior_color"),
        ("cabin_color", "interior_color"),
        ("exteriorColor", "exterior_color"),
        ("vehicleModel", "vehicle_model"),
        ("vehicleMake", "vehicle_make"),
        ("stockNumber", "stock_id"),
        ("stockId", "stock_id"),
        ("cityFuelEconomy", "city_fuel_economy"),
        ("highwayFuelEconomy", "highway_fuel_economy"),
        ("cityFuelEfficiency", "city_fuel_economy"),
        ("highwayFuelEfficiency", "highway_fuel_economy"),
        ("mpgCity", "city_fuel_economy"),
        ("mpgHighway", "highway_fuel_economy"),
        ("cityMPG", "city_fuel_economy"),
        ("highwayMPG", "highway_fuel_economy"),
        ("bodyStyle", "body_style"),
        ("inventoryType", "inventory_type"),
        ("newOrUsed", "inventory_type"),
        ("new_or_used", "inventory_type"),
        ("vehicleCondition", "inventory_type"),
        ("saleClass", "inventory_type"),
        ("inventoryCategory", "inventory_type"),
        ("mfYear", "mf_year"),
        ("vehicleYear", "vehicle_year"),
        ("engineDescription", "engine"),
        ("engine_description", "engine"),
    ]
    for camel, snake in pairs:
        cv = out.get(camel)
        if cv is None or is_effectively_empty(cv):
            continue
        if snake not in out or is_effectively_empty(out.get(snake)):
            out[snake] = cv
    return out


def _merge_nested_ep_vehicle(ep: dict[str, Any]) -> dict[str, Any]:
    """
    BMW / LD+JSON often nest analytics under ``vehicle``; copy missing keys up.
    """
    veh = ep.get("vehicle")
    if not isinstance(veh, dict):
        return ep
    nv = normalize_ep_field_aliases(dict(veh))
    for k, v in nv.items():
        if v is None or is_effectively_empty(v):
            continue
        cur = ep.get(k)
        if cur is None or is_effectively_empty(cur):
            ep[k] = v
    return ep


def _ep_first_interior_color_raw(ep: dict[str, Any]) -> Any:
    """First non-empty interior / upholstery signal (analytics + nested vehicle already merged)."""
    keys = (
        "interior_color",
        "interiorColor",
        "vehicleInteriorColor",
        "VehicleInteriorColor",
        "interiorTrimColor",
        "interiorUpholstery",
        "interior_upholstery",
        "upholsteryColor",
        "upholstery_color",
        "seatColor",
        "seat_color",
        "trimColor",
        "trim_color",
        "cabinColor",
        "cabin_color",
        "upholstery",
        "interior",
    )
    for k in keys:
        v = ep.get(k)
        if v is not None and not is_effectively_empty(v):
            return v
    return None


def _bmw_trace_ep_enabled(vin: Any) -> bool:
    raw = (os.environ.get("BMW_TRACE_VINS") or "").strip().upper()
    if not raw or not vin:
        return False
    v = str(vin).strip().upper()[:17]
    return v in {x.strip()[:17] for x in raw.split(",") if x.strip()}


def _coerce_ep_certified_bool(val: Any) -> bool | None:
    if val is True or val is False:
        return val
    if val is None:
        return None
    s = str(val).strip().lower()
    if s in ("true", "1", "yes", "y", "cpo"):
        return True
    if s in ("false", "0", "no", "n"):
        return False
    return None


def _coerce_mpg_number(val: Any) -> float | None:
    """GA sometimes sends MPG as a number, string, or {value: n} object."""
    if val is None:
        return None
    if isinstance(val, bool):
        return None
    if isinstance(val, (int, float)):
        f = float(val)
        return f if 4 <= f <= 200 else None
    if isinstance(val, dict):
        for k in ("value", "amount", "mpg", "city", "highway", "combined"):
            x = val.get(k)
            if isinstance(x, (int, float)):
                f = float(x)
                if 4 <= f <= 200:
                    return f
        return None
    s = str(val).strip()
    if not s:
        return None
    m = re.search(r"(\d{1,2}(?:\.\d+)?)", s)
    if not m:
        return None
    f = float(m.group(1))
    return f if 4 <= f <= 200 else None


def _mpg_missing(val: Any) -> bool:
    return val is None or val == 0


def _listing_is_placeholder_like_for_overlay(field: str, existing: Any) -> bool:
    """True when listing data is empty or a generic token we allow EP to replace."""
    if is_effectively_empty(existing):
        return True
    s = str(existing).strip().lower()
    junk = {
        "transmission": frozenset(
            {"automatic", "auto", "manual", "cvt", "n/a", "na", "-", "--", "tbd", "unknown", "unspecified"}
        ),
        "drivetrain": frozenset({"fwd", "rwd", "awd", "4wd", "4x4", "2wd", "n/a", "na", "-", "tbd", "unknown"}),
        "fuel_type": frozenset(
            {"gas", "gasoline", "unleaded", "flex fuel", "flex", "n/a", "na", "-", "tbd", "unknown"}
        ),
    }
    if field in junk and s in junk[field]:
        return True
    if len(s) <= 1:
        return True
    return False


def merge_analytics_ep_into_vehicle(
    vehicle: dict[str, Any],
    ep_raw: Any,
    diagnostics: dict[str, Any] | None = None,
) -> list[str]:
    """
    Merge normalized ep fields into *vehicle* in place. Returns list of field names filled/updated.
    Never treats analytics as primary: only fills missing or clearly lower-quality values.
    """
    ep = normalize_ep_field_aliases(flatten_ep_dict(ep_raw))
    ep = _merge_nested_ep_vehicle(ep)
    if not ep:
        if diagnostics is not None:
            diagnostics["ep_keys"] = []
            diagnostics["skipped"] = ["empty ep after flatten/alias"]
            diagnostics["filled"] = []
        return []

    skipped: list[str] = []
    eligible: list[str] = []

    if diagnostics is not None:
        diagnostics["ep_keys"] = sorted(str(k) for k in ep.keys())

    filled: list[str] = []
    make_u = str(vehicle.get("make") or "").strip().upper()

    vin_ep = normalize_optional_str(ep.get("vin") or ep.get("VIN"))
    if vin_ep and (not vehicle.get("vin") or str(vehicle.get("vin", "")).lower().startswith("unknown")):
        vehicle["vin"] = vin_ep.upper()[:17]
        filled.append("vin")
    elif diagnostics is not None and vin_ep:
        skipped.append("vin: existing vin kept")

    stock = normalize_optional_str(ep.get("stock_id") or ep.get("stock_number") or ep.get("stock"))
    if stock and is_effectively_empty(vehicle.get("stock_number")):
        vehicle["stock_number"] = stock[:80]
        filled.append("stock_number")

    y = ep.get("mf_year") or ep.get("year") or ep.get("vehicle_year")
    if y is not None and vehicle.get("year") in (None, 0):
        try:
            yi = int(float(y))
            if 1980 <= yi <= 2035:
                vehicle["year"] = yi
                filled.append("year")
        except (TypeError, ValueError):
            pass

    mk = normalize_optional_str(ep.get("vehicle_make") or ep.get("make"))
    if mk and is_effectively_empty(vehicle.get("make")):
        vehicle["make"] = mk[:60]
        filled.append("make")

    vm = ep.get("vehicle_model")
    vm_s = str(vm).strip() if vm is not None else ""
    if vm_s and vm_s.lower() not in ("na", "n/a", "null"):
        eligible.append("vehicle_model")
        vehicle.setdefault("model_full_raw", vm_s[:200])
        if make_u == "BMW" or (mk and str(mk).strip().upper() == "BMW"):
            m_part, t_part = parse_bmw_model_trim_from_vehicle_model(vm_s)
            if m_part and is_effectively_empty(vehicle.get("model")):
                vehicle["model"] = m_part[:80]
                filled.append("model")
            if t_part and is_effectively_empty(vehicle.get("trim")):
                vehicle["trim"] = t_part[:120]
                filled.append("trim")
        elif is_effectively_empty(vehicle.get("model")):
            vehicle["model"] = vm_s[:80]
            filled.append("model")

    trim_raw = ep.get("trim")
    trim_s = str(trim_raw).strip() if trim_raw is not None else ""
    if trim_s and trim_s.lower() not in ("na", "n/a", "null", "-"):
        if is_effectively_empty(vehicle.get("trim")):
            eligible.append("trim_ep")
            vehicle["trim"] = _title_case_phrase(trim_s)[:120]
            filled.append("trim")
        else:
            skipped.append("trim_ep: trim already set")

    tr = _norm_transmission(ep.get("transmission"))
    if tr:
        eligible.append("transmission")
        cur = vehicle.get("transmission")
        if is_effectively_empty(cur) or _listing_is_placeholder_like_for_overlay("transmission", cur):
            vehicle["transmission"] = tr
            filled.append("transmission")
        else:
            skipped.append(f"transmission: keep listing value {cur!r}")
    else:
        skipped.append("transmission: ep empty or not normalizable")

    dr = _norm_drivetrain(
        ep.get("drive_train")
        or ep.get("drivetrain")
        or ep.get("driveLine")
        or ep.get("driveline")
        or ep.get("drive_line")
    )
    if dr:
        eligible.append("drivetrain")
        cur = vehicle.get("drivetrain")
        if is_effectively_empty(cur) or _listing_is_placeholder_like_for_overlay("drivetrain", cur):
            vehicle["drivetrain"] = dr
            filled.append("drivetrain")
        else:
            skipped.append(f"drivetrain: keep listing value {cur!r}")
    else:
        skipped.append("drivetrain: ep empty or not normalizable")

    ic = _norm_color(_ep_first_interior_color_raw(ep))
    if ic:
        eligible.append("interior_color")
        cur_ic = vehicle.get("interior_color")
        if is_effectively_empty(cur_ic) or is_spec_overlay_junk(cur_ic):
            vehicle["interior_color"] = ic[:120]
            filled.append("interior_color")
        else:
            skipped.append("interior_color: already set")

    ec = _norm_color(ep.get("exterior_color"))
    if ec:
        eligible.append("exterior_color")
        cur_ec = vehicle.get("exterior_color")
        if _should_skip_exterior_overwrite(cur_ec, ec):
            skipped.append("exterior_color: skipped generic vs specific OEM color")
        elif is_effectively_empty(cur_ec) or is_spec_overlay_junk(cur_ec):
            vehicle["exterior_color"] = ec[:120]
            filled.append("exterior_color")
        else:
            skipped.append("exterior_color: already set")

    ft = _norm_fuel_type(ep.get("fuel_type"))
    if ft:
        eligible.append("fuel_type")
        cur = vehicle.get("fuel_type")
        if is_effectively_empty(cur) or _listing_is_placeholder_like_for_overlay("fuel_type", cur):
            vehicle["fuel_type"] = ft[:80]
            filled.append("fuel_type")
        else:
            skipped.append(f"fuel_type: keep listing value {cur!r}")
    else:
        skipped.append("fuel_type: ep empty or not normalizable")

    try:
        cc_raw = ep.get("city_fuel_economy")
        ch_raw = ep.get("highway_fuel_economy")
        cc = _coerce_mpg_number(cc_raw)
        ch = _coerce_mpg_number(ch_raw)
        if cc is not None:
            eligible.append("city_fuel_economy")
            if _mpg_missing(vehicle.get("mpg_city")):
                vehicle["mpg_city"] = int(round(cc))
                filled.append("mpg_city")
            else:
                skipped.append(f"mpg_city: already set to {vehicle.get('mpg_city')!r}")
        elif cc_raw is not None and not is_effectively_empty(cc_raw):
            skipped.append("mpg_city: could not parse city MPG from ep")
        if ch is not None:
            eligible.append("highway_fuel_economy")
            if _mpg_missing(vehicle.get("mpg_highway")):
                vehicle["mpg_highway"] = int(round(ch))
                filled.append("mpg_highway")
            else:
                skipped.append(f"mpg_highway: already set to {vehicle.get('mpg_highway')!r}")
        elif ch_raw is not None and not is_effectively_empty(ch_raw):
            skipped.append("mpg_highway: could not parse highway MPG from ep")
    except (TypeError, ValueError) as e:
        skipped.append(f"mpg: parse error {e}")

    bs = _norm_body_style(ep.get("body_style"))
    if bs:
        eligible.append("body_style")
        cur_bs = vehicle.get("body_style")
        if is_effectively_empty(cur_bs) or is_spec_overlay_junk(cur_bs):
            vehicle["body_style"] = bs[:80]
            filled.append("body_style")
        else:
            skipped.append("body_style: already set")

    inv_raw = (
        ep.get("inventory_type")
        or ep.get("newOrUsed")
        or ep.get("vehicleCondition")
        or ep.get("saleClass")
        or ep.get("inventoryCategory")
        or ""
    )
    inv = str(inv_raw).strip().lower()
    cert = _coerce_ep_certified_bool(ep.get("certified"))
    if cert is None:
        cert = _coerce_ep_certified_bool(ep.get("is_cpo") or ep.get("isCpo") or ep.get("cpo"))

    if cert is True:
        vehicle["is_cpo"] = 1
        filled.append("is_cpo")
    elif cert is False and vehicle.get("is_cpo") is None:
        vehicle["is_cpo"] = 0
        filled.append("is_cpo")

    cond_label: str | None = None
    if "certif" in inv or "cpo" in inv or inv in ("cp", "cpo", "certified"):
        cond_label = "Certified"
    elif inv in ("used", "pre-owned", "preowned", "pre owned"):
        cond_label = "Used"
    elif inv == "new":
        cond_label = "New"

    if is_effectively_empty(vehicle.get("condition")):
        if cert is True:
            vehicle["condition"] = "Certified"
            filled.append("condition")
        elif cond_label:
            vehicle["condition"] = cond_label[:80]
            filled.append("condition")

    eng = ep.get("engine")
    eng_s = str(eng).strip() if eng is not None else ""
    if eng_s and eng_s.lower() not in ("na", "n/a"):
        eligible.append("engine")
        if is_effectively_empty(vehicle.get("engine_description")):
            vehicle["engine_description"] = eng_s[:500]
            filled.append("engine_description")
        else:
            skipped.append("engine_description: already set")
        cyl = parse_cylinders_from_engine(eng_s)
        if cyl is not None and vehicle.get("cylinders") in (None, 0):
            vehicle["cylinders"] = cyl
            filled.append("cylinders")

    if diagnostics is not None and make_u == "BMW":
        diagnostics["bmw_inventory_inv"] = inv
        diagnostics["bmw_interior_ep_first_raw"] = _ep_first_interior_color_raw(ep)

    if _bmw_trace_ep_enabled(vehicle.get("vin")) and make_u == "BMW":
        logger.info(
            "BMW ep merge trace vin=%s interior_ep_first=%r interior_out=%r condition_out=%r is_cpo=%r inv=%r cert=%r filled=%s",
            str(vehicle.get("vin") or "")[:17],
            _ep_first_interior_color_raw(ep),
            vehicle.get("interior_color"),
            vehicle.get("condition"),
            vehicle.get("is_cpo"),
            inv,
            cert,
            filled,
        )

    if filled:
        logger.info(
            "analytics_ep merge vin=%s filled=%s",
            (vehicle.get("vin") or "")[:17],
            filled,
        )

    if diagnostics is not None:
        diagnostics["filled"] = filled
        diagnostics["eligible"] = eligible
        diagnostics["skipped"] = skipped

    return filled


def merge_ep_batch(vehicles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Strip ``_ep_analytics`` / ``analytics_ep`` and merge into each vehicle. For Node subprocess."""
    out: list[dict[str, Any]] = []
    for raw in vehicles:
        v = dict(raw)
        ep = v.pop("_ep_analytics", None) or v.pop("analytics_ep", None)
        if ep:
            merge_analytics_ep_into_vehicle(v, ep)
        out.append(v)
    return out


def apply_ep_from_scanner_dict(vehicle: dict[str, Any]) -> dict[str, Any]:
    """Single-vehicle helper for Python callers."""
    v = dict(vehicle)
    ep = v.pop("_ep_analytics", None) or v.pop("analytics_ep", None)
    if ep:
        merge_analytics_ep_into_vehicle(v, ep)
    return v
