"""Generate a synthesized window-sticker / build sheet for ANY scanned car.

Read-time only. The sheet is composed from the listing row (``cars``) plus
``verified_specs`` (which already merges EPA + extended specs at read time) plus
a best-effort catalog lookup for factory options & packages with prices. Nothing
here is persisted — it is a presentation-layer assembly, consistent with the
reference-store rule that catalog facts are never written back onto listing rows.

Unlike the OEM window-sticker path (which needs a per-VIN sticker PDF and only
covers a handful of eligible cars), this build sheet works for *every* car: it
degrades gracefully, emitting only the rows it has real data for.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Any

from backend.enrichment.knowledge_engine import _conn

# Presentation caps so a pathological catalog row can't flood the panel.
_MAX_CATALOG_TRIM_ROWS = 8
_MAX_CATALOG_PACKAGES = 24
_MAX_CATALOG_OPTIONS = 60
_MAX_PACKAGE_FEATURES = 20


def _clean(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s in ("—", "-", "N/A", "n/a", "NA", "None", "null"):
        return None
    return s


def _int_or_none(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(round(float(v)))
    except (TypeError, ValueError):
        return None


def _num_or_none(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _fmt_price(v: Any) -> str | None:
    n = _num_or_none(v)
    if n is None:
        return None
    n = int(round(n))
    if n < 0:
        return f"${abs(n):,} credit"
    return f"${n:,}"


def _row(label: str, value: Any) -> dict[str, str] | None:
    v = _clean(value)
    if v is None:
        return None
    return {"label": label, "value": v}


def _mpg_display(vs: dict[str, Any], car: dict[str, Any]) -> str | None:
    city = _int_or_none(vs.get("epa_city08")) or _int_or_none(car.get("mpg_city"))
    hwy = _int_or_none(vs.get("epa_highway08")) or _int_or_none(car.get("mpg_highway"))
    if city and hwy:
        return f"{city} city / {hwy} hwy mpg"
    if city:
        return f"{city} city mpg"
    if hwy:
        return f"{hwy} hwy mpg"
    # verified sometimes carries a pre-formatted string (e.g. EV MPGe)
    return _clean(vs.get("fuel_economy_display"))


def _engine_display(vs: dict[str, Any], car: dict[str, Any]) -> str | None:
    for cand in (
        car.get("engine_description"),
        vs.get("epa_engine_description"),
        vs.get("master_engine_string"),
    ):
        c = _clean(cand)
        if c:
            return c
    disp = _clean(vs.get("epa_displacement")) or _clean(car.get("engine_l"))
    cyl = _int_or_none(vs.get("cylinders")) or _int_or_none(car.get("cylinders"))
    parts = [p for p in (disp, f"{cyl}-cyl" if cyl else None) if p]
    return " ".join(parts) if parts else None


def _described_package_names(car: dict[str, Any]) -> list[str]:
    """Package names this listing itself names (from its parsed description).

    Reads the per-car ``cars.packages`` blob: ``factory_packages`` (listing
    package names) and any ``packages_normalized`` titles. These are the
    packages the dealer says the car has \u2014 the ones a generated sticker should
    price. Standard-equipment ``features`` are intentionally excluded.
    """
    import json

    raw = car.get("packages")
    if not raw or str(raw).strip() in ("{}", "[]", "null", ""):
        return []
    try:
        d = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        return []
    if not isinstance(d, dict):
        return []
    names: list[str] = []
    seen: set[str] = set()

    def _add(name: Any) -> None:
        s = _clean(name)
        if not s:
            return
        key = s.lower()
        if key not in seen:
            seen.add(key)
            names.append(s)

    for p in d.get("factory_packages") or []:
        _add(p)
    for entry in d.get("packages_normalized") or []:
        if isinstance(entry, dict):
            _add(entry.get("name") or entry.get("canonical_name") or entry.get("name_verbatim"))
        else:
            _add(entry)
    return names


def _catalog_equipment(car: dict[str, Any]) -> dict[str, Any]:
    """Price the packages THIS listing names \u2014 a generated sticker's line items.

    For a car with no OEM window sticker but whose listing names packages, we
    look up each package's observed price in the package-value registry (real
    OEM sticker prices where we have seen one for the same make/model). Packages
    we have no price for yet are still listed, without a price. See
    ``backend.enrichment.package_registry``.
    """
    make = _clean(car.get("make"))
    model = _clean(car.get("model"))
    empty = {"packages": [], "options": [], "priced_total": None, "priced_total_display": None}
    if not (make and model):
        return empty

    names = _described_package_names(car)
    if not names:
        return empty
    try:
        from backend.enrichment.package_registry import price_for_package
    except Exception:
        return empty

    year = car.get("year")
    trim = car.get("trim")
    packages: list[dict[str, Any]] = []
    for name in names[:_MAX_CATALOG_PACKAGES]:
        try:
            p = price_for_package(make, model, year, trim, name)
        except Exception:
            p = {"price": None, "source": None, "from_sticker": False}
        price = _int_or_none(p.get("price"))
        packages.append(
            {
                "name": name,
                "code": None,
                "price": price,
                "price_display": _fmt_price(price),
                "category": None,
                "from_sticker": bool(p.get("from_sticker")),
            }
        )
    priced_total = sum(e["price"] for e in packages if e["price"] and e["price"] > 0)
    return {
        "packages": packages,
        "options": [],
        "priced_total": priced_total or None,
        "priced_total_display": _fmt_price(priced_total) if priced_total else None,
    }


def build_generated_spec_sheet(
    car: dict[str, Any], verified_specs: dict[str, Any] | None = None
) -> dict[str, Any] | None:
    """Assemble a window-sticker-style build sheet for a single car.

    ``car`` is the raw listing row (``cars`` table dict). ``verified_specs`` is
    the output of ``merge_verified_specs`` (EPA + extended specs already merged);
    pass it in to avoid a second lookup. Returns ``None`` only when the row lacks
    even basic identity — every real car gets a sheet.
    """
    if not car:
        return None
    vs = verified_specs or {}

    year = _int_or_none(car.get("year"))
    make = _clean(car.get("make"))
    model = _clean(car.get("model"))
    if not (make and model):
        return None
    trim = _clean(car.get("trim"))

    identity = [
        r
        for r in (
            _row("Year", year),
            _row("Make", make),
            _row("Model", model),
            _row("Trim", trim),
            _row("Body style", _clean(vs.get("body_style_display")) or car.get("body_style")),
            _row("VIN", car.get("vin")),
            _row("Stock #", car.get("stock_number")),
        )
        if r
    ]

    powertrain = [
        r
        for r in (
            _row("Engine", _engine_display(vs, car)),
            _row("Cylinders", _int_or_none(vs.get("cylinders")) or car.get("cylinders")),
            _row("Forced induction", car.get("forced_induction")),
            _row("Transmission", _clean(vs.get("transmission_display")) or car.get("transmission")),
            _row("Drivetrain", _clean(vs.get("drivetrain_display")) or car.get("drivetrain")),
            _row("Fuel type", _clean(vs.get("epa_fuel_type")) or car.get("fuel_type")),
        )
        if r
    ]

    # EV range / battery only make sense for electrified powertrains. A wrong or
    # hybrid trim match can leave ev_range/battery on verified_specs for a gas
    # car — never surface those unless the fuel type says it's electrified.
    fuel_blob = f"{car.get('fuel_type') or ''} {vs.get('epa_fuel_type') or ''}".lower()
    is_electrified = any(
        tok in fuel_blob for tok in ("electric", "hybrid", "phev", "plug", "ev ")
    )
    ev_range = _int_or_none(vs.get("ev_range_miles")) if is_electrified else None
    battery = _num_or_none(vs.get("battery_kwh")) if is_electrified else None
    economy = [
        r
        for r in (
            _row("Fuel economy (EPA)", _mpg_display(vs, car)),
            _row("Electric range", f"{ev_range} mi" if ev_range else None),
            _row("Battery", f"{battery:g} kWh" if battery else None),
            _row(
                "Fuel tank",
                (lambda g: f"{g:g} gal" if g else None)(_num_or_none(vs.get("fuel_tank_gal"))),
            ),
        )
        if r
    ]

    hp = _int_or_none(vs.get("horsepower"))
    tq = _int_or_none(vs.get("torque_lb_ft"))
    zero60 = _num_or_none(vs.get("zero_to_60_sec"))
    curb = _int_or_none(vs.get("curb_weight_lb"))
    tow = _int_or_none(vs.get("tow_capacity_lb"))
    performance = [
        r
        for r in (
            _row("Horsepower", f"{hp} hp" if hp else None),
            _row("Torque", f"{tq} lb-ft" if tq else None),
            _row("0–60 mph", f"{zero60:g} sec" if zero60 else None),
            _row("Curb weight", f"{curb:,} lb" if curb else None),
            _row("Towing capacity", f"{tow:,} lb" if tow else None),
        )
        if r
    ]

    color = [
        r
        for r in (
            _row("Exterior", car.get("exterior_color")),
            _row("Interior", car.get("interior_color")),
        )
        if r
    ]

    msrp = _num_or_none(car.get("msrp"))
    price = _num_or_none(car.get("price"))
    savings = None
    if msrp and price and msrp > price:
        savings = int(round(msrp - price))
    pricing = {
        "msrp": msrp,
        "msrp_display": _fmt_price(msrp),
        "price": price,
        "price_display": _fmt_price(price),
        "savings": savings,
        "savings_display": _fmt_price(savings) if savings else None,
    }
    has_pricing = bool(pricing["msrp_display"] or pricing["price_display"])

    catalog = _catalog_equipment(car)
    has_catalog = bool(catalog["packages"] or catalog["options"])
    catalog_from_sticker = any(
        e.get("from_sticker") for e in catalog["packages"] + catalog["options"]
    )

    sections = [
        {"key": "identity", "title": "Vehicle", "rows": identity},
        {"key": "powertrain", "title": "Mechanical", "rows": powertrain},
        {"key": "economy", "title": "Fuel economy", "rows": economy},
        {"key": "performance", "title": "Performance & capacity", "rows": performance},
        {"key": "color", "title": "Color", "rows": color},
    ]
    sections = [s for s in sections if s["rows"]]

    subtitle_parts = [str(p) for p in (year, make, model, trim) if p]
    return {
        "title": " ".join(subtitle_parts) or "Vehicle build sheet",
        "vin": _clean(car.get("vin")),
        "sections": sections,
        "pricing": pricing,
        "has_pricing": has_pricing,
        "catalog": catalog,
        "has_catalog": has_catalog,
        "catalog_from_sticker": catalog_from_sticker,
    }
