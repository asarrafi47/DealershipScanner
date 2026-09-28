"""Side-by-side compare table builder for the compare page."""

from __future__ import annotations

import json
import re
from typing import Any

from backend.enrichment.knowledge_engine import prepare_car_detail_context
from backend.utils.car_serialize import DISPLAY_DASH, format_display_value, serialize_car_for_api


def parse_compare_car_ids(raw: str | None, *, limit: int = 4) -> list[int]:
    """Parse comma-separated car ids (deduped, order preserved, max *limit*)."""
    seen: set[int] = set()
    out: list[int] = []
    for part in (raw or "").split(","):
        part = part.strip()
        if not part.isdigit():
            continue
        cid = int(part)
        if cid <= 0 or cid in seen:
            continue
        seen.add(cid)
        out.append(cid)
        if len(out) >= limit:
            break
    return out


def _norm_cell(value: Any) -> str:
    s = format_display_value(value)
    if s == DISPLAY_DASH:
        return "—"
    return s.strip()


def _fmt_price(car: dict[str, Any]) -> str:
    price = car.get("price")
    msrp = car.get("msrp")
    try:
        p = float(price) if price is not None else None
    except (TypeError, ValueError):
        p = None
    try:
        m = float(msrp) if msrp is not None else None
    except (TypeError, ValueError):
        m = None
    if p is not None and p > 0:
        line = f"${p:,.0f}"
        if m is not None and m > 0 and abs(m - p) > 0.5:
            line += f" (MSRP ${m:,.0f})"
        return line
    if m is not None and m > 0:
        return f"MSRP ${m:,.0f}"
    return "Call for price"


def _fmt_mileage(car: dict[str, Any]) -> str:
    """Odometer, or "Mileage not listed" where 0/NULL is the feed's sentinel (DC-6)."""
    from backend.utils.mileage_display import mileage_not_listed

    m = car.get("mileage")
    not_listed = car.get("mileage_not_listed")
    if not isinstance(not_listed, bool):
        not_listed = mileage_not_listed(
            m, condition=car.get("condition"), is_cpo=car.get("is_cpo"), year=car.get("year")
        )
    if not_listed:
        return "Mileage not listed"
    try:
        return f"{int(float(m)):,} mi"
    except (TypeError, ValueError):
        return "Mileage not listed"


def _load_packages_blob(car: dict[str, Any]) -> dict[str, Any] | None:
    raw = car.get("packages")
    if raw is None or raw == "":
        return None
    try:
        pkg = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return pkg if isinstance(pkg, dict) else None


def _package_summary(car: dict[str, Any]) -> str:
    """Packages first, then the feed's ``features`` list.

    DC-7 (visual review 2026-09-28): 102,910 of 104,252 active packages blobs
    are ``{"features": [...], "warranty": [...]}`` and only 22 carry the
    ``packages_normalized`` / ``possible_packages`` keys, so reading those alone
    printed a dash for two cars that each list twenty features.
    """
    pkg = _load_packages_blob(car)
    if pkg is None:
        return "—"
    names: list[str] = []
    seen: set[str] = set()

    def _add(n: Any) -> None:
        if not isinstance(n, str):
            return
        n = n.strip()
        key = n.lower()
        if n and key not in seen:
            seen.add(key)
            names.append(n)

    for entry in pkg.get("packages_normalized") or []:
        if isinstance(entry, dict):
            _add(entry.get("canonical_name") or entry.get("name") or "")
    for n in pkg.get("possible_packages") or []:
        _add(n)
    if not names:
        for n in pkg.get("features") or []:
            _add(n)
    if not names:
        return "—"
    return ", ".join(names[:8]) + ("…" if len(names) > 8 else "")


_WARRANTY_SUFFIX_RE = re.compile(r"\s+(years?|miles(?:/km)?|km)\s*$", re.I)


def _warranty_summary(car: dict[str, Any]) -> str:
    """"Basic 3 yr / 36,000 mi; Drivetrain 5 yr / 60,000 mi; ..." from ``packages.warranty``.

    The feed files each coverage as two rows ("Basic Years" = 3, "Basic
    Miles/km" = 36,000); they are paired back by name, in feed order, and
    printed as filed.
    """
    pkg = _load_packages_blob(car)
    if pkg is None:
        return "—"
    raw = pkg.get("warranty")
    items: list[tuple[str, Any]] = []
    if isinstance(raw, list):
        for e in raw:
            if isinstance(e, dict):
                items.append((str(e.get("name") or ""), e.get("value")))
    elif isinstance(raw, dict):
        items = [(str(k), v) for k, v in raw.items()]
    order: list[str] = []
    cov: dict[str, dict[str, str]] = {}
    for name, value in items:
        val = str(value if value is not None else "").strip()
        m = _WARRANTY_SUFFIX_RE.search(name)
        if not m or not val:
            continue
        base = name[: m.start()].strip()
        unit = "yr" if m.group(1).lower().startswith("year") else "mi"
        if not base:
            continue
        if base not in cov:
            cov[base] = {}
            order.append(base)
        cov[base][unit] = val
    parts: list[str] = []
    for base in order:
        c = cov[base]
        bits = []
        if c.get("yr"):
            bits.append(f"{c['yr']} yr")
        if c.get("mi"):
            bits.append("unlimited mi" if c["mi"].lower() == "unlimited" else f"{c['mi']} mi")
        if bits:
            parts.append(f"{base} {' / '.join(bits)}")
    return "; ".join(parts) if parts else "—"


def _history_summary(car: dict[str, Any]) -> str:
    highlights = car.get("history_highlights")
    if isinstance(highlights, list) and highlights:
        bits = [str(h).strip() for h in highlights[:4] if str(h).strip()]
        if bits:
            return "; ".join(bits)
    url = car.get("carfax_url")
    if url and str(url).strip().startswith("http"):
        return "CARFAX report linked"
    return "—"


def serialize_car_for_compare(raw: dict[str, Any]) -> dict[str, Any]:
    """Full display payload for one compare column."""
    ctx = prepare_car_detail_context(dict(raw))
    verified = ctx.get("verified_specs") or {}
    car = serialize_car_for_api(dict(raw), include_verified=False, verified_specs=verified)
    gallery = car.get("gallery")
    image_url = car.get("image_url")
    if not image_url and isinstance(gallery, list) and gallery:
        image_url = gallery[0]
    car["compare_image_url"] = image_url
    car["compare_packages_summary"] = _package_summary(raw)
    car["compare_warranty_summary"] = _warranty_summary(raw)
    car["compare_history_summary"] = _history_summary(raw)
    return car


def _dealer_cell(car: dict[str, Any]) -> str:
    """The Dealer row states a fact in a table of facts, so it has to carry the caveat.

    Side by side with VIN and stock number, a bare dealer name reads as verified. When
    this listing's photos place the car elsewhere, the name stays (it is where the
    listing came from) and the cell says so.
    """
    name = _norm_cell(car.get("dealer_name"))
    if car.get("location_confirmed") is not False:
        return name
    rooftop = (car.get("observed_rooftop") or "").strip()
    if rooftop:
        return f"{name} (listed here; photos show {rooftop})"
    return f"{name} (location unconfirmed)"


def _compare_rows(cars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Spec rows aligned with the car detail spec sheet."""
    specs: list[tuple[str, str, Any]] = [
        ("price", "Price", _fmt_price),
        ("mileage", "Mileage", _fmt_mileage),
        ("year", "Year", lambda c: _norm_cell(c.get("year"))),
        ("make", "Make", lambda c: _norm_cell(c.get("make"))),
        ("model", "Model", lambda c: _norm_cell(c.get("model"))),
        ("trim", "Trim", lambda c: _norm_cell(c.get("trim"))),
        ("engine_display", "Engine", lambda c: _norm_cell(c.get("engine_display"))),
        ("transmission_display", "Transmission", lambda c: _norm_cell(c.get("transmission_display"))),
        ("drivetrain_display", "Drivetrain", lambda c: _norm_cell(c.get("drivetrain_display"))),
        ("body_style", "Body style", lambda c: _norm_cell(c.get("body_style"))),
        ("fuel_type", "Fuel type", lambda c: _norm_cell(c.get("fuel_type"))),
        ("condition", "Condition", lambda c: _norm_cell(c.get("condition"))),
        ("fuel_economy_display", "Efficiency", lambda c: _norm_cell(c.get("fuel_economy_display"))),
        ("cylinders", "Cylinders", lambda c: _norm_cell(c.get("cylinders"))),
        ("exterior_color", "Exterior", lambda c: _norm_cell(c.get("exterior_color"))),
        ("interior_color", "Interior", lambda c: _norm_cell(c.get("interior_color"))),
        ("vin", "VIN", lambda c: _norm_cell(c.get("vin"))),
        ("stock_number", "Stock #", lambda c: _norm_cell(c.get("stock_number"))),
        ("dealer_name", "Dealer", _dealer_cell),
        ("compare_packages_summary", "Features & options", lambda c: c.get("compare_packages_summary") or "—"),
        ("compare_warranty_summary", "Warranty", lambda c: c.get("compare_warranty_summary") or "—"),
        ("compare_history_summary", "History highlights", lambda c: c.get("compare_history_summary") or "—"),
    ]

    rows: list[dict[str, Any]] = []
    for key, label, fn in specs:
        values = [fn(c) for c in cars]
        key_fn = _DIFF_KEYS.get(key, _collapse_ws)
        normalized = {key_fn(v) for v in values}
        differs = len(normalized) > 1 and not (len(normalized) == 1 and "—" in normalized)
        rows.append(
            {
                "key": key,
                "label": label,
                "cells": values,
                "differs": differs,
            }
        )
    return rows


def _collapse_ws(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip()).lower()


def _transmission_diff_key(text: str) -> str:
    """e-CVT and CVT are one family; everything else compares as written (DC-7)."""
    from backend.utils.vpic_specs import transmission_family

    if transmission_family(text) == "cvt":
        return "cvt"
    return _collapse_ws(text)


_DIFF_KEYS = {"transmission_display": _transmission_diff_key}


def build_compare_context(raw_cars: list[dict[str, Any]]) -> dict[str, Any]:
    """Template context: serialized cars + spec rows with diff flags."""
    cars = [serialize_car_for_compare(c) for c in raw_cars]
    # One verdict read for the (at most four) columns, before the rows are built —
    # ``_dealer_cell`` reads these fields off the serialized car.
    from backend.db.repositories.cars_repo import car_attribution_states
    from backend.utils.car_serialize.attribution import attribution_public_fields

    states = car_attribution_states(c.get("id") for c in cars)
    if states:
        for car in cars:
            try:
                cid = int(car.get("id") or 0)
            except (TypeError, ValueError):
                continue
            car.update(attribution_public_fields(states.get(cid)))
    return {
        "cars": cars,
        "compare_rows": _compare_rows(cars),
        "compare_car_ids": [int(c.get("id") or 0) for c in cars if c.get("id")],
    }
