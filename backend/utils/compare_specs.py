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
    m = car.get("mileage")
    if m is None:
        return "—"
    if isinstance(m, str) and not m.strip():
        return "—"
    try:
        return f"{int(float(m)):,} mi"
    except (TypeError, ValueError):
        return "—"


def _package_summary(car: dict[str, Any]) -> str:
    raw = car.get("packages")
    if raw is None or raw == "":
        return "—"
    try:
        pkg = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError, json.JSONDecodeError):
        return "—"
    if not isinstance(pkg, dict):
        return "—"
    names: list[str] = []
    seen: set[str] = set()
    for entry in pkg.get("packages_normalized") or []:
        if not isinstance(entry, dict):
            continue
        n = (entry.get("canonical_name") or entry.get("name") or "").strip()
        key = n.lower()
        if n and key not in seen:
            seen.add(key)
            names.append(n)
    for n in pkg.get("possible_packages") or []:
        if isinstance(n, str) and n.strip():
            key = n.strip().lower()
            if key not in seen:
                seen.add(key)
                names.append(n.strip())
    if not names:
        return "—"
    return ", ".join(names[:8]) + ("…" if len(names) > 8 else "")


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
    car["compare_history_summary"] = _history_summary(raw)
    return car


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
        ("dealer_name", "Dealer", lambda c: _norm_cell(c.get("dealer_name"))),
        ("compare_packages_summary", "Packages & options", lambda c: c.get("compare_packages_summary") or "—"),
        ("compare_history_summary", "History highlights", lambda c: c.get("compare_history_summary") or "—"),
    ]

    rows: list[dict[str, Any]] = []
    for key, label, fn in specs:
        values = [fn(c) for c in cars]
        normalized = {_collapse_ws(v) for v in values}
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


def build_compare_context(raw_cars: list[dict[str, Any]]) -> dict[str, Any]:
    """Template context: serialized cars + spec rows with diff flags."""
    cars = [serialize_car_for_compare(c) for c in raw_cars]
    return {
        "cars": cars,
        "compare_rows": _compare_rows(cars),
        "compare_car_ids": [int(c.get("id") or 0) for c in cars if c.get("id")],
    }
