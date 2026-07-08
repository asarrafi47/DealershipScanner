"""
Pre-VDP field prefetch: fill vehicle rows from what we already know before
spending browser time. Two layers, both empty-slot-only:

1. DB merge — carry forward ONLY immutable-for-a-VIN enrichment fields from
   the previous scan's row (colors, engine, transmission, drivetrain, body
   style, fuel type, trim, condition, description, packages, mpg, cylinders)
   plus a gallery union. Price and mileage are NEVER merged: they change and
   must be re-acquired fresh every scan.
2. HTTP prefetch — plain GET of the car's detail page, parsing structured
   data only (JSON-LD/meta price, labeled color rows, spec rows) with the
   same conservative parsers the heal layer uses.

The browser VDP queue is built AFTER this runs, from the same field-emptiness
gates as before — so a car this module can't satisfy still gets the browser
visit exactly as it always did. Disable with SCANNER_VDP_DB_MERGE=0 /
SCANNER_VDP_HTTP_FIRST=0.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

logger = logging.getLogger("scanner")

# Facts that cannot change for a given VIN (or that dealers effectively never
# edit) — safe to carry forward from the prior scan when the fresh row is empty.
_DB_MERGE_TEXT_FIELDS: tuple[str, ...] = (
    "exterior_color",
    "interior_color",
    "engine_description",
    "engine_l",
    "transmission",
    "transmission_type",
    "drivetrain",
    "body_style",
    "fuel_type",
    "forced_induction",
    "trim",
    "condition",
    "description",
    "packages",
    "carfax_url",
)
_DB_MERGE_INT_FIELDS: tuple[str, ...] = ("cylinders", "mpg_city", "mpg_highway")

_HTTP_FETCH_TIMEOUT_S = 15.0
_HTTP_CONCURRENCY = 6


def _flag(name: str, default: str = "1") -> bool:
    return (os.environ.get(name) or default).strip().lower() not in ("0", "false", "no", "off")


def vdp_db_merge_enabled() -> bool:
    return _flag("SCANNER_VDP_DB_MERGE")


def vdp_http_first_enabled() -> bool:
    return _flag("SCANNER_VDP_HTTP_FIRST")


def _http_first_max() -> int:
    try:
        return max(0, min(2000, int((os.environ.get("SCANNER_VDP_HTTP_FIRST_MAX") or "400").strip())))
    except ValueError:
        return 400


def _empty(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and not v.strip():
        return True
    return False


def _load_prior_rows_by_vin(dealer_id: str, vins: list[str]) -> dict[str, dict[str, Any]]:
    """Previous-scan rows for this dealer's VINs (dealer-scoped: VINs can recur
    across dealers after trades)."""
    if not vins:
        return {}
    import sqlite3

    from backend.db.inventory_db import _parse_car_gallery, db_conn

    out: dict[str, dict[str, Any]] = {}
    with db_conn(row_factory=sqlite3.Row) as conn:
        cursor = conn.cursor()
        ph = ",".join("?" * len(vins))
        cursor.execute(
            f"SELECT * FROM cars WHERE dealer_id = ? AND vin IN ({ph})",
            [dealer_id, *vins],
        )
        for row in cursor.fetchall():
            d = dict(row)
            _parse_car_gallery(d)
            vin = str(d.get("vin") or "").strip().upper()
            if vin:
                out[vin] = d
    return out


def merge_known_fields_from_db(
    vehicles: list[dict[str, Any]],
    dealer_id: str,
) -> dict[str, int]:
    """Fill empty immutable fields on scraped rows from prior DB rows by VIN."""
    from backend.utils.gallery_merge import merge_inventory_row_galleries

    vins = sorted({
        str(v.get("vin") or "").strip().upper()
        for v in vehicles
        if len(str(v.get("vin") or "").strip()) == 17
    })
    prior = _load_prior_rows_by_vin(dealer_id, vins)
    stats = {"vins_known": len(prior), "fields_filled": 0, "vehicles_touched": 0}
    if not prior:
        return stats

    for v in vehicles:
        vin = str(v.get("vin") or "").strip().upper()
        row = prior.get(vin)
        if not row:
            continue
        touched = False
        for k in _DB_MERGE_TEXT_FIELDS:
            if _empty(v.get(k)) and not _empty(row.get(k)):
                v[k] = row[k]
                stats["fields_filled"] += 1
                touched = True
        for k in _DB_MERGE_INT_FIELDS:
            cur = v.get(k)
            try:
                cur_n = int(cur) if cur is not None and str(cur).strip() != "" else None
            except (TypeError, ValueError):
                cur_n = None
            if cur_n is not None and cur_n > 0:
                continue
            src = row.get(k)
            try:
                src_n = int(src) if src is not None and str(src).strip() != "" else None
            except (TypeError, ValueError):
                src_n = None
            if src_n is not None and src_n > 0:
                v[k] = src_n
                stats["fields_filled"] += 1
                touched = True
        prior_gallery = row.get("gallery")
        if isinstance(prior_gallery, list) and prior_gallery:
            before = len(v.get("gallery") or []) if isinstance(v.get("gallery"), list) else 0
            merge_inventory_row_galleries(v, {"gallery": prior_gallery, "image_url": row.get("image_url")})
            after = len(v.get("gallery") or []) if isinstance(v.get("gallery"), list) else 0
            if after > before:
                stats["fields_filled"] += 1
                touched = True
        if touched:
            stats["vehicles_touched"] += 1
    return stats


def _vehicle_wants_http_prefetch(v: dict[str, Any]) -> bool:
    from backend.scanner.utils.vdp_price_merge import listing_price_is_empty
    from backend.scanner.vdp.core import _vehicle_needs_spec_gap_vdp

    if listing_price_is_empty(v):
        return True
    if _vehicle_needs_spec_gap_vdp(v):
        return True
    return _empty(v.get("exterior_color")) or _empty(v.get("interior_color"))


def _fetch_html(url: str) -> str | None:
    import requests

    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml",
            },
            timeout=_HTTP_FETCH_TIMEOUT_S,
        )
        if resp.status_code != 200 or "html" not in (resp.headers.get("content-type") or ""):
            return None
        return resp.text
    except Exception:
        return None


def _apply_page_fields(v: dict[str, Any], html: str) -> int:
    """Fill empty fields from structured page data. Returns fields filled."""
    from backend.scanner.utils.vdp_price_merge import listing_price_is_empty
    from backend.scanner.utils.vdp_spec_parse import (
        parse_color_from_listing_html,
        parse_html_for_vehicle_specs,
        parse_price_from_listing_html,
    )

    filled = 0
    if listing_price_is_empty(v):
        price = parse_price_from_listing_html(html)
        if price:
            v["price"] = int(round(price))
            v.setdefault("_price_source", "vdp_http_prefetch")
            filled += 1
    if _empty(v.get("exterior_color")) or _empty(v.get("interior_color")):
        colors = parse_color_from_listing_html(html)
        if _empty(v.get("exterior_color")) and colors.get("exterior_color"):
            v["exterior_color"] = str(colors["exterior_color"])[:120]
            filled += 1
        if _empty(v.get("interior_color")) and colors.get("interior_color"):
            v["interior_color"] = str(colors["interior_color"])[:120]
            filled += 1
    specs = parse_html_for_vehicle_specs(html)
    for k in ("transmission", "drivetrain", "fuel_type", "body_style", "engine_description"):
        if _empty(v.get(k)) and specs.get(k):
            v[k] = str(specs[k])[:200]
            filled += 1
    for k in ("cylinders", "mpg_city", "mpg_highway"):
        if specs.get(k) is None:
            continue
        cur = v.get(k)
        try:
            has = cur is not None and int(cur) > 0
        except (TypeError, ValueError):
            has = False
        if not has:
            try:
                v[k] = int(specs[k])
                filled += 1
            except (TypeError, ValueError):
                pass
    return filled


async def http_prefetch_missing_fields(vehicles: list[dict[str, Any]]) -> dict[str, int]:
    """Concurrent HTTP prefetch for vehicles still missing queue-driving fields."""
    candidates: list[dict[str, Any]] = []
    for v in vehicles:
        url = str(v.get("_detail_url") or v.get("source_url") or "").strip()
        if url.startswith("http") and _vehicle_wants_http_prefetch(v):
            candidates.append(v)
    cap = _http_first_max()
    skipped_cap = max(0, len(candidates) - cap)
    candidates = candidates[:cap]
    stats = {"candidates": len(candidates), "fetched": 0, "fields_filled": 0, "skipped_cap": skipped_cap}
    if not candidates:
        return stats

    sem = asyncio.Semaphore(_HTTP_CONCURRENCY)

    async def _one(v: dict[str, Any]) -> None:
        url = str(v.get("_detail_url") or v.get("source_url") or "").strip()
        async with sem:
            html = await asyncio.to_thread(_fetch_html, url)
        if not html:
            return
        stats["fetched"] += 1
        stats["fields_filled"] += _apply_page_fields(v, html)

    await asyncio.gather(*(_one(v) for v in candidates))
    return stats


async def prefetch_before_vdp(
    vehicles: list[dict[str, Any]],
    dealer_id: str,
    dealer_name: str,
) -> dict[str, Any] | None:
    """Run both prefetch layers; the browser queue is built afterwards from the
    unchanged emptiness gates, so this can only shrink it, never mis-skip."""
    if not vehicles:
        return None
    out: dict[str, Any] = {}
    if vdp_db_merge_enabled():
        db_stats = await asyncio.to_thread(merge_known_fields_from_db, vehicles, dealer_id)
        out["db_merge"] = db_stats
    if vdp_http_first_enabled():
        http_stats = await http_prefetch_missing_fields(vehicles)
        out["http_first"] = http_stats
    if not out:
        return None
    logger.info(
        "VDP prefetch [%s]: db_merge filled %d field(s) on %d car(s) (%d known VINs); "
        "http_first fetched %d/%d page(s), filled %d field(s)",
        dealer_name,
        out.get("db_merge", {}).get("fields_filled", 0),
        out.get("db_merge", {}).get("vehicles_touched", 0),
        out.get("db_merge", {}).get("vins_known", 0),
        out.get("http_first", {}).get("fetched", 0),
        out.get("http_first", {}).get("candidates", 0),
        out.get("http_first", {}).get("fields_filled", 0),
    )
    return out
