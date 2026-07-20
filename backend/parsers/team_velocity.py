"""
Self-contained handler/parser for the **Team Velocity** ("Apollo" /
secureoffersites) inventory platform.

A Team Velocity dealer publishes two same-origin paginated JSON feeds — one per
condition — linked from ``inventorysitemap.xml``::

    https://{domain}/inventory-used.json?page=N     (used; CPO ⊆ used)
    https://{domain}/inventory-new.json?page=N      (new — disjoint VINs)

Shape::

    {"totalVehicles": N, "totalPages": P, "nextPage": ...,
     "pageSize": 50, "vehicles": [ {…}, … ]}

Every vehicle object carries a COMPLETE spec set — colors, engine, mileage,
price/msrp, trim, drivetrain, transmission, body, fuel, city/highway mpg,
condition — but ``imageUrls`` is always ``null``: the photo gallery lives only on
the VDP, emitted (for real browsers) as a Vue custom-element attribute::

    <oem-gallery-component :vin="'<vin>'" :imageid="'1'"
        :photoUrls="'https://…/a.jpg,https://…/b.jpg,…'">

The VDP is server-rendered behind a **User-Agent gate** — a bot/library UA gets
no gallery, a real browser UA gets the full comma-separated list. Image recovery
is therefore fully browser-free: plain HTTP with a browser UA + a regex.

This module is the platform's ONE home:

* :func:`parse` maps a feed page to complete vehicle rows (specs from the feed;
  ``image_url``/``gallery`` seeded with a placeholder pending completion).
* :func:`complete_from_vdp` is the modular IMAGE-COMPLETION step — given a VDP
  url it returns the gallery AND the per-car Carfax report link (also embedded in
  the VDP HTML). No browser, no separate script to remember.
* :func:`recover_dealer` is the DB-driven completion pass: for every Team Velocity
  car still missing images (and/or a Carfax link) it fetches the VDP, fills
  ``cars.image_url`` / ``cars.gallery`` / ``cars.carfax_url``, and re-syncs the
  incomplete-listings index. The delta path calls this per Team Velocity dealer
  after upsert; ``backend/scripts/recover_team_velocity_images.py`` is a thin CLI
  wrapper over it.

The TV feed carries NO features/packages/carfax/history fields — only scalar
specs — so ``packages`` / ``history_highlights`` stay empty from the feed and
``carfax_url`` is recovered from the VDP during completion.
"""
from __future__ import annotations

import json
import logging
import re
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Iterable

from backend.parsers.base import (
    dedupe_urls_order_prefer_large,
    norm_float,
    norm_int,
    norm_str,
)
from backend.utils.field_clean import clean_car_row_dict, normalize_optional_str

logger = logging.getLogger(__name__)

# The three known Team Velocity dealers (default target set for the CLI wrapper).
TEAM_VELOCITY_DEALERS: tuple[str, ...] = ("righttoyota-com", "markkia-com", "righthonda-com")

# Frontend placeholder; resolved relative to the app at render time.
FALLBACK_IMAGE_URL = "/static/placeholder.svg"

# A real browser UA is REQUIRED: with a library UA the VDP server drops the
# gallery entirely.
_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
_HEADERS = {
    "User-Agent": _BROWSER_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# The VDP gallery is a Vue attribute :photoUrls="'url,url,...'". It historically
# lived on <oem-gallery-component>, but TV renamed that element (2026-07), so the
# extractor anchors on the attribute itself, not the tag name. The old
# tag-anchored form is kept as a stricter fallback.
_GALLERY_RE = re.compile(r""":photoUrls="'([^"]*?)'\"""", re.S)
_GALLERY_RE_LEGACY = re.compile(r"<oem-gallery-component[^>]*?:photoUrls=\"'([^\"]*?)'\"", re.S)
# The real per-car Carfax vehicle-history REPORT link (not the badge SVGs). Token
# is url-safe base64 (letters, digits, '_' and '-').
_CARFAX_REPORT_RE = re.compile(
    r"https://www\.carfax\.com/vehiclehistory/[A-Za-z0-9/_\-]+", re.IGNORECASE
)


# ── Feed detection + iteration ───────────────────────────────────────────────

# Keys distinctive to a Team Velocity vehicle object (guard auto-detect against
# unrelated JSON — Dealer.com uses title[]/trackingPricing/images, never these).
def _looks_like_tv_vehicle(v: Any) -> bool:
    if not isinstance(v, dict):
        return False
    if not (v.get("vin") or v.get("VIN")):
        return False
    # ``imageUrls`` (always present, always null) + a TV price/vdp marker.
    has_marker = ("vdpUrl" in v) or ("sellingPrice" in v) or ("dealerDomain" in v)
    return has_marker and ("imageUrls" in v or "sellingPrice" in v)


def _iter_vehicles(raw_data: Any) -> Iterable[dict]:
    """Yield vehicle dicts from a TV feed page ({vehicles:[…]}) or a bare list."""
    if isinstance(raw_data, dict):
        vehicles = raw_data.get("vehicles")
        if isinstance(vehicles, list):
            yield from (v for v in vehicles if isinstance(v, dict))
            return
        if _looks_like_tv_vehicle(raw_data):  # a single vehicle object
            yield raw_data
    elif isinstance(raw_data, list):
        yield from (v for v in raw_data if isinstance(v, dict))


def detect(raw_data: Any) -> bool:
    """True when ``raw_data`` is a Team Velocity feed page (or vehicle)."""
    for v in _iter_vehicles(raw_data):
        return _looks_like_tv_vehicle(v)
    return False


# ── Field mapping ────────────────────────────────────────────────────────────

def _opt_str(v: Any) -> str | None:
    if v is None:
        return None
    return normalize_optional_str(norm_str(v))


def _price(v: dict) -> float:
    """sellingPrice is the rendered web price; fall back to msrp."""
    for key in ("sellingPrice", "internetPrice", "price"):
        n = norm_float(v.get(key))
        if n > 0:
            return n
    return norm_float(v.get("msrp"))


def _condition(v: dict) -> str:
    if v.get("certified") in (True, 1, "1", "true", "True"):
        return "Certified"
    it = norm_str(v.get("inventoryType") or v.get("type"))
    if it:
        low = it.lower()
        if low.startswith("new"):
            return "New"
        if "cert" in low:
            return "Certified"
        return "Used"
    return ""


def _title(v: dict, year: int, make: str, model: str) -> str | None:
    trim = norm_str(v.get("trim"))
    parts = [str(year) if year else "", make, model, trim]
    return normalize_optional_str(" ".join(p for p in parts if p).strip())


def _map_vehicle(
    v: dict, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str
) -> dict | None:
    vin = norm_str(v.get("vin") or v.get("VIN") or "")
    if not vin:
        return None

    year = norm_int(v.get("year") or v.get("modelYear"))
    make = norm_str(v.get("make"))
    model = norm_str(v.get("model"))

    # The feed carries imageUrls:null; seed a placeholder until the completion
    # step fills real photos from the VDP.
    row: dict[str, Any] = {
        "vin": vin,
        "stock_number": _opt_str(v.get("stockNumber") or v.get("stock_number")) or "",
        "year": year,
        "make": make,
        "model": model,
        "trim": _opt_str(v.get("trim")),
        "title": _title(v, year, make, model),
        "price": _price(v),
        "msrp": norm_float(v.get("msrp")) or None,
        "mileage": norm_int(v.get("miles") or v.get("mileage") or v.get("odometer")),
        "image_url": FALLBACK_IMAGE_URL,
        "gallery": [FALLBACK_IMAGE_URL],
        "dealer_id": dealer_id,
        "dealer_name": dealer_name or norm_str(v.get("dealerName")) or dealer_id,
        "dealer_url": dealer_url or norm_str(v.get("dealerDomain")) or base_url,
        "zip_code": _opt_str(v.get("dealerZip") or v.get("zipCode")),
        "fuel_type": _opt_str(v.get("fuelType") or v.get("fuel_type")),
        "transmission": _opt_str(v.get("transmission") or v.get("transmissionType")),
        "drivetrain": _opt_str(v.get("driveTrain") or v.get("drivetrain")),
        "exterior_color": _opt_str(v.get("exteriorColor") or v.get("exterior_color")) or "",
        "interior_color": _opt_str(v.get("interiorColor") or v.get("interior_color")) or "",
        "body_style": _opt_str(v.get("bodyStyle") or v.get("body_style")) or "",
        "engine_description": _opt_str(v.get("engine") or v.get("engineDescription")) or "",
        "cylinders": norm_int(v.get("engineCylinders") or v.get("cylinders")) or None,
        "mpg_city": norm_int(v.get("cityMpg") or v.get("mpgCity")) or None,
        "mpg_highway": norm_int(v.get("highwayMpg") or v.get("mpgHighway")) or None,
    }

    condition = _condition(v)
    if condition:
        row["condition"] = condition
        if condition == "Certified":
            row["is_cpo"] = 1
        elif condition == "Used":
            row["is_cpo"] = 0

    vdp = norm_str(v.get("vdpUrl") or v.get("vdp_url") or v.get("url"))
    if vdp.startswith("http"):
        row["source_url"] = vdp
        row["_detail_url"] = vdp
    return row


def parse(
    raw_data: Any,
    *,
    base_url: str = "",
    dealer_id: str = "",
    dealer_name: str = "",
    dealer_url: str = "",
) -> list[dict]:
    """Map a Team Velocity feed page to standard vehicle rows.

    Returns ``[]`` for anything that is not a TV feed, so it is safe in the
    shared auto-detect fallback.
    """
    if not detect(raw_data):
        return []
    out: list[dict] = []
    for v in _iter_vehicles(raw_data):
        mapped = _map_vehicle(v, base_url, dealer_id, dealer_name, dealer_url)
        if mapped:
            out.append(clean_car_row_dict(mapped))
    return out


# ── Modular image (+ carfax) completion step ─────────────────────────────────

def extract_gallery(html: str) -> list[str]:
    """Ordered, de-duplicated http photo list from the VDP ``:photoUrls`` attr."""
    m = _GALLERY_RE.search(html or "") or _GALLERY_RE_LEGACY.search(html or "")
    if not m:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for raw in m.group(1).split(","):
        u = raw.strip()
        if u.startswith("http") and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def extract_carfax_url(html: str) -> str | None:
    """The per-car Carfax vehicle-history REPORT link embedded in the VDP HTML.

    Ignores the static Carfax badge SVGs (partnerstatic.carfax.com) and the
    snapshot.js loader — only ``/vehiclehistory/`` report URLs are real.
    """
    m = _CARFAX_REPORT_RE.search(html or "")
    return m.group(0) if m else None


def fetch_vdp_html(url: str, *, timeout: float = 30.0) -> str:
    """Proxy-aware GET of a VDP with a browser UA; returns decoded HTML."""
    from backend.scanner.http_fetch import open_url

    req = urllib.request.Request(url, headers=_HEADERS)
    resp = open_url(req, timeout=timeout)
    charset = resp.headers.get_content_charset() or "utf-8"
    return resp.read().decode(charset, errors="replace")


def complete_from_vdp(url: str, *, timeout: float = 30.0) -> dict[str, Any]:
    """The completion step for ONE car: fetch its VDP and return the recovered
    gallery + Carfax link. Browser-free, DB-free — reusable by parse-time
    enrichment, the delta pass, and the CLI wrapper.

    Returns ``{"gallery": [...], "carfax_url": str | None}``.
    """
    html = fetch_vdp_html(url, timeout=timeout)
    return {
        "gallery": extract_gallery(html),
        "carfax_url": extract_carfax_url(html),
    }


def _image_url_fillable(cur_val: Any) -> bool:
    """image_url is fillable when empty or holding a placeholder / non-http path."""
    if cur_val is None or str(cur_val).strip() == "":
        return True
    return not str(cur_val).strip().startswith("http")


def _gallery_fillable(cur_val: Any) -> bool:
    """gallery (JSON-text list) is fillable when empty, unparseable, or when it
    holds NO real http image (e.g. ``["/static/placeholder.svg"]``)."""
    if cur_val is None or str(cur_val).strip() in ("", "[]", "null"):
        return True
    try:
        items = json.loads(cur_val)
    except (ValueError, TypeError):
        return True
    if not isinstance(items, list) or not items:
        return True
    return not any(isinstance(u, str) and u.strip().startswith("http") for u in items)


def _carfax_fillable(cur_val: Any) -> bool:
    """carfax_url is fillable when empty or not an http link."""
    if cur_val is None or str(cur_val).strip() == "":
        return True
    return not str(cur_val).strip().lower().startswith("http")


def _rows_needing_completion(dealer_id: str, limit: int | None):
    """Cars for this TV dealer still missing real images or a Carfax link."""
    from backend.db.inventory_db import db_conn

    sql = (
        "SELECT id, vin, source_url, image_url, gallery, carfax_url FROM cars "
        "WHERE dealer_id = ? "
        "AND ((image_url IS NULL OR image_url NOT LIKE 'http%') "
        "     OR (gallery IS NULL OR gallery NOT LIKE '%http%') "
        "     OR (carfax_url IS NULL OR carfax_url NOT LIKE 'http%')) "
        "AND COALESCE(listing_active, 1) = 1 "
        "AND source_url IS NOT NULL AND source_url LIKE 'http%' "
        "ORDER BY id"
    )
    params: tuple = (dealer_id,)
    if limit:
        sql += " LIMIT ?"
        params = (dealer_id, limit)
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()


def recover_dealer(
    dealer_id: str,
    *,
    dry_run: bool = False,
    limit: int | None = None,
    workers: int = 3,
) -> dict:
    """DB-driven completion pass for one Team Velocity dealer.

    ``workers`` is intentionally low (3): these dealers throttle a burst of
    concurrent VDP fetches into gallery-less/challenge pages (observed
    2026-07-20 — 10 workers yielded ~28% galleries; 3 workers recovers them).

    Fills ``cars.image_url`` / ``cars.gallery`` (from the VDP gallery) and
    ``cars.carfax_url`` (from the VDP's per-car Carfax report link) for every car
    that is still missing them. NEVER overwrites a real http value. Patched rows
    are re-synced in the incomplete-listings index. Safe to call for any dealer:
    a non-TV dealer simply yields no galleries.
    """
    from backend.db.incomplete_listings_db import sync_incomplete_listing_for_car_id
    from backend.db.inventory_db import db_conn

    rows = _rows_needing_completion(dealer_id, limit)
    stats = {
        "candidates": len(rows),
        "fetched": 0,
        "with_images": 0,
        "with_carfax": 0,
        "rows_patched": 0,
        "no_gallery": 0,
        "errors": 0,
        "total_photos": 0,
    }
    if not rows:
        return stats

    def _job(row):
        car_id, vin, source_url, image_url, gallery, carfax_url = row
        try:
            data = complete_from_vdp(source_url)
            return car_id, image_url, gallery, carfax_url, data, None
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as exc:
            return car_id, image_url, gallery, carfax_url, None, str(exc)

    patched_ids: list[int] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool, db_conn() as conn:
        cur = conn.cursor()
        futures = [pool.submit(_job, r) for r in rows]
        for i, fut in enumerate(as_completed(futures), 1):
            car_id, image_url, gallery, carfax_url, data, err = fut.result()
            if err is not None:
                stats["errors"] += 1
                logger.debug("[%s] car %s fetch error: %s", dealer_id, car_id, err)
                continue
            stats["fetched"] += 1
            imgs = dedupe_urls_order_prefer_large(data["gallery"]) if data["gallery"] else []
            carfax = data.get("carfax_url")

            updates: dict[str, Any] = {}
            if imgs:
                stats["with_images"] += 1
                stats["total_photos"] += len(imgs)
                if _image_url_fillable(image_url):
                    updates["image_url"] = imgs[0]
                if _gallery_fillable(gallery):
                    updates["gallery"] = json.dumps(imgs)
            else:
                stats["no_gallery"] += 1
            if carfax:
                stats["with_carfax"] += 1
                if _carfax_fillable(carfax_url):
                    updates["carfax_url"] = carfax

            if not updates:
                continue
            stats["rows_patched"] += 1
            if dry_run:
                continue
            set_sql = ", ".join(f"{f} = ?" for f in updates)
            cur.execute(
                f"UPDATE cars SET {set_sql} WHERE id = ?", (*updates.values(), car_id)
            )
            patched_ids.append(car_id)
            if len(patched_ids) % 200 == 0:
                conn.commit()
                logger.info("[%s] committed %d patched so far", dealer_id, len(patched_ids))
            if i % 250 == 0:
                logger.info(
                    "[%s] progress %d/%d (patched=%d)",
                    dealer_id, i, len(rows), stats["rows_patched"],
                )
        if not dry_run:
            conn.commit()

    for car_id in patched_ids:
        try:
            sync_incomplete_listing_for_car_id(car_id)
        except Exception:
            logger.exception("index sync failed for car %s", car_id)
    return stats


def is_team_velocity_dealer(dealer_id: str) -> bool:
    """Known Team Velocity dealer id (the delta hook also detects by feed shape)."""
    return dealer_id in TEAM_VELOCITY_DEALERS
