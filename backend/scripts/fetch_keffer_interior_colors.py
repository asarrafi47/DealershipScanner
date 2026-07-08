#!/usr/bin/env python3
"""
Fetch interior (and exterior) colors from dealer VDP pages via Schema.org JSON-LD.

Strategy A (urllib — fast, no JS): Keffer Jeep pages embed a simple ``Car``
JSON-LD block with populated ``vehicleInteriorColor``.

Strategy B (Playwright — JS-rendered): Tuttle Click Ford pages use a
``["Product","Car"]`` JSON-LD but leave ``vehicleInteriorColor`` empty; the
interior color is mentioned in the description text (e.g. "Ebony interior").

This script finds all cars missing ``interior_color`` from inventory.db, tries
Strategy A first, then falls back to Strategy B for pages that return HTTP 403
or where the JSON-LD color field is blank.
"""
import html as _html_lib
import json
import logging
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv
    load_project_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("fetch_interior_colors")

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

_JSONLD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)

# Handles two description patterns:
#   "{ext} exterior and {int} interior"  →  captures just {int}
#   "with/and {int} interior"            →  captures {int}
_INTERIOR_EXTERIOR_AND_RE = re.compile(
    r'\bexterior\s+and\s+([A-Za-z][A-Za-z /\-]{1,40}?)\s+interior\b',
    re.IGNORECASE,
)
_INTERIOR_DESC_RE = re.compile(
    r'\b(?:with\s+|and\s+)([A-Za-z][A-Za-z /\-]{1,40}?)\s+interior\b',
    re.IGNORECASE,
)


def _fetch_html(url: str, timeout: int = 15) -> tuple[str | None, bool]:
    """Return (html, used_fallback). used_fallback=False for urllib success."""
    req = urllib.request.Request(url, headers=_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace"), False
    except urllib.error.HTTPError as e:
        logger.debug(f"HTTP {e.code} on urllib for {url} — will try Playwright")
        return None, True
    except Exception as e:
        logger.debug(f"urllib error for {url}: {e} — will try Playwright")
        return None, True


async def _fetch_html_playwright(url: str) -> str | None:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.warning("playwright not installed; skipping JS-rendered fetch")
        return None
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ctx = await browser.new_context(user_agent=_HEADERS["User-Agent"])
            page = await ctx.new_page()
            await page.goto(url, wait_until="networkidle", timeout=30_000)
            content = await page.content()
            await browser.close()
            return content
    except Exception as e:
        logger.warning(f"Playwright error for {url}: {e}")
        return None


def _extract_car_colors(html: str) -> dict[str, str | None]:
    """
    Return {'interior': ..., 'exterior': ...} from Schema.org Car JSON-LD.

    Handles both ``@type: "Car"`` (Keffer) and ``@type: ["Product","Car"]``
    (Tuttle Click).  When ``vehicleInteriorColor`` is empty, falls back to
    parsing the description text for "… and {Color} interior".
    """
    result: dict[str, str | None] = {"interior": None, "exterior": None}
    for raw_block in _JSONLD_RE.findall(html):
        try:
            data = json.loads(raw_block.strip())
        except (json.JSONDecodeError, ValueError):
            continue
        if not isinstance(data, dict):
            continue
        block_type = data.get("@type", "")
        is_car = block_type in ("Car", "Vehicle", "car", "vehicle") or (
            isinstance(block_type, list) and any(t in ("Car", "Vehicle") for t in block_type)
        )
        if not is_car:
            continue

        interior = data.get("vehicleInteriorColor") or data.get("interiorColor")
        exterior = data.get("color") or data.get("vehicleExteriorColor")

        if isinstance(interior, str) and interior.strip():
            result["interior"] = interior.strip()

        # Fallback: parse description text for interior color.
        # Priority: "{ext} exterior and {int} interior" (more specific) then
        # the broader "with/and {int} interior" pattern.
        if not result["interior"]:
            desc = data.get("description", "") or ""
            desc_clean = _html_lib.unescape(desc)
            m = _INTERIOR_EXTERIOR_AND_RE.search(desc_clean) or _INTERIOR_DESC_RE.search(desc_clean)
            if m:
                candidate = m.group(1).strip().title()
                if len(candidate) >= 2:
                    result["interior"] = candidate

        if isinstance(exterior, str) and exterior.strip():
            result["exterior"] = exterior.strip()

        if result["interior"]:
            break
    return result


def _build_provenance(existing_json: str | None, new_fields: dict) -> str:
    try:
        base = json.loads(existing_json) if existing_json else {}
    except (TypeError, ValueError):
        base = {}
    if not isinstance(base, dict):
        base = {}
    base.update(new_fields)
    return json.dumps(base)


async def _process_cars(cars: list, conn: Any) -> tuple[int, int]:
    updated = 0
    skipped = 0

    for car in cars:
        car_id = car["id"]
        url = (car["source_url"] or "").strip()
        logger.info(f"  [{car_id}] {car['year']} {car['make']} {car['model']} — {url or '(no url)'}")

        if not url:
            skipped += 1
            continue

        html, need_playwright = _fetch_html(url)

        if need_playwright or not html:
            import asyncio as _asyncio
            html = _asyncio.get_event_loop().run_until_complete(_fetch_html_playwright(url))
            if not html:
                skipped += 1
                continue

        colors = _extract_car_colors(html)
        interior = colors["interior"]
        exterior = colors["exterior"]

        if not interior:
            logger.warning(f"    No interior color found")
            skipped += 1
            continue

        prov = _build_provenance(
            car["spec_source_json"],
            {
                "interior_color_source": "vdp_jsonld",
                "interior_color_scraped_url": url,
            },
        )

        fields: dict[str, object] = {
            "interior_color": interior,
            "spec_source_json": prov,
        }

        if exterior and (not car["exterior_color"] or not car["exterior_color"].strip()):
            fields["exterior_color"] = exterior
            logger.info(f"    interior={interior!r}  exterior (backfill)={exterior!r}")
        else:
            logger.info(f"    interior={interior!r}")

        set_clause = ", ".join(f"{k}=?" for k in fields)
        conn.execute(
            f"UPDATE cars SET {set_clause} WHERE id=?",
            list(fields.values()) + [car_id],
        )
        conn.commit()
        updated += 1

        time.sleep(0.5)

    return updated, skipped


def main() -> None:
    import asyncio

    from backend.db.inventory_db import get_conn

    conn = get_conn()
    conn.row_factory = sqlite3.Row
    try:
        _run(conn)
    finally:
        conn.close()


def _run(conn: Any) -> None:
    import asyncio

    cars = conn.execute(
        """
        SELECT id, make, model, year, vin, source_url, dealer_url, exterior_color,
               interior_color, spec_source_json
        FROM cars
        WHERE (interior_color IS NULL OR TRIM(interior_color) = '')
        ORDER BY id
        """
    ).fetchall()

    logger.info(f"{len(cars)} cars need interior color")

    loop = asyncio.new_event_loop()

    updated = 0
    skipped = 0
    for car in list(cars):
        car_id = car["id"]
        url = (car["source_url"] or "").strip()

        # For Keffer cars without a source_url, try the /viewdetails/Used/{VIN} pattern.
        if not url:
            vin = (car["vin"] or "").strip()
            dealer = (car["dealer_url"] or "").strip().rstrip("/")
            if vin and dealer and "kefferjeep" in dealer:
                url = f"{dealer}/viewdetails/Used/{vin}"
                logger.info(f"  [{car_id}] {car['year']} {car['make']} {car['model']} — (built url) {url}")
            else:
                logger.info(f"  [{car_id}] {car['year']} {car['make']} {car['model']} — (no url, skipping)")
                skipped += 1
                continue
        else:
            logger.info(f"  [{car_id}] {car['year']} {car['make']} {car['model']} — {url}")

        html, need_playwright = _fetch_html(url)

        if need_playwright or not html:
            html = loop.run_until_complete(_fetch_html_playwright(url))
            if not html:
                skipped += 1
                continue

        colors = _extract_car_colors(html)
        interior = colors["interior"]
        exterior = colors["exterior"]

        if not interior:
            logger.warning(f"    No interior color found")
            skipped += 1
            continue

        prov = _build_provenance(
            car["spec_source_json"],
            {
                "interior_color_source": "vdp_jsonld",
                "interior_color_scraped_url": url,
            },
        )

        fields: dict[str, object] = {
            "interior_color": interior,
            "spec_source_json": prov,
        }

        if exterior and (not car["exterior_color"] or not car["exterior_color"].strip()):
            fields["exterior_color"] = exterior
            logger.info(f"    interior={interior!r}  exterior (backfill)={exterior!r}")
        else:
            logger.info(f"    interior={interior!r}")

        set_clause = ", ".join(f"{k}=?" for k in fields)
        conn.execute(
            f"UPDATE cars SET {set_clause} WHERE id=?",
            list(fields.values()) + [car_id],
        )
        conn.commit()
        updated += 1

        time.sleep(0.5)

    loop.close()
    logger.info(f"Done. Updated {updated}, skipped {skipped}")


if __name__ == "__main__":
    main()
