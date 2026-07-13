"""
PixelMotion (WordPress pm-motors-plugin) inventory scraper.

Sites like McPeek's CDJR render inventory server-side (``vlpm3VehicleRow`` cards) with
optional API calls to ``pixelmotiondemo.com``. Inventory lives at ``/inventory/new/``,
``/inventory/used/``, etc. — not Dealer.com ``/new-inventory/index.htm`` paths.
"""
from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

_PIXEL_PATHS = (
    "/inventory/new/",
    "/inventory/used/",
    "/inventory/certified/",
    "/inventory/cpo/",
)
_VIN_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$", re.I)
_ROW_RE = re.compile(
    r'<div class="vlpm3VehicleRow[^"]*" id="([A-HJ-NPR-Z0-9]{17})">(.*?)(?=<div class="vlpm3VehicleRow|\Z)',
    re.I | re.S,
)
_DOLLAR_RE = re.compile(r"&dollar;([\d,]+)|\$\s*([\d,]+)")
# pm-motors-plugin cards ship a commented-out legacy <ul> whose first
# ``</strong><span>...</span>`` pair is the STOCK NUMBER; never read inside comments.
_COMMENT_RE = re.compile(r"<!--.*?-->", re.S)


def _is_pixel_motion_html(html: str) -> bool:
    if not html:
        return False
    low = html.lower()
    return (
        "pixelmotion" in low
        or "vlpm3vehiclerow" in low
        or "pm-motors-plugin" in low
    )


def _extract_span(block: str, class_name: str) -> str | None:
    """
    Extract one labeled value (``<strong>Label</strong>`` + value) from the card.

    The value may be wrapped in a ``<span>`` OR appear as bare text after
    ``</strong>``. The search is bounded to the matching element's inner HTML —
    an unbounded ``.*?</strong>\\s*<span>`` gap under ``re.S`` used to scan past
    the element boundary and capture the stock number from the commented-out
    legacy ``<ul>`` that pm-motors-plugin leaves in every card (stamping the
    stock code into exterior/interior color, drivetrain, transmission, engine
    and mileage). HTML comments are stripped first for the same reason.
    """
    block = _COMMENT_RE.sub("", block)
    m = re.search(
        rf'class="{re.escape(class_name)}"[^>]*>(.*?)</(?:div|li)>',
        block,
        re.I | re.S,
    )
    if not m:
        return None
    inner = m.group(1)
    vm = re.search(
        r"</strong>\s*(?:<span[^>]*>([^<]+)</span>|([^<]+))",
        inner,
        re.I | re.S,
    )
    if not vm:
        return None
    s = re.sub(r"\s+", " ", (vm.group(1) or vm.group(2) or "")).strip()
    return s or None


def _parse_title(title: str) -> tuple[str | None, int | None, str | None, str | None]:
    t = re.sub(r"\s+", " ", (title or "").strip())
    if not t:
        return None, None, None, None
    m = re.match(
        r"^(?:(New|Used|Certified(?:\s+Pre-Owned)?|CPO)\s+)?(\d{4})\s+(\S+)\s+(.+)$",
        t,
        re.I,
    )
    if not m:
        return None, None, None, t
    cond_raw, year_s, make, rest = m.groups()
    condition = None
    if cond_raw:
        cl = cond_raw.lower()
        if "cert" in cl or cl == "cpo":
            condition = "Certified Pre-Owned"
        elif "used" in cl:
            condition = "Used"
        elif "new" in cl:
            condition = "New"
    try:
        year = int(year_s)
    except (TypeError, ValueError):
        year = None
    return condition, year, make, rest.strip() or None


def _parse_price(block: str) -> int | None:
    # Prefer dealer selling price labeled McPeeks / Your Price / Sale Price.
    for label in ("McPeeks", "Your Price", "Sale Price", "Internet Price", "Price"):
        m = re.search(
            rf"{re.escape(label)}.*?price-item-value'>(?:&minus;\s*&amp;)?(?:&dollar;|\$)\s*([\d,]+)",
            block,
            re.I | re.S,
        )
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except ValueError:
                pass
    prices: list[int] = []
    for m in _DOLLAR_RE.finditer(block):
        raw = m.group(1) or m.group(2)
        if not raw:
            continue
        try:
            prices.append(int(raw.replace(",", "")))
        except ValueError:
            continue
    return min(prices) if prices else None


def _parse_mileage(raw: str | None) -> int | None:
    if not raw:
        return None
    # A stock-code-shaped token (e.g. "T0147", "UK0005") is not a mileage;
    # extracting its digits would fabricate odometer readings.
    if re.search(r"[A-Za-z]\d|\d[A-Za-z]", raw):
        return None
    m = re.search(r"([\d,]+)", raw.replace(",", ""))
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def parse_pixel_motion_inventory_html(
    html: str,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> list[dict[str, Any]]:
    """Parse SSR PixelMotion VLP rows from page HTML."""
    if not _is_pixel_motion_html(html):
        return []

    vehicles: list[dict[str, Any]] = []
    seen: set[str] = set()

    for vin, block in _ROW_RE.findall(html):
        vin = vin.upper()
        if not _VIN_RE.match(vin) or vin in seen:
            continue
        seen.add(vin)

        title_m = re.search(r'<h2 class="condition[^"]*">([^<]+)</h2>', block, re.I)
        title = title_m.group(1).strip() if title_m else ""
        condition, year, make, model_trim = _parse_title(title)
        if model_trim and " " in model_trim:
            parts = model_trim.split(None, 1)
            model, trim = parts[0], parts[1]
        else:
            model, trim = model_trim, None

        href_m = re.search(r'class="view-vehicle[^"]*"[^>]*\bhref="([^"]+)"', block, re.I)
        detail_path = href_m.group(1).strip() if href_m else ""
        detail_url = urljoin(base_url.rstrip("/") + "/", detail_path.lstrip("/")) if detail_path else ""

        img_m = re.search(
            r'vlpm3VehicleImage__static[^>]*>.*?src="(https?://[^"]+)"',
            block,
            re.I | re.S,
        )
        image_url = img_m.group(1) if img_m else ""

        row = {
            "vin": vin,
            "year": year,
            "make": make,
            "model": model,
            "trim": trim,
            "price": _parse_price(block),
            "mileage": _parse_mileage(_extract_span(block, "vlp-item-mileage")),
            "condition": condition,
            "exterior_color": _extract_span(block, "vlp-item-color-exterior"),
            "interior_color": _extract_span(block, "vlp-item-color-interior"),
            "transmission": _extract_span(block, "vlp-item-transmission"),
            "drivetrain": _extract_span(block, "vlp-item-drive"),
            "engine_description": _extract_span(block, "vlp-item-engine"),
            "stock_number": _extract_span(block, "vlp-item-stockNum"),
            "image_url": image_url,
            "gallery": [image_url] if image_url else [],
            "source_url": detail_url or None,
            "_detail_url": detail_url or None,
            "dealer_name": dealer_name,
            "dealer_url": dealer_url,
            "dealer_id": dealer_id,
        }
        vehicles.append(row)

    return vehicles


async def _dismiss_cookie_banner(page: Any) -> None:
    for sel in (
        'button:has-text("Allow all cookies")',
        'button:has-text("Accept All")',
        'button:has-text("Accept all")',
        '[data-action="accept"]',
    ):
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0 and await loc.is_visible():
                await loc.click(timeout=2000)
                return
        except Exception:
            continue


async def _scrape_path_with_pagination(
    page: Any,
    inv_base: str,
    path: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
    by_vin: dict[str, dict[str, Any]],
) -> int:
    """Navigate one PixelMotion VLP path and paginate via ``.vlpm3Pages__next``."""
    import asyncio

    target = inv_base.rstrip("/") + path
    added = 0
    try:
        await page.goto(target, wait_until="domcontentloaded", timeout=25000)
        await _dismiss_cookie_banner(page)
        try:
            await page.locator(".vlpm3VehicleRow").first.wait_for(state="attached", timeout=12000)
        except Exception:
            pass
    except Exception as e:
        logger.debug("PixelMotion: nav %s failed: %s", target, e)
        return 0

    max_pages = 20
    prev_total = len(by_vin)
    for _pag in range(max_pages):
        html = await page.content()
        batch = parse_pixel_motion_inventory_html(html, inv_base, dealer_id, dealer_name, dealer_url)
        for v in batch:
            vin = (v.get("vin") or "").upper()
            if vin and vin not in by_vin:
                by_vin[vin] = v
                added += 1
        if len(by_vin) == prev_total and _pag > 0:
            break
        prev_total = len(by_vin)

        next_btn = page.locator(".vlpm3Pages__next")
        try:
            if await next_btn.count() == 0:
                break
            first = next_btn.first
            if not await first.is_visible():
                break
            await first.click(timeout=5000)
            await asyncio.sleep(0.8)
        except Exception:
            break

    return added


async def scrape_pixel_motion_from_page(
    page: Any,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> list[dict[str, Any]]:
    """
    Extract PixelMotion inventory from the current page and standard VLP paths.
    """
    by_vin: dict[str, dict[str, Any]] = {}

    try:
        html = await page.content()
    except Exception as e:
        logger.debug("PixelMotion: could not read page content: %s", e)
        html = ""

    if _is_pixel_motion_html(html):
        for v in parse_pixel_motion_inventory_html(html, base_url, dealer_id, dealer_name, dealer_url):
            vin = (v.get("vin") or "").upper()
            if vin:
                by_vin[vin] = v

    if not _is_pixel_motion_html(html) and not by_vin:
        return []

    # Resolve inventory host (McPeek redirects anaheim → mcpeeks.com).
    inv_base = base_url.rstrip("/")
    try:
        cur = page.url or ""
        if cur.startswith("http"):
            from urllib.parse import urlparse

            p = urlparse(cur)
            inv_base = f"{p.scheme}://{p.netloc}"
    except Exception:
        pass

    for path in _PIXEL_PATHS:
        added = await _scrape_path_with_pagination(
            page, inv_base, path, dealer_id, dealer_name, dealer_url, by_vin
        )
        logger.debug(
            "PixelMotion: %s — +%d new (total %d) for %s",
            path,
            added,
            len(by_vin),
            dealer_name,
        )

    if by_vin:
        logger.info("PixelMotion: scraped %d vehicles for %s", len(by_vin), dealer_name)
    return list(by_vin.values())
