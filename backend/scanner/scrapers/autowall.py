"""
autoWALL (Gratis Technologies) server-rendered inventory scraper.

Sites like Long Automotive Group (Volvo, Mercedes-Benz, Genesis) use this
platform. Inventory is at /gs-vehicle/list with URL-based pagination (?page=N).
Each vehicle card is a ``div.col.vehicle-inventory-container[data-vin]``.
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Any
from urllib.parse import urljoin

import requests as _requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger("scanner")

_VIN_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$", re.I)
_PRICE_RE = re.compile(r"\$([\d,]+)")
_MILEAGE_RE = re.compile(r"([\d,]+)")
_MPG_RE = re.compile(r"^(\d{1,2})\s+MPG", re.I)

_AUTOWALL_PATHS = ("/gs-vehicle/list",)

_VDP_SPEC_FIELDS = ("exterior_color", "interior_color", "engine_description", "transmission", "body_style", "mpg_city", "mpg_highway")


def _is_autowall_html(html: str) -> bool:
    return "vehicle-inventory-container" in html and "gs-vehicle" in html


def _parse_price(text: str) -> int | None:
    m = _PRICE_RE.search(text or "")
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _parse_mileage(text: str) -> int | None:
    m = _MILEAGE_RE.search((text or "").replace(",", ""))
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _condition_normalize(raw: str) -> str | None:
    r = raw.strip().lower()
    if "certified" in r:
        return "Certified Pre-Owned"
    if "pre" in r or "used" in r:
        return "Used"
    if "new" in r:
        return "New"
    return raw.strip() or None


def _parse_card(container: Any, base_url: str, dealer_id: str, dealer_name: str) -> dict[str, Any] | None:
    """Parse one vehicle-inventory-container BeautifulSoup element."""
    vin = (container.get("data-vin") or "").strip().upper()
    if not _VIN_RE.match(vin):
        return None

    # Title link: "2012 Kia Sedona EX"
    name_tag = container.find(class_="product-name")
    title = name_tag.get_text(strip=True) if name_tag else ""
    source_url = ""
    if name_tag and name_tag.get("href"):
        source_url = urljoin(base_url, name_tag["href"])
    # Fall back to canonical autoWALL VDP URL if no href found on the card
    if not source_url.startswith("http"):
        source_url = f"{base_url.rstrip('/')}/gs-vehicle/detail/{vin}"

    # Parse year/make/model/trim from title
    year: int | None = None
    make: str | None = None
    model: str | None = None
    trim: str | None = None

    # Make from small.text-muted
    make_tag = container.find(class_="text-muted")
    if make_tag:
        make = make_tag.get_text(strip=True) or None

    # Title format: "YEAR MAKE MODEL TRIM"
    title_m = re.match(r"(\d{4})\s+\S+\s+(.+)", title)
    if title_m:
        try:
            year = int(title_m.group(1))
        except ValueError:
            pass
        rest = title_m.group(2).strip()
        if " " in rest:
            parts = rest.split(None, 1)
            model = parts[0]
            trim = parts[1]
        else:
            model = rest

    # Price from span.product-price
    price_tag = container.find(class_="product-price")
    price = _parse_price(price_tag.get_text() if price_tag else "")

    # Image
    img_tag = container.find(class_="product-imitation")
    img_tag = img_tag.find("img") if img_tag else None
    image_url = img_tag.get("src", "") if img_tag else ""

    # Labeled rows
    labeled: dict[str, str] = {}
    for row in container.find_all(class_="row"):
        cols = row.find_all(class_="col")
        if len(cols) == 2:
            label = cols[0].get_text(strip=True).rstrip(":")
            value = cols[1].get_text(strip=True)
            if label:
                labeled[label] = value

    condition = _condition_normalize(labeled.get("Condition", ""))
    stock_number = labeled.get("Stock #") or None
    fuel_type = labeled.get("Fuel Type") or None
    mileage_raw = labeled.get("Mileage", "")
    mileage = _parse_mileage(mileage_raw) if mileage_raw else None
    exterior_color = (
        labeled.get("Exterior Color")
        or labeled.get("Ext. Color")
        or labeled.get("Color")
        or labeled.get("Colour")
        or labeled.get("Exterior")
        or None
    )
    interior_color = (
        labeled.get("Interior Color")
        or labeled.get("Int. Color")
        or labeled.get("Interior")
        or None
    )

    return {
        "vin": vin,
        "year": year,
        "make": make,
        "model": model,
        "trim": trim,
        "price": price,
        "mileage": mileage,
        "condition": condition,
        "stock_number": stock_number,
        "fuel_type": fuel_type,
        "exterior_color": exterior_color or None,
        "interior_color": interior_color or None,
        "image_url": image_url,
        "gallery": [image_url] if image_url else [],
        "source_url": source_url or None,
        "_detail_url": source_url or None,
        "dealer_name": dealer_name,
        "dealer_url": base_url,
        "dealer_id": dealer_id,
    }


def parse_autowall_inventory_html(
    html: str,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> list[dict[str, Any]]:
    """Parse all vehicle cards from one autoWALL page of HTML."""
    if not _is_autowall_html(html):
        return []

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.warning("autoWALL: BeautifulSoup not available")
        return []

    soup = BeautifulSoup(html, "html.parser")
    containers = soup.find_all(class_=re.compile(r"\bvehicle-inventory-container\b"))
    vehicles: list[dict[str, Any]] = []
    for c in containers:
        v = _parse_card(c, base_url, dealer_id, dealer_name)
        if v:
            vehicles.append(v)
    return vehicles


def _parse_vdp_snapshot(html: str) -> dict[str, Any]:
    """Extract spec data from autoWALL VDP page's ul.vehicle-snapshot section."""
    try:
        from bs4 import BeautifulSoup, NavigableString, Tag
    except ImportError:
        return {}
    soup = BeautifulSoup(html, "html.parser")
    snapshot_ul = soup.find("ul", class_="vehicle-snapshot")
    if not snapshot_ul:
        return {}
    result: dict[str, Any] = {}
    for li in snapshot_ul.find_all("li"):
        sd = li.find(class_="snapshot-data")
        if not sd:
            continue
        direct = ""
        child_spans: list[str] = []
        for node in sd.children:
            if isinstance(node, NavigableString):
                t = str(node).strip()
                if t:
                    direct = t
            elif isinstance(node, Tag) and node.name == "span":
                t = node.get_text(strip=True)
                if t:
                    child_spans.append(t)
        if _MPG_RE.match(direct):
            val = int(_MPG_RE.match(direct).group(1))
            label = (child_spans[0] if child_spans else "").lower()
            if "city" in label:
                result.setdefault("mpg_city", val)
            elif "hwy" in label or "highway" in label:
                result.setdefault("mpg_highway", val)
        elif direct.startswith("Exterior Color:"):
            result["exterior_color"] = direct[len("Exterior Color:"):].strip() or None
            for s in child_spans:
                if s.startswith("Interior Color:"):
                    result["interior_color"] = s[len("Interior Color:"):].strip() or None
        elif not direct and any(s.startswith("Condition:") for s in child_spans):
            for s in child_spans:
                if s.startswith("Stock #:"):
                    result.setdefault("stock_number", s[len("Stock #:"):].strip())
                elif s.startswith("Mileage:"):
                    m = _MILEAGE_RE.search(s.replace(",", ""))
                    if m:
                        result.setdefault("mileage", int(m.group(1)))
        elif direct:
            # Engine / transmission / body style item
            result.setdefault("engine_description", direct)
            if len(child_spans) >= 1:
                result.setdefault("transmission", child_spans[0])
            if len(child_spans) >= 2:
                result.setdefault("body_style", child_spans[1])
    return result


def _enrich_vehicles_from_vdp(
    vehicles: list[dict[str, Any]],
    session: "_requests.Session",
    dealer_name: str,
) -> int:
    """
    Parallel HTTP fetch of VDP pages; enrich vehicles with vehicle-snapshot spec data.
    Skips vehicles where all spec fields are already populated. Returns enriched count.
    """
    from concurrent.futures import ThreadPoolExecutor

    to_enrich = [
        v for v in vehicles
        if v.get("_detail_url", "").startswith("http")
        and not all(v.get(f) for f in _VDP_SPEC_FIELDS)
    ]
    if not to_enrich:
        return 0

    def _one(v: dict[str, Any]) -> bool:
        try:
            r = session.get(v["_detail_url"], timeout=8, verify=False)
            if r.status_code != 200 or len(r.text) < 500:
                return False
            data = _parse_vdp_snapshot(r.text)
            changed = False
            for k, val in data.items():
                if val and not v.get(k):
                    v[k] = val
                    changed = True
            return changed
        except Exception as exc:
            logger.debug("autoWALL VDP enrich: %s — %s", v.get("_detail_url"), exc)
            return False

    with ThreadPoolExecutor(max_workers=4) as ex:
        results = list(ex.map(_one, to_enrich))

    enriched = sum(1 for r in results if r)
    if enriched:
        logger.info("autoWALL: %s — VDP snapshot enriched %d/%d vehicle(s)", dealer_name, enriched, len(to_enrich))
    return enriched


_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


async def fetch_autowall_inventory_http(
    base_url: str,
    dealer_id: str,
    dealer_name: str,
) -> list[dict[str, Any]]:
    """
    Fetch all autoWALL inventory via plain HTTP requests (no Playwright).

    autoWALL requires a homepage visit to establish a session cookie before
    inventory pages (/gs-vehicle/list?filter=All&page=N) will respond with 200.
    Uses a ``requests.Session`` to persist cookies across pages.
    """
    inv_base = base_url.rstrip("/")
    by_vin: dict[str, dict[str, Any]] = {}

    def _fetch_all() -> list[str]:
        session = _requests.Session()
        session.headers.update(_HTTP_HEADERS)
        # Establish session cookie via homepage
        try:
            session.get(inv_base + "/", timeout=15, verify=False)
        except Exception as exc:
            logger.debug("autoWALL HTTP: homepage warmup failed for %s: %s", dealer_name, exc)

        pages: list[str] = []
        for page_num in range(1, 50):
            url = f"{inv_base}/gs-vehicle/list?filter=All&page={page_num}"
            try:
                r = session.get(url, timeout=20, verify=False)
                if r.status_code != 200:
                    logger.debug("autoWALL HTTP: %s page %d → HTTP %d", dealer_name, page_num, r.status_code)
                    break
                if len(r.text) < 600 and page_num == 1:
                    logger.warning(
                        "autoWALL HTTP: %s — tiny response (%d bytes) on page 1, likely rate-limited",
                        dealer_name, len(r.text),
                    )
                    break
                pages.append(r.text)
            except Exception as exc:
                logger.debug("autoWALL HTTP: %s page %d failed: %s", dealer_name, page_num, exc)
                break
        return pages

    all_pages = await asyncio.to_thread(_fetch_all)

    for page_num, html in enumerate(all_pages, start=1):
        batch = parse_autowall_inventory_html(html, inv_base, dealer_id, dealer_name, inv_base)
        if not batch:
            logger.debug("autoWALL HTTP: %s page %d — 0 vehicles, stopping", dealer_name, page_num)
            break
        new_count = 0
        for v in batch:
            vin = (v.get("vin") or "").strip().upper()
            if vin and vin not in by_vin:
                by_vin[vin] = v
                new_count += 1
        logger.debug(
            "autoWALL HTTP: %s page %d — +%d new (total %d)",
            dealer_name, page_num, new_count, len(by_vin),
        )
        if new_count == 0 and page_num > 1:
            break

    if by_vin:
        logger.info("autoWALL HTTP: %s — %d vehicle(s) scraped", dealer_name, len(by_vin))

    def _run_vdp_enrich() -> int:
        session2 = _requests.Session()
        session2.headers.update(_HTTP_HEADERS)
        try:
            session2.get(inv_base + "/", timeout=10, verify=False)
        except Exception:
            pass
        return _enrich_vehicles_from_vdp(list(by_vin.values()), session2, dealer_name)

    await asyncio.to_thread(_run_vdp_enrich)
    return list(by_vin.values())


def fetch_autowall_vehicle_by_vin(
    base_url: str,
    want_vin: str,
    *,
    dealer_id: str = "",
    dealer_name: str = "",
    max_pages: int = 40,
) -> tuple[dict[str, Any], list[str]]:
    """
    Sync HTTP lookup of one autoWALL inventory row by VIN (recovery / backfill).

    Establishes a session via homepage, paginates ``/gs-vehicle/list``, optionally
    enriches from the vehicle VDP snapshot.
    """
    notes: list[str] = []
    vin_u = (want_vin or "").strip().upper()
    if not vin_u:
        return {}, notes
    inv_base = (base_url or "").strip().rstrip("/")
    if not inv_base.startswith("http"):
        return {}, notes

    session = _requests.Session()
    session.headers.update(_HTTP_HEADERS)
    try:
        session.get(inv_base + "/", timeout=15, verify=False)
        notes.append("autowall:homepage_ok")
    except _requests.RequestException as exc:
        notes.append(f"autowall:homepage_error:{str(exc)[:120]}")

    found: dict[str, Any] | None = None
    for page_num in range(1, max(1, int(max_pages)) + 1):
        url = f"{inv_base}/gs-vehicle/list?filter=All&page={page_num}"
        try:
            r = session.get(url, timeout=20, verify=False)
        except _requests.RequestException as exc:
            notes.append(f"autowall:http_error:page{page_num}:{str(exc)[:120]}")
            break
        if r.status_code != 200:
            notes.append(f"autowall:status:page{page_num}:{r.status_code}")
            break
        batch = parse_autowall_inventory_html(r.text, inv_base, dealer_id, dealer_name, inv_base)
        if not batch:
            notes.append(f"autowall:empty:page{page_num}")
            break
        for v in batch:
            if (v.get("vin") or "").strip().upper() == vin_u:
                found = v
                notes.append(f"autowall:match:page{page_num}")
                break
        if found:
            break
        if page_num > 1 and len(batch) == 0:
            break

    if not found:
        notes.append("autowall:no_vin_match")
        return {}, notes

    detail = (found.get("_detail_url") or found.get("source_url") or "").strip()
    if not detail.startswith("http"):
        detail = f"{inv_base}/gs-vehicle/detail/{vin_u}"
    found["_detail_url"] = detail
    _enrich_vehicles_from_vdp([found], session, dealer_name or "Dealer")
    return found, notes


_AUTOWALL_DESKTOP_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


async def scrape_autowall_via_playwright(
    page: Any,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
) -> list[dict[str, Any]]:
    """
    Scrape autoWALL inventory using a fresh desktop-UA browser context.

    autoWALL is server-rendered. If the scanner context used a mobile UA, the warmup
    sets mobile session cookies that cause subsequent navigations to redirect to a
    consolidated mobile domain (which lacks inventory). We bypass this by creating a
    fresh browser context with a fixed desktop UA so every request — homepage warmup
    and inventory list — uses the same clean desktop session.
    """
    inv_base = base_url.rstrip("/")
    by_vin: dict[str, dict[str, Any]] = {}

    # Open a fresh context with desktop UA (avoids mobile session cookie redirect issues)
    _fresh_ctx = None
    _wp: Any = page  # active page reference — replaced with fresh page if possible
    try:
        _browser = getattr(getattr(page, "context", None), "browser", None)
        if _browser is not None:
            _fresh_ctx = await _browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=_AUTOWALL_DESKTOP_UA,
            )
            _wp = await _fresh_ctx.new_page()
    except Exception as exc:
        logger.debug("autoWALL Playwright: fresh context failed for %s, using existing page: %s", dealer_name, exc)

    async def _nav_path(path: str, wait_sec: float = 6.0) -> bool:
        """Navigate to a relative path via window.location.href; waits for domcontentloaded."""
        try:
            nav_timeout_ms = max(15000, int(wait_sec * 1000) + 5000)
            async with _wp.expect_navigation(
                wait_until="domcontentloaded",
                timeout=nav_timeout_ms,
            ):
                await _wp.evaluate(f"window.location.href = {repr(path)}")
            await asyncio.sleep(1.5)
            return True
        except Exception as exc:
            logger.debug("autoWALL Playwright: nav to %s failed for %s: %s", path, dealer_name, exc)
            return False

    try:
        # Homepage visit establishes session cookies and resolves www→canonical redirect.
        try:
            await _wp.goto(inv_base + "/", wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(2)
        except Exception as exc:
            logger.info("autoWALL Playwright: homepage nav failed for %s — proceeding anyway: %s", dealer_name, str(exc)[:120])

        # Update inv_base to canonical URL after potential www→non-www redirect
        try:
            from urllib.parse import urlparse as _pu2
            _p2 = _pu2(_wp.url or "")
            if _p2.scheme and _p2.netloc:
                _canonical = f"{_p2.scheme}://{_p2.netloc}"
                if _canonical != inv_base:
                    logger.debug("autoWALL Playwright: %s canonical URL %s (was %s)", dealer_name, _canonical, inv_base)
                    inv_base = _canonical
        except Exception:
            pass

        # Page 1: relative path stays on the same canonical domain
        if not await _nav_path("/gs-vehicle/list?filter=All", wait_sec=7.0):
            return []

        for page_num in range(1, 50):
            if page_num > 1:
                if not await _nav_path(f"/gs-vehicle/list?filter=All&page={page_num}", wait_sec=5.0):
                    break

            try:
                html = await _wp.content()
            except Exception:
                await asyncio.sleep(3)
                html = await _wp.content()
            batch = parse_autowall_inventory_html(html, inv_base, dealer_id, dealer_name, inv_base)
            if page_num == 1 and not batch:
                import re as _re
                _containers = len(_re.findall(r'vehicle-inventory-container', html))
                logger.info("autoWALL Playwright: %s page 1 — 0 vehicles (url=%s html=%d containers=%d)", dealer_name, _wp.url, len(html), _containers)
                if len(html) < 600:
                    logger.warning(
                        "autoWALL Playwright: %s — tiny response (%d bytes), likely rate-limited; skipping",
                        dealer_name, len(html),
                    )
                    break
            if not batch:
                logger.debug("autoWALL Playwright: %s page %d — 0 vehicles, stopping", dealer_name, page_num)
                break

            new_count = 0
            for v in batch:
                vin = (v.get("vin") or "").upper()
                if vin and vin not in by_vin:
                    by_vin[vin] = v
                    new_count += 1

            logger.debug(
                "autoWALL Playwright: %s page %d — +%d new (total %d)",
                dealer_name, page_num, new_count, len(by_vin),
            )
            if new_count == 0 and page_num > 1:
                break

        # Carry Playwright session cookies into an HTTP session, then parallel-fetch
        # VDP pages to enrich all spec fields (color, engine, transmission, body, MPG).
        if by_vin and _fresh_ctx is not None:
            try:
                pw_cookies = await _fresh_ctx.cookies()
            except Exception:
                pw_cookies = []

            def _run_pw_vdp_enrich(cookies: list[dict]) -> int:
                sess = _requests.Session()
                sess.headers.update(_HTTP_HEADERS)
                for c in cookies:
                    try:
                        sess.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))
                    except Exception:
                        pass
                return _enrich_vehicles_from_vdp(list(by_vin.values()), sess, dealer_name)

            await asyncio.to_thread(_run_pw_vdp_enrich, pw_cookies)

    finally:
        if _fresh_ctx is not None:
            try:
                await _fresh_ctx.close()
            except Exception:
                pass

    if by_vin:
        logger.info("autoWALL: %s — %d vehicle(s) via Playwright", dealer_name, len(by_vin))
    return list(by_vin.values())


async def scrape_autowall_from_page(
    page: Any,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> list[dict[str, Any]]:
    """Playwright-based autoWALL scraper (delegates to scrape_autowall_via_playwright)."""
    return await scrape_autowall_via_playwright(page, base_url, dealer_id, dealer_name)
