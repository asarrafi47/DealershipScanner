"""
Crawl4AI-based inventory sniffer for non-Dealer.com sites.

Detects vehicle data via:
  1. Algolia search API credentials embedded in page scripts
  2. VinSolutions / window-global JSON blobs (window.__INVENTORY__, etc.)
  3. CDK / __PRELOADED_STATE__ blobs
  4. Generic inline JSON arrays with VIN + year/make/model fields

Crawls the homepage first, then a short list of common inventory path
suffixes if the homepage yields nothing.

Usage (from scanner.js):
  python3 -m scrapers.crawl4ai_inventory https://example-dealer.com

Prints one machine line to stdout:
  CRAWL4AI_INVENTORY:{...json...}

All logs go to stderr only.
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
from typing import Any
from urllib.parse import urlparse, urljoin

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None  # type: ignore[misc, assignment]

try:
    import requests as _requests_lib
except ImportError:
    _requests_lib = None  # type: ignore[assignment]

# ── Common inventory URL path suffixes to try when homepage yields nothing ──
_INVENTORY_PATHS = [
    "/inventory",
    "/new-inventory",
    "/used-inventory",
    "/new-vehicles",
    "/used-vehicles",
    "/vehicles",
    "/cars-for-sale",
    "/searchused.aspx",
    "/searchnew.aspx",
    "/inventory/new",
    "/inventory/used",
]

_VIN_RE = re.compile(r"\b[A-HJ-NPR-Z0-9]{17}\b")

# ── Algolia credential extraction ──────────────────────────────────────────

def _extract_algolia_credentials(scripts_text: list[str]) -> dict[str, str] | None:
    combined = "\n".join(scripts_text)
    # Only bother if 'algolia' appears at all
    if not re.search(r"algolia", combined, re.I):
        return None
    app_m = re.search(
        r'["\']?(?:applicationId|appId)["\']?\s*[:=]\s*["\']([A-Z0-9]{8,12})["\']',
        combined, re.I,
    )
    key_m = re.search(
        r'["\']?(?:apiKey|searchApiKey|searchOnlyApiKey)["\']?\s*[:=]\s*["\']([a-f0-9]{32})["\']',
        combined, re.I,
    )
    idx_m = re.search(
        r'["\']?indexName["\']?\s*[:=]\s*["\']([^"\']{3,80})["\']',
        combined, re.I,
    )
    if app_m and key_m and idx_m:
        return {
            "app_id": app_m.group(1),
            "api_key": key_m.group(1),
            "index_name": idx_m.group(1),
        }
    return None


def _query_algolia(creds: dict[str, str]) -> list[dict]:
    if _requests_lib is None:
        return []
    url = f"https://{creds['app_id']}-dsn.algolia.net/1/indexes/{creds['index_name']}/query"
    headers = {
        "X-Algolia-Application-Id": creds["app_id"],
        "X-Algolia-API-Key": creds["api_key"],
        "Content-Type": "application/json",
    }
    try:
        resp = _requests_lib.post(
            url,
            json={"query": "", "hitsPerPage": 500, "page": 0},
            headers=headers,
            timeout=15,
        )
        if resp.ok:
            return resp.json().get("hits", [])
    except Exception as e:
        print(f"[crawl4ai_inventory] Algolia query failed: {e}", file=sys.stderr)
    return []


def _normalize_algolia_hit(hit: dict) -> dict | None:
    vin = str(hit.get("vin") or hit.get("VIN") or hit.get("Vin") or "").strip().upper()
    if not _VIN_RE.fullmatch(vin):
        return None
    images = hit.get("images") or []
    image_url = images[0] if isinstance(images, list) and images else hit.get("image") or ""
    return {
        "vin": vin,
        "year": hit.get("year") or hit.get("modelYear") or "",
        "make": hit.get("make") or hit.get("brand") or "",
        "model": hit.get("model") or "",
        "trim": hit.get("trim") or "",
        "price": hit.get("price") or hit.get("listPrice") or hit.get("salePrice") or 0,
        "mileage": hit.get("mileage") or hit.get("odometer") or 0,
        "exterior_color": hit.get("exteriorColor") or hit.get("color") or "",
        "interior_color": hit.get("interiorColor") or "",
        "fuel_type": hit.get("fuelType") or hit.get("fuel") or "",
        "transmission": hit.get("transmission") or "",
        "drivetrain": hit.get("drivetrain") or hit.get("driveType") or "",
        "stock_number": hit.get("stockNumber") or hit.get("stock") or "",
        "image_url": image_url if isinstance(image_url, str) else "",
        "source": "algolia",
    }


# ── VinSolutions / window-global JSON ────────────────────────────────────

_WINDOW_GLOBALS_RE = re.compile(
    r"""(?:window\.(?:__INVENTORY__|__VEHICLES__|__PRELOADED_STATE__|inventoryData|vehicleData|carsData|listingData|ddcData|siteData)\s*=\s*)"""
    r"""(\{[\s\S]{50,}\}|\[[\s\S]{50,}\])""",
    re.I,
)

_VS_SCRIPT_RE = re.compile(r"vsadmin\.com|vinsolutions\.com|vinmanager\.com", re.I)


def _find_window_global_vehicles(scripts_text: list[str]) -> list[dict]:
    vehicles: list[dict] = []
    seen: set[str] = set()
    combined = "\n".join(scripts_text)
    for m in _WINDOW_GLOBALS_RE.finditer(combined):
        raw = m.group(1).rstrip(";").strip()
        try:
            data = json.loads(raw)
        except Exception:
            continue
        _harvest_vehicle_list(data, vehicles, seen, source="window-global")
    return vehicles


def _harvest_vehicle_list(
    data: Any,
    out: list[dict],
    seen: set[str],
    source: str = "inline-json",
    depth: int = 0,
) -> None:
    """Recursively find vehicle arrays inside a JSON blob."""
    if depth > 5:
        return
    if isinstance(data, list):
        vin_count = sum(
            1 for item in data
            if isinstance(item, dict) and _VIN_RE.fullmatch(
                str(item.get("vin") or item.get("VIN") or item.get("Vin") or "").strip().upper()
            )
        )
        if vin_count >= 1:
            for item in data:
                _normalize_and_collect(item, out, seen, source)
            return
        # Recurse into list items
        for item in data:
            _harvest_vehicle_list(item, out, seen, source, depth + 1)
    elif isinstance(data, dict):
        for key in ("inventory", "vehicles", "listings", "cars", "results", "hits", "data", "items"):
            if isinstance(data.get(key), (list, dict)):
                _harvest_vehicle_list(data[key], out, seen, source, depth + 1)


def _normalize_and_collect(item: Any, out: list[dict], seen: set[str], source: str) -> None:
    if not isinstance(item, dict):
        return
    vin = str(
        item.get("vin") or item.get("VIN") or item.get("Vin") or ""
    ).strip().upper()
    if not _VIN_RE.fullmatch(vin) or vin in seen:
        return
    year = item.get("year") or item.get("Year") or item.get("modelYear") or ""
    make = item.get("make") or item.get("Make") or ""
    model = item.get("model") or item.get("Model") or ""
    # Require at least two of the three key fields
    if sum(1 for f in (year, make, model) if f) < 2:
        return
    seen.add(vin)
    out.append({
        "vin": vin,
        "year": year,
        "make": make,
        "model": model,
        "trim": item.get("trim") or item.get("Trim") or "",
        "price": item.get("price") or item.get("Price") or item.get("listPrice") or item.get("salePrice") or 0,
        "mileage": item.get("mileage") or item.get("Mileage") or item.get("odometer") or 0,
        "exterior_color": item.get("exteriorColor") or item.get("color") or item.get("Color") or item.get("exterior_color") or "",
        "interior_color": item.get("interiorColor") or item.get("interior_color") or "",
        "fuel_type": item.get("fuelType") or item.get("fuel_type") or item.get("fuel") or "",
        "transmission": item.get("transmission") or item.get("Transmission") or "",
        "drivetrain": item.get("drivetrain") or item.get("driveType") or item.get("drive_type") or "",
        "stock_number": item.get("stockNumber") or item.get("stock_number") or item.get("stock") or "",
        "image_url": item.get("imageUrl") or item.get("image_url") or item.get("image") or item.get("primaryImage") or "",
        "source": source,
    })


# ── Generic inline <script> JSON array scan ───────────────────────────────

_JSON_ARRAY_RE = re.compile(r"\[(\{[\s\S]{100,}?\})\]")


def _find_generic_vin_json(scripts_text: list[str]) -> list[dict]:
    vehicles: list[dict] = []
    seen: set[str] = set()
    for text in scripts_text:
        for m in _JSON_ARRAY_RE.finditer(text):
            snippet = "[" + m.group(1) + "]"
            try:
                arr = json.loads(snippet)
            except Exception:
                continue
            if not isinstance(arr, list):
                continue
            _harvest_vehicle_list(arr, vehicles, seen, source="inline-json")
            if vehicles:
                return vehicles
    return vehicles


# ── JSON-LD Vehicle type ──────────────────────────────────────────────────

def _find_jsonld_vehicles(html: str) -> list[dict]:
    if BeautifulSoup is None:
        return []
    vehicles: list[dict] = []
    seen: set[str] = set()
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text() or ""
        if not raw.strip():
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        nodes: list[Any] = []
        _walk_ld(parsed, nodes)
        for node in nodes:
            if not isinstance(node, dict):
                continue
            types = node.get("@type") or []
            if isinstance(types, str):
                types = [types]
            if not any(re.search(r"Car|Vehicle|Automobile", str(t), re.I) for t in types):
                continue
            _normalize_and_collect(node, vehicles, seen, source="jsonld-vehicle")
    return vehicles


def _walk_ld(obj: Any, out: list) -> None:
    if obj is None:
        return
    if isinstance(obj, list):
        for x in obj:
            _walk_ld(x, out)
    elif isinstance(obj, dict):
        out.append(obj)
        if "@graph" in obj:
            _walk_ld(obj["@graph"], out)


# ── CDK __PRELOADED_STATE__ / dealer.on ───────────────────────────────────

_CDK_STATE_RE = re.compile(
    r"""window\.__PRELOADED_STATE__\s*=\s*JSON\.parse\(['"](.+?)['"]\)""",
    re.S,
)


def _find_cdk_vehicles(scripts_text: list[str]) -> list[dict]:
    vehicles: list[dict] = []
    seen: set[str] = set()
    combined = "\n".join(scripts_text)
    # Try JSON.parse pattern (CDK encodes state as string)
    for m in _CDK_STATE_RE.finditer(combined):
        try:
            raw = m.group(1).encode("utf-8").decode("unicode_escape")
            data = json.loads(raw)
        except Exception:
            continue
        _harvest_vehicle_list(data, vehicles, seen, source="cdk-state")
        if vehicles:
            return vehicles
    # Also try raw window.__PRELOADED_STATE__ = {...}
    for m in re.finditer(r"window\.__PRELOADED_STATE__\s*=\s*(\{[\s\S]{50,}?\});", combined):
        try:
            data = json.loads(m.group(1))
        except Exception:
            continue
        _harvest_vehicle_list(data, vehicles, seen, source="cdk-state")
        if vehicles:
            return vehicles
    return vehicles


# ── Main sniffer ──────────────────────────────────────────────────────────

async def sniff_inventory(url: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ok": False,
        "vehicles": [],
        "strategy": None,
        "error": None,
        "detail": None,
    }

    try:
        from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig
    except ImportError as e:
        result["error"] = "crawl4ai_not_installed"
        result["detail"] = str(e)
        return result

    parsed_base = urlparse(url)
    base_url = f"{parsed_base.scheme}://{parsed_base.netloc}"

    # Homepage first, then inventory path fallbacks
    urls_to_try = [url] + [urljoin(base_url, p) for p in _INVENTORY_PATHS]

    browser_cfg = BrowserConfig(headless=True, verbose=False)
    vehicles: list[dict] = []
    strategy_used: str | None = None

    print(f"[crawl4ai_inventory] Sniffing inventory at {url}", file=sys.stderr)

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        for i, try_url in enumerate(urls_to_try[:5]):  # cap at 5 URLs total
            run_cfg = CrawlerRunConfig(cache_mode=CacheMode.BYPASS, page_timeout=60000)
            try:
                print(f"[crawl4ai_inventory] Crawling {try_url}", file=sys.stderr)
                crawl_result = await crawler.arun(url=try_url, config=run_cfg)
            except Exception as e:
                print(f"[crawl4ai_inventory] Crawl failed for {try_url}: {e}", file=sys.stderr)
                continue

            html: str = (getattr(crawl_result, "html", None) or "") or ""
            if not html.strip():
                html = (getattr(crawl_result, "cleaned_html", None) or "") or ""
            if not html.strip():
                continue

            # Collect all script text
            scripts_text: list[str] = []
            if BeautifulSoup is not None:
                try:
                    soup = BeautifulSoup(html, "html.parser")
                    for sc in soup.find_all("script"):
                        t = sc.string or sc.get_text() or ""
                        if t.strip():
                            scripts_text.append(t)
                except Exception:
                    pass

            # ── Strategy 1: Algolia
            creds = _extract_algolia_credentials(scripts_text)
            if creds:
                print(
                    f"[crawl4ai_inventory] Found Algolia credentials (app={creds['app_id']} index={creds['index_name']})",
                    file=sys.stderr,
                )
                hits = _query_algolia(creds)
                for h in hits:
                    v = _normalize_algolia_hit(h)
                    if v:
                        vehicles.append(v)
                if vehicles:
                    strategy_used = "algolia"
                    break

            # ── Strategy 2: CDK / __PRELOADED_STATE__
            cdk_v = _find_cdk_vehicles(scripts_text)
            if cdk_v:
                vehicles = cdk_v
                strategy_used = "cdk"
                break

            # ── Strategy 3: VinSolutions / window globals
            win_v = _find_window_global_vehicles(scripts_text)
            if win_v:
                vehicles = win_v
                strategy_used = "vinsolutions"
                break

            # ── Strategy 4: JSON-LD Vehicle
            ld_v = _find_jsonld_vehicles(html)
            if ld_v:
                vehicles = ld_v
                strategy_used = "jsonld-vehicle"
                break

            # ── Strategy 5: Generic inline JSON scan
            gen_v = _find_generic_vin_json(scripts_text)
            if gen_v:
                vehicles = gen_v
                strategy_used = "inline-json"
                break

            # Homepage found nothing — continue to inventory path URLs
            if i == 0 and not vehicles:
                print("[crawl4ai_inventory] Homepage had no inventory; trying inventory paths…", file=sys.stderr)
                continue

    if vehicles:
        result["ok"] = True
        result["vehicles"] = vehicles
        result["strategy"] = strategy_used
        print(
            f"[crawl4ai_inventory] Found {len(vehicles)} vehicles via strategy={strategy_used}",
            file=sys.stderr,
        )
    else:
        result["error"] = "no_inventory_found"
        print("[crawl4ai_inventory] No inventory detected.", file=sys.stderr)

    return result


def main() -> None:
    if len(sys.argv) < 2:
        print(
            "CRAWL4AI_INVENTORY:" + json.dumps({"ok": False, "error": "usage", "detail": "need url"}),
            flush=True,
        )
        sys.exit(2)
    url = sys.argv[1].strip()
    if not url.startswith("http"):
        url = "https://" + url
    try:
        payload = asyncio.run(sniff_inventory(url))
    except KeyboardInterrupt:
        payload = {"ok": False, "error": "interrupted", "vehicles": [], "strategy": None}
    print("CRAWL4AI_INVENTORY:" + json.dumps(payload), flush=True)
    sys.exit(0)


if __name__ == "__main__":
    main()
