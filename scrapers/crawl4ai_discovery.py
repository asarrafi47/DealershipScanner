"""
Dealer homepage discovery via Crawl4AI (headless Chromium) + HTML heuristics.

Used by scanner.js smart-import when Puppeteer metadata is thin (missing city/state,
name looks like a domain). Prints one machine line to stdout:

  CRAWL4AI_DISCOVERY:{...json...}

Logs belong on stderr only.

Install Chromium for patchright (used by Crawl4AI 0.8), e.g.:
  python3 -m patchright install chromium
or run ./scripts/install_scraper_browsers.sh from the repo root.
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
from typing import Any
from urllib.parse import urlparse

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None  # type: ignore[misc, assignment]

_CITY_STATE_PATTERNS = [
    re.compile(r"\b([A-Za-z][A-Za-z\s'.-]{2,40}),\s*([A-Z]{2})\s+\d{5}(?:-\d{4})?\b"),
    re.compile(r"\b([A-Za-z][A-Za-z\s'.-]{2,40}),\s*([A-Z]{2})\b(?!\s*\d)"),
]

_US_STATE_FULL = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    "district of columbia": "DC",
}


def _extract_city_state_from_text(text: str) -> tuple[str, str]:
    if not text:
        return "", ""
    for pat in _CITY_STATE_PATTERNS:
        for m in pat.finditer(text):
            city = m.group(1).strip()
            st = m.group(2).strip().upper()[:2]
            if len(city) >= 2 and len(st) == 2:
                return city, st
    # "Indian Trail, North Carolina 28079"
    m2 = re.search(
        r"\b([A-Za-z][A-Za-z\s'.-]{2,40}),\s*([A-Za-z][a-z]+(?:\s+[A-Za-z][a-z]+)?)\s+\d{5}\b",
        text,
    )
    if m2:
        city = m2.group(1).strip()
        region = m2.group(2).strip().lower()
        st = _US_STATE_FULL.get(region, "")[:2]
        if len(city) >= 2 and len(st) == 2:
            return city, st
    return "", ""


def _walk_json_ld(obj: Any, out: list[dict[str, Any]]) -> None:
    if obj is None:
        return
    if isinstance(obj, list):
        for x in obj:
            _walk_json_ld(x, out)
        return
    if isinstance(obj, dict):
        out.append(obj)
        g = obj.get("@graph")
        if g is not None:
            _walk_json_ld(g, out)
        for v in obj.values():
            if isinstance(v, (dict, list)):
                _walk_json_ld(v, out)


def _types_of(node: dict[str, Any]) -> list[str]:
    t = node.get("@type")
    if t is None:
        return []
    if isinstance(t, list):
        return [str(x) for x in t]
    return [str(t)]


def extract_dealer_from_html(html: str) -> dict[str, Any]:
    """JSON-LD, Open Graph, title, footer — mirrors scanner.js discoverDealerMetadata heuristics."""
    sources: list[str] = []
    name = ""
    city = ""
    state = ""

    if BeautifulSoup is None:
        return {"name": "", "city": "", "state": "", "sources": []}

    soup = BeautifulSoup(html, "html.parser")

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text() or ""
        if not raw.strip():
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        nodes: list[dict[str, Any]] = []
        _walk_json_ld(parsed, nodes)
        for node in nodes:
            if not isinstance(node, dict):
                continue
            types_join = " ".join(_types_of(node))
            auto = bool(
                re.search(
                    r"AutomotiveBusiness|AutoDealer|CarDealer|MotorcycleDealer",
                    types_join,
                    re.I,
                )
            )
            org = bool(
                re.search(r"Organization|LocalBusiness|Store|AutomotiveBusiness", types_join, re.I)
            )
            n = (node.get("name") or "").strip() if isinstance(node.get("name"), str) else ""
            if (auto or (org and n)) and n and not name:
                name = n
                sources.append("jsonld-name")
            addr = node.get("address")
            if addr:
                lst = addr if isinstance(addr, list) else [addr]
                for a in lst:
                    if not isinstance(a, dict):
                        continue
                    c = str(a.get("addressLocality") or "").strip()
                    r = str(a.get("addressRegion") or "").strip()
                    if c and not city:
                        city = c
                    if r and not state:
                        ru = re.sub(r"\s+", "", r).upper()
                        if len(ru) == 2:
                            state = ru[:2]
                        else:
                            st2 = _US_STATE_FULL.get(r.lower(), "")
                            if st2:
                                state = st2
                    if city or state:
                        sources.append("jsonld-address")

    og_site = soup.find("meta", property="og:site_name") or soup.find("meta", attrs={"name": "og:site_name"})
    if not name and og_site and og_site.get("content"):
        name = str(og_site["content"]).strip()
        sources.append("og:site_name")

    og_title = soup.find("meta", property="og:title")
    if not name and og_title and og_title.get("content"):
        t = str(og_title["content"]).strip()
        if t:
            name = re.split(r"[|\-–]", t, maxsplit=1)[0].strip()
            sources.append("og:title")

    title_tag = soup.find("title")
    if not name and title_tag and title_tag.string:
        name = re.split(r"[|\-–]", title_tag.string, maxsplit=1)[0].strip()
        sources.append("title")

    footer = (
        soup.find("footer")
        or soup.find(attrs={"role": "contentinfo"})
        or soup.select_one(".footer, #footer, .site-footer")
    )
    foot_text = (footer.get_text("\n", strip=True) if footer else "")[:6000]
    body = soup.find("body")
    body_tail = ""
    if body:
        body_tail = body.get_text("\n", strip=True)[-8000:]
    addr_blob = f"{foot_text}\n{body_tail}"
    if not city or not state:
        fc, fs = _extract_city_state_from_text(addr_blob)
        if fc and not city:
            city = fc
        if fs and not state:
            state = fs
        if fc or fs:
            sources.append("footer-text")

    return {"name": name.strip(), "city": city.strip(), "state": state.strip().upper()[:2], "sources": sources}


def _hostname_slug(url: str) -> str:
    try:
        h = urlparse(url).hostname or ""
        h = re.sub(r"^www\.", "", h, flags=re.I)
        return re.sub(r"[^a-z0-9]+", "", h.split(".")[0].lower())
    except Exception:
        return ""


def _name_looks_like_domain(name: str, url: str) -> bool:
    n = re.sub(r"[^a-z0-9]+", "", (name or "").lower())
    if not n:
        return True
    if re.match(r"^[a-z0-9-]+\.[a-z]{2,}$", (name or "").strip(), re.I):
        return True
    host = _hostname_slug(url)
    return bool(host and n == host)


async def discover_with_crawl4ai(url: str) -> dict[str, Any]:
    out_base: dict[str, Any] = {
        "ok": False,
        "name": "",
        "city": "",
        "state": "",
        "sources": [],
        "error": None,
        "detail": None,
    }
    try:
        from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig
    except ImportError as e:
        out_base["error"] = "crawl4ai_not_installed"
        out_base["detail"] = str(e)
        return out_base

    browser_cfg = BrowserConfig(headless=True, verbose=False)
    run_cfg = CrawlerRunConfig(cache_mode=CacheMode.BYPASS, page_timeout=120000)

    try:
        async with AsyncWebCrawler(config=browser_cfg) as crawler:
            result = await crawler.arun(url=url, config=run_cfg)
    except Exception as e:
        out_base["error"] = "crawl_failed"
        out_base["detail"] = str(e)[:800]
        return out_base

    ok = bool(getattr(result, "success", True))
    html = (getattr(result, "html", None) or "") or ""
    if not html.strip():
        html = (getattr(result, "cleaned_html", None) or "") or ""

    md_parts: list[str] = []
    for attr in ("markdown", "fit_markdown", "markdown_v2"):
        chunk = getattr(result, attr, None) or ""
        if isinstance(chunk, str) and chunk.strip():
            md_parts.append(chunk)
    md = "\n".join(md_parts)

    ex = extract_dealer_from_html(html) if html.strip() else {"name": "", "city": "", "state": "", "sources": []}
    name, city, state = ex["name"], ex["city"], ex["state"]
    sources = list(ex.get("sources") or [])

    if (not city or not state) and md:
        fc, fs = _extract_city_state_from_text(md)
        if fc and not city:
            city = fc
        if fs and not state:
            state = fs
        if (fc or fs) and "crawl4ai-markdown" not in sources:
            sources.append("crawl4ai-markdown")

    # If name is still domain-like, prefer first H1
    if html and BeautifulSoup is not None and _name_looks_like_domain(name, url):
        try:
            soup = BeautifulSoup(html, "html.parser")
            h1 = soup.find("h1")
            if h1 and h1.get_text(strip=True):
                cand = h1.get_text(strip=True)
                if not _name_looks_like_domain(cand, url) and len(cand) >= 3:
                    name = cand[:200]
                    sources.append("h1-fallback")
        except Exception:
            pass

    got_content = bool(html.strip() or md.strip())
    out = {
        "ok": got_content and ok,
        "name": name,
        "city": city,
        "state": state[:2] if state else "",
        "sources": sources,
        "error": None if ok else "crawl_unsuccessful",
        "detail": getattr(result, "error_message", None) if not ok else None,
    }
    return out


def main() -> None:
    if len(sys.argv) < 2:
        print("CRAWL4AI_DISCOVERY:" + json.dumps({"ok": False, "error": "usage", "detail": "need url"}), flush=True)
        sys.exit(2)
    url = sys.argv[1].strip()
    if not url.startswith("http"):
        url = "https://" + url
    try:
        payload = asyncio.run(discover_with_crawl4ai(url))
    except KeyboardInterrupt:
        payload = {"ok": False, "error": "interrupted", "name": "", "city": "", "state": "", "sources": []}
    print("CRAWL4AI_DISCOVERY:" + json.dumps(payload), flush=True)
    sys.exit(0)


if __name__ == "__main__":
    main()
