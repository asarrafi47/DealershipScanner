"""Discover the real URL for a dealer whose listed domain is dead or redirecting."""
from __future__ import annotations

import asyncio
import logging
import re
from urllib.parse import quote_plus, urlparse, unquote

logger = logging.getLogger("scanner")

# Stop-words that don't help identify a specific dealership
_NAME_STOP_WORDS = frozenset({
    "of", "at", "the", "and", "car", "cars", "auto", "autos", "dealership",
    "dealer", "new", "used", "certified", "inc", "llc", "ltd",
})

# Auto brand names — generic, appear in any dealer of that brand, not specific enough for matching
_NAME_BRAND_WORDS = frozenset({
    "ford", "lincoln", "chevrolet", "chevy", "gmc", "buick", "cadillac",
    "honda", "acura", "toyota", "lexus", "nissan", "infiniti", "hyundai",
    "genesis", "kia", "subaru", "mazda", "mitsubishi", "volkswagen", "vw",
    "audi", "bmw", "mini", "mercedes", "benz", "porsche", "land", "rover",
    "jaguar", "volvo", "maserati", "ferrari", "lamborghini", "rivian",
    "lucid", "tesla", "polestar", "chrysler", "dodge", "jeep", "ram", "fiat",
    "alfa", "romeo", "bentley", "rolls", "royce", "mclaren", "aston", "martin",
})

_ALL_STOP = _NAME_STOP_WORDS | _NAME_BRAND_WORDS


def _name_tokens(name: str) -> set[str]:
    """Lowercase word tokens from a dealer name, minus stop-words and brand names.

    These are the 'fingerprint' tokens that should appear in the dealer's own domain
    (e.g. 'hixson' from 'Hixson Chrysler Dodge Jeep RAM').
    """
    return {t for t in re.split(r"[\s\-/]+", name.lower()) if t and t not in _ALL_STOP}


def _domain_matches_name(url: str, dealer_name: str) -> bool:
    """Return True if the domain contains at least one unique-dealer name token (≥4 chars)."""
    host = urlparse(url).netloc.lower().removeprefix("www.")
    # Strip TLD for substring search (e.g. "hixsonhasit" from "hixsonhasit.com")
    host_base = host.rsplit(".", 1)[0]
    name_toks = _name_tokens(dealer_name)
    for tok in name_toks:
        if len(tok) < 4:
            continue
        # Exact part match or substring of host_base
        parts = set(re.split(r"[\.\-_]+", host))
        if tok in parts or tok in host_base:
            return True
    return False

_AGGREGATORS = frozenset({
    # Search, social, directories
    "yelp.com", "google.com", "maps.google.com", "facebook.com", "instagram.com",
    "twitter.com", "x.com", "linkedin.com", "reddit.com",
    "yellowpages.com", "bbb.org", "mapquest.com", "bing.com", "duckduckgo.com",
    "wikipedia.org", "apple.com", "tripadvisor.com", "indeed.com", "glassdoor.com",
    "bizbuysell.com", "manta.com", "zoominfo.com",
    # Automotive aggregators
    "cars.com", "autotrader.com", "carfax.com", "cargurus.com", "truecar.com",
    "kbb.com", "kelleybluebook.com", "edmunds.com", "dealerrater.com",
    # OEM manufacturer sites — not dealer sites
    "jeep.com", "chrysler.com", "dodge.com", "ramtrucks.com", "ram.com",
    "ford.com", "lincoln.com", "chevrolet.com", "chevy.com", "gmc.com",
    "buick.com", "cadillac.com", "honda.com", "acura.com", "toyota.com",
    "lexus.com", "nissan.com", "infiniti.com", "hyundai.com", "genesis.com",
    "kia.com", "subaru.com", "mazda.com", "mitsubishi.com",
    "vw.com", "volkswagen.com", "audi.com", "bmw.com", "mini.com",
    "mercedes-benz.com", "mercedesbenz.com", "mbusa.com",
    "porsche.com", "landrover.com", "jaguarusa.com", "volvocarusa.com",
    "volvocars.com", "maserati.com", "ferrari.com", "lamborghini.com",
    "rivian.com", "lucidmotors.com", "tesla.com", "polestar.com",
})


def _root_url(url: str) -> str:
    p = urlparse(url if url.startswith("http") else "https://" + url)
    return f"{p.scheme or 'https'}://{p.netloc}"


def _is_aggregator(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower().removeprefix("www.")
        return any(host == d or host.endswith("." + d) for d in _AGGREGATORS)
    except Exception:
        return False


# DDG redirect links encode the real URL as ?uddg=<percent-encoded-url>
_DDG_UDDG_RE = re.compile(r'[?&]uddg=([^&"\']+)', re.I)


def _extract_url_from_ddg_link(href: str) -> str | None:
    """Resolve a DDG redirect href to the actual target URL."""
    if href.startswith("http"):
        return href
    m = _DDG_UDDG_RE.search(href)
    if m:
        decoded = unquote(m.group(1))
        if decoded.startswith("http"):
            return decoded
    return None


async def _ddg_search_candidates(
    browser: object,
    query: str,
    dealer_name: str,
    label: str = "",
) -> list[str]:
    """Run a DuckDuckGo search and return non-aggregator root URLs found."""
    search_url = f"https://duckduckgo.com/?q={quote_plus(query)}&ia=web"
    context = None
    html = ""
    try:
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()
        await page.goto(search_url, wait_until="domcontentloaded", timeout=20000)
        try:
            await page.wait_for_selector("a[data-testid='result-title-a'], h2 a, .result__a", timeout=10000)
        except Exception:
            await asyncio.sleep(2)
        html = await page.content()
    except Exception as exc:
        tag = f" ({label})" if label else ""
        logger.warning("URL discovery [%s]%s: Playwright search failed: %s", dealer_name, tag, exc)
        return []
    finally:
        if context is not None:
            try:
                await context.close()
            except Exception:
                pass

    href_re = re.compile(r'href="(/l/\?[^"]*uddg=[^"]+|https?://[^"]+)"', re.I)
    candidates: list[str] = []
    seen: set[str] = set()
    for raw_href in href_re.findall(html):
        resolved = _extract_url_from_ddg_link(raw_href)
        if not resolved:
            continue
        if _is_aggregator(resolved):
            continue
        root = _root_url(resolved)
        if root not in seen and root.startswith("http"):
            seen.add(root)
            candidates.append(root)
    return candidates


async def discover_dealer_url(
    dealer_name: str,
    original_url: str,
    browser: object,
    *,
    city: str = "",
    state: str = "",
) -> str | None:
    """
    When a dealer's listed URL is dead, open a Playwright headless page, search
    DuckDuckGo for the dealer, and return the first confirmed non-aggregator URL.

    browser: the already-running Playwright browser instance (from run_dealer).
    city / state: used for a location-scoped retry when the dealer name has no
        unique tokens (e.g. bare brand names like "Ford" or "Hyundai").
    Returns root URL (scheme://host) or None.
    """
    logger.info("URL discovery [%s]: opening headless browser search", dealer_name)

    candidates = await _ddg_search_candidates(
        browser, f"{dealer_name} car dealership", dealer_name
    )

    if not candidates:
        logger.warning("URL discovery [%s]: no non-aggregator results in search HTML", dealer_name)
        return None

    # Filter to only candidates whose domain shares tokens with the dealer name.
    # This prevents matching nearby competing dealers when the original dealer's domain is gone.
    matched = [c for c in candidates if _domain_matches_name(c, dealer_name)]
    if matched:
        candidates = matched
        logger.info("URL discovery [%s]: %d name-matched candidate(s): %s", dealer_name, len(matched), matched[:3])
    elif city:
        # Name has no unique tokens (bare brand name like "Ford" or "Hyundai").
        # Retry with a location-scoped query — city+state pins the result to this specific dealer.
        logger.info(
            "URL discovery [%s]: name tokens empty, retrying with location: %s %s",
            dealer_name, city, state,
        )
        loc_query = f"{dealer_name} dealership {city} {state}".strip()
        loc_candidates = await _ddg_search_candidates(browser, loc_query, dealer_name, label="location-retry")
        if not loc_candidates:
            logger.warning("URL discovery [%s]: no results in location-retry search", dealer_name)
            return None
        candidates = loc_candidates
        logger.info(
            "URL discovery [%s]: location-retry found %d candidate(s): %s",
            dealer_name, len(candidates), candidates[:3],
        )
    else:
        logger.warning(
            "URL discovery [%s]: no candidates share name tokens with dealer; "
            "skipping to avoid wrong-dealer attribution. Candidates were: %s",
            dealer_name, candidates[:5],
        )
        return None

    logger.info(
        "URL discovery [%s]: %d candidate(s): %s",
        dealer_name, len(candidates), candidates[:5],
    )

    # Verify candidates with a quick HTTP check via a new Playwright page
    try:
        verify_ctx = await browser.new_context(
            viewport={"width": 1280, "height": 900},
        )
        verify_page = await verify_ctx.new_page()
        try:
            for candidate in candidates[:5]:
                try:
                    resp = await verify_page.goto(
                        candidate + "/",
                        wait_until="domcontentloaded",
                        timeout=10000,
                    )
                    if resp and resp.status < 400:
                        final = verify_page.url
                        root = _root_url(final)
                        if not _is_aggregator(root):
                            logger.info(
                                "URL discovery [%s]: confirmed → %s (HTTP %d)",
                                dealer_name, root, resp.status,
                            )
                            return root
                except Exception as exc:
                    logger.debug(
                        "URL discovery [%s]: %s probe failed (%s): %s",
                        dealer_name, candidate, type(exc).__name__, str(exc)[:120],
                    )
        finally:
            await verify_ctx.close()
    except Exception as exc:
        logger.warning("URL discovery [%s]: verification step failed: %s", dealer_name, exc)

    logger.warning(
        "URL discovery [%s]: all %d candidate(s) failed HTTP check", dealer_name, len(candidates)
    )
    return None
