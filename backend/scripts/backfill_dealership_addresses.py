#!/usr/bin/env python3
"""
Backfill ``dealerships.street_address`` / ``zip_code`` from keyless sources.

Why this exists
---------------
The rooftop attribution gate (``backend.parsers.resolve_rooftop_attribution``)
picks the one rooftop in a group feed that IS the store being scanned. When the
feed's rooftops are unnamed address blocks -- which is what a CarsCommerce group
account publishes -- the only tiers that can identify the store are
``street_address`` and ``zip_code``, both read from the ``dealerships`` roster
by ``backend.scanner.delta_scan._roster_place``.

Measured on 2026-08-03: of the 178 dealerships with active inventory, 28 carried
a street_address and 41 a zip_code, so those tiers were inert for the other 150.
``bmwofmurrieta-com`` is the worked example -- its feed names 84 rooftops, two of
them in Murrieta CA 92562 ('41430 Auto Mall Pkwy', 282 rows, and '41300 Date
Street', 45 rows). City, state and ZIP are identical for both, so only the street
tier can separate them, and with a blank roster street the gate refuses the whole
payload as ``target_rooftop_unidentified``.

This is a script, not a one-shot UPDATE, because new dealers keep arriving and
the roster has to be re-fillable on demand. It is idempotent: a dealer that
already has a plausible street_address is skipped unless ``--force``.

Sources (strongest first; no API keys, nothing Google)
-----------------------------------------------------
``site_jsonld``   schema.org ``PostalAddress`` in the dealer's OWN homepage. The
                  page is fetched from the dealer's own host, so the record is
                  first-party by construction -- there is no "which business is
                  this?" step to get wrong.
``site_text``     a US address line in the same first-party HTML, used only when
                  the page publishes no JSON-LD address.
``osm_website``   OpenStreetMap via Overpass, and only for an element whose
                  ``website`` tag resolves to the dealer's own host.

96 of the 178 dealer sites answer a plain HTTP GET with a Cloudflare 403
interstitial (verified 2026-08-03). ``--browser`` re-fetches exactly those
through headless Chromium, which clears the challenge.

Verification -- the part that matters
-------------------------------------
A name-only lookup once put 15 of 135 dealers in the wrong city, one of them
2,497 miles off, so no candidate is accepted on a name match. Every candidate
must clear, in addition to its source's own host check:

* location agreement -- the candidate's ZIP centroid (offline pgeocode) within
  ``--agree-miles`` of the roster's coordinates, or, when the candidate carries
  no ZIP, an exact city+state match against the roster;
* one answer -- two different street addresses surviving for one dealer is a
  refusal, not a pick;
* shape -- a house number plus a street name, no PO boxes.

Anything that does not clear is left BLANK and reported. Blank means the gate
stays undecided and un-lists nothing; a wrong street means it confidently
attributes a payload to the wrong rooftop.

Usage
-----
    python -m backend.scripts.backfill_dealership_addresses --dry-run
    python -m backend.scripts.backfill_dealership_addresses --apply
    python -m backend.scripts.backfill_dealership_addresses --apply --browser
    python -m backend.scripts.backfill_dealership_addresses --apply --only bmwofmurrieta.com
    python -m backend.scripts.backfill_dealership_addresses --apply --sources osm
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

from backend.db.dealer_geo import normalize_dealer_host  # noqa: E402

log = logging.getLogger("backfill_dealership_addresses")

BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
HTTP_HEADERS = {
    "User-Agent": BROWSER_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
# Pages that carry the storefront address when the homepage does not. Tried in
# order, and only after the homepage came back without one.
CONTACT_PATHS = ("/dealership/contact.htm", "/contact-us/", "/hours-directions/")

# How far the candidate's own ZIP centroid may sit from the roster coordinate.
# A real lot is within a few miles of its own ZIP; 25 absorbs a large rural ZIP
# without absorbing the next metro.
DEFAULT_AGREE_MILES = 25.0

_LDJSON = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.S | re.I
)
_HTML_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")
_ZIP = re.compile(r"\b(\d{5})(?:-\d{4})?\b")
# "41430 Auto Mall Parkway, Murrieta, CA 92562" — house number, street words,
# a suffix, then the city/state/ZIP tail. Deliberately narrow: this tier only
# runs on first-party HTML and every hit still has to clear verification.
_STREET_SUFFIXES = (
    "street|st|avenue|ave|boulevard|blvd|road|rd|drive|dr|parkway|pkwy|lane|ln|way|"
    "highway|hwy|circle|cir|court|ct|place|pl|terrace|trail|trl|turnpike|tpke|"
    "expressway|expy|freeway|fwy|square|sq|plaza|pike|loop|route|rte|row|run|"
    "crossing|xing|bypass|byp|extension|ext|boulevard\\.|blvd\\."
)
_ADDRESS_LINE = re.compile(
    r"\b(\d{1,6}[A-Za-z]?)\s+"
    r"((?:[NSEW]\.?|North|South|East|West|Northeast|Northwest|Southeast|Southwest)\s+)?"
    r"((?:[A-Za-z0-9'\.\-]+\s+){0,4}?(?:" + _STREET_SUFFIXES + r")\.?)"
    r"\s*,?\s*(?:(?:Suite|Ste|Unit|#)\s*[A-Za-z0-9\-]+\s*,?\s*)?"
    r"([A-Za-z][A-Za-z\.\-' ]{2,28}),\s*([A-Z]{2})\.?\s+(\d{5})(?:-\d{4})?\b",
    re.I,
)
_PO_BOX = re.compile(r"\bP\.?\s*O\.?\s*Box\b", re.I)
_HAS_NUMBER = re.compile(r"^\s*\d")


# --------------------------------------------------------------------------- #
# data
# --------------------------------------------------------------------------- #
@dataclass
class Dealer:
    registry_id: int
    name: str
    site: str
    city: str
    state: str
    lat: float | None
    lon: float | None
    street_address: str
    zip_code: str
    active_cars: int

    @property
    def host(self) -> str:
        return normalize_dealer_host(self.site)


@dataclass
class Candidate:
    street: str
    city: str
    state: str
    zip_code: str
    source: str
    detail: str = ""


@dataclass
class Outcome:
    dealer: Dealer
    written: bool = False
    source: str = ""
    street: str = ""
    zip_code: str = ""
    refusals: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# parsing helpers
# --------------------------------------------------------------------------- #
def _txt(value: Any) -> str:
    return _WS.sub(" ", str(value or "")).strip(" \t,")


def _norm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _zip5(value: Any) -> str:
    hit = _ZIP.search(str(value or ""))
    return hit.group(1) if hit else ""


def _registrable(host: str) -> str:
    """Last two labels of *host* — enough to tell a rebrand redirect from a
    hand-off to a completely different company. US dealer sites are .com/.net."""
    parts = [p for p in (host or "").split(".") if p]
    return ".".join(parts[-2:]) if len(parts) >= 2 else host


def looks_like_street(value: str) -> bool:
    """True for '41430 Auto Mall Parkway'; false for '', 'Murrieta', a PO box."""
    text = _txt(value)
    if len(text) < 6 or len(text) > 120:
        return False
    if _PO_BOX.search(text):
        return False
    if not _HAS_NUMBER.match(text):
        return False
    return bool(re.search(r"[A-Za-z]{2}", text))


def _walk(node: Any, out: list[dict], depth: int = 0) -> None:
    if depth > 8:
        return
    if isinstance(node, dict):
        addr = node.get("address")
        if isinstance(addr, dict) and addr.get("streetAddress"):
            out.append({
                "street": _txt(addr.get("streetAddress")),
                "city": _txt(addr.get("addressLocality")),
                "state": _txt(addr.get("addressRegion")),
                "zip": _zip5(addr.get("postalCode")),
                "url": _txt(node.get("url")),
                "type": _txt(node.get("@type")),
            })
        elif isinstance(addr, list):
            for a in addr:
                if isinstance(a, dict) and a.get("streetAddress"):
                    out.append({
                        "street": _txt(a.get("streetAddress")),
                        "city": _txt(a.get("addressLocality")),
                        "state": _txt(a.get("addressRegion")),
                        "zip": _zip5(a.get("postalCode")),
                        "url": _txt(node.get("url")),
                        "type": _txt(node.get("@type")),
                    })
        for value in node.values():
            _walk(value, out, depth + 1)
    elif isinstance(node, list):
        for value in node[:200]:
            _walk(value, out, depth + 1)


def jsonld_addresses(html: str) -> list[dict]:
    """Every ``PostalAddress`` carrying a ``streetAddress`` in *html*'s JSON-LD."""
    out: list[dict] = []
    for block in _LDJSON.findall(html or ""):
        text = block.strip()
        if not text:
            continue
        try:
            _walk(json.loads(text), out)
        except (ValueError, RecursionError):
            continue
    return out


def text_addresses(html: str) -> list[dict]:
    """US address lines in the page's visible text (JSON-LD-free fallback)."""
    text = _WS.sub(" ", _HTML_TAG.sub(" ", html or ""))
    out: list[dict] = []
    for hit in _ADDRESS_LINE.finditer(text):
        num, direction, street, city, state, postal = hit.groups()
        line = _txt(f"{num} {direction or ''} {street}")
        out.append({
            "street": line,
            "city": _txt(city),
            "state": state.upper(),
            "zip": postal,
            "url": "",
            "type": "text",
        })
    return out


# --------------------------------------------------------------------------- #
# verification
# --------------------------------------------------------------------------- #
def _zip_coords(zip_code: str) -> tuple[float, float] | None:
    if not zip_code:
        return None
    try:
        from backend.db.geo import zip_to_coords

        coords = zip_to_coords(zip_code)
    except Exception:
        return None
    if not coords:
        return None
    try:
        lat, lon = float(coords[0]), float(coords[1])
    except (TypeError, ValueError):
        return None
    if math.isnan(lat) or math.isnan(lon):
        return None
    return lat, lon


def verify(cand: Candidate, dealer: Dealer, agree_miles: float) -> str:
    """``""`` when *cand* may be written for *dealer*, else the refusal reason.

    The source has already established that the record belongs to this dealer's
    own host. This is the second, independent check: the address it gave has to
    land where we already know this dealer is.
    """
    if not looks_like_street(cand.street):
        return "malformed_street"
    if cand.state and dealer.state and cand.state.upper() != dealer.state.upper():
        return f"state_mismatch({cand.state}!={dealer.state})"

    coords = _zip_coords(cand.zip_code)
    if coords and dealer.lat is not None and dealer.lon is not None:
        from backend.db.geo import haversine

        miles = haversine(dealer.lat, dealer.lon, coords[0], coords[1])
        if miles > agree_miles:
            return f"zip_{cand.zip_code}_is_{miles:.0f}mi_from_roster_point"
        return ""
    if cand.city and dealer.city and _norm(cand.city) == _norm(dealer.city):
        return ""
    return "location_unconfirmed(no usable zip, city mismatch)"


def choose(cands: Sequence[Candidate], dealer: Dealer, agree_miles: float,
           refusals: list[str]) -> Candidate | None:
    """The single verified candidate, or None.

    Distinct streets surviving verification means the page describes more than
    one storefront and nothing here says which is this one — a refusal, not a
    pick. Same street written two ways (``Pkwy``/``Parkway``) is one answer.
    """
    verified: list[Candidate] = []
    for cand in cands:
        reason = verify(cand, dealer, agree_miles)
        if reason:
            refusals.append(f"{cand.source}:{reason}:{cand.street[:40]}")
            continue
        verified.append(cand)
    if not verified:
        return None
    by_key: dict[str, Candidate] = {}
    for cand in verified:
        by_key.setdefault(_norm(cand.street), cand)
    if len(by_key) > 1:
        refusals.append(
            f"{verified[0].source}:ambiguous_{len(by_key)}_addresses:"
            + " | ".join(sorted(c.street[:32] for c in by_key.values()))[:160]
        )
        return None
    return next(iter(by_key.values()))


# --------------------------------------------------------------------------- #
# source: the dealer's own website
# --------------------------------------------------------------------------- #
def _same_site(final_url: str, dealer: Dealer) -> bool:
    final_host = normalize_dealer_host(final_url)
    if not final_host or not dealer.host:
        return False
    return final_host == dealer.host or _registrable(final_host) == _registrable(dealer.host)


def _cands_from_html(html: str, dealer: Dealer, source: str) -> list[Candidate]:
    out: list[Candidate] = []
    for rec in jsonld_addresses(html):
        # A JSON-LD node that names a different host is describing somebody
        # else's business (a platform vendor, a sister store's card).
        if rec["url"] and normalize_dealer_host(rec["url"]) and not _same_site(rec["url"], dealer):
            continue
        out.append(Candidate(rec["street"], rec["city"], rec["state"], rec["zip"],
                             source, rec["type"]))
    return out


def fetch_http(url: str, timeout: float) -> tuple[int, str, str]:
    import requests

    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=timeout, allow_redirects=True)
        status, text, final = resp.status_code, resp.text or "", resp.url or url
    except Exception as exc:  # a dealer site being down is not a script failure
        log.debug("http fetch failed %s: %s", url, exc)
        status, text, final = 0, "", url
    if status in (403, 405, 429, 503) or (status == 0):
        # Dealer Inspire / Cloudflare answer 403 to the requests TLS fingerprint,
        # not to the IP (memory: "Dealer Inspire curl 403 != IP ban"): 215 of 392
        # streetless dealers on the 2026-09-27 dry run. Retry with a browser
        # fingerprint (curl_cffi) the scanner already uses for recipe replay.
        try:
            from backend.scanner.recipe_synth import _fetch_impersonated

            html = _fetch_impersonated(url)
            if html:
                return 200, html, url
        except Exception as exc:  # noqa: BLE001
            log.debug("impersonated fetch failed %s: %s", url, exc)
    return status, text, final


def site_candidates_http(dealer: Dealer, timeout: float) -> tuple[list[Candidate], str]:
    """(candidates, note). ``note`` explains an empty list."""
    if not dealer.site:
        return [], "no_website_url"
    status, html, final = fetch_http(dealer.site, timeout)
    if status != 200 or not html:
        return [], f"http_{status or 'error'}"
    if not _same_site(final, dealer):
        return [], f"redirected_offsite({normalize_dealer_host(final)})"
    cands = _cands_from_html(html, dealer, "site_jsonld")
    if cands:
        return cands, ""
    cands = [Candidate(r["street"], r["city"], r["state"], r["zip"], "site_text")
             for r in text_addresses(html)]
    return cands, "" if cands else "no_address_in_html"


class BrowserFetcher:
    """Headless Chromium for the sites that 403 a plain GET.

    Cloudflare's interstitial is served with HTTP 403 and a full HTML body, so
    the status code alone does not say "blocked forever" — the challenge clears
    a few seconds later in a real browser and the dealer's own page renders.
    One browser for the whole run; one page per dealer.
    """

    def __init__(self, settle_ms: int = 6000, timeout_ms: int = 60000):
        self.settle_ms = settle_ms
        self.timeout_ms = timeout_ms
        self._pw = None
        self._browser = None
        self._context = None

    async def __aenter__(self):
        try:
            from playwright_stealth import Stealth
            from playwright.async_api import async_playwright

            self._ctx = Stealth().use_async(async_playwright())
        except Exception:
            from playwright.async_api import async_playwright

            self._ctx = async_playwright()
        self._pw = await self._ctx.__aenter__()
        self._browser = await self._pw.chromium.launch(
            headless=True, args=["--disable-blink-features=AutomationControlled"]
        )
        # One context for the run: a challenge cleared once stays cleared, so a
        # second page on the same site does not pay for it again.
        self._context = await self._browser.new_context(
            viewport={"width": 1366, "height": 900}, locale="en-US", user_agent=BROWSER_UA
        )
        return self

    async def __aexit__(self, *exc):
        try:
            if self._browser:
                await self._browser.close()
        finally:
            await self._ctx.__aexit__(*exc)

    async def html_for(self, dealer: Dealer, paths: Iterable[str] = ("",)) -> list[tuple[str, str]]:
        """[(final_url, html)] for each of *paths* under the dealer's own site."""
        out: list[tuple[str, str]] = []
        page = await self._context.new_page()
        try:
            for path in paths:
                url = dealer.site.rstrip("/") + path
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=self.timeout_ms // 2)
                    except Exception:
                        pass
                    await page.wait_for_timeout(self.settle_ms)
                    out.append((page.url, await page.content()))
                except Exception as exc:
                    log.debug("browser fetch failed %s: %s", url, exc)
        finally:
            await page.close()
        return out


def site_candidates_from_pages(pages: Sequence[tuple[str, str]],
                               dealer: Dealer) -> tuple[list[Candidate], str]:
    for final, html in pages:
        if not _same_site(final, dealer):
            continue
        cands = _cands_from_html(html, dealer, "site_jsonld_browser")
        if cands:
            return cands, ""
    for final, html in pages:
        if not _same_site(final, dealer):
            continue
        cands = [Candidate(r["street"], r["city"], r["state"], r["zip"], "site_text_browser")
                 for r in text_addresses(html)]
        if cands:
            return cands, ""
    return [], "no_address_in_rendered_html"


# --------------------------------------------------------------------------- #
# source: OpenStreetMap (Overpass), host-verified
# --------------------------------------------------------------------------- #
def osm_candidates(dealers: Sequence[Dealer], *, box_miles: float = 4.0,
                   chunk: int = 25, sleep_s: float = 6.0) -> dict[int, list[Candidate]]:
    """Overpass lookup for many dealers at once, keyed by registry id.

    Batched on purpose: one small bbox per dealer, ~25 dealers per request, so a
    150-dealer run is six queries rather than 150. Only elements whose
    ``website`` tag resolves to the dealer's own host are returned — an OSM node
    that merely shares the dealer's NAME is exactly the evidence that put 15
    dealers in the wrong city, and is never used here.

    OSM's house numbers are not authoritative either: on 2026-08-03 its
    ``BMW of Murrieta`` node (website bmwofmurrieta.com, so host-verified) read
    ``26825 Auto Mall Parkway`` while the dealer's own site and its own feed both
    say ``41430``. That is why this source ranks below the dealer's website.
    """
    import requests

    from backend.discovery.osm import _post_overpass, circle_to_bbox, overpass_endpoints

    placed = [d for d in dealers if d.lat is not None and d.lon is not None and d.host]
    by_host: dict[str, list[Dealer]] = {}
    for d in placed:
        by_host.setdefault(d.host, []).append(d)
    out: dict[int, list[Candidate]] = {}
    sess = requests.Session()
    endpoints = overpass_endpoints()

    for i in range(0, len(placed), chunk):
        batch = placed[i : i + chunk]
        boxes = []
        for d in batch:
            south, west, north, east = circle_to_bbox(d.lat, d.lon, box_miles)
            boxes.append(f'  nwr["shop"="car"]({south:.5f},{west:.5f},{north:.5f},{east:.5f});')
            boxes.append(
                f'  nwr["amenity"="car_dealer"]({south:.5f},{west:.5f},{north:.5f},{east:.5f});'
            )
        query = "[out:json][timeout:120];\n(\n" + "\n".join(boxes) + "\n);\nout center tags;\n"
        body = None
        for ep in endpoints:
            body = _post_overpass(ep, query, sess=sess, timeout_s=150.0, attempts=2)
            if body is not None:
                break
        if body is None:
            log.warning("Overpass: batch %d-%d unanswered by every mirror", i, i + len(batch))
            continue
        for el in body.get("elements") or []:
            tags = el.get("tags") or {}
            site = tags.get("website") or tags.get("contact:website") or ""
            host = normalize_dealer_host(site)
            if not host or host not in by_host:
                continue
            house = _txt(tags.get("addr:housenumber"))
            street = _txt(tags.get("addr:street"))
            if not house or not street:
                continue
            for d in by_host[host]:
                out.setdefault(d.registry_id, []).append(Candidate(
                    street=f"{house} {street}",
                    city=_txt(tags.get("addr:city")),
                    state=_txt(tags.get("addr:state")),
                    zip_code=_zip5(tags.get("addr:postcode")),
                    source="osm_website",
                    detail=f"{el.get('type')}/{el.get('id')}",
                ))
        if i + chunk < len(placed) and sleep_s > 0:
            time.sleep(sleep_s)
    return out


# --------------------------------------------------------------------------- #
# roster i/o
# --------------------------------------------------------------------------- #
def load_targets(only: str = "", limit: int = 0) -> list[Dealer]:
    """Dealerships that currently hold active inventory, richest first."""
    from backend.db.inventory_db import db_conn

    sql = """
        SELECT d.id, d.name,
               COALESCE(NULLIF(TRIM(d.website_url), ''), NULLIF(TRIM(d.dealer_website_url), ''), '') AS site,
               COALESCE(d.city, ''), COALESCE(d.state, ''), d.latitude, d.longitude,
               COALESCE(d.street_address, ''), COALESCE(d.zip_code, ''), a.n
          FROM dealerships d
          JOIN (
                SELECT dealership_registry_id AS id, COUNT(*) AS n
                  FROM cars
                 WHERE COALESCE(listing_active, 1) = 1
                   AND dealership_registry_id IS NOT NULL
              GROUP BY dealership_registry_id
               ) a ON a.id = d.id
      ORDER BY a.n DESC
    """
    with db_conn() as conn:
        rows = conn.execute(sql).fetchall()
        # Registry rows with no website of their own: fall back to the URL the
        # scanner actually scans for them, which is this store's site by
        # definition and is what every host check below compares against.
        blanks = [r[0] for r in rows if not str(r[2] or "").strip()]
        fallback: dict[int, str] = {}
        for rid in blanks:
            hit = conn.execute(
                """SELECT dealer_url, COUNT(*) c FROM cars
                    WHERE dealership_registry_id = ? AND COALESCE(listing_active, 1) = 1
                      AND dealer_url IS NOT NULL AND TRIM(dealer_url) <> ''
                 GROUP BY dealer_url ORDER BY c DESC""",
                (rid,),
            ).fetchall()
            # One store, one site. Several distinct dealer_urls under one
            # registry row means the row is shared and we cannot say whose host
            # to verify against — leave it blank and let it be reported.
            hosts = {normalize_dealer_host(str(h[0])) for h in hit}
            hosts.discard("")
            if len(hosts) == 1:
                fallback[rid] = str(hit[0][0])

    out: list[Dealer] = []
    for r in rows:
        site = str(r[2] or "").strip() or fallback.get(int(r[0]), "")
        out.append(Dealer(
            registry_id=int(r[0]), name=str(r[1] or ""), site=site,
            city=str(r[3] or ""), state=str(r[4] or ""),
            lat=float(r[5]) if r[5] is not None else None,
            lon=float(r[6]) if r[6] is not None else None,
            street_address=str(r[7] or "").strip(), zip_code=str(r[8] or "").strip(),
            active_cars=int(r[9] or 0),
        ))
    if only:
        needle = _norm(only)
        out = [d for d in out
               if needle in _norm(d.host) or needle in _norm(d.name) or only == str(d.registry_id)]
    return out[:limit] if limit else out


def store(dealer: Dealer, cand: Candidate) -> None:
    """Persist one verified address, with the tier that produced it.

    ``zip_code`` is only overwritten when the candidate carries one, so a
    source that answered with a street but no ZIP cannot blank an existing ZIP.
    """
    from backend.db.inventory_db import db_conn

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    with db_conn() as conn:
        conn.execute(
            """UPDATE dealerships
                  SET street_address = ?,
                      zip_code = CASE WHEN ? <> '' THEN ? ELSE zip_code END,
                      street_address_source = ?,
                      street_address_verified_at = ?
                WHERE id = ?""",
            (cand.street, cand.zip_code, cand.zip_code, cand.source, now, dealer.registry_id),
        )
        conn.commit()


def drop_shared_storefronts(outcomes: dict[int, Outcome]) -> int:
    """Withdraw any address that two dealers ended up sharing.

    The failure mode this catches: a group's platform serves the SAME
    schema.org block on every sibling host, so two rooftops resolve to one
    street. Writing it to both is worse than writing it to neither — the gate
    would then hand both stores the same rooftop's cars, which is the exact
    mis-attribution this roster field exists to prevent.

    Two franchises really can share a building, so a shared address is not proof
    that either value is wrong. It is proof that neither identifies a storefront,
    which is enough: both are withdrawn and reported.

    Compared against the addresses already in the roster too, not just the ones
    resolved in this run.
    """
    from backend.db.inventory_db import db_conn
    from backend.parsers import _street_key

    resolved = {rid: o for rid, o in outcomes.items() if o.street}
    if not resolved:
        return 0
    with db_conn() as conn:
        stored = conn.execute(
            """SELECT id, name, street_address FROM dealerships
                WHERE street_address IS NOT NULL AND TRIM(street_address) <> ''"""
        ).fetchall()
    by_key: dict[str, list[tuple[int, str]]] = {}
    for rid, name, street in ((int(r[0]), str(r[1] or ""), str(r[2] or "")) for r in stored):
        if rid not in resolved:
            by_key.setdefault(_street_key(street), []).append((rid, name))
    for rid, o in resolved.items():
        by_key.setdefault(_street_key(o.street), []).append((rid, o.dealer.name))

    withdrawn = 0
    for key, holders in by_key.items():
        if len(holders) < 2 or not key:
            continue
        names = ", ".join(sorted(n for _rid, n in holders))
        for rid, _name in holders:
            o = outcomes.get(rid)
            if o is None or not o.street:
                continue
            o.refusals.append(f"{o.source}:shared_storefront_with({names})[{o.street}]")
            o.street = ""
            o.zip_code = ""
            o.source = ""
            withdrawn += 1
    if withdrawn:
        print(f"  withdrew {withdrawn} address(es): two dealers resolved to the same storefront",
              flush=True)
    return withdrawn


# --------------------------------------------------------------------------- #
# driver
# --------------------------------------------------------------------------- #
def _needs_street(dealer: Dealer, force: bool) -> tuple[bool, str]:
    if force:
        return True, "forced"
    if not dealer.street_address:
        return True, "blank"
    if not looks_like_street(dealer.street_address):
        return True, f"malformed({dealer.street_address[:40]!r})"
    return False, "already_set"


def _record(outcome: Outcome, pick: Candidate) -> None:
    outcome.source = pick.source
    outcome.street = pick.street
    outcome.zip_code = pick.zip_code
    print(f"    + {outcome.dealer.name[:34]:34s} {pick.street[:40]:40s} "
          f"{pick.zip_code:6s} {pick.source}", flush=True)


async def run(args: argparse.Namespace) -> int:
    dealers = load_targets(args.only, args.limit)
    total = len(dealers)
    have_street = sum(1 for d in dealers if looks_like_street(d.street_address))
    have_zip = sum(1 for d in dealers if _zip5(d.zip_code))
    print(f"targets: {total} dealerships with active inventory", flush=True)
    print(f"before:  street_address {have_street}/{total}   zip_code {have_zip}/{total}", flush=True)

    todo: list[Dealer] = []
    outcomes: dict[int, Outcome] = {}
    for d in dealers:
        outcomes[d.registry_id] = Outcome(dealer=d)
        need, why = _needs_street(d, args.force)
        if need:
            todo.append(d)
            if why not in ("blank", "forced"):
                print(f"  will overwrite {d.name}: stored street_address is {why}", flush=True)
        else:
            outcomes[d.registry_id].refusals.append("skipped:already_set")
    print(f"needing a street address: {len(todo)}", flush=True)

    sources = {s.strip() for s in args.sources.split(",") if s.strip()} or {"site", "osm"}
    pending: list[Dealer] = []
    blocked: list[Dealer] = []

    # --- tier 1/2: the dealers' own websites, plain HTTP ---------------------
    if "site" in sources:
        for n, d in enumerate(todo, 1):
            cands, note = site_candidates_http(d, args.http_timeout)
            if not cands:
                out = outcomes[d.registry_id]
                out.refusals.append(f"site_http:{note}")
                # 403/503 is Cloudflare's interstitial, not an answer.
                (blocked if note.startswith("http_") else pending).append(d)
            else:
                pick = choose(cands, d, args.agree_miles, outcomes[d.registry_id].refusals)
                if pick:
                    _record(outcomes[d.registry_id], pick)
                else:
                    pending.append(d)
            if n % 25 == 0:
                print(f"  http {n}/{len(todo)}", flush=True)
            time.sleep(args.site_pace)
        print(f"  http tier: resolved {sum(1 for o in outcomes.values() if o.street)}, "
              f"challenged {len(blocked)}, no-address {len(pending)}")

    # --- tier 1/2 again for the challenged sites, through a real browser -----
    retry = [d for d in (blocked + pending) if not outcomes[d.registry_id].street]
    if args.browser and retry and "site" in sources:
        print(f"  browser tier: {len(retry)} site(s)", flush=True)
        async with BrowserFetcher(settle_ms=args.settle_ms) as bf:
            for n, d in enumerate(retry, 1):
                if not d.site:
                    continue
                pages = await bf.html_for(d)
                cands, note = site_candidates_from_pages(pages, d)
                if not cands:
                    pages = await bf.html_for(d, CONTACT_PATHS[:2])
                    cands, note = site_candidates_from_pages(pages, d)
                if not cands:
                    outcomes[d.registry_id].refusals.append(f"site_browser:{note}")
                else:
                    pick = choose(cands, d, args.agree_miles, outcomes[d.registry_id].refusals)
                    if pick:
                        _record(outcomes[d.registry_id], pick)
                if n % 10 == 0:
                    print(f"  browser {n}/{len(retry)} "
                          f"(resolved so far {sum(1 for o in outcomes.values() if o.street)})")
                time.sleep(args.site_pace)

    # --- tier 3: OSM, host-verified -----------------------------------------
    left = [d for d in todo if not outcomes[d.registry_id].street]
    if left and "osm" in sources:
        print(f"  osm tier: {len(left)} dealer(s) still unresolved", flush=True)
        found = osm_candidates(left)
        for d in left:
            cands = found.get(d.registry_id) or []
            if not cands:
                outcomes[d.registry_id].refusals.append("osm:no_host_verified_element")
                continue
            pick = choose(cands, d, args.agree_miles, outcomes[d.registry_id].refusals)
            if pick:
                _record(outcomes[d.registry_id], pick)

    # --- cross-dealer guard --------------------------------------------------
    drop_shared_storefronts(outcomes)

    # --- write ---------------------------------------------------------------
    wrote = 0
    for o in outcomes.values():
        if not o.street:
            continue
        if args.apply:
            store(o.dealer, Candidate(o.street, "", "", o.zip_code, o.source))
        o.written = True
        wrote += 1

    # --- report --------------------------------------------------------------
    from collections import Counter

    by_source = Counter(o.source for o in outcomes.values() if o.written)
    print(flush=True)
    print(("APPLIED" if args.apply else "DRY RUN — nothing written"), flush=True)
    print(f"resolved {wrote} of {len(todo)} dealers needing an address", flush=True)
    for src, n in by_source.most_common():
        print(f"  {src:24s} {n}", flush=True)
    for o in sorted((o for o in outcomes.values() if o.written),
                    key=lambda o: -o.dealer.active_cars):
        print(f"    {o.dealer.active_cars:5d} {o.dealer.name[:34]:34s} "
              f"{o.street[:38]:38s} {o.zip_code:6s} {o.source}")
    after_street = have_street + wrote
    print(f"after:   street_address {after_street}/{total}", flush=True)
    unresolved = [o for o in outcomes.values()
                  if not o.written and "skipped:already_set" not in o.refusals]
    print(f"unresolved (left blank on purpose): {len(unresolved)}", flush=True)
    reasons = Counter(r.split(":")[1].split("(")[0] for o in unresolved for r in o.refusals[-1:])
    for reason, n in reasons.most_common():
        print(f"  {reason:34s} {n}", flush=True)
    if args.verbose:
        for o in sorted(unresolved, key=lambda o: -o.dealer.active_cars):
            print(f"    {o.dealer.active_cars:5d} {o.dealer.name[:38]:38s} {o.refusals[-3:]}", flush=True)
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write to dealerships (default: dry run)")
    ap.add_argument("--dry-run", action="store_true", help="explicit no-op default")
    ap.add_argument("--browser", action="store_true",
                    help="re-fetch challenged sites through headless Chromium")
    ap.add_argument("--only", default="", help="restrict to one dealer (host, name or registry id)")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--force", action="store_true",
                    help="re-resolve dealers that already have a street_address")
    ap.add_argument("--sources", default="", help="comma list of site,osm (default: all)")
    ap.add_argument("--agree-miles", type=float, default=DEFAULT_AGREE_MILES)
    ap.add_argument("--site-pace", type=float, default=1.0, help="seconds between site fetches")
    ap.add_argument("--http-timeout", type=float, default=20.0)
    ap.add_argument("--settle-ms", type=int, default=6000,
                    help="how long to let a bot-challenge clear before reading the DOM")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    import asyncio

    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
