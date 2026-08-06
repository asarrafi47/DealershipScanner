"""
Reconnaissance: how does each OEM publish trim/spec information *today*, and can
a robots-respecting automated client reach it?

WHY THIS EXISTS, AND WHY IT IS NOT ``--probe-reachability``
-----------------------------------------------------------
``fetch_oem_brochures.py --probe-reachability`` answers one question: does this
page anchor a **PDF** we may fetch? That is the right question for the brochure
corpus and the wrong question for the current model year. Several makes were
recorded as publishing "nothing we can use" on the strength of a zero ``.pdf``
count while serving hundreds of kilobytes of real, server-rendered trim
specifications as HTML. ``no PDF links found`` and ``publishes nothing`` are
different findings, and collapsing them cost this project four makes.

An official HTML spec page is exactly as citable as a brochure page. It needs a
different locator -- URL + retrieval timestamp + content hash, instead of a page
number -- and a different reachability test, which is what this module adds:

  * :func:`visible_text` strips ``<script>``/``<style>``/``<template>`` before
    measuring anything, because a 1MB JS bundle and a 1MB spec table are the
    same number of bytes to ``len(html)``. Byte count alone was how earlier
    lanes mistook application shells for documents.
  * :func:`spec_signal` counts *spec vocabulary in visible text* plus table and
    definition-list structure, so "reachable" means "the server sent us the
    specifications", not "the server sent us 200 OK".
  * :func:`index_signal` counts anchors that name a vehicle, which is the
    difference between a walkable per-vehicle index and client-side search --
    BMW's exact blocker.

WHAT THIS MODULE WILL NOT DO
----------------------------
* It never downloads a document. It reads index and spec pages only.
* It honours :class:`~backend.enrichment.brochure_sources.RobotsPolicy` for
  every request, including its fail-closed rule that an unreadable robots.txt
  means no. There is no override flag and no exempt-host argument: the one
  exemption in this project is the archive host, which is not probed here.
* It does not touch :data:`~backend.enrichment.brochure_sources.OFFICIAL_HOSTS`
  and does not need to. Reading one index page is not fetching a document, and
  ``is_official_url`` still stands between any discovery and any download. A
  host that is not allowlisted is reported as ``host_allowlisted: false`` so the
  finding "we never registered this host" stays separate from "this host will
  not answer us".

Usage::

    python -m backend.scripts.probe_oem_discovery --make honda
    python -m backend.scripts.probe_oem_discovery --group current-year
    python -m backend.scripts.probe_oem_discovery --url https://... --dump-anchors

Every run appends to ``workspace/oem_discovery_report.json``; nothing in this
file writes to the corpus, the dictionary, or the database.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.enrichment.brochure_sources import (  # noqa: E402
    BROWSER_USER_AGENT,
    DEFAULT_DELAY_SECONDS,
    IDENTIFIED_USER_AGENT,
    REQUEST_TIMEOUT_SECONDS,
    RobotsPolicy,
    official_hosts_for,
    robots_agent_for,
)

REPORT_PATH = REPO_ROOT / "workspace" / "oem_discovery_report.json"

UA_LABELS = {"browser": BROWSER_USER_AGENT, "identified": IDENTIFIED_USER_AGENT}


# --------------------------------------------------------------------------
# HTML measurement
# --------------------------------------------------------------------------

_SCRIPTISH = re.compile(
    r"<(script|style|template|noscript)\b[^>]*>.*?</\1\s*>",
    re.IGNORECASE | re.DOTALL,
)
_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")


def visible_text(html: str) -> str:
    """
    Text a reader would see, with script/style/template bodies removed first.

    This is the whole point of the module. ``len(html)`` cannot tell a
    server-rendered spec table from a JavaScript bundle -- both are large -- and
    a lane that trusted byte count recorded 1,023,434 bytes of Chevrolet as
    evidence of content without checking whether any of it was prose.
    """
    stripped = _SCRIPTISH.sub(" ", html or "")
    return _WS.sub(" ", _TAG.sub(" ", stripped)).strip()


#: Vocabulary that appears in a real specification table and essentially nowhere
#: else on a marketing page. Deliberately unit- and label-oriented rather than
#: model-oriented: a model name proves the page is about the vehicle, not that it
#: carries specs.
_SPEC_TOKENS = (
    "horsepower",
    "torque",
    "lb-ft",
    "wheelbase",
    "curb weight",
    "towing",
    "payload",
    "fuel economy",
    "mpg",
    "displacement",
    "cylinder",
    "transmission",
    "drivetrain",
    "seating capacity",
    "cargo volume",
    "headroom",
    "legroom",
    "ground clearance",
    "compression ratio",
    "kwh",
    "range",
)

#: Words that mark a *trim-differentiated* table -- the thing we actually need.
#: A spec page that lists one configuration is far less useful than one that
#: says which trim adds what.
_TRIM_TOKENS = ("standard", "available", "optional", "package", "trim", "not available")

_TABLE_TAGS = re.compile(r"<(table|dl|th)\b", re.IGNORECASE)
_ROW_TAGS = re.compile(r"<(tr|dt)\b", re.IGNORECASE)
#: Year tokens we care about. Current-model-year coverage is the whole problem.
_YEAR = re.compile(r"\b(20[12][0-9])\b")


@dataclass
class SpecSignal:
    text_chars: int = 0
    text_ratio: float = 0.0
    spec_terms: int = 0
    distinct_spec_terms: int = 0
    trim_terms: int = 0
    tables: int = 0
    rows: int = 0
    years: list[str] = field(default_factory=list)
    verdict: str = ""


def spec_signal(html: str) -> SpecSignal:
    """
    Does this body actually contain server-rendered specifications?

    This is a TRIAGE heuristic, not proof. It is tuned to avoid the false
    *positive* that matters -- calling a JS shell a document -- and it will
    under-report pages that carry specs in unusual markup. Every "implementable"
    claim in the report was confirmed by reading the page text with ``--find``
    or ``--dump-text``, never by trusting this verdict alone.

    Verdicts:
      ``js_shell``        -- almost no visible text; the page is an application.
      ``marketing_only``  -- prose, but no spec vocabulary and no table structure.
      ``spec_prose``      -- spec vocabulary present, no table structure.
      ``spec_table``      -- spec vocabulary AND table/definition-list structure.
      ``trim_spec_table`` -- the above, plus standard/available trim language.
    """
    sig = SpecSignal()
    text = visible_text(html)
    sig.text_chars = len(text)
    sig.text_ratio = round(len(text) / max(len(html or ""), 1), 4)
    low = text.lower()
    hits = {t: low.count(t) for t in _SPEC_TOKENS}
    sig.spec_terms = sum(hits.values())
    sig.distinct_spec_terms = sum(1 for v in hits.values() if v)
    sig.trim_terms = sum(low.count(t) for t in _TRIM_TOKENS)
    sig.tables = len(_TABLE_TAGS.findall(html or ""))
    sig.rows = len(_ROW_TAGS.findall(html or ""))
    sig.years = sorted(set(_YEAR.findall(text)))[-6:]

    # Structure can be markup OR sheer density. hyundaiusa.com renders a
    # six-trim comparison grid entirely in <div>s -- zero <table>, zero <tr> --
    # and the table-tag test alone scored that real spec grid as unstructured.
    # A page repeating many distinct spec labels is a spec page whatever tags it
    # uses, so density counts as structure too.
    # A trim-comparison grid repeats standard/available markers once per cell,
    # so a very high trim-term count is itself structural evidence: Hyundai's
    # Tucson compare-specs page scores 1,093 of them with zero <table> tags.
    structured = (
        sig.tables >= 1
        or sig.rows >= 10
        or (sig.distinct_spec_terms >= 8 and sig.spec_terms >= 30)
        or (sig.distinct_spec_terms >= 8 and sig.trim_terms >= 50)
    )
    specced = sig.distinct_spec_terms >= 5 and sig.spec_terms >= 12
    if sig.text_chars < 1200:
        sig.verdict = "js_shell"
    elif not specced:
        sig.verdict = "marketing_only"
    elif not structured:
        sig.verdict = "spec_prose"
    elif sig.trim_terms >= 8:
        sig.verdict = "trim_spec_table"
    else:
        sig.verdict = "spec_table"
    return sig


class _Anchors(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[tuple[str, str]] = []
        self._href: str | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self._href = href
            self._text = []

    def handle_data(self, data):
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag):
        if tag.lower() == "a" and self._href is not None:
            self.links.append((self._href, _WS.sub(" ", "".join(self._text)).strip()))
            self._href = None
            self._text = []


_SITEMAP_LOC = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>", re.IGNORECASE)


def anchors(html: str, base_url: str) -> list[tuple[str, str]]:
    """
    Every link on the page, absolutised. Not filtered by extension.

    Handles XML sitemaps as well as HTML. An earlier version of this function
    only understood ``<a href>``, so ``sitemap.xml`` -- which is precisely the
    per-vehicle index several makes publish -- measured as *zero* links and
    would have been written up as "no index". A sitemap is a link index; it just
    spells its links ``<loc>``.
    """
    if _SITEMAP_LOC.search(html or ""):
        return [(urljoin(base_url, u), "") for u in _SITEMAP_LOC.findall(html or "")]
    parser = _Anchors()
    try:
        parser.feed(html or "")
    except Exception:  # noqa: BLE001 - malformed markup is data, not a crash
        pass
    out: list[tuple[str, str]] = []
    for href, text in parser.links:
        href = href.strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        out.append((urljoin(base_url, href), text))
    return out


#: A per-vehicle index is a page whose anchors name vehicles. These are the path
#: words that mark such an anchor without guessing model slugs.
_INDEX_WORDS = re.compile(
    r"/(specs?|specifications?|features?|trims?|models?|vehicles?|brochures?|"
    r"catalog|compare|build)\b",
    re.IGNORECASE,
)


@dataclass
class IndexSignal:
    anchors_total: int = 0
    pdf_links: int = 0
    pdf_hosts: list[str] = field(default_factory=list)
    spec_route_links: int = 0
    year_bearing_links: int = 0
    offsite_links: int = 0
    sample_spec_routes: list[str] = field(default_factory=list)
    sample_pdfs: list[str] = field(default_factory=list)


def index_signal(html: str, base_url: str) -> IndexSignal:
    """
    Is this page a walkable per-vehicle index, or a dead end?

    ``year_bearing_links`` is the field that matters most for our actual problem.
    BMW serves real PDFs but its article URLs are opaque ids carrying no model
    year, so nothing can be attributed to a model year without inference -- which
    this project does not do. A high anchor count with zero year-bearing links is
    that failure, and it is worth naming separately from "no links".
    """
    sig = IndexSignal()
    base_host = (urlparse(base_url).hostname or "").lower()
    links = anchors(html, base_url)
    sig.anchors_total = len(links)
    pdf_hosts: set[str] = set()
    for url, _text in links:
        host = (urlparse(url).hostname or "").lower()
        path = urlparse(url).path
        if host and host != base_host:
            sig.offsite_links += 1
        if path.lower().endswith(".pdf"):
            sig.pdf_links += 1
            pdf_hosts.add(host)
            if len(sig.sample_pdfs) < 5:
                sig.sample_pdfs.append(url)
        if _INDEX_WORDS.search(path):
            sig.spec_route_links += 1
            if len(sig.sample_spec_routes) < 12:
                sig.sample_spec_routes.append(url)
        if _YEAR.search(path):
            sig.year_bearing_links += 1
    sig.pdf_hosts = sorted(pdf_hosts)
    return sig


# --------------------------------------------------------------------------
# Paced client
# --------------------------------------------------------------------------


class PacedClient:
    """
    Sequential client with a hard floor between requests.

    Mirrors :class:`~backend.enrichment.brochure_sources.PacedFetcher`'s pacing
    contract but returns the whole response, because a probe has to report
    content-type and the redirect chain and ``PacedFetcher.get_text`` returns
    neither. No concurrency, by design.
    """

    def __init__(self, *, user_agent: str, delay: float = DEFAULT_DELAY_SECONDS) -> None:
        import requests

        self.user_agent = user_agent
        self.robots_agent = robots_agent_for(user_agent)
        self.delay = max(float(delay), DEFAULT_DELAY_SECONDS)
        self._session = requests.Session()
        self._last = 0.0
        self.request_count = 0

    def _pace(self) -> None:
        if self.request_count:
            elapsed = time.monotonic() - self._last
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
        self._last = time.monotonic()
        self.request_count += 1

    def get(self, url: str):
        self._pace()
        return self._session.get(
            url,
            headers={
                "User-Agent": self.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
            allow_redirects=True,
        )

    def get_text(self, url: str) -> tuple[int, str]:
        """Signature :class:`RobotsPolicy` expects."""
        resp = self.get(url)
        return resp.status_code, resp.text


# --------------------------------------------------------------------------
# Probe
# --------------------------------------------------------------------------


@dataclass
class Probe:
    make: str
    url: str
    purpose: str = ""
    host: str = ""
    user_agent: str = ""
    host_allowlisted: bool = False
    robots_status: str = ""
    robots_detail: str = ""
    robots_allows: bool = False
    http_status: str = ""
    final_url: str = ""
    redirected: bool = False
    content_type: str = ""
    bytes: int = 0
    sha256: str = ""
    elapsed_s: float = 0.0
    spec: dict | None = None
    index: dict | None = None
    verdict: str = ""
    detail: str = ""
    measured_at: str = ""
    #: Working field for --dump-anchors. Excluded from the report: a full link
    #: dump would balloon the artifact and none of it is a measurement.
    links: list = field(default_factory=list, repr=False)
    #: Working field for --dump-text, likewise excluded from the report. The
    #: report records measurements of documents, never their content: fetched
    #: pages are local verification substrate and this artifact is not a place
    #: to republish them.
    text: str = field(default="", repr=False)


def _client_for(label: str, delay: float, cache: dict) -> PacedClient:
    if label not in cache:
        cache[label] = PacedClient(user_agent=UA_LABELS[label], delay=delay)
    return cache[label]


def _robots_for(label: str, client: PacedClient, cache: dict) -> RobotsPolicy:
    if label not in cache:
        # No exempt_hosts argument: the archive exemption is not in scope here,
        # and a probe that could exempt a host would be a control with a bypass.
        cache[label] = RobotsPolicy(client.get_text, agent=client.robots_agent)
    return cache[label]


def probe_url(
    make: str,
    url: str,
    purpose: str,
    ua_label: str,
    clients: dict,
    robots_cache: dict,
    delay: float,
) -> Probe:
    """
    Measure one page. Never downloads a document; reads index/spec HTML only.

    Fails closed: if robots.txt is unreadable or disallows the path, the page is
    not fetched and the refusal is the finding.
    """
    host = (urlparse(url).hostname or "").lower()
    p = Probe(
        make=make,
        url=url,
        purpose=purpose,
        host=host,
        user_agent=ua_label,
        measured_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )
    p.host_allowlisted = host in official_hosts_for(make)

    client = _client_for(ua_label, delay, clients)
    robots = _robots_for(ua_label, client, robots_cache)

    allowed = robots.allows(url)
    verdict = robots.verdict_for(host)
    if verdict is not None:
        p.robots_status = verdict.status
        p.robots_detail = verdict.detail[:300]
    p.robots_allows = bool(allowed)
    if not allowed:
        readable = bool(verdict and verdict.readable)
        p.verdict = "robots_disallowed" if readable else "robots_unreadable"
        p.detail = f"robots.txt {p.robots_status}: {p.robots_detail}"
        return p

    started = time.monotonic()
    try:
        resp = client.get(url)
    except Exception as exc:  # noqa: BLE001 - transport failure IS the finding
        p.elapsed_s = round(time.monotonic() - started, 3)
        p.verdict = "transport_error"
        p.detail = f"{type(exc).__name__}: {str(exc)[:200]}"
        return p
    p.elapsed_s = round(time.monotonic() - started, 3)

    body = resp.content or b""
    p.http_status = str(resp.status_code)
    p.final_url = resp.url
    p.redirected = resp.url.rstrip("/") != url.rstrip("/")
    p.content_type = resp.headers.get("Content-Type", "")[:120]
    p.bytes = len(body)
    p.sha256 = hashlib.sha256(body).hexdigest()

    if resp.status_code != 200:
        p.verdict = f"http_{resp.status_code}"
        p.detail = f"GET -> HTTP {resp.status_code}, {p.bytes} bytes"
        return p

    ctype = p.content_type.lower()
    if "pdf" in ctype or body[:5] == b"%PDF-":
        p.verdict = "served_pdf"
        p.detail = f"{p.bytes} bytes of application/pdf"
        return p
    if "html" not in ctype and "xml" not in ctype and "text" not in ctype:
        p.verdict = "non_html"
        p.detail = f"content-type {p.content_type}, {p.bytes} bytes"
        return p

    html = resp.text
    sig = spec_signal(html)
    idx = index_signal(html, resp.url)
    p.spec = asdict(sig)
    p.index = asdict(idx)
    p.links = anchors(html, resp.url)
    p.text = visible_text(html)

    # A sitemap is classified by its links, never by its word count. The
    # text-length rule below is right for HTML and wrong here: a sitemap index
    # is deliberately a few hundred bytes of <loc> elements, and the first
    # version of this function called Jeep/RAM/Dodge/Nissan/Subaru "js_shell"
    # on that basis -- a false negative of exactly the kind this module exists
    # to stop, just pointing the other way.
    is_sitemap = bool(_SITEMAP_LOC.search(html))
    if is_sitemap:
        children = [u for u, _ in p.links if "sitemap" in u.lower() or u.lower().endswith(".xml")]
        p.verdict = "sitemap_index" if children else "sitemap"
        p.detail = (
            f"{idx.anchors_total} <loc> entries"
            + (f", {len(children)} of them child sitemaps" if children else "")
            + f", {idx.year_bearing_links} carry a model year"
        )
        return p

    # The verdict combines both axes, because "has specs" and "has a walkable
    # index" are separate questions and a make needs BOTH to be implementable.
    if sig.verdict == "js_shell":
        p.verdict = "js_shell"
        p.detail = (
            f"HTTP 200, {p.bytes} bytes, but only {sig.text_chars} chars of "
            "visible text: client-rendered"
        )
    elif sig.verdict in ("spec_table", "trim_spec_table"):
        p.verdict = "html_specs"
        p.detail = (
            f"{sig.verdict}: {sig.text_chars} chars visible text, "
            f"{sig.distinct_spec_terms} distinct spec terms, {sig.tables} tables, "
            f"{sig.rows} rows, {sig.trim_terms} trim terms"
        )
    elif idx.pdf_links:
        p.verdict = "pdf_index"
        p.detail = f"{idx.pdf_links} PDF link(s) on {', '.join(idx.pdf_hosts[:3])}"
    elif idx.spec_route_links >= 5:
        p.verdict = "link_index"
        p.detail = (
            f"{idx.spec_route_links} spec/model route link(s), "
            f"{idx.year_bearing_links} carry a model year"
        )
    else:
        p.verdict = sig.verdict
        p.detail = (
            f"HTTP 200, {p.bytes} bytes, {sig.text_chars} chars visible text, "
            f"{idx.anchors_total} anchors, {idx.pdf_links} PDFs, "
            f"{sig.distinct_spec_terms} distinct spec terms"
        )
    return p


# --------------------------------------------------------------------------
# Targets
# --------------------------------------------------------------------------

#: (make, url, purpose). Ranked by uncovered active-car count. Ford is present
#: as a single confirmation per host and nothing more -- it was already tested
#: per host, per protocol, per user agent on 2026-07-31 and 2026-08-01.
TARGETS: tuple[tuple[str, str, str], ...] = (
    # ---- Ford: confirmation only -------------------------------------
    ("ford", "https://www.ford.com/", "confirm edge refusal"),
    ("ford", "https://media.ford.com/", "confirm edge refusal"),
    # ---- Chevrolet ---------------------------------------------------
    (
        "chevrolet",
        "https://www.chevrolet.com/trucks/silverado/1500",
        "model page -- HTML specs?",
    ),
    (
        "chevrolet",
        "https://www.chevrolet.com/trucks/silverado/1500/specs",
        "per-model spec route",
    ),
    (
        "chevrolet",
        "https://www.chevrolet.com/trucks/silverado/1500/build-and-price/trim",
        "trim ladder route",
    ),
    ("chevrolet", "https://www.chevrolet.com/sitemap.xml", "index discovery"),
    ("chevrolet", "https://media.chevrolet.com/us/en/chevrolet/vehicles.html", "media index"),
    # ---- BMW ---------------------------------------------------------
    (
        "bmw",
        "https://www.bmwusa.com/vehicles/x/x5/sports-activity-vehicle/specifications.html",
        "per-model spec route",
    ),
    ("bmw", "https://www.bmwusa.com/sitemap.xml", "index discovery"),
    (
        "bmw",
        "https://www.press.bmwgroup.com/usa/article/topic/1/usa.html",
        "server-rendered article index?",
    ),
    # ---- Mercedes-Benz -----------------------------------------------
    ("mercedesbenz", "https://www.mbusa.com/en/vehicles/class/gle/suv", "class page"),
    (
        "mercedesbenz",
        "https://www.mbusa.com/en/vehicles/model/gle/suv/gle350w4#specs",
        "per-model spec route",
    ),
    ("mercedesbenz", "https://www.mbusa.com/sitemap.xml", "index discovery"),
    # ---- Honda -------------------------------------------------------
    ("honda", "https://hondanews.com/en-US/honda/releases", "release index -- walkable?"),
    ("honda", "https://hondanews.com/en-US/channels/honda-hr-v", "per-vehicle channel"),
    ("honda", "https://hondanews.com/sitemap.xml", "index discovery"),
    # ---- Toyota: verify the recent fix still holds ---------------------
    ("toyota", "https://www.toyota.com/brochures/", "verify redirect-into-one-category"),
    ("toyota", "https://www.toyota.com/brochures/trucks/", "verify trucks category"),
    # ---- Stellantis ---------------------------------------------------
    ("jeep", "https://www.jeep.com/wrangler.html", "model page -- HTML specs?"),
    (
        "jeep",
        "https://www.jeep.com/wrangler/compare-specs.html",
        "per-model spec route",
    ),
    (
        "ram",
        "https://www.ramtrucks.com/ram/1500/compare-specs.html",
        "per-model spec route",
    ),
    (
        "dodge",
        "https://www.dodge.com/durango/compare-specs.html",
        "per-model spec route",
    ),
    (
        "jeep",
        "https://media.stellantisnorthamerica.com/newsrelease.do?id=1",
        "Stellantis media newsroom reachability",
    ),
    # ---- Hyundai / Kia -------------------------------------------------
    (
        "hyundai",
        "https://www.hyundaiusa.com/us/en/vehicles/tucson/compare-specs",
        "per-model spec route",
    ),
    ("hyundai", "https://www.hyundaiusa.com/sitemap.xml", "index discovery"),
    ("kia", "https://www.kia.com/us/en/telluride/specifications", "per-model spec route"),
    ("kia", "https://www.kia.com/us/en/sitemap.xml", "index discovery"),
    # ---- Nissan / Subaru -----------------------------------------------
    (
        "nissan",
        "https://www.nissanusa.com/vehicles/trucks/frontier/specs.html",
        "per-model spec route",
    ),
    ("subaru", "https://www.subaru.com/vehicles/outback/specs.html", "per-model spec route"),
    # ---- VW / Audi -----------------------------------------------------
    (
        "volkswagen",
        "https://www.vw.com/en/models/tiguan/specs.html",
        "per-model spec route",
    ),
    ("audi", "https://www.audiusa.com/us/web/en/models/q5/q5/2026.html", "model page"),
    ("audi", "https://www.audiusa.com/robots.txt", "host reachability"),
)


#: Round 2, chosen AFTER round 1 rather than guessed alongside it. Round 1's
#: 404s (kia/nissan/subaru/vw/jeep ``*/specs.html``) were invented slugs, and
#: inventing slugs is the exact mistake the Toyota fix was about: the answer is
#: to read the site's own hrefs. These entries therefore go at sitemaps and real
#: index pages, not at guessed model paths.
TARGETS_ROUND2: tuple[tuple[str, str, str], ...] = (
    ("bmw", "https://www.bmwusa.com/robots.txt", "host reachability, identified UA"),
    ("toyota", "https://www.toyota.com/brochures/", "verify Toyota fix holds"),
    ("kia", "https://www.kia.com/us/en/sitemap.xml", "index discovery"),
    ("kia", "https://www.kia.com/us/en", "read own hrefs"),
    ("nissan", "https://www.nissanusa.com/sitemap.xml", "index discovery"),
    ("subaru", "https://www.subaru.com/sitemap.xml", "index discovery"),
    ("volkswagen", "https://www.vw.com/sitemap.xml", "index discovery"),
    ("jeep", "https://www.jeep.com/sitemap.xml", "index discovery"),
    ("ram", "https://www.ramtrucks.com/sitemap.xml", "index discovery"),
    ("dodge", "https://www.dodge.com/sitemap.xml", "index discovery"),
    ("hyundai", "https://www.hyundaiusa.com/us/en/vehicles", "lineup index"),
    ("mercedesbenz", "https://www.mbusa.com/en/vehicles/all-vehicles", "lineup index"),
    ("mercedesbenz", "https://www.mbusa.com/en/shopping-tools/brochures", "brochure index"),
)


def targets_for(makes: set[str] | None, group: str | None) -> list[tuple[str, str, str]]:
    rows = list(TARGETS_ROUND2) if group == "round2" else list(TARGETS)
    if group == "current-year":
        rows = [r for r in rows if r[0] not in ("ford",)]
    if makes:
        rows = [r for r in rows if r[0] in makes]
    return rows


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------


#: Per-make conclusions. Every ``evidence`` string is a measurement taken by
#: this lane on 2026-08-02 and is reproducible from the ``probes`` array in the
#: same report. ``uncovered_cars`` is NOT a number this lane measured -- it was
#: given in the task brief, and it is labelled as such rather than restated as
#: though it were our own count.
#:
#: ``status`` is one of:
#:   ``implementable``  -- an official, robots-permitted, server-rendered spec
#:                         source with a per-vehicle index AND a model-year
#:                         attribution. Ready to build.
#:   ``blocked``        -- reachable, but something specific is missing. The
#:                         missing thing is named.
#:   ``not_reachable``  -- the host refuses us, or publishes nothing usable.
MAKE_FINDINGS: dict[str, dict] = {
    "honda": {
        "rank_note": "Accord Sedan 408 + CR-V 354 uncovered (from brief)",
        "status": "implementable",
        "where": "hondanews.com press releases, as HTML. Not PDF -- which is why "
        "a PDF-only probe scored this make as publishing nothing.",
        "index": "Per-vehicle channels, enumerable from ANY channel page's own nav "
        "(no slug guessing): /honda-automobiles/channels/{honda-accord, honda-civic, "
        "honda-cr-v, cr-v-e-fcev, honda-hr-v, honda-odyssey, honda-passport, "
        "honda-pilot, prologue, honda-ridgeline}. Acura is the same platform on "
        "acuranews.com. Each channel has a Specs tab: "
        "/en-US/channels/<slug>?selectedTabId=<slug>-specs, which lists one "
        "'YYYY Honda <Model> Specifications & Features' release per model year, "
        "2016 through 2027.",
        "robots": "hondanews.com robots.txt HTTP 404, 0 bytes -> no rules published. "
        "automobiles.honda.com is a DIFFERENT host and 403s its own robots.txt; it "
        "stays off-limits.",
        "evidence": "The 2026 HR-V release measured trim_spec_table: 17,464 chars of "
        "visible text, 16 distinct spec terms, 5 tables, 192 rows, 15 trim terms. Its "
        "body is a trim-columned table headed 'LX Sport EX-L' with rows such as "
        "'Horsepower (SAE net @ rpm) 158 @ 6,500' and 'Displacement (cc) 1,996'.",
        "path": "Walk channel -> Specs tab -> release. Model year is in BOTH the "
        "release title and the URL slug, so attribution needs no inference. Locator = "
        "URL + retrieval timestamp + content hash.",
        "gotcha": "The table uses three markers that a parser MUST honour: '•' = "
        "standard on that trim, 'Available' = optional, and '<<' = SAME AS THE COLUMN "
        "TO THE LEFT. Treating '<<' as a literal value, or ignoring it, silently "
        "mis-assigns specs across trims.",
    },
    "mercedesbenz": {
        "rank_note": "GLE 464 uncovered (from brief)",
        "status": "implementable",
        "where": "www.mbusa.com per-trim model pages, as HTML.",
        "index": "The CLASS page is the index: /en/vehicles/class/<class>/<body> "
        "(e.g. /en/vehicles/class/gle/suv) measured 56 spec/model route links. Each "
        "model page also links its sibling trims under 'Similar Vehicles'. NOTE: "
        "/sitemap.xml and /en/vehicles/all-vehicles both 404 -- do not build on them.",
        "robots": "www.mbusa.com robots.txt HTTP 200 (browser UA); the probed pages "
        "were permitted.",
        "evidence": "/en/vehicles/model/gle/suv/gle350w4 measured spec_table: 12,103 "
        "chars visible text, 12 distinct spec terms. It carries a full 'Specifications' "
        "section -- 'Power 255 hp @ 5,800-6,100 rpm', 'Torque 295 lb-ft @ 1,800-4,000 "
        "rpm', 'Wheelbase - A 117.9 in', 'Curb weight 4,916 lbs', 'Towing capacity "
        "7,700 lbs', 'Tires 275/55R19' -- under the page title '2026 GLE 350 4MATIC SUV'.",
        "path": "Walk class page -> trim pages. One page = one trim = one model year, "
        "which is the cleanest attribution of any make here.",
        "gotcha": "The model year appears in the TITLE, not the URL. The URL "
        "(/gle350w4) is trim-coded and stable across years, so a stored document must "
        "record the title/hash or a re-fetch next year will silently overwrite this "
        "year's specs with next year's.",
    },
    "subaru": {
        "rank_note": "not in the brief's top list; included as a full-lineup make",
        "status": "implementable",
        "where": "www.subaru.com per-year spec pages, as HTML.",
        "index": "https://www.subaru.com/sitemap.xml -> dynamic_sitemap.xml, which "
        "measured 961 <loc> entries, 169 of them carrying a model year.",
        "robots": "www.subaru.com robots.txt readable; the sitemap was served.",
        "evidence": "Routes have the form "
        "/vehicles/<model>/[<variant>/]<YEAR>/specs-trim.html -- e.g. "
        "/vehicles/outback/wilderness/2025/specs-trim.html, "
        "/vehicles/crosstrek/gas/2025/specs-trim.html.",
        "path": "Walk dynamic_sitemap.xml and filter to /specs-trim.html. The MODEL "
        "YEAR IS IN THE URL PATH -- the best locator of any make probed, because "
        "attribution needs neither title parsing nor inference.",
        "gotcha": "NOT YET CONFIRMED: this lane read the sitemap but did NOT fetch a "
        "Subaru specs-trim.html body, so the page's spec content is unverified. Fetch "
        "one and check it before building on this. media.subaru.com (a different host) "
        "is separately recorded as returning a 0-byte body.",
    },
    "kia": {
        "rank_note": "not in the brief's top list",
        "status": "implementable",
        "where": "www.kia.com per-model spec-compare pages, as HTML.",
        "index": "Lineup at /us/en/vehicles; each model page links its own spec route.",
        "robots": "www.kia.com robots.txt permitted the probed pages (browser UA).",
        "evidence": "/us/en/telluride/specs-compare measured trim_spec_table: 154,165 "
        "chars visible text, 16 distinct spec terms, 2,957 trim terms, with ten trim "
        "columns -- 'Horsepower 274 hp @ 5,800 rpm' repeated across ten values, "
        "'Wheelbase 116.9 in.' across ten, 'Track: Front/Rear 67.7 in / 68.2 in.'.",
        "path": "Walk /us/en/vehicles -> model page -> read the '/specs-compare' href "
        "off the model page. Do NOT construct it: this lane's guessed "
        "'/us/en/telluride/specifications' returned HTTP 404, and '/specs-compare' was "
        "found only by reading the page's own anchors.",
        "gotcha": "kiamedia.com is a DIFFERENT host whose robots.txt disallows "
        "/*/*/download/*; that restriction is unchanged and does not apply to kia.com. "
        "Model year is not in the /specs-compare URL, but the model page exposes a "
        "year-bearing sibling route (/us/en/compare?model=Telluride&year=2027).",
    },
    "nissan": {
        "rank_note": "not in the brief's top list",
        "status": "implementable",
        "where": "www.nissanusa.com per-model specs-trims pages, as HTML.",
        "index": "/sitemap.xml -> /index.pages-sitemap.xml, which measured 676 <loc> "
        "entries, 103 carrying a model year.",
        "robots": "Served to the BROWSER user agent. The existing record notes Nissan "
        "403s an identified agent at robots.txt, so the UA choice is load-bearing.",
        "evidence": "/vehicles/cars/sentra/specs-trims.html measured trim_spec_table: "
        "15,875 chars visible text, 12 distinct spec terms, 196 trim terms, four trim "
        "columns -- 'Horsepower (hp) 149 @ 6,400 rpm' across four, 'Torque (lb-ft) "
        "146 @ 4,400 rpm' across four.",
        "path": "Walk index.pages-sitemap.xml, filter to /specs-trims.html. Some "
        "routes carry the year explicitly (/vehicles/cars/2026-altima-b/specs-trims.html); "
        "the undated ones are current-model-year and need the title or hash recorded.",
        "gotcha": "Nissan already has a working brochure path; this HTML route is an "
        "ADDITION for current model years, not a replacement.",
    },
    "hyundai": {
        "rank_note": "not in the brief's top list",
        "status": "implementable",
        "where": "www.hyundaiusa.com per-model compare-specs pages, as HTML.",
        "index": "/us/en/vehicles measured 502 spec/model route links; /sitemap.xml is "
        "~8 MB with 1,023 entries.",
        "robots": "www.hyundaiusa.com served the probed pages (browser UA). "
        "hyundainews.com -- a different host -- remains the 3,253-byte JS shell "
        "previously recorded.",
        "evidence": "/us/en/vehicles/tucson/compare-specs carries a six-trim grid: "
        "'Horsepower @ RPM 187 @ 6100' repeated across six columns, 'Displacement "
        "(liters) 2.5' across six, 'Compression ratio 13.0:1'. The page names its "
        "model year in text ('2026 TUCSON', '2026 TUCSON Hybrid').",
        "path": "Walk /us/en/vehicles -> /us/en/vehicles/<model>/compare-specs.",
        "gotcha": "ZERO <table> tags -- the grid is built from <div>s, and it is a Vue "
        "app whose price fields are unrendered mustaches ('{{ 38250 | toCurrency }}'). "
        "A parser keyed on table markup will extract nothing, and any price read from "
        "this page is a template placeholder, not a price.",
    },
    "bmw": {
        "rank_note": "X5 532 + X3 478 uncovered (from brief)",
        "status": "blocked",
        "where": "www.bmwusa.com technical-data pages carry real server-rendered specs.",
        "index": "REFUTES THE RECORDED BLOCKER. The record says BMW has 'no per-vehicle "
        "index, its search is client-side'. That is true of press.bmwgroup.com and NOT "
        "of www.bmwusa.com, whose /sitemap.xml measured 367 <loc> entries including "
        "/vehicles/x-series/suv/bmw-x5-technical-data.html.",
        "robots": "www.bmwusa.com gives the two user agents OPPOSITE answers, "
        "reconfirmed here: the BROWSER string read-times-out at 45s, the IDENTIFIED "
        "string is served and the pages fetch normally. All BMW results here are under "
        "the identified agent.",
        "evidence": "bmw-x5-technical-data.html measured spec_table: 8,199 chars "
        "visible text, 60 tables, 55 rows, real values -- 'Wheelbase (in) 119.5', "
        "'Curb Weight (lbs) 5,216', 'Length (in) 197.2', 'Horsepower (bhp @ rpm) 483'.",
        "blocker": "MODEL YEAR. The page states none: the title is 'BMW X5: Technical "
        "data, dimensions, engines, and more' and the URL carries no year. The only "
        "'2026'/'2027' tokens on the page are the footer copyright ('(c) 2026 BMW of "
        "North America, LLC') and an 'i3 coming in 2027' teaser -- neither is a "
        "statement about these specs. Trim attribution is also weak: the page scored "
        "just 1 trim term and separates variants by powertrain block (a 483 hp "
        "Plug-in Hybrid and a 570 hp Electric appear with no trim label in the "
        "flattened text).",
        "path": "None that satisfies the iron rules as-is. Assigning these specs to a "
        "model year would be inference, which is exactly what this project forbids. A "
        "real path needs a year-bearing BMW page; the sitemap does contain year-bearing "
        "variant slugs (e.g. /bmw-x5/x5-2026-g65-icephev-exterior.html) that were NOT "
        "probed here and are the obvious next step.",
    },
    "chevrolet": {
        "rank_note": "Silverado 1500 604 uncovered (from brief)",
        "status": "blocked",
        "where": "www.chevrolet.com has a /specs-models route per model.",
        "index": "PARTIALLY REFUTES THE RECORD. The record says 'sitemap.xml lists no "
        "brochure or catalog page' -- correct -- but the sitemap is nonetheless a "
        "walkable VEHICLE index: it measured 648 <loc> entries, including "
        "/trucks/silverado/2500hd-3500hd/specs-models and a 'previous-year' mirror of "
        "each model path.",
        "robots": "www.chevrolet.com robots.txt HTTP 200 to the browser UA, HTTP 403 "
        "to the identified one (unchanged from the record). media.chevrolet.com "
        "robots.txt is readable and DISALLOWS the model path probed.",
        "evidence": "The blocker is that the spec CONTENT is not server-rendered. "
        "/trucks/silverado/2500hd-3500hd/specs-models is 581,037 bytes but yields only "
        "3,516 chars of visible text and ZERO spec terms. The build-and-price trim "
        "route is 35,806 bytes with 41 chars of visible text. Both are client-rendered.",
        "blocker": "No server-rendered spec text to quote. An index exists; the "
        "document behind it does not arrive over HTTP.",
        "path": "None as HTML. Note this is a DIFFERENT finding from the record's "
        "'GM publishes no PDF', which the fetch ledger already refuted -- GM's brochure "
        "PDFs exist but were discovered out of band. Nothing here changes that.",
    },
    "volkswagen": {
        "rank_note": "not in the brief's top list",
        "status": "blocked",
        "where": "www.vw.com model pages; trim data lives in a builder app.",
        "index": "Walkable, but with the SAME structural surprise as Toyota: the root "
        "/en/models.html lists NO models (114 anchors, all navigation). The category "
        "pages do -- /en/suv.html links /en/models/taos.html, /en/models/tiguan.html, "
        "/en/models/atlas.html, /en/models/id-4.html. /sitemap.xml 404s.",
        "robots": "www.vw.com served the probed pages (browser UA).",
        "evidence": "/en/models/tiguan.html exposes no spec or technical-data href "
        "among its 176 anchors; the only trim entry point is "
        "/en/builder.html/__app/tiguan.app, a JavaScript application. This lane's "
        "guessed /en/models/tiguan/specs.html returned HTTP 404.",
        "blocker": "No server-rendered per-trim spec page located.",
        "path": "None confirmed. media.vw.com remains the 2,560-byte JS shell already "
        "on record.",
    },
    "jeep": {
        "rank_note": "Jeep/RAM/Dodge, grouped in the brief",
        "status": "not_reachable",
        "where": "Nothing server-rendered located, on either the consumer or media host.",
        "index": "The Stellantis consumer sitemaps are stubs, not indexes: jeep.com "
        "15 <loc> entries, ramtrucks.com 22, dodge.com 26 -- and the Jeep entries are "
        "site-wide utility pages (/gab.html, /vehicle-selector.bmo.html), with no "
        "per-vehicle page at all.",
        "robots": "All three consumer hosts served robots.txt and the probed pages.",
        "evidence": "/wrangler.html is 388,341 bytes yielding 10,616 chars of visible "
        "text and ONE distinct spec term -- marketing, not specifications. Guessed "
        "compare-specs.html routes 404 on all three brands. "
        "media.stellantisnorthamerica.com is reachable but the one release probed was "
        "32,392 bytes / 2,226 chars / 1 spec term.",
        "path": "None found. HONEST LIMIT: this lane probed the Stellantis media host "
        "only once, at an arbitrary release id. That host publishes press-kit spec "
        "tables for other brands and deserves a proper index probe before Stellantis "
        "is written off.",
    },
    "toyota": {
        "rank_note": "verification only -- already working",
        "status": "implementable",
        "where": "www.toyota.com PDF brochures. Unchanged.",
        "index": "THE FIX STILL HOLDS, and the trap is still live: "
        "https://www.toyota.com/brochures/ STILL silently redirects to "
        "/brochures/cars-minivan/ (measured: final URL differs from requested). "
        "/brochures/trucks/ serves directly and does not redirect.",
        "robots": "Served to the browser UA (Toyota 403s an identified agent, per the "
        "existing record).",
        "evidence": "/brochures/ -> 11 PDF links, all cars-minivan "
        "(crown, camry, corolla). /brochures/trucks/ -> 4 PDF links including "
        "/content/dam/toyota/brochures/pdf/2026/tundra_ebrochure.pdf. PDF paths carry "
        "the model year as a path segment.",
        "path": "No change needed. TOYOTA_BROCHURE_INDEX_PAGES' five category URLs are "
        "what makes this work -- anyone 'simplifying' that constant back to the single "
        "root URL silently loses every truck and SUV again.",
    },
    "ford": {
        "rank_note": "largest gap in the brief; already answered",
        "status": "not_reachable",
        "where": "n/a",
        "index": "n/a",
        "robots": "CONFIRMED, one request per host and nothing further: www.ford.com "
        "and media.ford.com both read-timed-out fetching robots.txt at the 45s ceiling "
        "(2026-08-02). This reproduces the 2026-07-31 and 2026-08-01 records.",
        "evidence": "Unreadable robots.txt is a refusal of automated access and this "
        "project treats it as no. No page on either host was requested.",
        "path": "None. Do not re-probe Ford; it has now been measured on three separate "
        "days, per host, per protocol and per user agent.",
    },
    "audi": {
        "rank_note": "not in the brief's top list",
        "status": "not_reachable",
        "where": "n/a",
        "index": "n/a",
        "robots": "www.audiusa.com robots.txt returned HTTP 403 to the browser user "
        "agent, so no page on that host may be fetched.",
        "evidence": "media.audiusa.com is separately on record as a 2,497-byte JS "
        "shell with no PDF links.",
        "path": "None.",
    },
}


def load_report() -> dict:
    if REPORT_PATH.exists():
        try:
            return json.loads(REPORT_PATH.read_text())
        except Exception:  # noqa: BLE001
            return {}
    return {}


def save_report(report: dict) -> None:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")


def merge_probes(report: dict, probes: list[Probe]) -> dict:
    report.setdefault("schema", 1)
    report.setdefault(
        "note",
        "Reconnaissance only. Every row is a measured HTTP response; nothing here "
        "was inferred. Documents were never downloaded. robots.txt was honoured "
        "for every request and an unreadable robots.txt was treated as a refusal.",
    )
    rows = {(*(r["url"],), r["user_agent"]): r for r in report.get("probes", [])}
    for p in probes:
        row = asdict(p)
        row.pop("links", None)
        row.pop("text", None)
        rows[(p.url, p.user_agent)] = row
    report["findings"] = MAKE_FINDINGS
    report["probes"] = sorted(rows.values(), key=lambda r: (r["make"], r["url"], r["user_agent"]))
    report["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--make", action="append", default=[])
    ap.add_argument("--group", default=None)
    ap.add_argument("--url", default=None, help="probe one ad-hoc URL")
    ap.add_argument("--purpose", default="ad-hoc")
    ap.add_argument(
        "--user-agent",
        default="browser",
        choices=["browser", "identified", "both"],
    )
    ap.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS)
    ap.add_argument("--dump-anchors", type=int, default=0, help="print N anchors")
    ap.add_argument("--grep", default="", help="regex filter for --dump-anchors")
    ap.add_argument("--find", default="", help="regex to locate within visible text")
    ap.add_argument(
        "--dump-text",
        type=int,
        default=0,
        help="print N chars of VISIBLE text -- use this to confirm by eye that a "
        "page classed html_specs really carries specs, before anyone builds on it",
    )
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args(argv)

    ua_labels = ["browser", "identified"] if args.user_agent == "both" else [args.user_agent]
    clients: dict = {}
    robots_cache: dict = {}

    if args.url:
        rows = [(args.make[0] if args.make else "unknown", args.url, args.purpose)]
    else:
        rows = targets_for(set(args.make) or None, args.group)
    if args.limit:
        rows = rows[: args.limit]

    probes: list[Probe] = []
    for make, url, purpose in rows:
        for label in ua_labels:
            p = probe_url(make, url, purpose, label, clients, robots_cache, args.delay)
            probes.append(p)
            print(
                f"[{p.verdict:<20}] {make:<13} {label:<10} {url}\n"
                f"    {p.detail}",
                flush=True,
            )
            # Anchors come off the response we already read -- never a second
            # request. Re-fetching a page to look at its links doubles our load
            # on a host for no new information.
            if args.dump_anchors and p.links:
                shown = [
                    (u, t) for u, t in p.links if not args.grep or re.search(args.grep, u, re.I)
                ]
                for href, text in shown[: args.dump_anchors]:
                    print(f"      {href}  |  {text[:70]}", flush=True)
                print(f"      ({len(shown)} matched of {len(p.links)} links)", flush=True)
            if args.dump_text and p.text:
                print("      --- visible text ---", flush=True)
                print(p.text[: args.dump_text], flush=True)
            if args.find and p.text:
                # Multi-megabyte pages cannot be eyeballed with --dump-text, and
                # "it has 5.3M chars of text" is not evidence of specifications.
                # This shows the text AROUND a term so a human can judge whether
                # the page states a spec or merely contains the word.
                hits = list(re.finditer(args.find, p.text, re.I))
                print(f"      --- {len(hits)} match(es) for {args.find!r} ---", flush=True)
                for m in hits[:6]:
                    lo, hi = max(0, m.start() - 120), min(len(p.text), m.end() + 200)
                    print(f"      ...{p.text[lo:hi]}...", flush=True)

    save_report(merge_probes(load_report(), probes))
    print(f"\nwrote {REPORT_PATH} ({len(probes)} probe rows this run)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
