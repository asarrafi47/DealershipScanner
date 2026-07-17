"""
Platform-learning registry: classify a dealer's site platform cheaply and
*accumulate* what we learn so each odd dealer we figure out makes the next one on
the same platform automatic.

Why this exists
---------------
``backend.scanner.recipe_synth`` already fingerprints platforms from HTML markers
and can synthesize browser-free inventory recipes for the JSON-API platforms it
knows (CarsCommerce, Dealer.com, DealerOn cosmos, Typesense, Team Velocity). Two
gaps:

  1. HTML fingerprinting is useless when a site is Cloudflare-walled — plain HTTP
     returns a 403 / "Just a moment" challenge shell, so there is no real HTML to
     match markers against.
  2. It only understands platforms with a replayable JSON API. Some platforms
     have NO JSON API and render inventory as server-rendered HTML (harvested by
     ``backend.scanner.html_jsonld_harvest`` / ``backend/scripts/harvest_html_jsonld.py``),
     and some genuinely need a browser.

The killer signal is DNS. A site's CNAME chain resolves even behind Cloudflare
(DNS is not behind the challenge), and dealer platforms host their customers on
distinctive CNAME targets:

    www.sandersonford.com  ->  client-sandersonford.jazelc.com  ->  jazel-cdn.com   (Jazel)
    www.camelbacktoyota.com ->  le0016.secure.dealer.com.edgekey.net               (Dealer.com)
    www.courtesychev.com    ->  pod47.dealerinspire.com                            (CarsCommerce / DealerInspire)
    www.bellroadtoyota.com  ->  www.dealeronsite.com                               (DealerOn)
    www.righttoyota.com     ->  teamvelocitymarketing.map.fastly.net               (Team Velocity)

So DNS CNAME -> platform is a cheap, robust FIRST-LINE fingerprint that works
even when HTTP is blocked. This module builds the registry around it.

Public API
----------
* :data:`SEED_PLATFORMS` — the built-in platform seed (code).
* :func:`load_registry` — seed merged with the JSON overrides file, so new
  platforms can be added over time without editing this module.
* :func:`classify_dealer` — the core learning entrypoint: DNS CNAME first, then
  proxy-aware HTTP HTML markers, else log the dealer's signals to the
  unclassified-platforms log for a human/agent to name later.
* :func:`cname_chain` — best-effort DNS CNAME chain for a host (dig -> nslookup).

The learning loop
-----------------
An unrecognized dealer returns ``platform=None`` AND appends every signal it
gathered (CNAME chain, HTTP status, page size, api/inventory hosts seen) to
``workspace/unclassified_platforms.json``, deduped by dealer host. A human/agent
reads that log, names the platform, and adds ONE entry to
``backend/dictionary/platform_registry.json`` (cname_patterns / html_markers +
strategy). Every current and future dealer on that platform then classifies
automatically — no re-investigation.
"""
from __future__ import annotations

import json
import logging
import re
import subprocess
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from backend.scanner.http_fetch import open_url

logger = logging.getLogger("scanner")

# ── Locations ─────────────────────────────────────────────────────────────────

_REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_JSON = _REPO_ROOT / "backend" / "dictionary" / "platform_registry.json"
UNCLASSIFIED_LOG = _REPO_ROOT / "workspace" / "unclassified_platforms.json"

# Strategy values.
STRATEGY_SYNTHESIZE = "synthesize"  # recipe_synth template exists -> browser-free JSON API
STRATEGY_HTML_HARVEST = "html_harvest"  # server-rendered HTML -> harvest_html_jsonld (browser-free if reachable)
STRATEGY_BROWSER = "browser"  # genuinely needs a browser scan
_STRATEGIES = frozenset({STRATEGY_SYNTHESIZE, STRATEGY_HTML_HARVEST, STRATEGY_BROWSER})


# ── Registry entry ────────────────────────────────────────────────────────────


@dataclass
class PlatformEntry:
    """One learnable platform.

    ``cname_patterns`` are lowercase substrings that identify the platform in a
    DNS CNAME chain (the cheapest, Cloudflare-proof signal). ``html_markers`` are
    lowercase substrings to look for in server-rendered HTML as a fallback (these
    borrow from ``recipe_synth``'s fingerprints; for the JSON-API platforms we
    also delegate to :func:`recipe_synth.fingerprint_platform` for its richer,
    disambiguating detection). ``strategy`` is how we actually scan the platform.
    """

    name: str
    cname_patterns: list[str] = field(default_factory=list)
    html_markers: list[str] = field(default_factory=list)
    strategy: str = STRATEGY_BROWSER
    synthesizable: bool = False
    cloudflare: bool = False
    notes: str = ""

    def matches_cname(self, chain: list[str]) -> bool:
        joined = " ".join(chain).lower()
        return any(p.lower() in joined for p in self.cname_patterns if p)

    def matches_html(self, html_low: str) -> bool:
        return any(m.lower() in html_low for m in self.html_markers if m)


# ── Built-in seed ─────────────────────────────────────────────────────────────
#
# CNAME patterns were captured by resolving known-platform dealers from
# workspace/recipes/*.json (see module docstring). html_markers mirror
# recipe_synth's detect() markers so HTML fallback stays consistent.

SEED_PLATFORMS: list[PlatformEntry] = [
    PlatformEntry(
        name="carscommerce",
        cname_patterns=["dealerinspire.com", "carscommerce"],
        html_markers=["websites-search.api.carscommerce.inc", "carscommerce", "dealerinspire"],
        strategy=STRATEGY_SYNTHESIZE,
        synthesizable=True,
        cloudflare=False,
        notes="Cars Commerce / DealerInspire. JSON search API (per-dealer ccid + shared apiKey). "
        "recipe_synth _synth_carscommerce.",
    ),
    PlatformEntry(
        name="dealer_dot_com",
        cname_patterns=["dealer.com", "ddc.", ".edgekey.net"],
        html_markers=["ws-inv-data", "data-widget-name", "/api/widget/ws-inv-data/getinventory"],
        strategy=STRATEGY_SYNTHESIZE,
        synthesizable=True,
        cloudflare=False,
        notes="Dealer.com (Cox/DDC). ws-inv-data getInventory POST keyed by siteId. "
        "recipe_synth _synth_dealer_com. CNAME target is *.secure.dealer.com.edgekey.net.",
    ),
    PlatformEntry(
        name="dealer_on_cosmos",
        cname_patterns=["dealeronsite.com", "dealeron"],
        html_markers=['site-provider="dealeron"', "data-website-id=\"do-", "vhcliaa", "cosmos/srp/vehicles"],
        strategy=STRATEGY_SYNTHESIZE,
        synthesizable=True,
        cloudflare=False,
        notes="DealerOn cosmos SRP GET (account + SRP pageId). recipe_synth _synth_dealer_on_cosmos. "
        "Some DealerOn dealers pin the renderer during warmup (anti-bot).",
    ),
    PlatformEntry(
        name="typesense",
        # Typesense dealers CNAME to their own apex (no distinctive DNS target),
        # so they are identified by HTML markers, not DNS.
        cname_patterns=[],
        html_markers=["typesense.net", "__tshost", "__tsapikey"],
        strategy=STRATEGY_SYNTHESIZE,
        synthesizable=True,
        cloudflare=False,
        notes="Hosted Typesense multi_search collection (per-dealer collection, shared search key, "
        "all client-side). recipe_synth _synth_typesense. No distinctive CNAME -> HTML-only signal.",
    ),
    PlatformEntry(
        name="team_velocity",
        cname_patterns=["teamvelocity", "teamvelocitymarketing"],
        html_markers=["teamvelocityportal", "inventoryapibaseurl"],
        strategy=STRATEGY_SYNTHESIZE,
        synthesizable=True,
        cloudflare=False,
        notes="Team Velocity Apollo. Same-origin /inventory-used.json feed (?page=N). "
        "recipe_synth _synth_team_velocity. CNAME target is teamvelocitymarketing.map.fastly.net.",
    ),
    PlatformEntry(
        name="dealer_eprocess",
        cname_patterns=["dealereprocess"],
        html_markers=["dealereprocess", "cdn.dealereprocess.org", "dealerimages.dealereprocess.com"],
        strategy=STRATEGY_SYNTHESIZE,
        synthesizable=True,
        cloudflare=True,
        notes="Dealer eProcess ('Phoenix'). No JSON API: server-rendered SRP with per-card "
        "JSON-LD @type:Vehicle, paginated ?p=N (12/page). recipe_synth _synth_dealer_eprocess "
        "emits used+new SRP page-walk recipes (dealer_eprocess parser). Parameterized by domain "
        "only. Cloudflare-fronted: plain HTTP needs browser-navigation headers (UA + Sec-Fetch-*).",
    ),
    PlatformEntry(
        name="sister_tv",
        cname_patterns=["sister.tv"],
        html_markers=["es-data-v2.sister.tv", "sister.tv/vehicles"],
        strategy=STRATEGY_BROWSER,
        synthesizable=False,
        cloudflare=False,
        notes="sister.tv Elasticsearch. Recognized but NOT synthesizable: no HTML-extractable "
        "library_id on current dealers (they migrated to CarsCommerce). Needs a browser until a "
        "dealer resurfaces exposing library_id.",
    ),
    PlatformEntry(
        name="jazel",
        cname_patterns=["jazelc.com", "jazel-cdn.com"],
        html_markers=["jazel", "jazelc.com", "jazel-cdn"],
        strategy=STRATEGY_HTML_HARVEST,
        synthesizable=False,
        cloudflare=True,
        notes="Jazel platform (identified purely from DNS on Sanderson Ford: "
        "www.sandersonford.com -> client-sandersonford.jazelc.com -> jazel-cdn.com). No JSON "
        "inventory API; server-rendered HTML with VDP URLs like /new-inventory/vin-XXX.htm (same "
        "family as the McKenna dealer-group platform) -> use html_jsonld_harvest. Fronted by "
        "Cloudflare: plain HTTP needs SCANNER_HTTP_PROXY to reach real HTML.",
    ),
]


# ── Registry load (seed + JSON overrides) ─────────────────────────────────────


def _entry_from_dict(d: dict[str, Any]) -> PlatformEntry | None:
    name = (d.get("name") or "").strip()
    if not name:
        return None
    strat = (d.get("strategy") or STRATEGY_BROWSER).strip()
    if strat not in _STRATEGIES:
        logger.warning("platform_registry: entry %r has unknown strategy %r; defaulting to browser", name, strat)
        strat = STRATEGY_BROWSER
    return PlatformEntry(
        name=name,
        cname_patterns=list(d.get("cname_patterns") or []),
        html_markers=list(d.get("html_markers") or []),
        strategy=strat,
        synthesizable=bool(d.get("synthesizable", strat == STRATEGY_SYNTHESIZE)),
        cloudflare=bool(d.get("cloudflare", False)),
        notes=str(d.get("notes") or ""),
    )


def _read_overrides(path: Path | None = None) -> list[dict[str, Any]]:
    p = path or REGISTRY_JSON
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    items = raw.get("platforms") if isinstance(raw, dict) else raw
    return [d for d in (items or []) if isinstance(d, dict)]


def load_registry(json_path: Path | None = None) -> list[PlatformEntry]:
    """Return the built-in seed merged with the JSON overrides.

    Overrides are merged by ``name``: an entry whose name already exists in the
    seed REPLACES that seed entry (letting a human correct/extend a platform);
    a new name is appended. This is what makes the registry learnable without a
    code edit — add one object to ``platform_registry.json`` and it takes effect.
    """
    by_name: dict[str, PlatformEntry] = {e.name: e for e in SEED_PLATFORMS}
    for d in _read_overrides(json_path):
        entry = _entry_from_dict(d)
        if entry is not None:
            by_name[entry.name] = entry
    return list(by_name.values())


# ── DNS CNAME resolution (dig -> nslookup, never raises) ───────────────────────

_IP_RE = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")


def _dig_chain(host: str) -> list[str]:
    """CNAME chain via ``dig +short`` (non-IP answer lines), or ``[]``."""
    try:
        out = subprocess.run(
            ["dig", "+short", host],
            capture_output=True, text=True, timeout=8.0,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return []
    chain: list[str] = []
    for line in out.splitlines():
        v = line.strip().rstrip(".")
        if v and not _IP_RE.match(v) and ":" not in v:  # skip IPv4/IPv6 answers
            chain.append(v)
    return chain


def _nslookup_chain(host: str) -> list[str]:
    """CNAME chain via ``nslookup -type=CNAME`` (canonical-name lines), or ``[]``."""
    try:
        out = subprocess.run(
            ["nslookup", "-type=CNAME", host],
            capture_output=True, text=True, timeout=8.0,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return []
    chain: list[str] = []
    for line in out.splitlines():
        m = re.search(r"canonical name\s*=\s*([\w.-]+)", line, re.I)
        if m:
            chain.append(m.group(1).strip().rstrip("."))
    return chain


def cname_chain(host: str) -> list[str]:
    """Best-effort DNS CNAME chain for *host* (no library dependency).

    Tries ``dig`` then falls back to ``nslookup``; returns ``[]`` if neither
    resolves (or neither tool exists). Never raises. Works behind Cloudflare
    because DNS is not behind the challenge.
    """
    host = (host or "").strip()
    if not host:
        return []
    chain = _dig_chain(host)
    if chain:
        return chain
    return _nslookup_chain(host)


# ── HTTP probe (records signals even on a Cloudflare challenge) ─────────────────

_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_CHALLENGE_MARKERS = (
    "just a moment", "checking your browser", "cf-challenge", "__cf_chl",
    "attention required", "enable javascript and cookies", "cf-browser-verification",
    "px-captcha", "/cdn-cgi/challenge-platform",
)
_HOST_RE = re.compile(r"https?://([a-z0-9.-]+)", re.I)


@dataclass
class HttpProbe:
    status: int | None = None
    size: int = 0
    html: str | None = None  # only set when the body looks like real HTML
    challenge: bool = False
    error: str | None = None
    api_hosts: list[str] = field(default_factory=list)


def _http_probe(url: str, *, timeout: float = 20.0) -> HttpProbe:
    """Fetch *url* through the proxy-aware opener, recording signals even on a
    Cloudflare/JS-challenge response (status, byte size, challenge flag, any
    ``*api*`` / ``*inventory*`` hosts referenced). Unlike ``recipe_synth``'s
    fetch, this NEVER discards the body — the whole point is to capture what a
    walled/unknown site looks like so a human can name its platform later."""
    probe = HttpProbe()
    headers = {
        "User-Agent": _BROWSER_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "identity",
    }
    body = ""
    try:
        req = urllib.request.Request(url, headers=headers)
        resp = open_url(req, timeout=timeout)
        probe.status = getattr(resp, "status", None) or resp.getcode()
        raw = resp.read()
        probe.size = len(raw)
        body = raw.decode(resp.headers.get_content_charset() or "utf-8", "replace")
    except urllib.error.HTTPError as e:
        probe.status = e.code
        try:
            raw = e.read()
            probe.size = len(raw)
            body = raw.decode("utf-8", "replace")
        except Exception:
            body = ""
    except Exception as e:  # URLError, timeout, decode, ...
        probe.error = str(e)[:160]
        return probe

    low = body.lower()
    probe.challenge = any(m in low for m in _CHALLENGE_MARKERS)
    # Distinctive third-party hosts referenced by the page — the most useful
    # human hint for an unknown platform (an "*api*"/"*inventory*" host usually
    # names the vendor).
    seen: list[str] = []
    for h in _HOST_RE.findall(body):
        h = h.lower()
        if ("api" in h or "inventory" in h) and h not in seen:
            seen.append(h)
    probe.api_hosts = seen[:15]
    # Only expose the body as usable HTML when it is real (not a thin challenge shell).
    if not probe.challenge and probe.size >= 2000:
        probe.html = body
    return probe


# ── Unclassified-platforms log (the learning ledger) ───────────────────────────


def _host_of(url: str) -> str:
    p = urlparse(url if "://" in url else "https://" + url)
    return (p.hostname or p.path or url).lower()


def _load_unclassified(path: Path) -> dict[str, dict]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if isinstance(raw, dict):
        return {k: v for k, v in raw.items() if isinstance(v, dict)}
    # tolerate a list form: key by host
    return {r.get("host", str(i)): r for i, r in enumerate(raw) if isinstance(r, dict)}


def _append_unclassified(host: str, record: dict, path: Path | None = None) -> bool:
    """Record an unrecognized dealer's signals, deduped by host. Returns True if
    this host was NEW to the log (so callers can count fresh discoveries)."""
    import time

    p = path or UNCLASSIFIED_LOG
    p.parent.mkdir(parents=True, exist_ok=True)
    existing = _load_unclassified(p)
    is_new = host not in existing
    record = {**record, "host": host, "last_seen": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    if not is_new:
        record.setdefault("first_seen", existing[host].get("first_seen"))
    else:
        record["first_seen"] = record["last_seen"]
    existing[host] = record
    try:
        p.write_text(json.dumps(existing, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except OSError as e:
        logger.warning("platform_registry: could not write unclassified log: %s", e)
    return is_new


# ── The core learning entrypoint ───────────────────────────────────────────────


def classify_dealer(
    dealer_url: str,
    *,
    registry: list[PlatformEntry] | None = None,
    do_http: bool = True,
    log_unknown: bool = True,
    unclassified_path: Path | None = None,
) -> dict[str, Any]:
    """Classify *dealer_url*'s platform, cheapest signal first.

    Steps:
      (a) DNS CNAME lookup -> match ``cname_patterns`` (works behind Cloudflare);
      (b) if inconclusive and *do_http*, fetch the page over proxy-aware plain
          HTTP and match ``html_markers`` (delegating to
          ``recipe_synth.fingerprint_platform`` first for its richer detection);
      (c) if nothing matches, return ``platform=None`` and (when *log_unknown*)
          append every gathered signal to the unclassified-platforms log so the
          platform can be named later with a one-line registry entry.

    Returns a dict: ``platform``, ``strategy``, ``synthesizable``, ``cloudflare``,
    ``source`` (``"dns"`` | ``"html"`` | ``"unknown"``), ``signals`` (the raw
    evidence), and ``logged_new`` (True when this call added a NEW host to the
    unclassified log).
    """
    reg = registry if registry is not None else load_registry()
    host = _host_of(dealer_url)
    url = dealer_url if "://" in dealer_url else "https://" + dealer_url
    signals: dict[str, Any] = {"host": host}

    # (a) DNS CNAME — cheapest, Cloudflare-proof.
    chain = cname_chain(host)
    signals["cname_chain"] = chain
    for entry in reg:
        if entry.matches_cname(chain):
            return _result(entry, "dns", signals)

    # (b) HTML markers over proxy-aware plain HTTP.
    if do_http:
        probe = _http_probe(url)
        signals["http_status"] = probe.status
        signals["page_size"] = probe.size
        signals["cloudflare_challenge"] = probe.challenge
        if probe.api_hosts:
            signals["api_hosts"] = probe.api_hosts
        if probe.error:
            signals["http_error"] = probe.error
        if probe.html:
            # Prefer recipe_synth's richer, disambiguating fingerprint for the
            # JSON-API platforms; fall back to our own marker substrings.
            html_low = probe.html.lower()
            name = _recipe_synth_fingerprint(probe.html, url)
            by_name = {e.name: e for e in reg}
            if name and name in by_name:
                return _result(by_name[name], "html", signals)
            for entry in reg:
                if entry.matches_html(html_low):
                    return _result(entry, "html", signals)

    # (c) Unknown — record the evidence for the learning loop.
    logged_new = False
    if log_unknown:
        logged_new = _append_unclassified(host, {"dealer_url": url, "signals": signals}, unclassified_path)
    return {
        "platform": None,
        "strategy": None,
        "synthesizable": False,
        "cloudflare": bool(signals.get("cloudflare_challenge")),
        "source": "unknown",
        "signals": signals,
        "logged_new": logged_new,
    }


def _result(entry: PlatformEntry, source: str, signals: dict[str, Any]) -> dict[str, Any]:
    return {
        "platform": entry.name,
        "strategy": entry.strategy,
        "synthesizable": entry.synthesizable,
        "cloudflare": entry.cloudflare or bool(signals.get("cloudflare_challenge")),
        "source": source,
        "signals": signals,
        "notes": entry.notes,
        "logged_new": False,
    }


def _recipe_synth_fingerprint(html: str, url: str) -> str | None:
    """Delegate to recipe_synth's fingerprint (imported lazily to avoid a hard
    import cycle and to keep this module importable if recipe_synth changes)."""
    try:
        from backend.scanner.recipe_synth import fingerprint_platform

        return fingerprint_platform(html, url)
    except Exception:
        return None


def entry_to_dict(entry: PlatformEntry) -> dict[str, Any]:
    """Serialize a :class:`PlatformEntry` (e.g. to append to the JSON overrides)."""
    return asdict(entry)
