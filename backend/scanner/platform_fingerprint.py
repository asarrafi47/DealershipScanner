"""Platform fingerprints from discovery records, and clustering by fingerprint.

Why this exists (docs/HTTP_ONLY_SCANS_PLAN.md Phase 3, "new-platform playbook"):
a dealer no template describes lands in ``needs_discovery`` and someone has to
notice that three other dealers failed the same way on the same unknown
platform before it is worth writing a template. Nobody noticed audihuntsville
and the other oneaudi stores were one platform for two days. This module makes
the code notice.

Input is the ``discovery_<stamp>.json`` that ``discovery_probe.probe_dealer``
writes (field names in the module docstring of ``backend/scripts/discovery_probe.py``):
``homepage``, ``challenge``, ``fingerprint.detect``, ``signals`` (script hosts,
api hints, inline JSON keys, generator), ``synth.candidates`` and, with paths,
the inventory-path probes. A browser ``capture_<stamp>.json`` (its ``endpoints``)
may be passed alongside; its URLs become API-path features too.

:func:`fingerprint` turns one record into weighted features and a stable
``signature`` (the strongest 3-5 features, sorted). :func:`cluster` groups
dealers by signature and then merges near-duplicate groups by Jaccard
similarity of their full feature sets (threshold 0.6 by default), so a dealer
whose fifth-strongest feature is a chat widget still lands with its siblings.

Pure: no I/O, no HTTP. The file walking and the markdown live in
``backend/scripts/platform_candidates.py``.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable
from urllib.parse import urlparse

# --------------------------------------------------------------------------
# what is NOT a platform signal
# --------------------------------------------------------------------------

# Script hosts every second site loads: tag managers, ad and social pixels, public
# JS CDNs. They say nothing about who built the inventory pages.
_CDN_HOST_SUFFIXES = (
    "googleapis.com", "googletagmanager.com", "google-analytics.com", "googleadservices.com",
    "googlesyndication.com", "doubleclick.net", "google.com", "gstatic.com", "youtube.com",
    "facebook.net", "facebook.com", "fbcdn.net", "cdnjs.cloudflare.com", "jsdelivr.net",
    "unpkg.com", "jquery.com", "bootstrapcdn.com", "cloudflareinsights.com", "twitter.com",
    "tiktok.com", "bing.com", "clarity.ms", "hotjar.com", "adobedtm.com", "adnxs.com",
    "newrelic.com", "nr-data.net", "polyfill.io", "fontawesome.com", "recaptcha.net",
    "linkedin.com", "licdn.com", "pinterest.com", "snapchat.com", "criteo.com", "microsoft.com",
)

# Dealer add-ons (chat, compliance, video, price tools, call tracking). Real third
# parties, but a dealer bolts them on to any platform: kept as features, low weight.
_WIDGET_HOST_SUFFIXES = (
    "complyauto.com", "foureyes.io", "wistia.com", "gubagoo.io", "gubagoo.com", "callrail.com",
    "clarivoy.com", "edmunds.com", "swipetospin.com", "tradepending.com", "elfsightcdn.com",
    "elfsight.com", "trustindex.io", "callrevu.com", "ipredictive.com", "secureoffersites.com",
    "capitalone.com", "nitroleads.ai", "maxinsights.biz", "brainwall.ai", "carnow.com",
    "conversations.tv", "podium.com", "birdeye.com", "roadster.com", "cars.com", "kbb.com",
    "autotrader.com", "carfax.com", "carfaxonline.com", "sirv.com", "cloudflare.com",
    "onetrust.com", "cookiebot.com", "usercentrics.eu", "hubspot.com", "hs-scripts.com",
    "zendesk.com", "intercom.io", "drift.com", "livechatinc.com", "tawk.to", "activengage.com",
)

# ``signals.api_hints`` counts substrings; three of them match ordinary English
# ("automotive", "fox" in any hash, "cdk" in minified bundles) on nearly every page.
_NOISY_HINTS = frozenset({"fox", "motive", "cdk"})

# A path probe outcome only says something when it differs from the crowd; the
# aggregate (all 404 / all 403 / any VINs) carries the platform signal.
_PATH_STATUS_BUCKETS = {200: "200", 301: "3xx", 302: "3xx", 303: "3xx", 307: "3xx", 308: "3xx",
                        401: "401", 403: "403", 404: "404", 410: "404", 429: "429"}

# Feature weights: how strongly one feature identifies a platform. The signature
# is the strongest 3-5; the full weighted set drives the Jaccard merge.
W_DETECT = 4.0          # a template's detect() said yes
W_CHALLENGE = 3.0       # a strong challenge marker (cloudflare / incapsula / …)
W_API = 3.0             # an inventory API path pattern
W_INLINE = 2.5          # window.<platformObject> = { … }
W_HOST = 2.0            # third-party script host that is not a CDN / widget
W_GENERATOR = 2.0       # <meta name=generator>
W_HOMEPAGE_STATUS = 2.0  # non-200 homepage
W_HINT = 1.5            # vendor keyword seen in the page
W_WIDGET = 0.75         # known add-on host
W_BEACON = 1.0          # a beacon marker
W_SERVER = 1.0          # Server header
W_PATHS_AGG = 1.0       # aggregate probe outcome
W_PATH = 0.5            # one inventory path's outcome

SIGNATURE_MIN = 3
SIGNATURE_MAX = 5
SIGNATURE_PER_KIND = 2   # at most two features of one kind (two challenge markers, two hosts, ...)
SIGNATURE_MIN_WEIGHT = 1.0
JACCARD_THRESHOLD = 0.6


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _host(url: str | None) -> str:
    try:
        return (urlparse(url or "").hostname or "").lower()
    except ValueError:
        return ""


def _registrable(host: str) -> str:
    """``www.audihuntsville.com`` -> ``audihuntsville.com`` (last two labels; good
    enough for dealer sites, which are never on multi-label public suffixes)."""
    parts = [p for p in host.lower().split(".") if p]
    return ".".join(parts[-2:]) if len(parts) >= 2 else host.lower()


def _suffix_match(host: str, suffixes: Iterable[str]) -> bool:
    return any(host == s or host.endswith("." + s) for s in suffixes)


def own_hosts(rec: dict[str, Any]) -> set[str]:
    """The dealer's own registrable domains: the probed URL, the final URL after
    redirects, and every hop in the redirect chain."""
    home = rec.get("homepage") or {}
    out = set()
    for u in (rec.get("url"), home.get("url"), home.get("final_url")):
        h = _host(u)
        if h:
            out.add(_registrable(h))
    for hop in home.get("redirects") or []:
        # "301 https://www.example.com/"
        u = str(hop).split(" ", 1)[-1]
        h = _host(u)
        if h:
            out.add(_registrable(h))
    return out


def classify_host(host: str, own: set[str]) -> str:
    """``own`` | ``cdn`` | ``widget`` | ``third``."""
    h = host.lower().strip()
    if not h:
        return "cdn"
    if _registrable(h) in own:
        return "own"
    if _suffix_match(h, _CDN_HOST_SUFFIXES):
        return "cdn"
    if _suffix_match(h, _WIDGET_HOST_SUFFIXES):
        return "widget"
    return "third"


_NUM_RE = re.compile(r"\d+")
_HEX_RE = re.compile(r"\b[0-9a-f]{12,}\b", re.I)
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)


def api_pattern(url: str, own: set[str]) -> str | None:
    """An API URL reduced to its shape: host (dropped when it is the dealer's
    own) + path with ids / hashes / numbers collapsed, query dropped.
    ``https://websites-search.api.carscommerce.inc/api/v1/listings/6059599/search``
    -> ``websites-search.api.carscommerce.inc/api/v1/listings/N/search``;
    ``https://www.dealer.com/inventory-used.json`` -> ``/inventory-used.json``.
    """
    try:
        p = urlparse(url)
    except ValueError:
        return None
    host = (p.hostname or "").lower()
    path = p.path or "/"
    path = _UUID_RE.sub("ID", path)
    path = _HEX_RE.sub("ID", path)
    path = _NUM_RE.sub("N", path)
    path = re.sub(r"/+", "/", path)
    if len(path) > 80:
        path = path[:80]
    if not host or _registrable(host) in own:
        return path if path != "/" else None
    return f"{host}{path}"


def _bucket(status: Any) -> str:
    try:
        s = int(status)
    except (TypeError, ValueError):
        return "err"
    if s in _PATH_STATUS_BUCKETS:
        return _PATH_STATUS_BUCKETS[s]
    if 500 <= s < 600:
        return "5xx"
    return str(s)


def _strip_version(gen: str) -> str:
    return re.sub(r"\s*[\d.]+\s*$", "", gen.strip())


_INLINE_KEY_RE = re.compile(r"(?:window\.|var\s+)([A-Za-z_][A-Za-z0-9_.]{2,40})")


def _inline_key(entry: str) -> str | None:
    """``window.oadd.vendorIntegrations = {`` -> ``window.oadd``;
    ``var DGDataHub = {`` -> ``var DGDataHub``; ``__NEXT_DATA__`` stays."""
    e = str(entry).strip()
    if e in ("__NEXT_DATA__", "__NUXT__", "data-vehicle="):
        return e
    m = _INLINE_KEY_RE.match(e)
    if not m:
        return None
    key = m.group(1)
    root = key.split(".")[0]
    kind = "window" if e.startswith("window") else "var"
    return f"{kind}.{root}"


# --------------------------------------------------------------------------
# fingerprint
# --------------------------------------------------------------------------

@dataclass
class Fingerprint:
    dealer_id: str
    signature: str
    features: dict[str, float] = field(default_factory=dict)  # feature -> weight
    platform: str | None = None                                 # fingerprint.platform when a template detected
    nearest_template: tuple[str, float] | None = None           # highest partial detect score, if the map carries scores
    classification: str | None = None
    stamp: str | None = None

    @property
    def feature_set(self) -> frozenset[str]:
        return frozenset(self.features)

    def as_dict(self) -> dict[str, Any]:
        return {"dealer_id": self.dealer_id, "signature": self.signature, "features": dict(self.features),
                "platform": self.platform, "nearest_template": list(self.nearest_template) if self.nearest_template else None,
                "classification": self.classification, "stamp": self.stamp}


def _add(features: dict[str, float], name: str, weight: float) -> None:
    if not name:
        return
    if weight > features.get(name, 0.0):
        features[name] = weight


def extract_features(rec: dict[str, Any], capture: dict[str, Any] | None = None) -> dict[str, float]:
    """Every feature the record supports, ``{feature: weight}``. Feature names are
    prefixed by kind so the same string cannot collide across kinds:
    ``detect:``, ``challenge:``, ``beacon:``, ``api:``, ``inline:``, ``host:``,
    ``widget:``, ``generator:``, ``hint:``, ``homepage:``, ``server:``, ``paths:``,
    ``path:``.
    """
    own = own_hosts(rec)
    features: dict[str, float] = {}

    home = rec.get("homepage") or {}
    status = home.get("status")
    if home.get("error"):
        _add(features, "homepage:error", W_HOMEPAGE_STATUS)
    elif status is not None and _bucket(status) != "200":
        _add(features, f"homepage:{_bucket(status)}", W_HOMEPAGE_STATUS)
    server = str(home.get("server") or "").strip().lower()
    if server:
        _add(features, f"server:{server.split('/')[0][:30]}", W_SERVER)

    ch = rec.get("challenge") or {}
    for m in ch.get("strong_markers") or []:
        _add(features, f"challenge:{m}", W_CHALLENGE)
    for m in ch.get("beacon_markers") or []:
        _add(features, f"beacon:{m}", W_BEACON)

    detect = (rec.get("fingerprint") or {}).get("detect") or {}
    for name, val in detect.items():
        if val is True:
            _add(features, f"detect:{name}", W_DETECT)
        elif isinstance(val, (int, float)) and not isinstance(val, bool) and val >= 1:
            _add(features, f"detect:{name}", W_DETECT)

    sg = rec.get("signals") or {}
    for h in sg.get("script_hosts") or []:
        kind = classify_host(str(h), own)
        if kind == "third":
            _add(features, f"host:{str(h).lower()}", W_HOST)
        elif kind == "widget":
            _add(features, f"widget:{_registrable(str(h))}", W_WIDGET)
    for hint in (sg.get("api_hints") or {}):
        if hint not in _NOISY_HINTS:
            _add(features, f"hint:{hint}", W_HINT)
    for entry in sg.get("inline_json") or []:
        key = _inline_key(str(entry))
        if key:
            _add(features, f"inline:{key}", W_INLINE)
    gen = sg.get("generator")
    if gen:
        _add(features, f"generator:{_strip_version(str(gen)).lower()}", W_GENERATOR)

    synth = rec.get("synth") or {}
    for c in synth.get("candidates") or []:
        pat = api_pattern(str(c.get("url") or ""), own)
        if pat:
            method = str(c.get("method") or "GET").upper()
            _add(features, f"api:{method} {pat}", W_API)
        if "graphql" in str(c.get("url") or "").lower():
            _add(features, f"api:graphql@{_host(c.get('url')) or 'own'}", W_API)
    for ep in (capture or {}).get("endpoints") or []:
        pat = api_pattern(str(ep.get("url") or ""), own)
        if pat:
            method = str(ep.get("method") or "GET").upper()
            _add(features, f"api:{method} {pat}", W_API)
        if "graphql" in str(ep.get("url") or "").lower():
            _add(features, f"api:graphql@{_host(ep.get('url')) or 'own'}", W_API)

    paths = rec.get("paths") or []
    if paths:
        buckets = []
        any_vins = False
        own_origin = _host(home.get("final_url") or rec.get("url"))
        for p in paths:
            b = "err" if p.get("error") else _bucket(p.get("status"))
            buckets.append(b)
            name = urlparse(str(p.get("url") or "")).path or "/"
            final_host = _host(p.get("final_url"))
            hop = ""
            if final_host and own_origin and _registrable(final_host) != _registrable(own_origin):
                hop = f"->{final_host}"
            vins = int(p.get("vins") or 0)
            any_vins = any_vins or vins > 0
            _add(features, f"path:{name}={b}{':vins' if vins else ''}{hop}", W_PATH)
        distinct = set(buckets)
        if len(distinct) == 1:
            _add(features, f"paths:all_{buckets[0]}", W_PATHS_AGG)
        if any_vins:
            _add(features, "paths:vins_in_html", W_PATHS_AGG)
        if any(p.get("challenge") for p in paths):
            _add(features, "paths:challenge", W_PATHS_AGG)
    return features


def make_signature(features: dict[str, float]) -> str:
    """The strongest 3-5 features, sorted, joined with ``|``. Deterministic:
    ties break on the feature name; at most two features of one kind so the
    signature reads across kinds (challenge + server + inline rather than four
    cloudflare markers). Features under weight 1 join only when fewer than three
    stronger ones exist; an empty feature set signs as ``(none)``."""
    ranked = sorted(features.items(), key=lambda kv: (-kv[1], kv[0]))
    top: list[str] = []
    per_kind: dict[str, int] = {}
    for name, w in ranked:
        if w < SIGNATURE_MIN_WEIGHT or len(top) >= SIGNATURE_MAX:
            break
        kind = name.split(":", 1)[0]
        if per_kind.get(kind, 0) >= SIGNATURE_PER_KIND:
            continue  # four cloudflare markers say one thing; leave room for another kind
        per_kind[kind] = per_kind.get(kind, 0) + 1
        top.append(name)
    if len(top) < SIGNATURE_MIN:
        for name, _ in ranked:
            if name not in top:
                top.append(name)
            if len(top) >= SIGNATURE_MIN:
                break
    if not top:
        return "(none)"
    return "|".join(sorted(top))


def nearest_template(rec: dict[str, Any]) -> tuple[str, float] | None:
    """The template with the highest partial detect score (``0 < score < 1``)
    when ``fingerprint.detect`` carries numeric scores; ``None`` for the boolean
    map the probe writes today."""
    detect = (rec.get("fingerprint") or {}).get("detect") or {}
    best: tuple[str, float] | None = None
    for name, val in detect.items():
        if isinstance(val, bool) or not isinstance(val, (int, float)):
            continue
        if 0 < val < 1 and (best is None or val > best[1]):
            best = (str(name), float(val))
    return best


def fingerprint(rec: dict[str, Any], capture: dict[str, Any] | None = None) -> Fingerprint:
    """``{signature, features}`` for one discovery record (plus the optional
    browser capture record's endpoints)."""
    feats = extract_features(rec, capture)
    return Fingerprint(
        dealer_id=str(rec.get("dealer_id") or ""),
        signature=make_signature(feats),
        features=feats,
        platform=(rec.get("fingerprint") or {}).get("platform"),
        nearest_template=nearest_template(rec),
        classification=rec.get("classification"),
        stamp=rec.get("stamp"),
    )


# --------------------------------------------------------------------------
# clustering
# --------------------------------------------------------------------------

def jaccard(a: Iterable[str], b: Iterable[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 1.0
    union = sa | sb
    return len(sa & sb) / len(union) if union else 0.0


@dataclass
class Cluster:
    signature: str                       # the largest member group's signature
    members: list[Fingerprint]
    signatures: list[str]                # every signature merged into this cluster
    shared_features: list[str]           # features every member carries, strongest first

    @property
    def dealer_ids(self) -> list[str]:
        return [m.dealer_id for m in self.members]

    def as_dict(self) -> dict[str, Any]:
        return {"signature": self.signature, "signatures": list(self.signatures), "dealers": self.dealer_ids,
                "shared_features": list(self.shared_features)}


def _shared(members: list[Fingerprint]) -> list[str]:
    if not members:
        return []
    common = set(members[0].features)
    for m in members[1:]:
        common &= set(m.features)
    weight = {f: max(m.features.get(f, 0.0) for m in members) for f in common}
    return sorted(common, key=lambda f: (-weight[f], f))


def cluster(fingerprints: Iterable[Fingerprint], *, threshold: float = JACCARD_THRESHOLD) -> list[Cluster]:
    """Group by exact signature, then merge groups whose members are near-duplicates:
    two groups merge when any cross pair of members has Jaccard similarity of
    their full feature sets >= ``threshold`` (single linkage; the fleet is a few
    hundred dealers, the pair loop is cheap). Clusters come back largest first,
    then by signature; members keep their input order."""
    fps = list(fingerprints)
    n = len(fps)
    parent = list(range(n))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i: int, j: int) -> None:
        ri, rj = find(i), find(j)
        if ri != rj:
            parent[max(ri, rj)] = min(ri, rj)

    by_sig: dict[str, list[int]] = {}
    for i, fp in enumerate(fps):
        by_sig.setdefault(fp.signature, []).append(i)
    for idxs in by_sig.values():
        for j in idxs[1:]:
            union(idxs[0], j)

    sets = [fp.feature_set for fp in fps]
    for i in range(n):
        for j in range(i + 1, n):
            if find(i) == find(j):
                continue
            if not sets[i] or not sets[j]:
                continue
            if jaccard(sets[i], sets[j]) >= threshold:
                union(i, j)

    groups: dict[int, list[int]] = {}
    for i in range(n):
        groups.setdefault(find(i), []).append(i)

    out: list[Cluster] = []
    for idxs in groups.values():
        members = [fps[i] for i in idxs]
        sig_count: dict[str, int] = {}
        for m in members:
            sig_count[m.signature] = sig_count.get(m.signature, 0) + 1
        lead = sorted(sig_count.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
        out.append(Cluster(signature=lead, members=members, signatures=sorted(sig_count),
                           shared_features=_shared(members)))
    out.sort(key=lambda c: (-len(c.members), c.signature))
    return out


# --------------------------------------------------------------------------
# what to do about a cluster
# --------------------------------------------------------------------------

# A vendor keyword or host seen in the page -> the template that usually covers it.
_HINT_TO_TEMPLATE = {
    "dealerinspire": "carscommerce", "carscommerce": "carscommerce", "typesense": "typesense",
    "algolia": "typesense", "dealeron": "dealer_on_cosmos", "dealer.com": "dealer_dot_com",
    "teamvelocity": "team_velocity", "dealereprocess": "dealer_eprocess", "overfuel": "overfuel",
    "nabthat": "nabthat", "jazel": "jazel", "wp-json": "wp_vehicles_index",
}


def suggest_next_step(c: Cluster) -> str:
    """One sentence a builder can act on. Order of evidence: a template already
    detects -> its synth is the bug; a partial detect score names the nearest
    template; a challenge wall -> browser-capture one member from a clean IP; a
    vendor hint -> extend that template's detect; an API path pattern in the
    features -> write a template for it; an inventory path that answered VINs
    -> HTTP probe that path; otherwise browser-capture one member."""
    members = c.members
    shared = c.shared_features
    lead = members[0].dealer_id if members else "<dealer>"
    detects = sorted({f.split(":", 1)[1] for f in shared if f.startswith("detect:")})
    if detects:
        return (f"template `{detects[0]}` detects every member but no recipe survived: "
                f"debug its synth / validation on {lead} (discovery_probe --dealers {lead})")
    scored = [m.nearest_template for m in members if m.nearest_template]
    if scored:
        name, score = max(scored, key=lambda t: t[1])
        return f"nearest known template `{name}` (partial detect {score:.2f}): extend its detect() on {lead}, then re-synth"
    if any(f.startswith("challenge:") for f in shared) or "homepage:403" in shared or "paths:all_403" in shared:
        return (f"challenge wall on every member: browser-capture one member from a clean IP "
                f"(SCANNER_ALLOW_BROWSER=1 discovery_probe --browser-capture --dealers {lead}); "
                f"a TLS-impersonated GET may be enough first (memory: curl 403 is not an IP ban)")
    hints = [f.split(":", 1)[1] for f in shared if f.startswith("hint:")]
    for h in hints:
        if h in _HINT_TO_TEMPLATE:
            return f"page names vendor `{h}`: extend template `{_HINT_TO_TEMPLATE[h]}`'s detect()/synth to this variant on {lead}"
    apis = [f.split(":", 1)[1] for f in shared if f.startswith("api:") and "graphql" not in f]
    if apis:
        return f"write template X for `{apis[0]}` (every member replays it; start from {lead}'s candidate)"
    vin_paths = sorted(f for f in shared if f.startswith("path:") and ":vins" in f)
    if vin_paths:
        path = vin_paths[0].split(":", 1)[1].split("=", 1)[0]
        return f"HTTP probe path `{path}` (answers VINs on every member): html_cards / JSON-LD parser, no browser"
    hosts = [f.split(":", 1)[1] for f in shared if f.startswith("host:")]
    if hosts:
        return f"shared script host `{hosts[0]}` names the platform: browser-capture one member ({lead}) to learn its endpoint, then a template"
    return f"browser-capture one member ({lead}) to learn the endpoint, then write the template"
