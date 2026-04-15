"""
Wappalyzer-style site profiling for crawl routing, stack clustering, and weak ownership hints.

Signals are heuristics on HTML + response headers — supporting evidence only, not ground truth.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from SCRAPING.constants import COPYRIGHT_RE

logger = logging.getLogger("SCRAPING.site_profile")

ROOT = Path(__file__).resolve().parent.parent
_STACK_FAMILIES_PATH = ROOT / "data" / "site_stack_families.json"

# --- Normalization: hosts that should not be treated as canonical dealer roots ---
_CHECKOUT_SUBSTRINGS = (
    "stripe.com",
    "checkout.com",
    "paypal.com",
    "braintree",
    "adyen.com",
    "squareup.com",
    "shop.app",
    "myshopify.com",
    "fastspring.com",
    "recurly.com",
)
_MAPS_SUBSTRINGS = (
    "maps.google.com",
    "maps.apple.com",
    "google.com/maps",
    "goo.gl/maps",
)
_VENDOR_ONLY_HINTS = (
    "doubleclick.net",
    "googletagmanager.com",
    "facebook.net",
    "newrelic.com",
)

# (needles in lowered HTML blob, label)
_TECH_RULES: list[tuple[tuple[str, ...], str]] = [
    (("__next_data__", "next/static", "/_next/"), "Next.js"),
    (("__nuxt__", "/_nuxt/"), "Nuxt"),
    (("react-dom", "react.production", "jsx-runtime"), "React"),
    (("vue.js", "vue.min.js", "/vue@", "/vue/"), "Vue.js"),
    (("angular.js", "angular.min.js", "@angular/"), "Angular"),
    (("/svelte", "svelte.js"), "Svelte"),
    (("wp-content", "wordpress", "wp-includes"), "WordPress"),
    (("squarespace", "static1.squarespace"), "Squarespace"),
    (("dealer.com", "dealercom", "digitec", "ignite.dealer", "/ddc/", "ddc.jquery"), "Dealer.com"),
    (("dealeron", "dealer-on", "static.dealeron"), "DealerOn"),
    (("dealerinspire", "dealer inspire"), "Dealer Inspire"),
    (("cdn.jsdelivr.net", "unpkg.com", "cdnjs.cloudflare"), "Public CDN"),
    (("cloudflare", "cf-ray"), "Cloudflare"),
    (("bootstrap", "getbootstrap"), "Bootstrap"),
    (("tailwind", "tailwindcss"), "Tailwind CSS"),
    (("googletagmanager", "_tag.gtm"), "Google Tag Manager"),
    (("google-analytics", "analytics.js", "gtag(", "googletagservices"), "Google Analytics"),
    (("connect.facebook.net", "fbevents.js", "facebook.net/en_us/fbevents"), "Facebook Pixel"),
    (("hotjar.com", "static.hotjar"), "Hotjar"),
    (("heap-analytics", "heapanalytics"), "Heap"),
    (("segment.com", "cdn.segment"), "Segment"),
    (("hubspot", "hs-scripts"), "HubSpot"),
    (("pardot", "salesforce.com"), "Salesforce"),
    (("maps.googleapis.com", "google.com/maps/embed"), "Google Maps"),
    (("mapbox", "api.mapbox"), "Mapbox"),
    (("jquery", "jquery.min.js"), "jQuery"),
]

_SERVER_LABELS: tuple[tuple[str, str], ...] = (
    ("nginx", "nginx"),
    ("apache", "Apache"),
    ("cloudflare", "Cloudflare"),
    ("microsoft-iis", "IIS"),
    ("envoy", "Envoy"),
    ("akamaighost", "Akamai"),
    ("gfe", "Google Frontend"),
)

_INDUSTRY_KEYS = (
    ("automotive", "car dealer", "dealership", "vehicle", "inventory"),
    "Automotive retail",
)


def _load_fingerprint_labels() -> dict[str, str]:
    try:
        raw = _STACK_FAMILIES_PATH.read_text(encoding="utf-8")
        data = json.loads(raw)
        fp = data.get("fingerprints") or {}
        return {str(k): str(v) for k, v in fp.items() if k and v}
    except (OSError, json.JSONDecodeError, TypeError) as e:
        logger.debug("No stack family map: %s", e)
        return {}


_FINGERPRINT_LABELS = _load_fingerprint_labels()


def canonical_url_warning(final_url: str, original_url: str | None = None) -> str:
    """Non-empty string if the final URL is unlikely to be a dealer's canonical website."""
    try:
        p = urlparse(final_url)
        net = (p.netloc or "").lower()
        path = (p.path or "").lower()
    except Exception:
        return ""
    if not net:
        return "missing host"
    for sub in _CHECKOUT_SUBSTRINGS:
        if sub in net:
            return f"host matches payment/checkout pattern ({sub})"
    for sub in _MAPS_SUBSTRINGS:
        if sub in net or sub in f"{net}{path}":
            return "host/path looks like maps or directions, not a dealer homepage"
    if net.count(".") <= 1 and net.split(".")[0] in ("js", "cdn", "static", "assets", "img"):
        return "host looks like a static asset subdomain only"
    for v in _VENDOR_ONLY_HINTS:
        if net == v or net.endswith("." + v):
            return f"host is a third-party vendor domain ({v})"
    if original_url:
        o = urlparse(original_url)
        if o.netloc and p.netloc and o.netloc.lower() != net:
            # redirect already tracked elsewhere; light hint only
            pass
    return ""


def _server_from_headers(headers: dict[str, str] | None) -> list[str]:
    if not headers:
        return []
    srv = ""
    for k, v in headers.items():
        if k.lower() == "server":
            srv = (v or "").lower()
            break
    if not srv:
        return []
    out: list[str] = []
    for needle, label in _SERVER_LABELS:
        if needle in srv:
            out.append(label)
    if not out and srv:
        out.append(srv[:80])
    via = headers.get("Via") or headers.get("via") or ""
    if "cloudflare" in via.lower() and "Cloudflare" not in out:
        out.append("Cloudflare (Via)")
    return list(dict.fromkeys(out))


def _collect_script_src_blob(soup: BeautifulSoup) -> str:
    parts: list[str] = []
    for s in soup.find_all("script", src=True):
        parts.append(s.get("src") or "")
    for s in soup.find_all("link", href=True):
        rel = s.get("rel")
        if rel and "stylesheet" in [x.lower() for x in rel if isinstance(x, str)]:
            parts.append(s.get("href") or "")
    return " ".join(parts).lower()


def _detect_tech(html_lower: str, script_blob: str) -> dict[str, list[str]]:
    combined = f"{html_lower}\n{script_blob}"
    buckets: dict[str, list[str]] = {
        "js_frameworks": [],
        "cdn": [],
        "tag_managers": [],
        "analytics": [],
        "crm_cdp": [],
        "maps": [],
        "ui_frameworks": [],
        "ecommerce_platform": [],
        "other": [],
    }
    seen: set[str] = set()

    def add(bucket: str, label: str) -> None:
        if label in seen:
            return
        seen.add(label)
        if bucket not in buckets:
            bucket = "other"
        buckets[bucket].append(label)

    for needles, label in _TECH_RULES:
        if any(n in combined for n in needles):
            low = label.lower()
            if any(x in low for x in ("next", "nuxt", "react", "vue", "angular", "svelte")):
                add("js_frameworks", label)
            elif "cdn" in low or "cloudflare" in low or "jsdelivr" in combined:
                add("cdn", label)
            elif "tag manager" in low:
                add("tag_managers", label)
            elif "analytics" in low or "pixel" in low or "hotjar" in low or "heap" in low:
                add("analytics", label)
            elif any(x in low for x in ("hubspot", "salesforce", "segment")):
                add("crm_cdp", label)
            elif "maps" in low or "mapbox" in low:
                add("maps", label)
            elif "bootstrap" in low or "tailwind" in low:
                add("ui_frameworks", label)
            elif any(x in low for x in ("wordpress", "squarespace", "dealer.com", "dealeron")):
                add("ecommerce_platform", label)
            else:
                add("other", label)

    return buckets


def _iter_ld_objects(data: Any) -> list[dict]:
    if isinstance(data, dict):
        if "@graph" in data and isinstance(data["@graph"], list):
            return [x for x in data["@graph"] if isinstance(x, dict)]
        return [data]
    if isinstance(data, list):
        out: list[dict] = []
        for x in data:
            out.extend(_iter_ld_objects(x))
        return out
    return []


def _type_names(obj: dict) -> list[str]:
    t = obj.get("@type")
    if t is None:
        return []
    if isinstance(t, list):
        return [str(x) for x in t]
    return [str(t)]


def _extract_schema_and_org(soup: BeautifulSoup) -> tuple[list[str], str, str, list[str], list[str]]:
    types: list[str] = []
    company = ""
    about = ""
    same_as: list[str] = []
    phones: list[str] = []
    for script in soup.find_all("script", attrs={"type": True}):
        if "ld+json" not in (script.get("type") or "").lower():
            continue
        raw = script.string or script.get_text() or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for obj in _iter_ld_objects(data):
            if not isinstance(obj, dict):
                continue
            types.extend(_type_names(obj))
            tn = " ".join(_type_names(obj)).lower()
            if any(
                x in tn
                for x in (
                    "organization",
                    "automotive",
                    "localbusiness",
                    "store",
                    "autodealer",
                )
            ):
                n = obj.get("name")
                if isinstance(n, str) and n.strip() and len(n.strip()) > 1:
                    company = n.strip()[:200]
                desc = obj.get("description")
                if isinstance(desc, str) and desc.strip():
                    about = desc.strip()[:1200]
                sa = obj.get("sameAs")
                if isinstance(sa, str):
                    same_as.append(sa)
                elif isinstance(sa, list):
                    same_as.extend(str(x) for x in sa if x)
                tel = obj.get("telephone") or obj.get("phone")
                if isinstance(tel, str) and tel.strip():
                    phones.append(tel.strip()[:40])
                elif isinstance(tel, list):
                    phones.extend(str(t)[:40] for t in tel if t)
    return types, company, about, same_as, phones


def _meta(soup: BeautifulSoup, prop: str | None = None, name: str | None = None) -> str | None:
    if prop:
        t = soup.find("meta", attrs={"property": prop})
        if t and t.get("content"):
            return str(t["content"]).strip()
    if name:
        t = soup.find("meta", attrs={"name": name})
        if t and t.get("content"):
            return str(t["content"]).strip()
    return None


def _visible_text_snippet(soup: BeautifulSoup, max_len: int = 800) -> str:
    for sel in ("main", "article", '[role="main"]'):
        el = soup.select_one(sel)
        if el:
            t = el.get_text(separator=" ", strip=True)
            if len(t) > 80:
                return re.sub(r"\s+", " ", t)[:max_len]
    body = soup.find("body")
    if body:
        t = body.get_text(separator=" ", strip=True)
        return re.sub(r"\s+", " ", t)[:max_len]
    return ""


def _extract_emails(html: str) -> list[str]:
    return list(
        dict.fromkeys(
            re.findall(
                r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+",
                html[:500_000],
            )
        )
    )[:8]


def _infer_industry(text_sample: str) -> str:
    low = text_sample.lower()[:4000]
    keys, label = _INDUSTRY_KEYS
    if any(k in low for k in keys):
        return label
    return ""


def _copyright_hint(html: str) -> str:
    m = COPYRIGHT_RE.search(html[:800_000])
    if not m:
        return ""
    return (m.group(1) or "").strip()[:400]


def _likely_vendor(tech: dict[str, list[str]], html_lower: str) -> str:
    # Dealer.com DDC (Digital Dealer Channel) — strong template signal
    if "/ddc/" in html_lower or "ddc.jquery" in html_lower:
        return "dealer_dot_com"
    flat = " ".join(sum(tech.values(), [])).lower()
    if "dealer.com" in html_lower or "dealer.com" in flat:
        return "dealer_dot_com"
    if "dealeron" in html_lower or "dealeron" in flat:
        return "dealer_on"
    if "dealer inspire" in html_lower or "dealerinspire" in html_lower:
        return "dealer_inspire"
    return ""


def _stack_fingerprint(tech: dict[str, list[str]], likely_vendor: str) -> str:
    tags: list[str] = []
    for bucket in sorted(tech.keys()):
        for x in sorted(tech[bucket]):
            tags.append(f"{bucket}:{x}")
    if likely_vendor:
        tags.append(f"vendor:{likely_vendor}")
    raw = "|".join(tags)
    if not raw:
        raw = "empty"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def _site_stack_family(fp: str, likely_vendor: str, tech: dict[str, list[str]]) -> str:
    if fp in _FINGERPRINT_LABELS:
        return _FINGERPRINT_LABELS[fp]
    parts: list[str] = []
    if likely_vendor:
        parts.append(likely_vendor)
    for fw in tech.get("js_frameworks") or []:
        parts.append(fw.lower().replace(".", "").replace(" ", "_"))
    if not parts:
        return "unknown_stack"
    return "_".join(parts[:4])[:80]


def _heavy_js_score(html: str, soup: BeautifulSoup, html_lower: str, tech: dict[str, list[str]]) -> tuple[bool, int]:
    score = 0
    n_scripts = len(soup.find_all("script"))
    if n_scripts > 38:
        score += 2
    elif n_scripts > 22:
        score += 1
    if any(
        x in html_lower
        for x in (
            "__next_data__",
            "__nuxt__",
            "__preloaded_state__",
            "webpackchunk",
            "chunk-vendors",
        )
    ):
        score += 3
    if tech.get("js_frameworks"):
        score += 2
    body_txt = _visible_text_snippet(soup, 2000)
    if len(body_txt) < 420 and n_scripts > 8:
        score += 2
    return score >= 4, score


def recommend_crawl_strategy(
    heavy_js: bool,
    *,
    requests_fetch_failed: bool = False,
    http_403_on_requests: bool = False,
) -> str:
    """Advisory strategy label before crawl (not the executed mode)."""
    if requests_fetch_failed or http_403_on_requests:
        return "playwright_first"
    if heavy_js:
        return "playwright_first"
    return "requests_first"


def build_site_profile(
    html: str,
    final_url: str,
    response_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    Build a JSON-serializable profile (tech + company + routing hints).

    Wappalyzer-style categories are approximate substring matches, not product guarantees.
    """
    warn = canonical_url_warning(final_url)
    soup = BeautifulSoup(html or "", "html.parser")
    html_lower = (html or "").lower()
    script_blob = _collect_script_src_blob(soup)
    tech = _detect_tech(html_lower, script_blob)
    web_servers = _server_from_headers(response_headers)

    schema_types, ld_company, ld_about, same_as, ld_phones = _extract_schema_and_org(soup)
    og_site = _meta(soup, prop="og:site_name")
    og_title = _meta(soup, prop="og:title")
    meta_desc = _meta(soup, name="description") or _meta(soup, prop="og:description")
    app_name = _meta(soup, name="application-name")

    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    company_name = ld_company or og_site or app_name or ""
    inferred_company = og_site or title or ""
    if company_name and inferred_company and company_name.lower() != inferred_company.lower():
        pass
    elif not company_name:
        company_name = inferred_company

    about_text = ld_about or (meta_desc or "")[:1200]
    if len(about_text) < 40:
        about_text = _visible_text_snippet(soup, 500)

    industry = _infer_industry(f"{meta_desc or ''} {about_text} {' '.join(schema_types)}")

    emails = _extract_emails(html)
    copyright_text = _copyright_hint(html)

    social = [u for u in same_as if u.startswith("http")][:12]
    for a in soup.find_all("a", href=True)[:400]:
        h = (a.get("href") or "").lower()
        if any(
            d in h
            for d in (
                "facebook.com/",
                "instagram.com/",
                "twitter.com/",
                "x.com/",
                "linkedin.com/",
                "youtube.com/",
                "tiktok.com/",
            )
        ):
            social.append(a["href"][:300])
    social = list(dict.fromkeys(social))[:12]

    likely_vendor = _likely_vendor(tech, html_lower)
    fp = _stack_fingerprint(tech, likely_vendor)
    family = _site_stack_family(fp, likely_vendor, tech)
    heavy_js, js_score = _heavy_js_score(html, soup, html_lower, tech)

    profile: dict[str, Any] = {
        "tech": {
            **tech,
            "web_servers": web_servers,
        },
        "company": {
            "company_name": company_name[:200],
            "inferred_company_name": (inferred_company or company_name)[:200],
            "industry": industry[:120],
            "about_text": about_text[:1200],
            "locations": [],  # reserved; geo extraction is fragile without structured data
            "people": [],
            "phones": ld_phones[:6],
            "emails": emails[:6],
            "social_urls": social,
            "copyright_text": copyright_text[:500],
            "schema_org_types": list(dict.fromkeys(schema_types))[:30],
        },
        "stack_fingerprint": fp,
        "site_stack_family": family,
        "likely_vendor": likely_vendor,
        "heavy_js": heavy_js,
        "heavy_js_score": js_score,
        "recommended_crawl_strategy": recommend_crawl_strategy(
            heavy_js,
            requests_fetch_failed=False,
            http_403_on_requests=False,
        ),
        "canonical_site_warning": warn,
    }
    return profile


def build_site_profile_for_failed_peek(
    final_url: str,
    *,
    error: str | None,
    flags: list[str],
) -> dict[str, Any]:
    """Minimal profile when homepage HTML is unavailable (routing / logging)."""
    http_403 = "http_403" in flags or "403" in (error or "")
    strat = recommend_crawl_strategy(
        False,
        requests_fetch_failed=bool(error),
        http_403_on_requests=http_403,
    )
    return {
        "tech": {},
        "company": {
            "company_name": "",
            "inferred_company_name": "",
            "industry": "",
            "about_text": "",
            "locations": [],
            "people": [],
            "phones": [],
            "emails": [],
            "social_urls": [],
            "copyright_text": "",
            "schema_org_types": [],
        },
        "stack_fingerprint": "",
        "site_stack_family": "unknown_stack",
        "likely_vendor": "",
        "heavy_js": False,
        "heavy_js_score": 0,
        "recommended_crawl_strategy": strat,
        "canonical_site_warning": canonical_url_warning(final_url),
        "peek_error": (error or "")[:500],
        "peek_flags": list(flags),
    }


def apply_profile_to_site_result(sr: Any, profile: dict[str, Any], *, crawl_strategy_executed: str) -> None:
    """Copy profile summaries onto SiteResult for JSON/CSV and evidence packages."""
    co = profile.get("company") or {}
    sr.site_profile = profile
    sr.site_stack_family = profile.get("site_stack_family") or ""
    sr.crawl_strategy = crawl_strategy_executed
    sr.likely_vendor = profile.get("likely_vendor") or ""
    sr.heavy_js = bool(profile.get("heavy_js"))
    sr.ownership_hint_company_name = (co.get("company_name") or co.get("inferred_company_name") or "")[:300]
    sr.ownership_hint_about_text = (co.get("about_text") or "")[:1500]
    sr.ownership_hint_copyright = (co.get("copyright_text") or "")[:600]
    sr.canonical_site_warning = profile.get("canonical_site_warning") or ""
    _nc = "site_profile:non_canonical_url_hint"
    if sr.canonical_site_warning and _nc not in sr.flags:
        sr.flags.append(_nc)


def apply_site_profile_from_html(
    sr: Any,
    html: str,
    final_url: str,
    response_headers: dict[str, str] | None,
    *,
    crawl_strategy_executed: str,
) -> None:
    if not html or len(html) < 50:
        prof = build_site_profile_for_failed_peek(
            final_url, error="html_too_small", flags=list(sr.flags)
        )
        apply_profile_to_site_result(sr, prof, crawl_strategy_executed=crawl_strategy_executed)
        return
    prof = build_site_profile(html, final_url, response_headers)
    apply_profile_to_site_result(sr, prof, crawl_strategy_executed=crawl_strategy_executed)
