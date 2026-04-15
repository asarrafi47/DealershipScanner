"""
Infer dealership manifest fields from a public website URL (homepage fetch + HTML heuristics).

Used by the developer console; not a guarantee of correctness — always review before saving.
"""
from __future__ import annotations

import ipaddress
import json
import logging
import re
import socket
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from backend.dev_dealers import DEALER_ID_RE
from SCRAPING.text_utils import collapse_ws

logger = logging.getLogger(__name__)

# Strong platform signals in raw HTML (lowercased blob)
_DOT_COM_MARKERS = (
    "dealer.com/",
    "dealer.com\\",
    "//dealer.com",
    ".dealer.com",
    "getinventory",
    "ignite.dealer",
    "trackingpricing",
    "digitec",
    "dealereprocess",
    "cdn.dealer",
    # Dealer.com DDC (Digital Dealer Channel) assets — common on modern templates
    "/ddc/",
    "ddc.jquery",
    "ddc.jquery.async",
)
_DEALER_ON_MARKERS = (
    "dealeron.com",
    "dealeron.net",
    "//dealeron",
    "static.dealeron",
    "api.dealeron",
    "__preloaded_state__",
    "window.inventorydata",
)

# JSON-in-page keys that often hold the Dealer.com / site slug (first match wins)
_DEALER_ID_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r'"dealerId"\s*:\s*"([a-zA-Z0-9_-]{2,120})"', re.I), "dealerId"),
    (re.compile(r'"dealer_id"\s*:\s*"([a-zA-Z0-9_-]{2,120})"', re.I), "dealer_id"),
    (re.compile(r'"dealerKey"\s*:\s*"([a-zA-Z0-9_-]{2,120})"', re.I), "dealerKey"),
    (re.compile(r'"dealerCode"\s*:\s*"([a-zA-Z0-9_-]{2,120})"', re.I), "dealerCode"),
    (re.compile(r'"siteId"\s*:\s*"([a-zA-Z0-9_-]{2,120})"', re.I), "siteId"),
    (re.compile(r"/dealership/([a-z0-9][a-z0-9-]{1,118})/", re.I), "path"),
)

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


def strip_url_query_and_fragment(raw: str) -> str:
    """
    Remove #fragment and ?query from a URL string without running them through
    urlparse first. Marketing params (utm_*, gclid) and malformed queries
    (e.g. unencoded [ ] in utm_campaign) can break strict parsers or requests;
    the dealer site root is enough for inference.
    """
    s = (raw or "").strip()
    if not s:
        return s
    if "#" in s:
        s = s.split("#", 1)[0].rstrip()
    if "?" in s:
        s = s.split("?", 1)[0].rstrip()
    return s


def normalize_root_url(url: str) -> str:
    u = strip_url_query_and_fragment(url or "")
    if not u:
        raise ValueError("URL is empty")
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    parsed = urlparse(u)
    if parsed.scheme not in ("http", "https"):
        raise ValueError("Only http and https URLs are allowed")
    if not parsed.netloc:
        raise ValueError("Invalid URL (missing host)")
    # Homepage root only (no path) for manifest consistency
    root = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    return root


def _host_blocked(hostname: str) -> bool | str:
    """Return False if allowed, or a short reason string if blocked."""
    h = hostname.strip().lower().rstrip(".")
    if h == "localhost" or h.endswith(".localhost"):
        return "localhost targets are not allowed"
    try:
        infos = socket.getaddrinfo(h, None, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return False
    for fam, _, _, _, sockaddr in infos:
        if fam not in (socket.AF_INET, socket.AF_INET6):
            continue
        ip_str = sockaddr[0]
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            continue
        if ip.is_loopback or ip.is_private or ip.is_link_local:
            return "That host resolves to a private or loopback address"
        if ip.is_multicast or ip.is_reserved:
            return "That host resolves to a non-public address"
    return False


def _is_strong_dealer_dot_com_ddc(html: str) -> bool:
    """
    Dealer.com DDC signals in raw page source (paths, script names, JSON-LD).
    When matched, treat provider as dealer_dot_com with high confidence (no weak default).
    """
    if not html:
        return False
    low = html.lower()
    if "/ddc/" in low or "/ddc?" in low:
        return True
    if "ddc.jquery" in low:
        return True
    if "ddc.jquery.async" in low:
        return True
    # DDC + schema.org often co-occur in DDC JSON-LD blocks
    if "schema.org" in low and "ddc" in low:
        return True
    if "ld+json" in low and "ddc" in low:
        return True
    # Uppercase DDC in markup/scripts (e.g. DDC Schema.org templates)
    if "DDC" in html and ("schema.org" in low or "ld+json" in low):
        return True
    return False


def _score_provider(html_lower: str) -> tuple[int, int]:
    com_score = sum(1 for m in _DOT_COM_MARKERS if m in html_lower)
    on_score = sum(1 for m in _DEALER_ON_MARKERS if m in html_lower)
    return com_score, on_score


def _pick_provider(com_score: int, on_score: int) -> tuple[str, list[str]]:
    hints: list[str] = []
    if on_score > com_score:
        hints.append("Detected DealerOn-style assets or scripts (dealer_on).")
        return "dealer_on", hints
    if com_score > 0:
        hints.append("Detected Dealer.com-style assets or API hints (dealer_dot_com).")
        return "dealer_dot_com", hints
    hints.append(
        "Could not confidently detect platform; defaulting to dealer_dot_com — change if inventory fails."
    )
    return "dealer_dot_com", hints


def _normalize_candidate_id(raw: str) -> str | None:
    s = raw.strip().lower().replace("_", "-")
    if DEALER_ID_RE.match(s):
        return s
    return None


def _extract_dealer_id_from_html(html: str) -> tuple[str | None, str | None]:
    for rx, label in _DEALER_ID_PATTERNS:
        m = rx.search(html)
        if not m:
            continue
        cand = _normalize_candidate_id(m.group(1))
        if cand:
            return cand, label
    return None, None


def slug_from_hostname(hostname: str) -> str:
    """Fallback dealer_id from registrable-ish hostname (best-effort, not marketing names)."""
    host = hostname.lower().split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    parts = host.split(".")
    if len(parts) >= 3 and parts[-2] == "co" and parts[-1] == "uk":
        core = ".".join(parts[:-2])
    elif len(parts) >= 2:
        core = ".".join(parts[:-1])
    else:
        core = host
    slug = re.sub(r"[^a-z0-9]+", "-", core.replace(".", "-"))
    slug = re.sub(r"-+", "-", slug).strip("-")
    if not slug:
        slug = "dealer"
    if not DEALER_ID_RE.match(slug):
        slug = re.sub(r"^[^a-z0-9]+", "", slug) or "dealer"
    if not DEALER_ID_RE.match(slug):
        slug = "dealer"
    return slug


def _clean_title(title: str) -> str:
    t = collapse_ws(title)
    for sep in (" | ", " - ", " – ", " — "):
        if sep in t:
            t = t.split(sep)[0].strip()
    return t[:160].strip() or "Dealership"


def _meta_content(soup: BeautifulSoup, *, prop: str | None = None, name: str | None = None) -> str | None:
    if prop:
        tag = soup.find("meta", attrs={"property": prop})
        if tag and tag.get("content"):
            return collapse_ws(tag["content"])
    if name:
        tag = soup.find("meta", attrs={"name": name})
        if tag and tag.get("content"):
            return collapse_ws(tag["content"])
    return None


def _iter_ld_objects(data) -> list[dict]:
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


def _is_dealer_schema(types: list[str]) -> bool:
    joined = " ".join(types).lower()
    return any(
        x in joined
        for x in (
            "autodealer",
            "automotivebusiness",
            "cardealer",
            "localbusiness",
            "automobiledealer",
        )
    )


def _extract_ld_dealer_name_brand(soup: BeautifulSoup) -> tuple[str | None, str | None]:
    name_b: str | None = None
    brand_b: str | None = None
    for script in soup.find_all("script", attrs={"type": True}):
        t = (script.get("type") or "").lower()
        if "ld+json" not in t:
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
            if not _is_dealer_schema(_type_names(obj)):
                continue
            n = obj.get("name")
            if isinstance(n, str) and n.strip():
                name_b = collapse_ws(n)
            b = obj.get("brand")
            if isinstance(b, dict):
                bn = b.get("name")
                if isinstance(bn, str) and bn.strip():
                    brand_b = collapse_ws(bn)
            elif isinstance(b, str) and b.strip():
                brand_b = collapse_ws(b)
            if name_b:
                break
        if name_b:
            break
    return name_b, brand_b


def extract_from_html(final_url: str, html: str) -> dict:
    """Return {name, url, provider, dealer_id, brand?} plus side-channel hints list."""
    hints: list[str] = []
    parsed = urlparse(final_url)
    host = parsed.netloc
    html_lower = html.lower()

    if _is_strong_dealer_dot_com_ddc(html):
        provider = "dealer_dot_com"
        hints.append(
            "Provider dealer_dot_com (strong): Dealer.com DDC markers in page source "
            "(/ddc/, ddc.jquery, or DDC + schema.org / JSON-LD)."
        )
    else:
        com_score, on_score = _score_provider(html_lower)
        provider, ph = _pick_provider(com_score, on_score)
        hints.extend(ph)

    soup = BeautifulSoup(html, "html.parser")

    name = _meta_content(soup, prop="og:site_name")
    if name:
        hints.append("Name from og:site_name.")
    if not name:
        name = _meta_content(soup, name="application-name")
        if name:
            hints.append("Name from application-name meta.")
    if not name:
        og_title = _meta_content(soup, prop="og:title")
        if og_title:
            name = _clean_title(og_title)
            hints.append("Name from og:title (cleaned).")
    if not name and soup.title and soup.title.string:
        name = _clean_title(soup.title.string)
        hints.append("Name from <title> (cleaned).")
    if not name:
        name = "Dealership"
        hints.append("Could not find a site name; using placeholder — edit before saving.")

    ld_name, ld_brand = _extract_ld_dealer_name_brand(soup)
    if ld_name and len(ld_name) >= 3:
        name = ld_name
        hints.append("Name from JSON-LD (AutoDealer / similar).")
    brand: str | None = ld_brand
    if brand:
        hints.append("Brand from JSON-LD.")

    dealer_id, src = _extract_dealer_id_from_html(html)
    if dealer_id:
        hints.append(f"Dealer ID inferred from page ({src}).")
    else:
        dealer_id = slug_from_hostname(host)
        hints.append(f"Dealer ID derived from hostname ({host}) — verify against your Dealer.com / DealerOn config.")

    root = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    out: dict = {
        "name": name,
        "url": root,
        "provider": provider,
        "dealer_id": dealer_id,
    }
    if brand:
        out["brand"] = brand
    return {"dealer": out, "hints": hints}


_MAX_PASTED_HTML = 3_000_000  # bytes (UTF-8)


def infer_dealer_from_url(
    url: str,
    *,
    html: str | None = None,
    timeout: float = 20.0,
) -> dict:
    """
    Infer manifest fields by fetching the homepage, or from pasted HTML (same heuristics).

    When dealer CDNs return 403 to server-side HTTP clients, paste "View Page Source" HTML
    from your browser and pass it as ``html`` alongside the site URL.

    Returns {"ok": True, "dealer": {...}, "hints": [...], "final_url": "..."}
    or {"ok": False, "error": "..."}.
    """
    raw_input = (url or "").strip()
    had_query_or_fragment = "?" in raw_input or "#" in raw_input
    try:
        root = normalize_root_url(url)
    except ValueError as e:
        return {"ok": False, "error": str(e)}

    parsed = urlparse(root)
    blocked = _host_blocked(parsed.hostname or "")
    if blocked:
        return {"ok": False, "error": blocked}

    if html is not None:
        raw = html.strip()
        if not raw:
            return {"ok": False, "error": "Pasted HTML is empty"}
        if len(raw.encode("utf-8")) > _MAX_PASTED_HTML:
            return {"ok": False, "error": "Pasted HTML is too large (max ~3MB)"}
        extracted = extract_from_html(root, raw)
        extracted["ok"] = True
        extracted["final_url"] = root
        hints_out = extracted.setdefault("hints", [])
        if had_query_or_fragment:
            hints_out.insert(
                0,
                "Removed query string and fragment from the URL (e.g. utm_*, gclid, agency tags); "
                "using the dealer root for the saved link.",
            )
        hints_out.insert(
            0, "Parsed pasted HTML (no network fetch) — confirm URL matches this page's dealer."
        )
        return extracted

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": _USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"macOS"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        }
    )
    try:
        r = session.get(root, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        ct = (r.headers.get("content-type") or "").lower()
        if "html" not in ct and "text/" not in ct:
            return {"ok": False, "error": f"Expected HTML; got Content-Type {ct!r}"}
        try:
            final = normalize_root_url(r.url)
        except ValueError:
            return {"ok": False, "error": "Unexpected redirect target"}
        if not final.startswith(("http://", "https://")):
            return {"ok": False, "error": "Unexpected redirect target"}
        fp = urlparse(final)
        fb = _host_blocked(fp.hostname or "")
        if fb:
            return {"ok": False, "error": fb}
        text = r.text or ""
        if len(text) < 200:
            return {"ok": False, "error": "Homepage body too small to analyze"}
        extracted = extract_from_html(final, text)
        extracted["ok"] = True
        extracted["final_url"] = final
        hints_out = extracted.setdefault("hints", [])
        if had_query_or_fragment:
            hints_out.insert(
                0,
                "Removed query string and fragment from the URL before fetch (e.g. utm_*, gclid); "
                "those parameters are not needed and can break URL parsing.",
            )
        return extracted
    except requests.exceptions.Timeout:
        return {"ok": False, "error": "Request timed out — try again or enter fields manually"}
    except requests.exceptions.RequestException as e:
        logger.info("infer_dealer_from_url fetch failed: %s", e)
        msg = f"Could not fetch URL: {e}"
        resp = getattr(e, "response", None)
        if resp is not None and resp.status_code == 403:
            msg = (
                "403 Forbidden — this host often blocks server-side requests (e.g. Akamai). "
                "Paste the page HTML from your browser (View Page Source) into the optional field and try again."
            )
        return {"ok": False, "error": msg}
