"""BeautifulSoup helpers: footer text, evidence-only internal links, page blobs."""
from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from SCRAPING.text_utils import collapse_ws

# Path or anchor must suggest corporate / policy / ownership evidence
_EVIDENCE_PATH_MARKERS = (
    "about",
    "about-us",
    "about_us",
    "privacy",
    "terms",
    "legal",
    "careers",
    "employment",
    "jobs",
    "company",
    "our-company",
    "group",
    "ownership",
    "staff",
    "team",
    "collision",
    "holdings",
    "corporate",
)

# Strong exclude: product, finance, service marketing
_EXCLUDE_PATH_MARKERS = (
    "/inventory",
    "/vehicle",
    "/vdp",
    "/cars/",
    "/used",
    "/new",
    "/certified",
    "/specials",
    "/finance",
    "/service",
    "/parts",
    "/schedule",
    "/payment",
    "/calculator",
    "/f150",
    "/f-150",
    "/lightning",
    "/mach-e",
    "/ev-",
    "/ev/",
    "/truck",
    "/suv",
    "/sedan",
    "/offers",
    "/coupon",
)


def extract_footer_html(soup: BeautifulSoup) -> str | None:
    ft = soup.find("footer")
    if not ft:
        ft = soup.find(id=re.compile("footer", re.I))
    if not ft:
        ft = soup.find(class_=re.compile(r"footer|site-footer|page-footer", re.I))
    if ft:
        return ft.get_text(separator=" ")
    return None


def _path_suggests_evidence(path: str, href_l: str, label_l: str) -> bool:
    p = (path or "").lower()
    blob = f"{p} {href_l} {label_l}"
    if any(x in p for x in _EXCLUDE_PATH_MARKERS):
        return False
    if any(x in blob for x in _EVIDENCE_PATH_MARKERS):
        return True
    return False


# Third-party destinations we never treat as ownership evidence
_EXTERNAL_LINK_BLOCKLIST = (
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "tiktok.com",
    "youtube.com",
    "linkedin.com",
    "pinterest.com",
    "google.com/maps",
    "maps.google.com",
    "goo.gl",
    "bit.ly",
    "doubleclick",
    "googletagmanager",
    "schema.org",
)


def collect_cross_domain_evidence_links(
    base_url: str,
    soup: BeautifulSoup,
    max_links: int = 6,
) -> tuple[list[str], list[dict[str, str]]]:
    """
    External links (different registrable host) that look like policy / corporate / group evidence.
    Returns (urls_to_fetch, skipped with reasons).
    """
    base = urlparse(base_url)
    seen: set[str] = set()
    out: list[str] = []
    skipped: list[dict[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        label = collapse_ws(a.get_text() or "")
        label_l = label.lower()
        href_l = href.lower()
        abs_u = urljoin(base_url, href)
        p = urlparse(abs_u)
        if p.scheme not in ("http", "https"):
            continue
        host_l = (p.netloc or "").lower()
        if host_l == base.netloc.lower():
            continue
        if any(b in host_l or b in href_l for b in _EXTERNAL_LINK_BLOCKLIST):
            skipped.append({"url": abs_u, "reason": "blocklisted_host"})
            continue
        path_lower = (p.path or "").lower()
        blob = f"{path_lower} {href_l} {label_l}"
        if not _path_suggests_evidence(path_lower, href_l, label_l):
            skipped.append({"url": abs_u, "reason": "path_not_evidence"})
            continue
        if any(x in path_lower for x in _EXCLUDE_PATH_MARKERS):
            skipped.append({"url": abs_u, "reason": "excluded_path"})
            continue
        if abs_u not in seen:
            seen.add(abs_u)
            out.append(abs_u)
        if len(out) >= max_links:
            break
    return out, skipped


def collect_internal_links(
    base_url: str,
    soup: BeautifulSoup,
    max_links: int = 16,
) -> list[str]:
    """
    Same-origin links that plausibly carry ownership / policy copy.
    Excludes model pages, finance, service CTAs, inventory.
    """
    base = urlparse(base_url)
    seen: set[str] = set()
    out: list[str] = []
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        label = collapse_ws(a.get_text() or "")
        label_l = label.lower()
        href_l = href.lower()
        abs_u = urljoin(base_url, href)
        p = urlparse(abs_u)
        if p.scheme not in ("http", "https"):
            continue
        if p.netloc.lower() != base.netloc.lower():
            continue
        path_lower = (p.path or "").lower()
        if not _path_suggests_evidence(path_lower, href_l, label_l):
            continue
        if any(x in path_lower for x in _EXCLUDE_PATH_MARKERS):
            continue
        if abs_u not in seen:
            seen.add(abs_u)
            out.append(abs_u)
        if len(out) >= max_links:
            break
    return out


def html_to_blobs(
    html: str,
    page_url: str,
    page_label: str,
) -> tuple[list[tuple[str, str, str]], BeautifulSoup]:
    soup = BeautifulSoup(html, "html.parser")
    body_text = collapse_ws(soup.get_text(separator=" "))
    footer_text = extract_footer_html(soup)
    blobs: list[tuple[str, str, str]] = []
    blobs.append((page_url, page_label, body_text))
    if footer_text:
        ft = collapse_ws(footer_text)
        if ft and ft != body_text[: len(ft)]:
            blobs.append((page_url, "footer", ft))
    return blobs, soup
