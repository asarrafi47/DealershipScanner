"""URL normalization, whitespace, DNS checks, vendor/boilerplate helpers."""
from __future__ import annotations

import re
import socket
from urllib.parse import urlparse

from SCRAPING.constants import BOILERPLATE_HINTS, VENDOR_SUBSTRINGS


def normalize_root(url: str) -> str:
    u = url.strip()
    if not u:
        return ""
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    p = urlparse(u)
    if not p.netloc:
        return ""
    return f"{p.scheme}://{p.netloc}"


def collapse_ws(text: str) -> str:
    t = text.replace("\xa0", " ").replace("\u2009", " ")
    return re.sub(r"\s+", " ", t).strip()


def is_vendor_text(s: str) -> bool:
    sl = s.lower()
    return any(v in sl for v in VENDOR_SUBSTRINGS)


def is_boilerplate_only(s: str) -> bool:
    sl = s.lower()
    if len(sl) < 12:
        return True
    if any(b in sl for b in BOILERPLATE_HINTS) and not re.search(
        r"[A-Z][a-z]+\s+[A-Z][a-z]+", s
    ):
        return True
    return False


def classify_page_kind(url: str) -> str:
    u = url.lower()
    if "collision" in u:
        return "company"
    for k in ("about", "privacy", "terms", "legal", "careers"):
        if k in u:
            return k
    return "homepage"


def classify_cross_domain_page_kind(url: str) -> str:
    u = url.lower()
    if any(x in u for x in ("privacy", "terms", "legal", "cookie", "do-not-sell", "your-privacy")):
        return "cross_domain_legal"
    return "cross_domain_group"


def dns_check(host: str, timeout: float = 3.0) -> tuple[bool, str]:
    host = host.split(":")[0]
    try:
        socket.setdefaulttimeout(timeout)
        socket.getaddrinfo(host, None, socket.AF_UNSPEC)
        return True, "ok"
    except OSError as e:
        return False, f"dns_failed:{e!s}"
    finally:
        socket.setdefaulttimeout(None)
