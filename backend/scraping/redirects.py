"""Detect suspicious cross-domain redirects away from the dealer site."""
from __future__ import annotations

import re
from urllib.parse import urlparse

# Unrelated / spam-like TLDs or obvious non-dealer destinations
_SUSPICIOUS_HOST_SUBSTRINGS = (
    "parking",
    "sedo",
    "godaddy",
    "domainfor",
    "placeholder",
    "healthline",
    "webmd",
    "wikipedia",
    "facebook.com",
    "twitter.com",
    "instagram.com",
    "bit.ly",
    "tinyurl",
)

_GENERIC_TOKENS = frozenset(
    {
        "www",
        "com",
        "net",
        "org",
        "co",
        "us",
        "dealer",
        "dealers",
        "dealership",
        "dealerships",
        "auto",
        "automotive",
        "motors",
        "cars",
        "car",
        "group",
        "inc",
        "llc",
    }
)


def _registrable_domain(host: str) -> str:
    h = (host or "").lower().split(":")[0].strip(".")
    if h.startswith("www."):
        h = h[4:]
    parts = h.split(".")
    if len(parts) >= 2:
        if len(parts) >= 3 and parts[-2] in ("co", "com", "net", "org") and len(parts[-1]) == 2:
            return ".".join(parts[-3:])
        return ".".join(parts[-2:])
    return h


def _alpha_tokens(host: str) -> set[str]:
    h = host.lower().replace("www.", "")
    return {t for t in re.findall(r"[a-z]{3,}", h) if t not in _GENERIC_TOKENS}


def domains_plausibly_related(original_url: str, final_url: str) -> bool:
    """
    True if final hop looks like same business / family (same site, subdomain, or token overlap).
    False if clearly unrelated (different world with no shared tokens).
    """
    o = urlparse(original_url)
    f = urlparse(final_url)
    oh, fh = o.netloc.lower(), f.netloc.lower()
    if not oh or not fh:
        return True
    if _registrable_domain(oh) == _registrable_domain(fh):
        return True

    for bad in _SUSPICIOUS_HOST_SUBSTRINGS:
        if bad in fh:
            return False

    to = _alpha_tokens(oh)
    tf = _alpha_tokens(fh)
    if to & tf:
        return True

    # Shared substring (e.g. hendrick in both) — already covered by tokens often
    oh_clean = oh.replace("www.", "")
    fh_clean = fh.replace("www.", "")
    if len(oh_clean) >= 5 and oh_clean.split(".")[0] in fh_clean:
        return True
    if len(fh_clean) >= 5 and fh_clean.split(".")[0] in oh_clean:
        return True

    return False


def describe_redirect(
    original_url: str,
    final_url: str,
) -> tuple[bool, bool, str]:
    """
    Returns (redirected, redirect_mismatch, final_domain).

    *redirect_mismatch* is True when the hostname changes to something not plausibly
    the same dealer / family (cross-domain parking, unrelated site).
    """
    o = urlparse(original_url)
    f = urlparse(final_url)
    fd = f.netloc.lower() or ""
    a = original_url.split("#")[0].rstrip("/")
    b = final_url.split("#")[0].rstrip("/")
    redirected = a != b
    same_host = o.netloc.lower() == f.netloc.lower()
    mismatch = False
    if redirected and not same_host:
        mismatch = not domains_plausibly_related(original_url, final_url)
    return redirected, mismatch, fd
