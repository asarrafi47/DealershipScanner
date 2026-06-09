"""Filter junk / non-carousel URLs out of VDP gallery harvest batches."""
from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse

_JUNK_PATH_FRAGMENTS = (
    "logo",
    "icon",
    "badge",
    "banner",
    "placeholder",
    "carfax",
    "kbb",
    "cfximg",
    "cfx/",
    "/cfx",
    "certified",
    "bmw-certified",
    "m-performance",
    "/m-logo",
    "m_logo",
    "favicon",
    "sprite",
    "spinner",
    "loading",
    "blank.",
    "pixel.",
    "1x1",
    "avatar",
    "social",
    "facebook",
    "instagram",
    "youtube",
    "twitter",
    "dealer-logo",
    "dealerlogo",
    "brand-logo",
    "powered-by",
    "value-your-trade",
    "apply-for-financing",
    "warranty-tile",
    "quick-link",
    "marketing",
    "promo",
    "advertisement",
    "/ads/",
    "static/dealer",
    "stackadapt",
    "srv.stackadapt",
    "oem_vin_stock",
    "oem-vin-stock",
    "generic-bmw",
    "m-performance",
    "m_performance",
    "/m/logo",
    "carnow.com",
    "static.app.carnow",
    "/agents/agent",
    "agent-0",
)

_JUNK_FILENAME_RE = re.compile(
    r"(?i)(?:^|/)(?:certified|badge|logo|banner|icon|placeholder|spinner|loading|m[-_]?logo|bmw[-_]?certified)[^/]*\.(?:jpe?g|png|webp|gif)$"
)

_TINY_DIM_RE = re.compile(r"(?i)(?:[?&/]|^)(?:w|h|width|height)=(\d{1,3})(?:&|$|/)")


def _max_tiny_dim(url: str) -> int | None:
    """Return largest width/height query param if all parsed dims are tiny (likely thumbs)."""
    try:
        q = parse_qs(urlparse(url).query, keep_blank_values=False)
    except ValueError:
        return None
    dims: list[int] = []
    for key in ("w", "h", "width", "height"):
        for raw in q.get(key) or []:
            try:
                n = int(str(raw).strip())
            except (TypeError, ValueError):
                continue
            if 0 < n <= 320:
                dims.append(n)
    if not dims:
        m = _TINY_DIM_RE.search(url)
        if m:
            try:
                return int(m.group(1))
            except (TypeError, ValueError):
                return None
        return None
    return max(dims)


def is_junk_vdp_gallery_url(url: str) -> bool:
    """
    True when *url* is unlikely to be a real vehicle photo from the main carousel
    (badges, logos, marketing tiles, tiny thumbs, etc.).
    """
    u = (url or "").strip()
    if not u.lower().startswith("https://"):
        return True
    low = u.lower()
    if _JUNK_FILENAME_RE.search(low):
        return True
    for frag in _JUNK_PATH_FRAGMENTS:
        if frag in low:
            return True
    # Dealer.com CFX / marketing image hosts used for tiles, not inventory photos.
    if "pictures.dealer.com" in low and "/cfx/" in low:
        return True
    if re.search(r"(?i)/(?:static|assets)/(?:images/)?(?:badges?|logos?|icons?|marketing)/", low):
        return True
    tiny = _max_tiny_dim(u)
    if tiny is not None and tiny <= 120:
        return True
    if re.search(r"(?i)impolicy=(?:resize|downsize)|downsize_bkpt", low):
        wm = re.search(r"(?i)[?&]w=(\d{2,3})(?:&|$)", low)
        if wm and int(wm.group(1)) <= 414:
            return True
    return False


def filter_vdp_gallery_urls(urls: list[str]) -> list[str]:
    """Preserve order; drop junk URLs."""
    out: list[str] = []
    seen: set[str] = set()
    for raw in urls:
        if not isinstance(raw, str):
            continue
        u = raw.strip()
        if not u or u in seen or is_junk_vdp_gallery_url(u):
            continue
        seen.add(u)
        out.append(u)
    return out
