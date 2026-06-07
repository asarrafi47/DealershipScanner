"""URL heuristics for dealer gallery listing images. No LLM dependency."""
from __future__ import annotations

import logging
import re
from urllib.parse import unquote, urlparse

logger = logging.getLogger(__name__)

_VIN_LIKE_IN_URL_RE = re.compile(r"[^a-z0-9]([a-hj-npr-z0-9]{11,17})[^a-z0-9]", re.I)

# Do not mark these host substrings as fluff (dealer DMS / lot photography CDNs)
_DEALER_CDN_KEEP_HINTS: frozenset[str] = frozenset(
    {
        "pictures.dealer",
        "dealerinspire",
        "cstatic-images",
        "getvehicleimage",
        "invphoto",
        "invphotos",
        "inventoryphoto",
    }
)

# Third-party badge / guide hosts — never a vehicle photo, skip vision
_GALLERY_HOST_DROP_SUBSTR: frozenset[str] = frozenset(
    {
        "kbb.com",
        "kbb-",
        "kelleybluebook",
        "gubagoo.io",
        "pureinfluencer",
        "idrove.it",
        "autotrader.com/kbb",
        "carnow.com",
        "payments.dealer",
        "dealer-fx",
        "dealerfx",
        "routeone",
        "capitalone",
        "creditapp",
        "autofi",
        "darwin.cx",
        "impel.io/widget",
        "spin.car",
        "vinsolutions",
        "eleadcrm",
    }
)

# Dealer-site UI assets scraped into gallery JSON (icons, transfer tiles, chat widgets).
_GALLERY_PATH_JUNK_FRAGMENTS: frozenset[str] = frozenset(
    {
        "transferbadge",
        "transfer_badge",
        "directions-icon",
        "directions_icon",
        "photoswipe",
        "default-skin",
        "/customwork/",
        "vehicle images coming soon",
        "images coming soon",
        "incentive+",
        "+incentive",
        "shop-click-drive",
        "shopclickdrive",
        "express-checkout",
        "expresscheckout",
        "get-pre-approved",
        "getpreapproved",
        "value-your-trade",
        "valueyourtrade",
        "sell-us-your",
        "sellusyour",
        "schedule-service",
        "scheduleservice",
        "service-appointment",
        "chat-icon",
        "chat_icon",
        "live-chat",
        "livechat",
        "placeholder-image",
        "no-image-available",
        "coming-soon",
        "image-not-available",
    }
)


def heuristic_listing_gallery_fluff_url(url: str) -> bool:
    """
    True if the URL is very unlikely to be a real **dealer lot** inventory image (OEM stock, F&I
    tiles, KBB, CARFAX badges, iPacket, BMW press/configurator, etc.).

    Used: (1) before calling vision, (2) after a keep to veto OEM URLs the model sometimes mislabels.
    """
    if not url or not isinstance(url, str):
        return True
    s = url.strip()
    sl = s.lower()
    if not sl.startswith("http"):
        return True
    if any(keep in sl for keep in _DEALER_CDN_KEEP_HINTS):
        if any(
            x in sl
            for x in ("/inv/", "vehicle-phot", "vdp-phot", "getvehicleimage", "invphoto", "invphotos")
        ):
            return False
    try:
        p = urlparse(s)
        h = (p.netloc or "").lower()
        path = unquote((p.path or "").lower())
    except Exception as e:
        logger.debug("heuristic_listing_gallery_fluff_url: urlparse failed for %r: %s", s[:300], e)
        return True
    for needle in _GALLERY_HOST_DROP_SUBSTR:
        if needle in h or needle in path:
            return True
    # Spyne 3D renders are synthetic CG models — not real photos, always drop
    if "spyne" in h:
        fname = path.rsplit("/", 1)[-1].split("?")[0]
        if fname.startswith("3d_renders_"):
            return True
    if "autotrader.com" in h and ("/kbb" in sl or "kelley" in sl):
        return True
    if h.endswith("carfax.com") and ("/img/" in sl or "brand" in sl or "badge" in sl or "1-owner" in sl or "1owner" in sl):
        return True
    if "cfximg" in h and "carfax" in h:
        return True
    if "ipacket" in sl or "i-packet" in sl or "i_packet" in sl:
        return True
    # BMW / global OEM: marketing, press, build-your-own — not the dealer's camera roll.
    if "media.bmw.com" in h or "buildyour.bmw" in h or "configurator" in sl:
        return True
    if h.endswith("bmw.com") and ("binaries" in sl or "/content/dam" in sl):
        return True
    if "static.bmwusa.com" in h or "cache.bmwusa.com" in h:
        if not any(
            x in sl
            for x in (
                "vehicle-phot",
                "vdp-phot",
                "inventory",
                "dealer",
                "invphot",
            )
        ):
            return True
    # BMW / M brand slop (stripes, roundel mark, lifestyle packs) — not vehicle listing photos
    if not any(
        x in sl
        for x in ("/inv/", "vehicle-phot", "vdp-phot", "getvehicleimage", "invphot", "dealerinspire/inv")
    ):
        for frag in (
            "m-stripe",
            "m_stripes",
            "mperformance",
            "m-performance",
            "m-composition",
            "lifestyle-",
            "/lifestyle/",
            "brand-hero",
            "oempack",
            "roundel-",
            "m-badge",
            "m_series_hero",
            "mpower-",
        ):
            if frag in sl:
                return True
    if "edmunds.com" in h and ("/g/" in sl or "/static/img" in sl or "kbb" in sl or "icon" in sl):
        return True
    _fi_path_needles: tuple[str, ...] = (
        "/vpp/",
        "vpp-",
        "-vpp-",
        "vpp_",
        "vehicleprotection",
        "vehicle_protection",
        "vehicle-protection",
        "vehicleprotectionplan",
        "protectionplan",
        "protection_plan",
        "warrantybadge",
        "warranty_badge",
        "fi_badge",
        "f&i_",
        "f&i-",
        "f&i.",
        "fandibadge",
        "gap_logo",
        "plan_overview",
        "planoverview",
        "plan-overview",
        "coveragelevel",
        "covered_care",
        "ipocket",
    )
    q = unquote((p.query or "").lower())
    pq = f"{path}?{q}"
    for nd in _fi_path_needles:
        if nd in pq:
            return True
    for frag in _GALLERY_PATH_JUNK_FRAGMENTS:
        if frag in sl:
            return True
    return False


_CARSCOMMERCE_THUMB_PATH_RE = re.compile(r"/thumbnails/(?:large|medium|small)/", re.I)


def prefer_full_gallery_url(url: str) -> str:
    """Upgrade syndicated thumbnail paths to full-size assets when the CDN supports both."""
    s = (url or "").strip()
    if not s:
        return ""
    if "vehicle-images.carscommerce.inc" in s.lower() and _CARSCOMMERCE_THUMB_PATH_RE.search(s):
        return _CARSCOMMERCE_THUMB_PATH_RE.sub("/", s, count=1)
    return s


def filter_public_gallery_urls(urls: list[str] | None) -> list[str]:
    """
    Drop non-vehicle gallery URLs for public car pages. Prefer dealer lot photos when sorting.
    """
    if not urls:
        return []
    kept: list[str] = []
    seen: set[str] = set()
    for u in urls:
        if not u or not isinstance(u, str):
            continue
        s = u.strip()
        if not s or s in seen:
            continue
        if heuristic_listing_gallery_fluff_url(s):
            continue
        seen.add(s)
        kept.append(s)
    if not kept:
        return []
    kept.sort(key=dealer_lot_photo_score, reverse=True)
    out: list[str] = []
    seen_full: set[str] = set()
    for u in kept:
        full = prefer_full_gallery_url(u)
        if full and full not in seen_full:
            seen_full.add(full)
            out.append(full)
    return out


def heuristic_drop_gallery_listing_url(url: str) -> bool:
    """
    True = drop the URL without calling the vision model (see :func:`heuristic_listing_gallery_fluff_url`).
    """
    return heuristic_listing_gallery_fluff_url(url)


def dealer_lot_photo_score(url: str) -> int:
    """
    Higher = more likely a dealer-photographed listing asset (for hero / sort order). Heuristic only.
    """
    if not url:
        return 0
    u = unquote(url).lower()
    n = 0
    for token in (
        "/inv",
        "/inventory",
        "vehicle-phot",
        "vdp-phot",
        "in_transit",
        "stock_",
        "getvehicleimage",
        "vehicleimage",
    ):
        if token in u:
            n += 40
    if _VIN_LIKE_IN_URL_RE.search(u) or re.search(
        r"vin[_\s=%\-][a-hj-npr-z0-9]{6,20}", u, re.I
    ):
        n += 28
    for token in (
        "dealerinspire",
        "cstatic-images",
        "cloudinary",
        "s3.amazonaws.com",
        "cloudfront",
        "azureedge",
    ):
        if token in u:
            n += 8
    # Spyne bg-removed photos are clean listing shots — lift above plain originals
    if "spyne" in u:
        fname = u.rsplit("/", 1)[-1].split("?")[0]
        if fname.startswith("car_replace_bg_"):
            n += 20
    for token in ("buildyour", "configurator", "bmgusa", "bimmerpost"):
        if token in u:
            n -= 22
    if "media.bmw.com" in u or (".bmw.com" in u and "binaries" in u):
        n -= 16
    # De-prioritize common F&I / plan-artifact paths (still classified by vision, but fall behind true lot
    # shots for hero and empty-Gallery fallback). Broad tokens only where inventory URLs rarely use them.
    for token in (
        "vpp-",
        "/vpp/",
        "-vpp-",
        "vpp_",
        "vehicleprotect",
        "protectionplan",
        "warranty_badge",
        "warrantybadge",
        "fandi",
        "fi_badge",
        "plan_overview",
        "planoverview",
        "plan-overview",
        "coveragelevel",
    ):
        if token in u:
            n -= 32
    return n


def dealer_listing_gallery_passthrough_vision(url: str) -> bool:
    """
    True when the URL is very likely a real **inventory** photo (not a main-page F&I tile) and
    **safe to keep without** running vision classification — preserves full VDP carousels on
    trusted CDNs.

    Requires a strong lot signal from :func:`dealer_lot_photo_score` and not a
    :func:`heuristic_listing_gallery_fluff_url` match.
    """
    if not url or not isinstance(url, str):
        return False
    if heuristic_listing_gallery_fluff_url(url):
        return False
    if dealer_lot_photo_score(url) < 40:
        return False
    return True
