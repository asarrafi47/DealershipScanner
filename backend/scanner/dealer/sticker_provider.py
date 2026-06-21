"""Dealer-level window sticker provider detection (iPacket, listing embed, none)."""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

PROVIDER_UNKNOWN = "unknown"
PROVIDER_NONE = "none"
PROVIDER_IPACKET = "ipacket"
PROVIDER_LISTING_EMBED = "listing_embed"

_KNOWN_PROVIDERS = frozenset(
    {PROVIDER_UNKNOWN, PROVIDER_NONE, PROVIDER_IPACKET, PROVIDER_LISTING_EMBED}
)
_IPACKET_FAILS_BEFORE_NONE = 3


def detect_sticker_provider_from_html(html: str) -> str | None:
    """Return a provider hint from listing/VDP HTML, or None when inconclusive."""
    blob = (html or "").strip()
    if not blob:
        return None
    low = blob.lower()
    if "autoipacket.com" in low or re.search(r"\bipacket\b", low):
        return PROVIDER_IPACKET
    if re.search(
        r"monroney|window[-_ ]?sticker|sticker-puller|document-viewer\.autoipacket",
        low,
    ):
        return PROVIDER_LISTING_EMBED
    return None


def detect_sticker_provider_from_urls(urls: list[str]) -> str | None:
    for raw in urls or []:
        u = str(raw or "").strip().lower()
        if not u.startswith("http"):
            continue
        if "autoipacket.com" in u or "ipacket.us" in u:
            return PROVIDER_IPACKET
        if any(tok in u for tok in ("monroney", "window-sticker", "window_sticker", "/sticker/")):
            return PROVIDER_LISTING_EMBED
    return None


def _registry_id_for_car(car: dict[str, Any]) -> int | None:
    raw = car.get("dealership_registry_id")
    if raw is None:
        return None
    try:
        rid = int(raw)
    except (TypeError, ValueError):
        return None
    return rid if rid > 0 else None


def get_dealer_sticker_provider(dealership_registry_id: int | None) -> str:
    if not dealership_registry_id:
        return PROVIDER_UNKNOWN
    try:
        from backend.db.dealerships_db import get_dealer_sticker_provider_row

        row = get_dealer_sticker_provider_row(int(dealership_registry_id))
    except Exception as e:
        logger.debug("get_dealer_sticker_provider failed id=%s: %s", dealership_registry_id, e)
        return PROVIDER_UNKNOWN
    if not row:
        return PROVIDER_UNKNOWN
    provider = str(row.get("sticker_provider") or PROVIDER_UNKNOWN).strip().lower()
    return provider if provider in _KNOWN_PROVIDERS else PROVIDER_UNKNOWN


def get_car_dealer_sticker_provider(car: dict[str, Any]) -> str:
    return get_dealer_sticker_provider(_registry_id_for_car(car))


def note_dealer_sticker_provider(
    dealership_registry_id: int | None,
    provider: str,
    *,
    source: str = "",
) -> None:
    """Persist a positive or negative sticker-provider signal for a dealership."""
    if not dealership_registry_id:
        return
    provider = str(provider or PROVIDER_UNKNOWN).strip().lower()
    if provider not in _KNOWN_PROVIDERS:
        return
    try:
        from backend.db.dealerships_db import update_dealer_sticker_provider

        update_dealer_sticker_provider(
            int(dealership_registry_id),
            provider,
            source=source,
            reset_fail_count=provider in (PROVIDER_IPACKET, PROVIDER_LISTING_EMBED),
        )
    except Exception as e:
        logger.debug("note_dealer_sticker_provider failed id=%s: %s", dealership_registry_id, e)


def note_sticker_signals_from_vdp(
    car: dict[str, Any],
    *,
    sticker_urls: list[str] | None = None,
    html: str | None = None,
) -> None:
    """Update dealer sticker capability from VDP scrape signals."""
    rid = _registry_id_for_car(car)
    if not rid:
        return
    from_urls = detect_sticker_provider_from_urls(list(sticker_urls or []))
    if from_urls:
        note_dealer_sticker_provider(rid, from_urls, source="vdp_url")
        return
    from_html = detect_sticker_provider_from_html(html or "")
    if from_html:
        note_dealer_sticker_provider(rid, from_html, source="vdp_html")


def record_ipacket_probe_result(car: dict[str, Any], *, success: bool) -> None:
    """Track iPacket website-plugin probe outcomes per dealership."""
    rid = _registry_id_for_car(car)
    if not rid:
        return
    try:
        from backend.db.dealerships_db import (
            bump_dealer_ipacket_fail_count,
            get_dealer_sticker_provider_row,
            update_dealer_sticker_provider,
        )

        if success:
            update_dealer_sticker_provider(
                rid,
                PROVIDER_IPACKET,
                source="ipacket_probe_ok",
                reset_fail_count=True,
            )
            return
        row = get_dealer_sticker_provider_row(rid) or {}
        current = str(row.get("sticker_provider") or PROVIDER_UNKNOWN).lower()
        if current == PROVIDER_IPACKET:
            return
        fails = bump_dealer_ipacket_fail_count(rid)
        if fails >= _IPACKET_FAILS_BEFORE_NONE and current != PROVIDER_LISTING_EMBED:
            update_dealer_sticker_provider(
                rid,
                PROVIDER_NONE,
                source="ipacket_probe_miss",
                reset_fail_count=False,
            )
    except Exception as e:
        logger.debug("record_ipacket_probe_result failed id=%s: %s", rid, e)


def should_try_ipacket_website_plugin(
    car: dict[str, Any],
    html: str | None = None,
) -> bool:
    """Whether to call the public iPacket website-plugin API for this car."""
    from backend.scanner.post_scan.window_sticker import is_cdjr_stellantis_car

    if is_cdjr_stellantis_car(car):
        return False
    provider = get_car_dealer_sticker_provider(car)
    if provider == PROVIDER_NONE:
        return False
    if provider == PROVIDER_IPACKET:
        return True
    if detect_sticker_provider_from_html(html or "") == PROVIDER_IPACKET:
        return True
    ws = str(car.get("window_sticker_url") or "").lower()
    if "autoipacket.com" in ws:
        return True
    return provider == PROVIDER_UNKNOWN


def car_listing_sticker_fetch_eligible(car: dict[str, Any]) -> bool:
    """True when post-scan / gap-fill should attempt a listing sticker fetch (non-OEM)."""
    from backend.scanner.post_scan.window_sticker import (
        _car_has_sticker_options_in_packages,
        car_has_listing_sticker_signal,
        is_cdjr_stellantis_car,
    )

    if is_cdjr_stellantis_car(car):
        return False
    if car_has_listing_sticker_signal(car):
        return True
    if _car_has_sticker_options_in_packages(car):
        return True
    provider = get_car_dealer_sticker_provider(car)
    if provider == PROVIDER_NONE:
        return False
    if provider in (PROVIDER_IPACKET, PROVIDER_LISTING_EMBED):
        return True
    url = str(
        car.get("source_url") or car.get("_detail_url") or car.get("listing_vdp_url") or ""
    ).strip()
    if url.lower().startswith("http"):
        return True
    desc = str(car.get("description") or "").lower()
    return any(
        tok in desc
        for tok in (
            "ipacket",
            "vehicle records",
            "original msrp",
            "msrp / options",
            "window sticker",
            "monroney",
        )
    )
