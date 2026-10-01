"""Step 3: window-sticker eligibility, URLs and the rendered sticker-card state."""

from __future__ import annotations

from dataclasses import dataclass

from flask import url_for

from backend.billing import access as paid_access
from backend.billing.catalog import FEATURE_WINDOW_STICKER
from backend.routes.car_detail.packages_ensure import _window_sticker_status


def _car_window_sticker_preview_url(car_id: int) -> str:
    return f"/car/{car_id}/window-sticker-preview.png"


@dataclass(frozen=True)
class WindowStickerView:
    """Everything the page needs to know about this car's window sticker."""

    cdjr_eligible: bool
    show_ui: bool
    ready: bool
    visual: bool
    listing_urls: list
    listing_image_url: str | None
    preview_api_url: str
    preview_embed_url: str | None
    fetch_pending: bool
    oem_url: str | None
    pdf_url: str | None


def resolve_window_sticker_view(car_id: int, car_raw: dict, ctx: dict) -> WindowStickerView:
    from backend.enrichment.window_sticker_service import (
        window_sticker_available,
        window_sticker_has_visual,
    )
    from backend.scanner.window_sticker import (
        car_listing_may_have_sticker,
        car_listing_sticker_urls,
        cdjr_oem_window_sticker_eligible,
        get_window_sticker_url,
        show_window_sticker_panel,
        sticker_embed_preview_url,
    )

    cdjr_sticker_eligible = cdjr_oem_window_sticker_eligible(car_raw)
    show_sticker_ui = show_window_sticker_panel(car_raw, ctx)
    sticker_ready = show_sticker_ui and window_sticker_available(car_raw)
    sticker_visual = show_sticker_ui and window_sticker_has_visual(car_raw)
    listing_sticker_urls = car_listing_sticker_urls(car_raw) if car_listing_may_have_sticker(car_raw) else []
    listing_sticker_image_url = listing_sticker_urls[0] if listing_sticker_urls else None
    # Sticker preview embed: only for a viewer the sticker preview API will serve.
    premium_viewer = paid_access.shows(FEATURE_WINDOW_STICKER)
    sticker_preview_api = _car_window_sticker_preview_url(car_id)
    # Window sticker fetch runs async via car_packages.js — avoid blocking VDP render.
    sticker_preview_embed_url = (
        sticker_embed_preview_url(
            car_raw,
            preview_api_url=sticker_preview_api,
            has_paid_access=premium_viewer,
        )
        if show_sticker_ui
        else None
    )
    sticker_fetch_pending = bool(
        show_sticker_ui and not sticker_ready and (listing_sticker_image_url or cdjr_sticker_eligible)
    )

    window_sticker_oem_url = None
    window_sticker_pdf_url = None
    if cdjr_sticker_eligible:
        window_sticker_oem_url = get_window_sticker_url(str(car_raw.get("vin") or ""))
    if show_sticker_ui and premium_viewer:
        if sticker_visual or cdjr_sticker_eligible:
            window_sticker_pdf_url = url_for("api_car_window_sticker", car_id=car_id)
        if not window_sticker_oem_url and listing_sticker_urls:
            window_sticker_oem_url = listing_sticker_urls[0]
    if show_sticker_ui and premium_viewer and not sticker_preview_embed_url:
        sticker_preview_embed_url = sticker_preview_api if sticker_visual else None

    return WindowStickerView(
        cdjr_eligible=cdjr_sticker_eligible,
        show_ui=show_sticker_ui,
        ready=sticker_ready,
        visual=sticker_visual,
        listing_urls=listing_sticker_urls,
        listing_image_url=listing_sticker_image_url,
        preview_api_url=sticker_preview_api,
        preview_embed_url=sticker_preview_embed_url,
        fetch_pending=sticker_fetch_pending,
        oem_url=window_sticker_oem_url,
        pdf_url=window_sticker_pdf_url,
    )


def has_real_sticker(sticker: WindowStickerView, ctx: dict) -> bool:
    """A genuine OEM/listing sticker exists (or is being fetched) for this car.

    When it does, that is authoritative and we never fabricate our own build
    sheet alongside it.
    """
    return bool(
        sticker.show_ui
        and (
            sticker.ready
            or sticker.visual
            or sticker.preview_embed_url
            or sticker.pdf_url
            or sticker.oem_url
            or sticker.listing_urls
            or sticker.cdjr_eligible
            or sticker.fetch_pending
            or ctx.get("listing_sticker_options")
            or ctx.get("listing_sticker_option_sections")
        )
    )


def rendered_window_sticker_status(sticker: WindowStickerView, *, fetch_in_flight: bool) -> str:
    """Rendered state for the sticker card so it can settle before (and
    independently of) the /packages/ensure XHR. Same vocabulary as the
    endpoint: "unavailable" = no sticker source at all, "pending" = a source
    exists but nothing is stored and nothing is running yet. Rendering this page
    starts no fetch, so "fetching" only appears when a worker from an earlier
    ensure call happens to still be alive. *fetch_in_flight* is
    ``_packages_ensure_worker_alive(car_id)``, read by the caller.
    """
    return _window_sticker_status(
        show_sticker_ui=sticker.show_ui,
        sticker_ready=sticker.ready,
        sticker_visual=sticker.visual,
        has_source=bool(sticker.listing_urls or sticker.cdjr_eligible),
        fetch_in_flight=fetch_in_flight,
    )
