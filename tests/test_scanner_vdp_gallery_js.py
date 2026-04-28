"""Static checks for VDP gallery harvest JS (no Playwright in CI)."""

from __future__ import annotations

import scanner_vdp as sv


def test_gallery_collect_js_scopes_to_dialog_and_defines_root_picker() -> None:
    js = sv.GALLERY_COLLECT_URLS_JS
    assert "pickGalleryRoot" in js
    assert 'role="dialog"' in js or "[role=\"dialog\"]" in js
    assert "aria-modal" in js
    assert "pickGalleryRoot()" in js and "document" in js
    assert "isLikelyVdpJunkContext" in js


def test_page_extract_js_screens_vdp_tiles() -> None:
    js = sv.PAGE_EXTRACT_JS
    assert "isLikelyVdpJunkImage" in js
    assert "imgSelectorsSpecific" in js


def test_gallery_modal_nudge_js() -> None:
    js = sv.GALLERY_MODAL_NUDGE_JS
    assert "scrollWidth" in js
    assert "swiper" in js.lower()
