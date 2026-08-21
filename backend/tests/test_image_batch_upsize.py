"""
URL-transform tests for image_batch._upsize_url.

Every URL shape here is a real production gallery URL measured on 2026-08-18
(see the pattern comment block in image_batch.py for the empirical evidence).
Pure-function tests: no network.
"""

from __future__ import annotations

from backend.scripts.image_batch import _looks_like_image, _upsize_url


# ---------------------------------------------------------------------------
# assets.cai-media-management.com -- /resize/WxH/ segment; dropping it serves
# the master at original quality (640x480 -> 1280x960 measured).
# ---------------------------------------------------------------------------

def test_cai_resize_1024_drops_segment():
    url = (
        "https://assets.cai-media-management.com/resize/1024x1024/"
        "common-vehicle-media/6ceba06b-7d91-4dd9-b03d-efde3ce14e39.jpg"
    )
    out = _upsize_url(url)
    assert out[0] == (
        "https://assets.cai-media-management.com/"
        "common-vehicle-media/6ceba06b-7d91-4dd9-b03d-efde3ce14e39.jpg"
    )
    assert out[-1] == url


def test_cai_resize_640_drops_segment():
    url = (
        "https://assets.cai-media-management.com/resize/640x640/"
        "common-vehicle-media/2c4007b9-56f4-4287-ab3e-bda6d9e1ef21.jpg"
    )
    out = _upsize_url(url)
    assert out[0] == (
        "https://assets.cai-media-management.com/"
        "common-vehicle-media/2c4007b9-56f4-4287-ab3e-bda6d9e1ef21.jpg"
    )


def test_cai_bare_path_untouched():
    url = (
        "https://assets.cai-media-management.com/"
        "common-vehicle-media/2c4007b9-56f4-4287-ab3e-bda6d9e1ef21.jpg"
    )
    assert _upsize_url(url) == [url]


# ---------------------------------------------------------------------------
# media.rti.toyota.com -- Adobe AEM delivery; stripping the binding size param
# served the master (1179x663 -> 6000x3375 measured).
# ---------------------------------------------------------------------------

def test_rti_toyota_strips_size_param():
    url = (
        "https://media.rti.toyota.com/adobe/assets/"
        "urn:aaid:aem:90eb625d-d13c-45fc-b33b-648edc189d0f/as/image.png?size=1200,663"
    )
    out = _upsize_url(url)
    assert out[0] == (
        "https://media.rti.toyota.com/adobe/assets/"
        "urn:aaid:aem:90eb625d-d13c-45fc-b33b-648edc189d0f/as/image.png"
    )
    assert out[-1] == url


def test_rti_toyota_encoded_comma_variant():
    # The %2C-encoded form appears in production galleries too.
    url = (
        "https://media.rti.toyota.com/adobe/assets/"
        "urn:aaid:aem:8fc2f896-9d68-43fb-b414-e36919c64f8a/as/image.png?size=1200%2C663"
    )
    out = _upsize_url(url)
    assert out[0].endswith("/as/image.png")
    assert "size=" not in out[0]


def test_rti_toyota_keeps_other_params_when_stripping_size():
    url = (
        "https://media.rti.toyota.com/adobe/assets/"
        "urn:aaid:aem:aaaa/as/image.png?fmt=png-alpha&size=1200,663"
    )
    out = _upsize_url(url)
    assert out[0] == (
        "https://media.rti.toyota.com/adobe/assets/urn:aaid:aem:aaaa/as/image.png?fmt=png-alpha"
    )


def test_assetscs_toyota_not_transformed():
    # delivery.via.assetscs.toyota.com stores width=1920, which already exceeds
    # its 1200x675 masters -- measured a no-op, so no candidates are minted.
    url = (
        "https://delivery.via.assetscs.toyota.com/adobe/assets/"
        "urn:aaid:aem:e367efa0-0ef5-40b9-acd3-582ddba22954/as/image.png"
        "?fmt=png-alpha,rgb,none&width=1920"
    )
    assert _upsize_url(url) == [url]


# ---------------------------------------------------------------------------
# content.homenetiol.com -- /WxH/ fit box in the path; 0x0 means uncapped
# (1600x1200 -> 2000x1500 measured).
# ---------------------------------------------------------------------------

def test_homenet_sized_path_becomes_0x0():
    url = (
        "https://content.homenetiol.com/2002170/2153875/1600x1200/"
        "e4297f94c4544c8a8bff7b8734aba9b9/1C4SJVDP0RS167535-0.jpg"
    )
    out = _upsize_url(url)
    assert out[0] == (
        "https://content.homenetiol.com/2002170/2153875/0x0/"
        "e4297f94c4544c8a8bff7b8734aba9b9/1C4SJVDP0RS167535-0.jpg"
    )
    assert out[-1] == url


def test_homenet_already_0x0_untouched():
    url = (
        "https://content.homenetiol.com/2000157/2065512/0x0/"
        "16199cbd1aad412a86b909ce128e4eba.jpg"
    )
    assert _upsize_url(url) == [url]


# ---------------------------------------------------------------------------
# images.otf3.pixelmotiondemo.com -- /WxH/ path segment; a 2048-wide box
# resolved 200 at 2048x1536 (364x273 stored).
# ---------------------------------------------------------------------------

def test_pixelmotion_scales_to_2048_preserving_aspect():
    url = "https://images.otf3.pixelmotiondemo.com/364x273/BKi2p-20260804203048.jpg"
    out = _upsize_url(url)
    assert out[0] == (
        "https://images.otf3.pixelmotiondemo.com/2048x1536/BKi2p-20260804203048.jpg"
    )
    assert out[-1] == url


def test_pixelmotion_already_large_untouched():
    url = "https://images.otf3.pixelmotiondemo.com/2048x1536/BKi2p-20260804203048.jpg"
    assert _upsize_url(url) == [url]


# ---------------------------------------------------------------------------
# Hosts measured and deliberately left alone.
# ---------------------------------------------------------------------------

def test_untransformed_hosts_pass_through():
    for url in (
        # Bare pictures.dealer.com already serves the master (1600x1200 measured).
        "https://pictures.dealer.com/a/autonationhondacostamesa/0926/"
        "02ac15847cce6964dff1d9f1833c1849x.jpg",
        # carscommerce masters served as stored (2880x2160 measured).
        "https://vehicle-images.carscommerce.inc/7348-110004231/"
        "1C4RJGBR9TC249652/959631b9389d8c77a978d3f39caf8566.jpg",
        # chapmanchoice /640/ is the only rendition (larger 404s).
        "https://photos.chapmanchoice.com/vehicles/CAE/640/1C4RJHBR9TC303635-3.jpg",
        # overfuel _640_ is the only rendition (larger 403s).
        "https://static.overfuel.com/photos/1675/1658830/2022RMT110047_640_01.webp",
        "https://content.homenetiol.com/nvi/whatever.jpg",
    ):
        assert _upsize_url(url) == [url]


# ---------------------------------------------------------------------------
# Contract: original always last, no duplicates, bounded attempts.
# ---------------------------------------------------------------------------

def test_original_always_last_and_bounded():
    urls = [
        "https://assets.cai-media-management.com/resize/1024x1024/common-vehicle-media/a.jpg",
        "https://media.rti.toyota.com/adobe/assets/urn:aaid:aem:x/as/image.png?size=1200,663",
        "https://content.homenetiol.com/1/2/640x480/x/a.jpg",
        "https://images.otf3.pixelmotiondemo.com/364x273/a.jpg",
        "https://example.com/plain.jpg",
        "not-a-url",
        "",
    ]
    for u in urls:
        out = _upsize_url(u)
        if not u:
            assert out == []
            continue
        assert out[-1] == u
        assert len(out) <= 3  # at most 2 extra attempts before the original
        assert len(out) == len(set(out))


def test_empty_url():
    assert _upsize_url("") == []


# ---------------------------------------------------------------------------
# _looks_like_image -- an upsize candidate that 200s with an HTML error page
# must never replace a working original.
# ---------------------------------------------------------------------------

def test_looks_like_image_accepts_real_formats():
    assert _looks_like_image(b"\xff\xd8\xff\xe0" + b"\x00" * 20)          # JPEG
    assert _looks_like_image(b"\x89PNG\r\n\x1a\n" + b"\x00" * 20)         # PNG
    assert _looks_like_image(b"RIFF\x00\x00\x00\x00WEBP" + b"\x00" * 8)   # WebP
    assert _looks_like_image(b"\x00\x00\x00\x20ftypavif" + b"\x00" * 8)   # AVIF
    assert _looks_like_image(b"GIF89a" + b"\x00" * 20)


def test_looks_like_image_rejects_html_and_junk():
    assert not _looks_like_image(b"<!DOCTYPE html><html>error page</html>")
    assert not _looks_like_image(b"<html><body>404</body></html>")
    assert not _looks_like_image(b"")
    assert not _looks_like_image(b"\xff\xd8")  # truncated


# ---------------------------------------------------------------------------
# _select_images: gallery sampling
# ---------------------------------------------------------------------------

from backend.scripts.image_batch import _select_images


def _indices(n: int, budget: int) -> list[int]:
    urls = [f"u{i}" for i in range(n)]
    return [int(u[1:]) for u in _select_images(urls, budget)]


def test_head_slides_always_sampled():
    # Ken Grody Ford parks a full-frame Monroney scan at gallery position 2-3
    # of 48; the old pure even spread skipped indices 1-3 entirely and the
    # sticker was reported "not seen" while sitting clean in the gallery.
    for budget in (8, 10):
        picked = _indices(48, budget)
        assert {1, 2, 3} <= set(picked), picked
        assert len(picked) == budget


def test_spread_still_covers_mid_gallery():
    # The incident that created this sampler: highlights slides at positions
    # 6, 12 and 18 of 40. The spread half must keep real mid-gallery coverage
    # (within a couple of slots of any mid position), not collapse to the head.
    picked = _indices(40, 10)
    for target in (6, 13, 20, 26):
        assert any(abs(p - target) <= 2 for p in picked), (target, picked)
    assert picked[-1] == 39  # tail still included


def test_small_galleries_returned_whole():
    assert _indices(5, 8) == [0, 1, 2, 3, 4]
    assert _indices(2, 1) == [0]
