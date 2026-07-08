"""
Unit tests for 360-spin asset detection (backend/scanner/vdp/spin_capture.py).

URL fixtures are recorded from live Impel viewer traffic (2026-07-06):
- bigislandmotorshilo / 3KPF24AD0PE622046 — real 64-frame spin + cubemap interior pano
- normreeveshondasuperstoreirvine / 2HGFE2F52TH592402 — closeups-only Impel (no spin)
"""

from backend.scanner.vdp.spin_capture import (
    build_impel_manifest_url_candidates,
    extract_spin_assets,
    is_spin_provider_url,
    is_spin_reserved_url,
    parse_impel_spin_manifest,
    spin_url_key,
)

_PREFIX = (
    "https://cdn.impel.io/swipetospin-viewers/bigislandmotorshilo/"
    "3kpf24ad0pe622046/20260214035030.SCMQ9GOO/"
)

# Recorded arrival order is scrambled (progressive loading: every 8th, then 4th, ...).
_RECORDED_FRAME_INDICES = [0, 48, 32, 40, 16, 8, 24, 56, 52, 60, 44, 4, 36, 12, 28, 20] + [
    i for i in range(64) if i % 4 != 0
]
_FRAME_URLS = [f"{_PREFIX}ec/0-{i}.jpg" for i in _RECORDED_FRAME_INDICES]

_CLOSEUPS = [f"{_PREFIX}closeups/cu-{i}.jpg" for i in range(21)]
_ULTRA_CLOSEUP = f"{_PREFIX}ultra-res/closeups/cu-1.jpg"
_PANO_CUBE_FACES = [f"{_PREFIX}pano/pano_{f}.jpg" for f in ("f", "b", "l", "r", "u", "d")]
_MISC = [
    f"{_PREFIX}thumb-sm.jpg",
    f"{_PREFIX}placeholder/placeholder.jpg",
    "https://consumer-assets.impel.io/thumbnails/Feature_Tour_Images/honda_ACC.jpg",
    "https://cdn.impel.io/spincar-static/20190909/ui/icon_expand.svg",
]


class TestClassifiers:
    def test_spin_provider_url(self):
        assert is_spin_provider_url(_FRAME_URLS[0])
        assert is_spin_provider_url("https://api.impel.io/spin/x/y")
        assert not is_spin_provider_url("https://pictures.dealer.com/f/abc/1.jpg")
        assert not is_spin_provider_url("")

    def test_reserved_paths_exclude_frames_and_panos_but_not_closeups(self):
        assert is_spin_reserved_url(_FRAME_URLS[0])
        assert is_spin_reserved_url(f"{_PREFIX}low-res/ec/0-0.jpg")
        assert is_spin_reserved_url(_PANO_CUBE_FACES[0])
        for u in _CLOSEUPS + [_ULTRA_CLOSEUP] + _MISC:
            assert not is_spin_reserved_url(u), u
        assert not is_spin_reserved_url("https://pictures.dealer.com/ec/1.jpg")

    def test_spin_url_key_drops_query(self):
        assert spin_url_key(f"{_FRAME_URLS[0]}?w=640") == spin_url_key(_FRAME_URLS[0])


class TestExtractSpinAssets:
    def test_orders_frames_by_numeric_index_not_arrival(self):
        r = extract_spin_assets(_FRAME_URLS + _CLOSEUPS + _PANO_CUBE_FACES + _MISC)
        frames = r["spin_frames"]
        assert len(frames) == 64
        assert frames[0].endswith("/ec/0-0.jpg")
        assert frames[1].endswith("/ec/0-1.jpg")
        assert frames[-1].endswith("/ec/0-63.jpg")
        # no closeups / panos / junk leaked into the sequence
        assert all("/ec/" in u for u in frames)

    def test_cubemap_pano_is_not_reported_as_equirect(self):
        r = extract_spin_assets(_FRAME_URLS + _PANO_CUBE_FACES)
        assert r["interior_pano"] is None

    def test_viewer_ui_chrome_never_counts_as_pano(self):
        # Regression: live capture on bigislandhyundai.com picked up the Impel UI cursor
        # (spincar-static/.../spin_pano_cursors/cursor-spin.png) as an interior pano.
        ui = [
            "https://cdn.impel.io/spincar-static/20190909/ui/spin_pano_cursors/cursor-spin.png",
            "https://cdn.impel.io/spincar-static/20190909/ui/spin_pano_cursors/cursor-spin-drag.png",
        ]
        r = extract_spin_assets(_FRAME_URLS + ui)
        assert r["interior_pano"] is None
        assert len(r["spin_frames"]) == 64

    def test_single_pano_candidate_is_reported(self):
        r = extract_spin_assets(_FRAME_URLS + [f"{_PREFIX}pano/pano.jpg"])
        assert r["interior_pano"] == f"{_PREFIX}pano/pano.jpg"

    def test_closeups_alone_do_not_form_a_spin(self):
        # normreeveshondasuperstoreirvine-style vehicle: closeups only, no ec sequence
        r = extract_spin_assets(_CLOSEUPS + [_ULTRA_CLOSEUP] + _MISC)
        assert r["spin_frames"] == []
        assert r["interior_pano"] is None

    def test_below_min_frames_is_rejected(self):
        r = extract_spin_assets([f"{_PREFIX}ec/0-{i}.jpg" for i in range(7)])
        assert r["spin_frames"] == []

    def test_prefers_full_res_over_low_res_tier(self):
        low = [f"{_PREFIX}low-res/ec/0-{i}.jpg" for i in range(64)]
        r = extract_spin_assets(low + _FRAME_URLS)
        assert len(r["spin_frames"]) == 64
        assert all("low-res" not in u for u in r["spin_frames"])

    def test_low_res_only_still_counts(self):
        low = [f"{_PREFIX}low-res/ec/0-{i}.jpg" for i in range(32)]
        r = extract_spin_assets(low)
        assert len(r["spin_frames"]) == 32

    def test_duplicate_urls_and_query_variants_deduped(self):
        dupes = _FRAME_URLS + [f"{u}?cb=1" for u in _FRAME_URLS[:10]]
        r = extract_spin_assets(dupes)
        assert len(r["spin_frames"]) == 64

    def test_non_provider_numbered_sequences_ignored(self):
        urls = [f"https://pictures.dealer.com/gallery/img-{i}.jpg" for i in range(40)]
        r = extract_spin_assets(urls)
        assert r["spin_frames"] == []


class TestImpelManifest:
    _MANIFEST = {
        "cdn_image_prefix": (
            "//cdn.impel.io/swipetospin-viewers/bigislandmotorshilo/"
            "3kpf24ad0pe622046/20260214035030.SCMQ9GOO/"
        ),
        "info": {
            "options": {"numImgCloseup": 21, "numImgEC": 64},
            "views": {
                "exterior": {"load_images_from": "ec", "source": "app"},
                "pano": {"has_low_res": True, "source": "app"},
            },
        },
    }

    def test_builds_ordered_frames_from_manifest(self):
        r = parse_impel_spin_manifest(self._MANIFEST)
        frames = r["spin_frames"]
        assert len(frames) == 64
        assert frames[0] == f"{_PREFIX}ec/0-0.jpg"
        assert frames[-1] == f"{_PREFIX}ec/0-63.jpg"
        assert r["interior_pano"] is None  # Impel pano is a cubemap, never equirect

    def test_closeups_only_manifest_yields_nothing(self):
        m = {
            "cdn_image_prefix": "//cdn.impel.io/swipetospin-viewers/norm/2hgfe/20260705.OW2F/",
            "info": {
                "options": {"numImgCloseup": 16},
                "views": {"exterior": {"load_images_from": "ec"}},
            },
        }
        assert parse_impel_spin_manifest(m)["spin_frames"] == []

    def test_garbage_manifest_is_safe(self):
        assert parse_impel_spin_manifest(None)["spin_frames"] == []
        assert parse_impel_spin_manifest({})["spin_frames"] == []
        assert parse_impel_spin_manifest({"cdn_image_prefix": "ftp://x/"})["spin_frames"] == []
        assert (
            parse_impel_spin_manifest({"cdn_image_prefix": "//cdn.impel.io/x/", "info": {"options": {"numImgEC": "NaN"}}})[
                "spin_frames"
            ]
            == []
        )

    def test_manifest_url_candidates(self):
        config = [
            "https://cdn.impel.io/spincar-static/settings.json",
            "https://api.impel.io/spin/bigislandmotorshilo/3kpf24ad0pe622046?v=20160212",
            "https://regioner.impel.io/bigislandmotorshilo",
        ]
        assets = [_CLOSEUPS[0]]
        iframes = [
            "https://cdn.impel.io/spincar-static/fh/index.html?_=b0#!customer=bigislandmotorshilo"
            "!vin=3kpf24ad0pe622046!static_env=prod"
        ]
        cands = build_impel_manifest_url_candidates(config, assets, iframes)
        assert cands[0] == "https://api.impel.io/spin/bigislandmotorshilo/3kpf24ad0pe622046"
        # derivations collapse into the same manifest URL (deduped)
        assert len(cands) == 1

    def test_manifest_url_derived_from_asset_when_not_observed(self):
        cands = build_impel_manifest_url_candidates([], [_CLOSEUPS[0]], [])
        assert cands == ["https://api.impel.io/spin/bigislandmotorshilo/3kpf24ad0pe622046"]
