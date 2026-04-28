"""Ollama LLaVA client (mocked HTTP; no live Ollama)."""

from __future__ import annotations

import base64
from io import BytesIO

import pytest

import backend.vision.ollama_llava as llv


def test_classify_listing_image_from_image_b64_keep_and_drop(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_chat(*, system: str, user_text: str, image_b64_jpeg: str, timeout_s=None):
        assert "Monroney" in system or "window" in system.lower()
        return '{"keep": true, "category": "exterior", "confidence": 0.91}'

    monkeypatch.setattr(llv, "_is_trivially_blank_or_icon_b64", lambda s: False)
    monkeypatch.setattr(llv, "_ollama_vision_chat_json", fake_chat)
    out = llv.classify_listing_image_from_image_b64("Zm9v")
    assert out is not None
    assert out["keep"] is True
    assert out["category"] == "exterior"

    def fake_chat_drop(*, system: str, user_text: str, image_b64_jpeg: str, timeout_s=None):
        return '{"keep": false, "category": "not_vehicle", "confidence": 0.88}'

    monkeypatch.setattr(llv, "_ollama_vision_chat_json", fake_chat_drop)
    out2 = llv.classify_listing_image_from_image_b64("Zm9v")
    assert out2 is not None
    assert out2["keep"] is False


def test_listing_image_drops_without_explicit_keep_true(monkeypatch: pytest.MonkeyPatch) -> None:
    """Category alone no longer forces a keep; model must set keep: true."""

    def fake_exterior_no_keep(*, system: str, user_text: str, image_b64_jpeg: str, timeout_s=None):
        return '{"category": "exterior", "confidence": 0.99}'

    monkeypatch.setattr(llv, "_is_trivially_blank_or_icon_b64", lambda s: False)
    monkeypatch.setattr(llv, "_ollama_vision_chat_json", fake_exterior_no_keep)
    out = llv.classify_listing_image_from_image_b64("Zm9v")
    assert out is not None
    assert out["keep"] is False


def test_listing_image_drops_when_keep_true_but_category_bad(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake(*, system: str, user_text: str, image_b64_jpeg: str, timeout_s=None):
        return '{"keep": true, "category": "fluff", "confidence": 0.9}'

    monkeypatch.setattr(llv, "_is_trivially_blank_or_icon_b64", lambda s: False)
    monkeypatch.setattr(llv, "_ollama_vision_chat_json", fake)
    out = llv.classify_listing_image_from_image_b64("Zm9v")
    assert out is not None
    assert out["keep"] is False


def test_listing_image_strip_shape_overrides_exterior(monkeypatch: pytest.MonkeyPatch) -> None:
    """F&I-style bar images can be misread as a vehicle; dimensions force drop (no Ollama path required)."""
    from PIL import Image  # type: ignore[import-untyped]

    im = Image.new("RGB", (1000, 150), (40, 80, 120))
    buf = BytesIO()
    im.save(buf, format="JPEG", quality=88)
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")

    def fake_says_exterior(*, system: str, user_text: str, image_b64_jpeg: str, timeout_s=None):
        return '{"keep": true, "category": "exterior", "confidence": 0.95}'

    monkeypatch.setattr(llv, "_is_trivially_blank_or_icon_b64", lambda s: False)
    monkeypatch.setattr(llv, "_ollama_vision_chat_json", fake_says_exterior)
    out = llv.classify_listing_image_from_image_b64(b64)
    assert out is not None
    assert out["keep"] is False
    assert out.get("category") == "marketing_strip"


def test_filter_gallery_fallback_skips_warranty_flyer_page_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    urls = ["https://d.com/a.jpg", "https://d.com/b.jpg"]

    def fake(u: str, **kwargs: object) -> dict:
        if "a" in u:
            return {
                "keep": False,
                "category": "warranty_flyer_page",
                "confidence": 0.0,
                "model": "x",
                "image_b64_len": 1,
            }
        # Soft failure: not in _GALLERY_NO_FALLBACK_CATEGORIES, so it may be restored when fallback=1
        return {"keep": False, "category": "inconclusive", "confidence": 0.4, "model": "x", "image_b64_len": 1}

    monkeypatch.setattr(llv, "classify_listing_image_from_url", fake)
    monkeypatch.setenv("SCANNER_GALLERY_VISION_FALLBACK_ON_EMPTY", "1")
    kept = llv.filter_gallery_urls_for_vehicle_listing(urls, max_workers=1)
    assert kept == ["https://d.com/b.jpg"]


def test_filter_gallery_fallback_skips_marketing_strip_row(monkeypatch: pytest.MonkeyPatch) -> None:
    """Empty fallback should not re-insert F&I strip rows (excluded from lot-order restore)."""
    # Avoid /inv/ and vehicle-phot — those passthrough without LLaVA
    urls = ["https://d.com/1.jpg", "https://d.com/2.jpg"]

    def fake(u: str, **kwargs: object) -> dict:
        if "1.jpg" in u:
            return {"keep": False, "category": "marketing_strip", "confidence": 0.0, "model": "x", "image_b64_len": 1}
        return {"keep": False, "category": "inconclusive", "confidence": 0.4, "model": "x", "image_b64_len": 1}

    monkeypatch.setattr(llv, "classify_listing_image_from_url", fake)
    monkeypatch.setenv("SCANNER_GALLERY_VISION_FALLBACK_ON_EMPTY", "1")
    kept = llv.filter_gallery_urls_for_vehicle_listing(urls, max_workers=1)
    assert kept == ["https://d.com/2.jpg"]


def test_heuristic_drops_fi_badge_path() -> None:
    assert (
        llv.heuristic_drop_gallery_listing_url("https://cdn.d.example/assets/fi_badge-hero-800.png")
        is True
    )
    assert llv.heuristic_drop_gallery_listing_url("https://d.example/stock-photos/vehicle-photos-1.jpg") is False


def test_dealer_lot_score_demotes_vpp_path_tokens() -> None:
    inv = "https://cdn.dealerinspire.com/inv/abc/vehicle-photos-1.jpg"
    vpp = "https://cdn.x.com/vehicleprotection-art/vpp-slide.jpg"
    assert llv.dealer_lot_photo_score(inv) > llv.dealer_lot_photo_score(vpp)


def test_dealer_listing_passthrough_requires_lot_signal() -> None:
    assert llv.dealer_listing_gallery_passthrough_vision(
        "https://cdn.dealerinspire.com/inv/abc/vehicle-photos-1/1.jpg"
    )
    assert not llv.dealer_listing_gallery_passthrough_vision("https://cdn.dealerinspire.com/widgets/x.jpg")
    assert not llv.dealer_listing_gallery_passthrough_vision("https://www.kbb.com/badge.png")


def test_heuristic_drops_oem_lifestyle_on_dealer_cdn() -> None:
    """pictures.dealer can host M / lifestyle slop: only /inv+vehicle paths are auto-trusted."""
    assert (
        llv.heuristic_listing_gallery_fluff_url(
            "https://pictures.dealer.com/a/lifestyle-hero-bmw-m-stripe-800.jpg"
        )
        is True
    )
    assert (
        llv.heuristic_listing_gallery_fluff_url(
            "https://pictures.dealer.com/inv/x/vehicle-photography-1/01.jpg"
        )
        is False
    )


def test_filter_gallery_passthrough_skips_ollama_for_trusted_cdn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: list[str] = []

    def boom(u: str, **kwargs: object) -> dict:
        called.append(u)
        return {"keep": True, "category": "exterior", "confidence": 0.9, "model": "x", "image_b64_len": 1}

    monkeypatch.setattr(llv, "classify_listing_image_from_url", boom)
    urls = ["https://cdn.dealerinspire.com/inv/abc/vehicle-photos-1/1.jpg"]
    out = llv.filter_gallery_urls_for_vehicle_listing(urls, max_workers=1)
    assert out == urls
    assert called == []


def test_filter_gallery_passthrough_off_still_runs_vision(monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[str] = []

    def track(u: str, **kwargs: object) -> dict:
        called.append(u)
        return {"keep": True, "category": "exterior", "confidence": 0.9, "model": "x", "image_b64_len": 1}

    monkeypatch.setattr(llv, "classify_listing_image_from_url", track)
    monkeypatch.setenv("SCANNER_GALLERY_VISION_PASSTHROUGH_TRUSTED_CDN", "0")
    urls = ["https://cdn.dealerinspire.com/inv/abc/vehicle-photos-1/1.jpg"]
    out = llv.filter_gallery_urls_for_vehicle_listing(urls, max_workers=1)
    assert out == urls
    assert called == [urls[0]]


def test_classify_listing_forwards_referer_to_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, str | None] = {}

    def capture(url: str, timeout: int = 25, **kwargs) -> str | None:
        seen["referer"] = kwargs.get("referer")
        return None

    monkeypatch.setattr(llv, "_fetch_image_b64_optimized", capture)
    monkeypatch.setenv("SCANNER_GALLERY_VISION_KEEP_UNFETCHABLE", "0")
    out = llv.classify_listing_image_from_url(
        "https://images.cdn.example/inv/1.jpg", page_referer="https://dealer.com/used/vin-ABC.htm"
    )
    assert out is not None
    assert out.get("category") == "unfetchable"
    assert seen.get("referer") == "https://dealer.com/used/vin-ABC.htm"


def test_classify_listing_url_veto_after_model_keep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Press/OEM URL that still loads a car-like image: model keep=true is overridden by URL heuristics."""

    def fake_says_exterior(*, system: str, user_text: str, image_b64_jpeg: str, timeout_s=None):
        return '{"keep": true, "category": "exterior", "confidence": 0.95}'

    monkeypatch.setattr(llv, "_fetch_image_b64_optimized", lambda _u, **kwargs: "Zm9v")
    monkeypatch.setattr(llv, "_is_trivially_blank_or_icon_b64", lambda s: False)
    monkeypatch.setattr(llv, "_b64_gallery_fluff_category", lambda s: None)
    monkeypatch.setattr(llv, "_ollama_vision_chat_json", fake_says_exterior)
    out = llv.classify_listing_image_from_url("https://media.bmw.com/press/hero.jpg", page_referer="https://d.com/v.htm")
    assert out is not None
    assert out["keep"] is False
    assert out["category"] == "url_veto"
    assert out.get("confidence", 1.0) <= 0.4


def test_listing_image_keeps_when_keep_true_and_category_vehicle_synonym(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LLaVA often returns category=vehicle; map to exterior so we do not drop every lot photo."""

    def fake(*, system: str, user_text: str, image_b64_jpeg: str, timeout_s=None):
        return '{"keep": true, "category": "vehicle", "confidence": 0.9}'

    monkeypatch.setattr(llv, "_is_trivially_blank_or_icon_b64", lambda s: False)
    monkeypatch.setattr(llv, "_ollama_vision_chat_json", fake)
    out = llv.classify_listing_image_from_image_b64("Zm9v")
    assert out is not None
    assert out["keep"] is True
    assert out["category"] == "vehicle"


def test_filter_gallery_urls_for_vehicle_listing_drops_not_vehicle(monkeypatch: pytest.MonkeyPatch) -> None:
    urls = ["https://a/1.jpg", "https://a/2.jpg"]

    def fake_classify(u: str, **kwargs: object):
        if "1.jpg" in u:
            return {"keep": True, "category": "exterior", "confidence": 0.9, "model": "x", "image_b64_len": 1}
        return {"keep": False, "category": "not_vehicle", "confidence": 0.8, "model": "x", "image_b64_len": 1}

    monkeypatch.setattr(llv, "classify_listing_image_from_url", fake_classify)
    kept = llv.filter_gallery_urls_for_vehicle_listing(urls, max_workers=1)
    assert kept == ["https://a/1.jpg"]


def test_filter_gallery_urls_legacy_keep_when_classify_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """``None`` from classify is only for legacy ``SCANNER_GALLERY_VISION_KEEP_UNFETCHABLE=1`` + fetch flake."""
    monkeypatch.setattr(llv, "classify_listing_image_from_url", lambda u, **kwargs: None)
    monkeypatch.setenv("SCANNER_GALLERY_VISION_KEEP_UNFETCHABLE", "1")
    urls = ["https://a/x.jpg"]
    assert llv.filter_gallery_urls_for_vehicle_listing(urls) == urls


def test_filter_gallery_urls_drops_unfetchable_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SCANNER_GALLERY_VISION_KEEP_UNFETCHABLE", raising=False)
    monkeypatch.setattr(
        llv,
        "classify_listing_image_from_url",
        lambda u, **kwargs: {
            "keep": False,
            "category": "unfetchable",
            "confidence": 0.0,
            "model": "t",
            "image_b64_len": 0,
        },
    )
    assert llv.filter_gallery_urls_for_vehicle_listing(["https://a/x.jpg"]) == []


def test_heuristic_drops_unparseable_url(monkeypatch: pytest.MonkeyPatch) -> None:
    def bad_parse(_url: str) -> object:
        raise ValueError("forced urlparse failure")

    monkeypatch.setattr(llv, "urlparse", bad_parse)
    assert llv.heuristic_drop_gallery_listing_url("https://dealer.example/ok.jpg") is True


def test_heuristic_drops_kbb_before_vision(monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[str] = []

    def track(u: str, **kwargs: object) -> dict:
        called.append(u)
        return {"keep": True, "category": "exterior", "confidence": 0.9, "model": "x", "image_b64_len": 1}

    monkeypatch.setattr(llv, "classify_listing_image_from_url", track)
    out = llv.filter_gallery_urls_for_vehicle_listing(
        [
            "https://www.kbb.com/autos/widget/badge.png",
            "https://dealer.example/stock/photo-123.jpg",
        ]
    )
    assert "kbb.com" not in " ".join(called)
    assert "dealer.example" in called[0]
    assert out == ["https://dealer.example/stock/photo-123.jpg"]


def test_filter_gallery_fallback_when_all_dropped(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    not_vehicle and similar labels do not re-enter the gallery in fallback (avoid junk).
    With a soft “inconclusive” drop, fallback=1 can restore in lot-score order.
    """
    urls = [
        "https://a.example/p/x1.jpg",
        "https://a.example/p/x2.jpg",
    ]

    def fake_inconclusive(u: str, **kwargs: object) -> dict:
        return {"keep": False, "category": "inconclusive", "confidence": 0.3, "model": "x", "image_b64_len": 1}

    def fake_not_vehicle(_u: str, **kwargs: object) -> dict:
        return {"keep": False, "category": "not_vehicle", "confidence": 0.9, "model": "x", "image_b64_len": 1}

    monkeypatch.setattr(llv, "classify_listing_image_from_url", fake_not_vehicle)
    monkeypatch.setenv("SCANNER_GALLERY_VISION_FALLBACK_ON_EMPTY", "1")
    assert llv.filter_gallery_urls_for_vehicle_listing(urls, max_workers=1) == []

    monkeypatch.setattr(llv, "classify_listing_image_from_url", fake_inconclusive)
    monkeypatch.setenv("SCANNER_GALLERY_VISION_FALLBACK_ON_EMPTY", "1")
    kept = llv.filter_gallery_urls_for_vehicle_listing(urls, max_workers=1)
    assert len(kept) == 2

    monkeypatch.setenv("SCANNER_GALLERY_VISION_FALLBACK_ON_EMPTY", "0")
    assert llv.filter_gallery_urls_for_vehicle_listing(urls, max_workers=1) == []


def test_filter_gallery_sorts_higher_lot_score_first(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_classify(u: str, **kwargs: object) -> dict:
        return {"keep": True, "category": "exterior", "confidence": 0.9, "model": "x", "image_b64_len": 1}

    monkeypatch.setattr(llv, "classify_listing_image_from_url", fake_classify)
    # Same category/confidence: dealer "inventory" path outranks static OEM path
    urls = [
        "https://static.bmwusa.com/press/hero.jpg",
        "https://cdn.dealerinspire.com/vehicle-photos/1.jpg",
    ]
    kept = llv.filter_gallery_urls_for_vehicle_listing(urls, max_workers=1)
    assert kept[0] == "https://cdn.dealerinspire.com/vehicle-photos/1.jpg"


def test_analyze_interior_from_image_url_parses_strict_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(llv, "_fetch_image_b64_optimized", lambda url, **kwargs: "Zm9v")

    class FakeResp:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "message": {
                    "content": (
                        '{"interior_buckets":["black"],'
                        '"interior_guess_text":"Charcoal leather",'
                        '"confidence":0.82,'
                        '"evidence":"dark seats"}'
                    )
                }
            }

    def fake_post(url: str, json=None, timeout=None, **kwargs):
        assert "/api/chat" in url
        return FakeResp()

    monkeypatch.setattr(llv.requests, "post", fake_post)
    out = llv.analyze_interior_from_image_url("https://example.com/photo.jpg")
    assert out is not None
    assert out["interior_buckets"] == ["black"]
    assert out["confidence"] == pytest.approx(0.82)
    assert "Charcoal" in out["interior_guess_text"]
    assert out.get("inference_context") == "cabin"
    assert "seat" in llv._SYSTEM_PROMPT.lower()


def test_analyze_interior_guess_material_only_is_upgraded_to_color(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(llv, "_fetch_image_b64_optimized", lambda url: "Zm9v")

    class FakeResp:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "message": {
                    "content": (
                        '{"interior_buckets":["black","gray"],'
                        '"interior_guess_text":"leather",'
                        '"confidence":0.91,'
                        '"evidence":"seats visible"}'
                    )
                }
            }

    monkeypatch.setattr(llv.requests, "post", lambda *a, **k: FakeResp())
    out = llv.analyze_interior_from_image_url("https://example.com/photo.jpg")
    assert out is not None
    assert "Black" in out["interior_guess_text"] or "Gray" in out["interior_guess_text"]

    captured: dict[str, str] = {}

    def fake_chat_ctx(*, system: str, user_text: str, image_b64_jpeg: str, timeout_s=None):
        captured["system"] = system
        captured["user"] = user_text
        return (
            '{"interior_buckets":["tan"],"interior_guess_text":"Tan seats",'
            '"confidence":0.45,"evidence":"dim view through side window"}'
        )

    monkeypatch.setattr(llv, "_ollama_vision_chat_json", fake_chat_ctx)
    out2 = llv.analyze_interior_from_image_url(
        "https://example.com/exterior.jpg",
        inference_context="through_windows",
    )
    assert out2 is not None
    assert out2.get("inference_context") == "through_windows"
    assert "through" in captured["system"].lower() or "window" in captured["system"].lower()
    assert "glass" in captured["user"].lower() or "window" in captured["user"].lower()
    assert "seat" in captured["system"].lower() or "seat" in captured["user"].lower()


def test_analyze_interior_non_json_reply_returns_low_conf_other(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(llv, "_fetch_image_b64_optimized", lambda url: "Zm9v")

    def fake_chat(*, system: str, user_text: str, image_b64_jpeg: str, timeout_s=None):
        return "The interior is not visible through glass. Sorry!"

    monkeypatch.setattr(llv, "_ollama_vision_chat_json", fake_chat)
    out = llv.analyze_interior_from_image_url("https://example.com/x.jpg", inference_context="through_windows")
    assert out is not None
    assert out["interior_buckets"] == ["other"]
    assert float(out["confidence"]) < 0.2
    assert out.get("parse_error") == "non_json"


def test_analyze_interior_non_json_salvages_color_words(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(llv, "_fetch_image_b64_optimized", lambda url: "Zm9v")

    def fake_chat(*, system: str, user_text: str, image_b64_jpeg: str, timeout_s=None):
        return "The seat upholstery color in this vehicle appears to be black leather."

    monkeypatch.setattr(llv, "_ollama_vision_chat_json", fake_chat)
    out = llv.analyze_interior_from_image_url("https://example.com/x.jpg", inference_context="cabin")
    assert out is not None
    assert out.get("parse_error") == "non_json"
    assert "black" in out["interior_buckets"]
    gt = out.get("interior_guess_text") or ""
    assert "Black" in gt
    assert "leather" in gt.lower()
