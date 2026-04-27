"""Ollama LLaVA client (mocked HTTP; no live Ollama)."""

from __future__ import annotations

import pytest

import backend.vision.ollama_llava as llv


def test_classify_listing_image_from_image_b64_keep_and_drop(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_chat(*, system: str, user_text: str, image_b64_jpeg: str, timeout_s=None):
        assert "window sticker" in system
        return '{"keep": true, "category": "exterior", "confidence": 0.91}'

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


def test_filter_gallery_urls_for_vehicle_listing_drops_not_vehicle(monkeypatch: pytest.MonkeyPatch) -> None:
    urls = ["https://a/1.jpg", "https://a/2.jpg"]

    def fake_classify(u: str):
        if "1.jpg" in u:
            return {"keep": True, "category": "exterior", "confidence": 0.9, "model": "x", "image_b64_len": 1}
        return {"keep": False, "category": "not_vehicle", "confidence": 0.8, "model": "x", "image_b64_len": 1}

    monkeypatch.setattr(llv, "classify_listing_image_from_url", fake_classify)
    kept = llv.filter_gallery_urls_for_vehicle_listing(urls, max_workers=1)
    assert kept == ["https://a/1.jpg"]


def test_filter_gallery_urls_keeps_on_classify_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(llv, "classify_listing_image_from_url", lambda u: None)
    urls = ["https://a/x.jpg"]
    assert llv.filter_gallery_urls_for_vehicle_listing(urls) == urls


def test_analyze_interior_from_image_url_parses_strict_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(llv, "_fetch_image_b64_optimized", lambda url: "Zm9v")

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
