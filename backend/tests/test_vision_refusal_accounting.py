"""Vision refusals are first-class outcomes, never counted as success."""
from __future__ import annotations

import json
import threading
from unittest.mock import MagicMock, patch

from backend.enrichment.service import (
    InventoryEnricher,
    _parse_vision_json_response,
    _stop_reason_is_truncation,
    _vision_analyze_car,
)
from backend.vision.equipment_vision import (
    VisionRefusal,
    analyze_car_equipment_from_gallery,
)


# ---------------------------------------------------------------------------
# Parser: refusal vs parse failure vs payload
# ---------------------------------------------------------------------------


def test_parse_refusal_returns_typed_sentinel() -> None:
    out = _parse_vision_json_response("I cannot identify features in this image.")
    assert isinstance(out, VisionRefusal)
    assert not out  # falsy: legacy "if not vis" guards still skip the merge
    assert isinstance(out, dict)


def test_parse_non_json_returns_none_not_refusal() -> None:
    out = _parse_vision_json_response("The image shows a red truck.")
    assert out is None
    assert not isinstance(out, VisionRefusal)


def test_parse_valid_json_returns_plain_dict() -> None:
    out = _parse_vision_json_response('{"observed_features": ["Tow hitch"]}')
    assert out == {"observed_features": ["Tow hitch"]}
    assert not isinstance(out, VisionRefusal)


# ---------------------------------------------------------------------------
# stop_reason: truncation is detected, not guessed
# ---------------------------------------------------------------------------


def test_stop_reason_truncation_detection() -> None:
    assert _stop_reason_is_truncation("max_tokens") is True
    assert _stop_reason_is_truncation("length") is True
    assert _stop_reason_is_truncation("end_turn") is False
    assert _stop_reason_is_truncation(None) is False


def test_parse_truncated_payload_repaired_with_stop_reason() -> None:
    truncated = '{"observed_features": ["Tow hitch", "Pano'
    out = _parse_vision_json_response(truncated, stop_reason="max_tokens")
    assert isinstance(out, dict)
    assert out.get("observed_features") == ["Tow hitch"]


def _mock_vision_response(payload: dict) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.raise_for_status.return_value = None
    resp.json.return_value = payload
    return resp


def test_vision_analyze_car_reads_stop_reason(monkeypatch, caplog) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    row = {"id": 42, "image_url": "https://cdn.example.com/a.jpg"}
    api_payload = {
        "stop_reason": "max_tokens",
        "content": [{"text": '{"observed_features": ["Tow hitch", "Pano'}],
    }
    with patch("backend.enrichment.service._fetch_image_b64_optimized", return_value="e30="):
        with patch("backend.vision.claude_rate_limit.acquire_vision_slot", lambda: None):
            with patch("requests.post", return_value=_mock_vision_response(api_payload)):
                with caplog.at_level("WARNING"):
                    out = _vision_analyze_car(row, None, url_override=row["image_url"])
    assert out == {"observed_features": ["Tow hitch"]}
    assert any("stop_reason=max_tokens" in r.message for r in caplog.records)


def test_vision_analyze_car_refusal_not_logged_as_non_json(monkeypatch, caplog) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    row = {"id": 42, "image_url": "https://cdn.example.com/a.jpg"}
    api_payload = {
        "stop_reason": "end_turn",
        "content": [{"text": "I'm unable to identify optional equipment here."}],
    }
    with patch("backend.enrichment.service._fetch_image_b64_optimized", return_value="e30="):
        with patch("backend.vision.claude_rate_limit.acquire_vision_slot", lambda: None):
            with patch("requests.post", return_value=_mock_vision_response(api_payload)):
                with caplog.at_level("WARNING"):
                    out = _vision_analyze_car(row, None, url_override=row["image_url"])
    assert isinstance(out, VisionRefusal)
    assert not any("non-JSON" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# Gallery scan: refusals land in their own stats bucket
# ---------------------------------------------------------------------------


def test_gallery_scan_counts_refusals_separately(monkeypatch) -> None:
    monkeypatch.setenv("EQUIPMENT_VISION_BATCH", "0")
    urls = [
        "https://cdn.example.com/1.jpg",
        "https://cdn.example.com/2.jpg",
        "https://cdn.example.com/3.jpg",
    ]
    outcomes = {
        urls[0]: VisionRefusal(),  # model refused
        urls[1]: None,  # fetch/parse failure
        urls[2]: VisionRefusal(),  # model refused
    }
    merged, stats = analyze_car_equipment_from_gallery(
        {"packages": None},
        gallery_urls=urls,
        analyze_url=lambda row, url: outcomes[url],
        merge_observations=lambda existing, vis: json.dumps(vis),
    )
    assert merged is None
    assert stats["refusals"] == 2  # None (failure) is NOT a refusal
    assert stats["urls_analyzed"] == 0


def test_gallery_scan_all_empty_is_assessed_not_refused(monkeypatch) -> None:
    """A plain {} means the model answered "nothing observable" — an assessed
    image. It must NOT land in the refusals bucket, or an all-empty car comes
    back ok=False/refused=True and is re-queued forever."""
    monkeypatch.setenv("EQUIPMENT_VISION_BATCH", "0")
    urls = ["https://cdn.example.com/1.jpg", "https://cdn.example.com/2.jpg"]
    merged, stats = analyze_car_equipment_from_gallery(
        {"packages": None},
        gallery_urls=urls,
        analyze_url=lambda row, url: {},  # assessed, nothing observable
        merge_observations=lambda existing, vis: json.dumps(vis),
    )
    assert merged is None  # nothing to write, but that is not a refusal
    assert stats["refusals"] == 0
    assert stats["urls_analyzed"] == len(urls)


def test_gallery_scan_empty_vs_refusal_distinct(monkeypatch) -> None:
    monkeypatch.setenv("EQUIPMENT_VISION_BATCH", "0")
    urls = ["https://cdn.example.com/1.jpg", "https://cdn.example.com/2.jpg"]
    outcomes = {
        urls[0]: VisionRefusal(),  # model refused
        urls[1]: {},  # model assessed, nothing observable
    }
    merged, stats = analyze_car_equipment_from_gallery(
        {"packages": None},
        gallery_urls=urls,
        analyze_url=lambda row, url: outcomes[url],
        merge_observations=lambda existing, vis: json.dumps(vis),
    )
    assert merged is None
    assert stats["refusals"] == 1
    assert stats["urls_analyzed"] == 1


def test_gallery_scan_mixed_refusal_and_success(monkeypatch) -> None:
    monkeypatch.setenv("EQUIPMENT_VISION_BATCH", "0")
    urls = ["https://cdn.example.com/1.jpg", "https://cdn.example.com/2.jpg"]
    outcomes = {
        urls[0]: VisionRefusal(),
        urls[1]: {"observed_features": ["Tow hitch"]},
    }
    merged, stats = analyze_car_equipment_from_gallery(
        {"packages": None},
        gallery_urls=urls,
        analyze_url=lambda row, url: outcomes[url],
        merge_observations=lambda existing, vis: json.dumps(vis),
    )
    assert merged is not None
    assert stats["refusals"] == 1
    assert stats["urls_analyzed"] == 1


# ---------------------------------------------------------------------------
# Batch lane: a whole-batch refusal is a refusal, not a transport failure
# ---------------------------------------------------------------------------


def test_batch_call_refusal_returns_typed_sentinel(monkeypatch) -> None:
    """analyze_equipment_from_image_urls used to collapse a model refusal into
    ``{}`` via the generic exception handler ("batch failed"), so the gallery
    scan could never count it. It must come back as the typed sentinel."""
    from backend.vision import claude_vision

    monkeypatch.setattr(claude_vision, "_api_key", lambda: "test-key")
    monkeypatch.setattr(claude_vision, "_fetch_image_b64", lambda u, r: "e30=")
    msg = MagicMock()
    msg.content = [MagicMock(text="I'm unable to identify optional equipment in these photos.")]
    with patch("backend.vision.claude_rate_limit.anthropic_messages_create", return_value=msg):
        out = claude_vision.analyze_equipment_from_image_urls(["https://cdn.example.com/1.jpg"])
    assert isinstance(out, VisionRefusal)
    assert not out


def test_batch_call_non_json_still_plain_empty(monkeypatch) -> None:
    from backend.vision import claude_vision

    monkeypatch.setattr(claude_vision, "_api_key", lambda: "test-key")
    monkeypatch.setattr(claude_vision, "_fetch_image_b64", lambda u, r: "e30=")
    msg = MagicMock()
    msg.content = [MagicMock(text="The photos show a red truck with alloy wheels.")]
    with patch("backend.vision.claude_rate_limit.anthropic_messages_create", return_value=msg):
        out = claude_vision.analyze_equipment_from_image_urls(["https://cdn.example.com/1.jpg"])
    assert out == {}
    assert not isinstance(out, VisionRefusal)


def test_gallery_batch_refusal_counted_not_success(monkeypatch) -> None:
    monkeypatch.setenv("EQUIPMENT_VISION_BATCH", "1")
    urls = ["https://cdn.example.com/1.jpg", "https://cdn.example.com/2.jpg"]
    with patch(
        "backend.vision.claude_vision.analyze_equipment_from_image_urls",
        return_value=VisionRefusal(),
    ):
        merged, stats = analyze_car_equipment_from_gallery(
            {"packages": None},
            gallery_urls=urls,
            analyze_url=lambda row, url: VisionRefusal(),  # per-url pass also refuses
            merge_observations=lambda existing, vis: json.dumps(vis),
        )
    assert merged is None  # never stamped as scanned
    assert stats["urls_analyzed"] == 0
    assert stats["refusals"] == 1 + len(urls)  # batch refusal + per-url refusals


# ---------------------------------------------------------------------------
# enrich_one: all-refused car is surfaced as refused, not ok
# ---------------------------------------------------------------------------


def _bare_enricher() -> InventoryEnricher:
    """Instance without __init__ (no catalog/model/DB side effects)."""
    e = object.__new__(InventoryEnricher)
    e._catalog_lock = threading.Lock()
    e._db_write_lock = threading.Lock()
    e._write_buffer = []
    e._prefetch_cache = None
    return e


def _patch_needs(monkeypatch, row: dict) -> None:
    monkeypatch.setattr("backend.enrichment.service.get_car_by_id", lambda cid: dict(row))
    monkeypatch.setattr("backend.enrichment.service._needs_catalog", lambda r: False)
    monkeypatch.setattr("backend.enrichment.service._needs_vision", lambda r: True)


def test_enrich_one_all_refused_is_not_ok(monkeypatch) -> None:
    row = {"id": 7, "image_url": "https://cdn.example.com/a.jpg"}
    _patch_needs(monkeypatch, row)
    e = _bare_enricher()
    monkeypatch.setattr(
        e, "apply_vision", lambda r: ({}, [], {"refusals": 3, "urls_analyzed": 0})
    )
    out = e.enrich_one(7)
    assert out["ok"] is False
    assert out["refused"] is True
    assert out["vision_refusals"] == 3
    assert out["healed"] == []


def test_enrich_one_partial_refusal_with_heals_stays_ok(monkeypatch) -> None:
    row = {"id": 7, "image_url": "https://cdn.example.com/a.jpg"}
    _patch_needs(monkeypatch, row)
    e = _bare_enricher()
    monkeypatch.setattr(
        e,
        "apply_vision",
        lambda r: ({"packages": "{}"}, ["vision_observations"], {"refusals": 1, "urls_analyzed": 2}),
    )
    monkeypatch.setattr(e, "save_enriched_data", lambda cid, data: list(data.keys()))
    out = e.enrich_one(7)
    assert out["ok"] is True
    assert "refused" not in out
    assert out["vision_refusals"] == 1
    assert out["healed"] == ["vision_observations"]


def test_enrich_one_no_refusals_no_refused_key(monkeypatch) -> None:
    row = {"id": 7, "image_url": "https://cdn.example.com/a.jpg"}
    _patch_needs(monkeypatch, row)
    e = _bare_enricher()
    monkeypatch.setattr(
        e, "apply_vision", lambda r: ({}, [], {"refusals": 0, "urls_analyzed": 0})
    )
    out = e.enrich_one(7)
    assert out["ok"] is True
    assert "refused" not in out
    assert "vision_refusals" not in out


# ---------------------------------------------------------------------------
# run_all: third bucket — refused is neither ok nor error
# ---------------------------------------------------------------------------


def test_run_all_counts_refused_bucket(monkeypatch) -> None:
    fake_conn = MagicMock()
    monkeypatch.setattr("backend.enrichment.service.get_conn", lambda: fake_conn)
    monkeypatch.setattr(
        "backend.enrichment.service.ensure_enrichment_columns", lambda conn: None
    )
    monkeypatch.setattr(
        "backend.enrichment.service.fetch_enrichment_candidate_ids",
        lambda conn, **kw: [1, 2, 3],
    )
    monkeypatch.setattr(
        "backend.enrichment.service._fetch_vision_urls_for_ids", lambda conn, ids: {}
    )

    results = {
        1: {"ok": True, "id": 1, "healed": ["fuel type"]},
        2: {"ok": False, "refused": True, "id": 2, "healed": [], "vision_refusals": 4},
        3: {"ok": False, "error": "exception", "id": 3},
    }
    monkeypatch.setattr(
        InventoryEnricher,
        "_enrich_one_job",
        lambda self, cid, vision_only: results[cid],
    )

    e = _bare_enricher()
    stats = e.run_all(max_workers=1)
    assert stats["processed"] == 3
    assert stats["ok"] == 1
    assert stats["refused"] == 1
    assert stats["errors"] == 1
