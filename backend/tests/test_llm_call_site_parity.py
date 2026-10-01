"""Golden parity for every Anthropic call site (audit 2026-10-01, enrich.md P1 #6).

Each call site is driven with a realistic input (car 89732 from the local
inventory: 2025 Toyota Camry LE, VIN 4T1DAACK0SU083969) against a fake
transport that answers either the raw-HTTP path (``requests.post``) or the SDK
path (``anthropic.Anthropic().messages.create``). The harness records:

* the request shape the call site sends: model, max_tokens, system, messages,
  temperature, timeout (image bytes replaced by their sha256), and
* what the call site returns for four model outcomes: ok, fenced JSON,
  max_tokens truncation, and an API ``refusal`` stop.

The golden file was captured from the pre-consolidation code (raw urllib /
direct SDK / claude_rate_limit wrapper). After the move to
``backend.llm.client`` the same harness must reproduce it, except for the
entries listed in ``EXPECTED_CHANGES`` (each one a bug fix, with the reason).

Regenerate (only when a change is intended):  LLM_GOLDEN_REGEN=1 pytest this file.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import types
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

GOLDEN = Path(__file__).parent / "fixtures" / "llm_call_site_goldens.json"

CAR = {
    "id": 89732,
    "vin": "4T1DAACK0SU083969",
    "year": 2025,
    "make": "Toyota",
    "model": "Camry",
    "trim": "LE",
    "image_url": "https://cdn.example.com/89732/1.jpg",
}
DESCRIPTION = (
    "2025 Toyota Camry LE: Redefining the Daily Drive The 2025 Toyota Camry LE sets the "
    "benchmark for modern midsize sedans, blending exceptional fuel economy with sophisticated "
    "style and everyday utility. Dressed in striking Celestial Silver Metallic, this "
    "front-wheel-drive sedan has a Black Fabric interior and the Convenience Package with "
    "power driver seat and wireless charging."
)
STICKER_TEXT = (
    "2025 CAMRY LE  VIN 4T1DAACK0SU083969\nBASE PRICE $28,400\n"
    "CONVENIENCE PACKAGE  $1,050\nCARPET FLOOR MATS (FE) $299\nTOTAL MSRP $30,844"
)

OK_DICT = (
    '{"observed_features": ["Wireless Charging Pad"], "options": [{"name": "Convenience Package", '
    '"price": 1050, "code": "CP"}], "interior_color_bucket": "black", "confidence": 0.9, '
    '"summary": "dealer site 403", "category": "network", "root_cause": "blocked", '
    '"retry_recommended": true, "retry_strategy": "simple_retry", "retry_actions": [], '
    '"engine_l": 2.5, "cylinders": 4, "transmission": "8-Speed Automatic", "drivetrain": "FWD", '
    '"fuel_type": "Gasoline", "mpg_city": 28, "mpg_highway": 39, '
    '"interior_color_hint": "black fabric", "packages": []}'
)
OK_ARRAY = '[{"idx": 0, "keep": false, "category": "dealer_banner", "quality": "good"}]'
TRUNC = '{"observed_features": ["Wireless Charging Pad", "Pano'

SCENARIOS = {
    "ok": ("end_turn", "{OK}"),
    "fenced": ("end_turn", "```json\n{OK}\n```"),
    "truncated": ("max_tokens", TRUNC),
    "refusal": ("refusal", None),  # API refusal stop: no content blocks
}


# ---------------------------------------------------------------------------
# Fake transports
# ---------------------------------------------------------------------------


class _Block:
    def __init__(self, text: str) -> None:
        self.type = "text"
        self.text = text


class _Msg:
    def __init__(self, stop: str, text: str | None) -> None:
        self.content = [] if text is None else [_Block(text)]
        self.stop_reason = stop
        self.model = "fake"
        self.usage = None


def _norm_content(content: Any) -> Any:
    if isinstance(content, list):
        out = []
        for b in content:
            if isinstance(b, dict) and b.get("type") == "image":
                src = dict(b["source"])
                src["data"] = "sha256:" + hashlib.sha256(src["data"].encode()).hexdigest()[:16]
                out.append({**b, "source": src})
            else:
                out.append(b)
        return out
    return content


def _shape(body: dict[str, Any], timeout: Any) -> dict[str, Any]:
    msgs = [
        {"role": m["role"], "content": _norm_content(m["content"])} for m in body.get("messages") or []
    ]
    return {
        "model": body.get("model"),
        "max_tokens": body.get("max_tokens"),
        "system": body.get("system"),
        "temperature": body.get("temperature"),
        "messages": msgs,
        "timeout": timeout,
    }


class Recorder:
    def __init__(self, stop: str, text: str | None) -> None:
        self.stop, self.text = stop, text
        self.requests: list[dict[str, Any]] = []

    # raw HTTP path
    def post(self, url: str, *a: Any, **kw: Any) -> Any:
        assert "api.anthropic.com" in url
        self.requests.append(_shape(kw.get("json") or {}, kw.get("timeout")))
        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {}
        resp.raise_for_status.return_value = None
        content = [] if self.text is None else [{"type": "text", "text": self.text}]
        resp.json.return_value = {"content": content, "stop_reason": self.stop}
        return resp

    # SDK path
    def sdk_module(self) -> types.ModuleType:
        rec = self

        class _Messages:
            def create(self, **kw: Any) -> _Msg:
                timeout = kw.pop("timeout", None)
                rec.requests.append(_shape(kw, timeout))
                return _Msg(rec.stop, rec.text)

        class Anthropic:
            def __init__(self, api_key: str | None = None, **kw: Any) -> None:
                self.api_key = api_key
                self.messages = _Messages()

        real = sys.modules.get("anthropic")
        mod = types.ModuleType("anthropic")
        if real is not None:  # keep exception classes for isinstance checks
            for name in dir(real):
                if name.endswith("Error"):
                    setattr(mod, name, getattr(real, name))
        mod.Anthropic = Anthropic
        return mod


def _jpeg_bytes() -> bytes:
    from io import BytesIO

    from PIL import Image

    buf = BytesIO()
    Image.new("RGB", (900, 600), (40, 40, 40)).save(buf, format="JPEG", quality=90)
    return buf.getvalue()


def _image_get(*a: Any, **kw: Any) -> Any:
    r = MagicMock()
    r.status_code = 200
    r.content = _jpeg_bytes()
    r.raise_for_status.return_value = None
    return r


# ---------------------------------------------------------------------------
# Call sites
# ---------------------------------------------------------------------------


def _site_haiku_specs() -> Any:
    from backend.enrichment import service

    with patch.object(service, "_load_haiku_cache", return_value=None), patch.object(
        service, "_save_haiku_cache"
    ):
        return service._ask_haiku_specs("Toyota", "Camry", 2025, "LE")


def _site_vision_analyze_car() -> Any:
    from backend.enrichment import service

    with patch.object(service, "_fetch_image_b64_optimized", return_value="e30="):
        return service._vision_analyze_car(dict(CAR), None, url_override=CAR["image_url"])


def _site_sticker_image() -> Any:
    from backend.enrichment import window_sticker_service as ws

    return ws._analyze_sticker_image_with_claude(b"\xff\xd8\xff\xe0" + b"\x00" * 9000, dict(CAR))


def _site_sticker_text() -> Any:
    from backend.enrichment import window_sticker_service as ws

    return ws._analyze_sticker_text_with_claude(STICKER_TEXT, dict(CAR))


def _site_agent_chat() -> Any:
    from backend.intelligence.ai import agent

    return agent._claude_reply("You are a car assistant.", "Does it have AWD?", max_tokens=600)


def _site_llm_client() -> Any:
    from backend.utils import llm_client

    return llm_client.complete("Summarize this car.", system="Be brief.", provider="claude")


def _site_job_diagnosis() -> Any:
    from backend.scanner import job_diagnosis

    return job_diagnosis._llm_diagnosis(
        error="HTTP 403 on inventory feed",
        log_tail="GET https://www.example-toyota.com/apis/widget 403",
        job_type="scan",
        dealer_id="example-toyota",
        payload={"dealer_id": "example-toyota"},
    )


def _site_interior_vision() -> Any:
    from backend.scanner.post_scan import pipeline

    return pipeline._analyze_interior_with_claude("https://cdn.example.com/89732/interior.jpg")


def _site_listing_description() -> Any:
    from backend.utils import listing_description_extract as lde

    return lde._llm_extract(DESCRIPTION, dict(CAR))


def _site_gallery_classify() -> Any:
    from backend.vision import claude_vision as cv

    with patch.object(cv, "_fetch_image_b64", return_value="e30="):
        return cv._classify_batch(
            [(0, "https://cdn.example.com/89732/0.jpg"), (1, "https://cdn.example.com/89732/1.jpg")],
            "https://www.example-toyota.com/",
            "sk-test",
        )


def _site_equipment_batch() -> Any:
    from backend.vision import claude_vision as cv

    with patch.object(cv, "_fetch_image_b64", return_value="e30="):
        return cv.analyze_equipment_from_image_urls(
            ["https://cdn.example.com/89732/0.jpg", "https://cdn.example.com/89732/1.jpg"]
        )


def _site_brochure() -> Any:
    from backend.scripts import analyze_brochure_with_llm as br

    client = br._get_client()
    return br._call_claude(client, "2025 Toyota Camry trims: LE, SE, XLE, XSE")


SITES = {
    "enrichment.haiku_specs": _site_haiku_specs,
    "enrichment.vision_analyze_car": _site_vision_analyze_car,
    "window_sticker.image": _site_sticker_image,
    "window_sticker.text": _site_sticker_text,
    "agent.claude_reply": _site_agent_chat,
    "llm_client.complete_claude": _site_llm_client,
    "job_diagnosis.llm": _site_job_diagnosis,
    "post_scan.interior_vision": _site_interior_vision,
    "listing_description.llm_extract": _site_listing_description,
    "claude_vision.classify_batch": _site_gallery_classify,
    "claude_vision.equipment_batch": _site_equipment_batch,
    "brochure.call_claude": _site_brochure,
}

# (site, scenario) -> reason. Each is a deliberate answer change (bug fix).
EXPECTED_CHANGES: dict[tuple[str, str], str] = {
    ("enrichment.haiku_specs", "truncated"): (
        "was: partial JSON repaired into a spec dict and cached per make/model/year/trim; "
        "now: max_tokens cut -> None, nothing cached"
    ),
    ("enrichment.vision_analyze_car", "refusal"): (
        "was: API refusal logged as 'model returned non-JSON' -> None (a failure); "
        "now: VisionRefusal, the refusal bucket"
    ),
    ("claude_vision.equipment_batch", "refusal"): (
        "was: {} = 'assessed, nothing observable' (refusal counted as success); "
        "now: VisionRefusal"
    ),
    ("brochure.call_claude", "refusal"): (
        "was: IndexError on empty content; now: LLMRefusal (caller reports error:api:...)"
    ),
    ("brochure.call_claude", "truncated"): (
        "was: partial JSON text returned for parse/repair into an overlay; now: LLMTruncated"
    ),
}

# What each changed case must now return.
EXPECTED_NEW_RESULTS: dict[tuple[str, str], dict[str, Any]] = {
    ("enrichment.haiku_specs", "truncated"): {"type": "NoneType", "value": None},
    ("enrichment.vision_analyze_car", "refusal"): {"type": "VisionRefusal", "value": {}},
    ("claude_vision.equipment_batch", "refusal"): {"type": "VisionRefusal", "value": {}},
    ("brochure.call_claude", "refusal"): {"type": "raises", "exc": "LLMRefusal"},
    ("brochure.call_claude", "truncated"): {"type": "raises", "exc": "LLMTruncated"},
}


def _serialize(out: Any) -> Any:
    tag = type(out).__name__
    if isinstance(out, BaseException):
        return {"type": "raises", "exc": tag}
    try:
        val = json.loads(json.dumps(out, default=str))
    except (TypeError, ValueError):
        val = repr(out)
    if isinstance(out, list):  # tuples become lists; keep stable
        val = json.loads(json.dumps(out, default=str))
    extra = {}
    for attr in ("truncated", "stop_reason", "provider"):
        if hasattr(out, attr) and not isinstance(out, dict):
            extra[attr] = getattr(out, attr)
    return {"type": tag, "value": val, **extra}


def _run(site: str, scenario: str, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    stop, text = SCENARIOS[scenario]
    ok = OK_ARRAY if site == "claude_vision.classify_batch" else OK_DICT
    if text is not None:
        text = text.replace("{OK}", ok)
    rec = Recorder(stop, text)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-api03-parity-test-key")
    monkeypatch.setenv("LISTING_DESC_PARSE_USE_LLM", "1")
    monkeypatch.setenv("LLM_PROVIDER", "claude")
    for var in ("LLM_CLAUDE_MODEL", "LLM_CLAUDE_MAX_TOKENS"):
        monkeypatch.setenv(var, "")
    for k in list(os.environ):
        if k.startswith("ANTHROPIC_MODEL_"):
            monkeypatch.delenv(k)
    monkeypatch.setitem(sys.modules, "anthropic", rec.sdk_module())
    monkeypatch.setattr("requests.post", rec.post)
    monkeypatch.setattr("requests.get", _image_get)
    monkeypatch.setattr("backend.vision.claude_rate_limit.acquire_vision_slot", lambda *a, **k: None)
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)
    try:
        out = SITES[site]()
    except Exception as exc:  # noqa: BLE001 - recorded as an outcome
        out = exc
    return {"requests": rec.requests, "result": _serialize(out)}


def _all_cases() -> list[tuple[str, str]]:
    return [(s, sc) for s in SITES for sc in SCENARIOS]


def test_regenerate_goldens(monkeypatch: pytest.MonkeyPatch) -> None:
    if os.environ.get("LLM_GOLDEN_REGEN") != "1":
        pytest.skip("set LLM_GOLDEN_REGEN=1 to recapture")
    data: dict[str, Any] = {}
    for site, scenario in _all_cases():
        with monkeypatch.context() as mp:
            data[f"{site}|{scenario}"] = _run(site, scenario, mp)
    GOLDEN.write_text(json.dumps(data, indent=1, sort_keys=True, ensure_ascii=False) + "\n")


CHAT_SITES = ("agent.claude_reply", "llm_client.complete_claude")


@pytest.mark.parametrize("site,scenario", _all_cases())
def test_call_site_matches_golden(site: str, scenario: str, monkeypatch: pytest.MonkeyPatch) -> None:
    golden = json.loads(GOLDEN.read_text())[f"{site}|{scenario}"]
    got = _run(site, scenario, monkeypatch)
    # Request shape: model, max_tokens, system, content, temperature, timeout.
    want_requests = golden["requests"]
    if site in CHAT_SITES:
        # Deliberate: the chat role now runs under a 20 s overall deadline, so each
        # request carries the time left as its timeout (before: SDK default 600 s).
        want_requests = [dict(r) for r in want_requests]
        for got_r, want_r in zip(got["requests"], want_requests):
            assert want_r["timeout"] is None
            assert 0 < got_r["timeout"] <= 20.0
            want_r["timeout"] = got_r["timeout"]
    assert got["requests"] == want_requests, site
    if (site, scenario) in EXPECTED_CHANGES:
        assert got["result"] != golden["result"], EXPECTED_CHANGES[(site, scenario)]
        assert got["result"] == EXPECTED_NEW_RESULTS[(site, scenario)]
    else:
        assert got["result"] == golden["result"]
