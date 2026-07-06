"""Narrator grounding guards + adaptive model selection (no server needed)."""
from __future__ import annotations

import backend.utils.vehicle_narrator as vn
from backend.utils import local_llm

ROW = {
    "year": 2026, "make": "BMW", "model": "228i", "trim": "Gran Coupe",
    "price": 49745, "mileage": 11, "exterior_color": "Black Sapphire",
    "drivetrain": "FWD", "transmission": "7-Speed Automatic",
    "engine_description": "2.0L I-4 cyl", "body_style": "Coupe",
}


def test_known_facts_formats_and_omits():
    facts = vn._known_facts(ROW)
    assert "Price (USD): $49,745" in facts
    assert "Mileage: 11 miles" in facts
    assert not any("MPG" in f for f in facts)  # absent → omitted, not invented


def test_claim_and_degenerate_detectors():
    assert vn._has_unverifiable_claim("This one-owner gem is pristine.")
    assert not vn._has_unverifiable_claim("A 2026 BMW 228i in Black Sapphire.")
    assert vn._is_degenerate("Year: 2026<br>Make: BMW<br>Model: 228i")
    assert vn._is_degenerate("Year: 2026\nMake: BMW\nModel: 228i\nTrim: x")
    assert not vn._is_degenerate("This 2026 BMW 228i is finished in Black Sapphire.")


def test_banned_word_allowed_when_in_facts():
    # "Pristine White" is a real color value we handed the model — must NOT be flagged.
    values = ["Pristine White", "Navigator", "Reserve"]
    assert not vn._has_unverifiable_claim("A sedan in Pristine White paint.", values)
    # but an invented "pristine condition" (the value phrase isn't there) is still caught.
    assert vn._has_unverifiable_claim("In pristine condition throughout.", values)
    # bare call (no values) still catches hype
    assert vn._has_unverifiable_claim("This immaculate one-owner gem.")


def test_degenerate_catches_single_line_semicolon_dump():
    dump = ("Year: 2025; Make: Honda; Model: CR-V; Trim: LX; Body style: SUV; "
            "Price: $31,542; Mileage: 18,852")
    assert vn._is_degenerate(dump)
    assert not vn._is_degenerate(
        "This 2025 Honda CR-V LX is an SUV priced at $31,542 with 18,852 miles."
    )


def test_narrate_regenerates_on_claim(monkeypatch):
    calls = []

    def fake_generate(prompt, **kw):
        calls.append(prompt)
        return ("This well-maintained one-owner gem is pristine." if len(calls) == 1
                else "A 2026 BMW 228i Gran Coupe in Black Sapphire with 11 miles.")

    monkeypatch.setattr(vn, "generate", fake_generate)
    out = vn.narrate_vehicle(ROW)
    assert len(calls) == 2  # first tripped the claim guard → regenerated
    assert not vn._has_unverifiable_claim(out)


def test_narrate_falls_back_to_template_when_model_keeps_failing(monkeypatch):
    monkeypatch.setattr(vn, "generate", lambda *a, **k: "Year: 2026<br>Make: BMW<br>Model: 228i")
    out = vn.narrate_vehicle(ROW)
    assert "<br" not in out
    assert "BMW" in out and "228i" in out  # deterministic fact sentence


def test_narrate_strips_html(monkeypatch):
    monkeypatch.setattr(vn, "generate", lambda *a, **k: "A clean <b>2026 BMW</b> 228i in Black Sapphire.")
    assert "<b>" not in vn.narrate_vehicle(ROW)


def test_too_few_facts_uses_template_no_llm(monkeypatch):
    monkeypatch.setattr(vn, "generate", lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not call")))
    out = vn.narrate_vehicle({"make": "BMW"})
    assert "BMW" in out


# ── adaptive model selection ──────────────────────────────────────────────────


def test_pick_model_light_when_scanner_running(monkeypatch):
    monkeypatch.delenv("LOCAL_LLM_FORCE_MODEL", raising=False)
    monkeypatch.setattr(local_llm, "_scanner_running", lambda: True)
    c = local_llm.pick_model()
    assert c.tier == "light" and c.reason == "scanner_running"


def test_pick_model_heavy_when_idle(monkeypatch):
    monkeypatch.delenv("LOCAL_LLM_FORCE_MODEL", raising=False)
    monkeypatch.setattr(local_llm, "_scanner_running", lambda: False)
    monkeypatch.setattr(local_llm, "_free_ram_gb", lambda: 20.0)
    assert local_llm.pick_model().tier == "heavy"


def test_pick_model_light_on_low_ram(monkeypatch):
    monkeypatch.delenv("LOCAL_LLM_FORCE_MODEL", raising=False)
    monkeypatch.setattr(local_llm, "_scanner_running", lambda: False)
    monkeypatch.setattr(local_llm, "_free_ram_gb", lambda: 2.0)
    assert local_llm.pick_model().tier == "light"


def test_force_model_overrides(monkeypatch):
    monkeypatch.setenv("LOCAL_LLM_FORCE_MODEL", "qwen2.5:14b-instruct")
    c = local_llm.pick_model()
    assert c.tier == "forced" and c.model == "qwen2.5:14b-instruct"
