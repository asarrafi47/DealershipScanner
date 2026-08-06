"""
Trust rules for the local VLM escalation (backend/vision/vlm_ollama.py).

The model is used to answer questions OCR cannot -- which dealership a car is standing
at, whether an image is a document. That makes it an attribution input, and a wrong
attribution is the failure this project keeps paying for. So what is pinned here is not
the model's cleverness but the boundary around it: a low-confidence answer, a null, or a
malformed reply must all reach the caller as "no evidence", never as a dealer name.

No network: every test stubs the HTTP call.
"""

from __future__ import annotations

import json

from backend.vision import vlm_ollama


def _stub(monkeypatch, payload: object) -> None:
    """Make the model return *payload* as its response body."""
    text = payload if isinstance(payload, str) else json.dumps(payload)
    monkeypatch.setattr(vlm_ollama, "_ask", lambda *a, **k: (
        json.loads(text) if text.strip().startswith("{") else None
    ))


def test_confident_dealer_is_returned(monkeypatch) -> None:
    _stub(monkeypatch, {"dealer_name": "Audi Huntsville", "evidence": "signage", "confidence": 0.95})
    out = vlm_ollama.identify_dealer("/tmp/x.jpg")
    assert out and out["dealer_name"] == "Audi Huntsville"
    assert out["confidence"] == 0.95


def test_low_confidence_dealer_is_discarded(monkeypatch) -> None:
    """A guess from weak cues must not become a rooftop."""
    _stub(monkeypatch, {"dealer_name": "Audi Huntsville", "evidence": "maybe", "confidence": 0.40})
    assert vlm_ollama.identify_dealer("/tmp/x.jpg") is None


def test_explicit_null_is_respected(monkeypatch) -> None:
    """The model returning 'no evidence' is the behaviour that makes this safe."""
    _stub(monkeypatch, {"dealer_name": None, "evidence": "no signage visible", "confidence": 0.9})
    assert vlm_ollama.identify_dealer("/tmp/x.jpg") is None


def test_stringified_null_is_not_a_dealer_name(monkeypatch) -> None:
    for value in ("null", "none", "unknown", "  "):
        _stub(monkeypatch, {"dealer_name": value, "evidence": "", "confidence": 0.99})
        assert vlm_ollama.identify_dealer("/tmp/x.jpg") is None, value


def test_missing_confidence_is_treated_as_zero(monkeypatch) -> None:
    """An answer with no stated confidence is not an answer worth acting on."""
    _stub(monkeypatch, {"dealer_name": "Somewhere Ford", "evidence": "x"})
    assert vlm_ollama.identify_dealer("/tmp/x.jpg") is None


def test_unparseable_reply_yields_none(monkeypatch) -> None:
    monkeypatch.setattr(vlm_ollama, "_ask", lambda *a, **k: None)
    assert vlm_ollama.identify_dealer("/tmp/x.jpg") is None
    assert vlm_ollama.classify_document("/tmp/x.jpg") is None


def test_document_kind_must_be_from_the_vocabulary(monkeypatch) -> None:
    _stub(monkeypatch, {"kind": "invoice", "has_prices": True, "confidence": 0.99})
    assert vlm_ollama.classify_document("/tmp/x.jpg") is None


def test_document_classification_passes_through(monkeypatch) -> None:
    _stub(monkeypatch, {"kind": "window_sticker", "has_prices": True, "confidence": 0.9})
    out = vlm_ollama.classify_document("/tmp/x.jpg")
    assert out == {"kind": "window_sticker", "has_prices": True, "confidence": 0.9}


def test_unavailable_server_is_not_an_error(monkeypatch) -> None:
    """A box without Ollama running must degrade to 'no escalation', not raise."""
    def _boom(*a, **k):
        raise OSError("connection refused")

    monkeypatch.setattr(vlm_ollama.urllib.request, "urlopen", _boom)
    assert vlm_ollama.vlm_available() is False
