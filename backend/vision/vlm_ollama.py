"""
Local vision-language model (Ollama) for the images OCR cannot answer.

Apple Vision reads *text*. That covers the printed cases well -- a Monroney, a "Vehicle
Highlights" slide -- and it is fast and free enough to run across 114,901 galleries. What
it cannot do is answer a question about a picture: whether an image is a window sticker
at all, or which dealership a car is standing in front of when the only evidence is a
building, a logo, or signage rendered as artwork rather than as text.

``qwen3-vl:30b`` running under Ollama answers those. It is local, so there is no API key,
no quota and nothing leaves the machine.

Cost decides where it is used
-----------------------------
Roughly 5 seconds per image against roughly 1 for OCR. Fleet-wide that is the difference
between a day and a decade, so this is strictly an ESCALATION layer:

  * OCR runs everywhere and settles the images that carry legible text.
  * The VLM is asked only where OCR came back empty or unclassified, and only for the
    handful of images per car that could plausibly answer the question.

Trust
-----
The model returns an honest null when the evidence is not there -- asked which dealership
a car sits at, given a photo of an unbranded multi-brand lot, it answered
``{"dealer_name": null, "evidence": "No visible dealership signage..."}`` rather than
guessing. That behaviour is what makes it usable here, and it is enforced rather than
assumed: a name below ``_MIN_CONFIDENCE`` is discarded, and callers must still resolve
any name to a rooftop that exists before acting on it. The model is evidence, never a
verdict.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

_log = logging.getLogger(__name__)

_DEFAULT_HOST = "http://localhost:11434"
_DEFAULT_MODEL = "qwen3-vl:30b"

# Below this the model is guessing from weak cues; a wrong rooftop is worse than none.
_MIN_CONFIDENCE = 0.70

# Generous: a 30B model on first call pays a model-load penalty (~19s observed) before
# settling to ~5s per image.
_TIMEOUT_S = 300


def vlm_host() -> str:
    return (os.environ.get("OLLAMA_HOST") or _DEFAULT_HOST).rstrip("/")


def vlm_model() -> str:
    return (os.environ.get("VLM_MODEL") or _DEFAULT_MODEL).strip()


_AVAILABILITY_CACHE: dict[str, bool] = {}


def vlm_available() -> bool:
    """
    True when the Ollama server is up and the configured model is present.

    Cached: a fleet pass calls this once per image batch, and re-probing /api/tags
    thousands of times adds latency to a path that is already the slow one.
    """
    key = f"{vlm_host()}|{vlm_model()}"
    if key in _AVAILABILITY_CACHE:
        return _AVAILABILITY_CACHE[key]
    _AVAILABILITY_CACHE[key] = _probe_availability()
    return _AVAILABILITY_CACHE[key]


def _probe_availability() -> bool:
    try:
        with urllib.request.urlopen(f"{vlm_host()}/api/tags", timeout=5) as resp:
            payload = json.loads(resp.read())
    except (urllib.error.URLError, OSError, ValueError):
        return False
    names = {m.get("name", "") for m in payload.get("models", [])}
    want = vlm_model()
    return any(n == want or n.startswith(want.split(":")[0]) for n in names)


def _ask(image_path: Path | str, prompt: str, *, expect_json: bool = True) -> Any | None:
    """
    One image + one prompt.

    Returns a parsed JSON object by default. With ``expect_json=False`` the raw text is
    returned instead, which is what the transcription path in image_text wants -- a
    verbatim reading of a window sticker is not JSON and must not be forced through a
    JSON parser that would discard it.
    """
    p = Path(image_path)
    if not p.exists():
        return None
    try:
        blob = base64.b64encode(p.read_bytes()).decode()
    except OSError:
        return None

    body = json.dumps({
        "model": vlm_model(),
        "prompt": prompt,
        "images": [blob],
        "stream": False,
        # Deterministic: the same photo must not yield a different dealer on a re-run.
        "options": {"temperature": 0},
    }).encode()

    try:
        req = urllib.request.Request(
            f"{vlm_host()}/api/generate", data=body, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=_TIMEOUT_S) as resp:
            payload = json.loads(resp.read())
    except (urllib.error.URLError, OSError, ValueError) as exc:
        _log.debug("vlm request failed for %s: %s", p.name, str(exc)[:120])
        return None

    text = (payload.get("response") or "").strip()
    if not text:
        return None
    if not expect_json:
        return text
    # Models wrap JSON in prose or fences often enough that this is worth doing.
    match = re.search(r"\{.*\}", text, re.S)
    if not match:
        return None
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


_DEALER_PROMPT = (
    "Look at any signage, buildings, logos or watermarks in this vehicle listing photo. "
    "Which car dealership was this photo taken at? If there is no clear evidence, answer null "
    "- do not guess from the car's brand, because dealers sell other brands as trade-ins. "
    'Reply ONLY with compact JSON: {"dealer_name": string or null, "evidence": string, '
    '"confidence": number between 0 and 1}'
)

_DOCUMENT_PROMPT = (
    "Is this image a vehicle document rather than a photograph of a car? "
    'Reply ONLY with compact JSON: {"kind": one of "window_sticker"|"highlights_slide"|'
    '"other_document"|"photo", "has_prices": true/false, "confidence": number between 0 and 1}'
)


def identify_dealer(image_path: Path | str) -> dict[str, Any] | None:
    """
    Which dealership this photo was taken at, or None when there is no usable evidence.

    Returns ``{"dealer_name", "evidence", "confidence"}``. A null name, a name below the
    confidence floor, or an unparseable reply all collapse to None, so a caller only ever
    sees a claim the model was willing to stand behind.
    """
    result = _ask(image_path, _DEALER_PROMPT)
    if not result:
        return None
    name = result.get("dealer_name")
    if not isinstance(name, str):
        return None
    # Strip first: a whitespace-only name is as empty as a missing one, and models
    # also spell "no answer" as the strings below rather than as JSON null.
    name = name.strip()
    if not name or name.lower() in ("null", "none", "unknown", "n/a", "unclear"):
        return None
    try:
        confidence = float(result.get("confidence") or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    if confidence < _MIN_CONFIDENCE:
        _log.debug("vlm dealer %r below confidence floor (%.2f)", name, confidence)
        return None
    return {
        "dealer_name": name,
        "evidence": str(result.get("evidence") or "")[:300],
        "confidence": confidence,
    }


def classify_document(image_path: Path | str) -> dict[str, Any] | None:
    """
    Whether an image is a sticker/slide/photo.

    Worth the escalation only where OCR returned nothing usable: a sticker photographed
    at an angle, or one whose text is rendered as an image, both defeat OCR while
    remaining perfectly legible to a model.
    """
    result = _ask(image_path, _DOCUMENT_PROMPT)
    if not result:
        return None
    kind = str(result.get("kind") or "").strip().lower()
    if kind not in ("window_sticker", "highlights_slide", "other_document", "photo"):
        return None
    try:
        confidence = float(result.get("confidence") or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    return {
        "kind": kind,
        "has_prices": bool(result.get("has_prices")),
        "confidence": confidence,
    }
