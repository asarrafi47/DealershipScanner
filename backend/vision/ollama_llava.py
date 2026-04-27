"""
Ollama LLaVA (vision) client for cabin / interior inference and listing-gallery relevance.

Environment (document for operators):

- ``OLLAMA_HOST`` — base URL, default ``http://127.0.0.1:11434`` (same as enrichment).
- ``OLLAMA_VISION_MODEL`` — vision model id, default ``llava:13b``. Run ``ollama pull llava:13b``
  for the recommended default image.
- ``OLLAMA_INTERIOR_VISION_TIMEOUT_S`` — HTTP timeout seconds (default ``120``), used for
  interior analysis and listing-image classification.

Persistence / merge thresholds live on the merge layer: ``INTERIOR_VISION_CONFIDENCE``,
``INTERIOR_VISION_OVERWRITE`` (see ``backend.vision.interior_vision_merge``). When no cabin
photo exists, ``backend.scanner_post_pipeline`` can still send the hero frame with
``inference_context=through_windows`` (see ``INTERIOR_VISION_FALLBACK_THROUGH_WINDOWS``).

The chat API returns JSON only (enforced in prompt). Interior buckets must be from the fixed
allowlist (see ``INTERIOR_BUCKET_ALLOWLIST`` below — keep aligned with
``backend.utils.interior_color_buckets``).
"""
from __future__ import annotations

import base64
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests

from backend.enrichment_service import _fetch_image_b64_optimized

logger = logging.getLogger(__name__)

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://127.0.0.1:11434").rstrip("/")
OLLAMA_VISION_MODEL = os.environ.get("OLLAMA_VISION_MODEL", "llava:13b")
OLLAMA_INTERIOR_VISION_TIMEOUT_S = float(os.environ.get("OLLAMA_INTERIOR_VISION_TIMEOUT_S", "120"))

# Last request diagnostics for operator-facing CLIs (best-effort).
_LAST_OLLAMA_ERROR: str | None = None
_LAST_OLLAMA_RAW_SNIPPET: str | None = None


def last_ollama_diagnostics() -> dict[str, Any]:
    """Best-effort diagnostics from the last Ollama chat attempt (for CLI output)."""
    return {
        "last_error": _LAST_OLLAMA_ERROR,
        "last_raw_snippet": _LAST_OLLAMA_RAW_SNIPPET,
        "host": OLLAMA_HOST,
        "model": OLLAMA_VISION_MODEL,
        "timeout_s": OLLAMA_INTERIOR_VISION_TIMEOUT_S,
    }

# Fixed allowlist for model output (subset of lexicon buckets; "other" catches remainder).
INTERIOR_BUCKET_ALLOWLIST: tuple[str, ...] = (
    "black",
    "white",
    "gray",
    "silver",
    "red",
    "blue",
    "green",
    "brown",
    "tan",
    "beige",
    "orange",
    "yellow",
    "other",
)

_SYSTEM_PROMPT = (
    "You analyze a vehicle interior/cabin photo. Prioritize **seat upholstery color** as the primary "
    "signal for the interior color (dash/trim can be secondary). Reply with STRICT JSON only, no markdown, "
    "no prose outside JSON. Keys: interior_buckets (array of strings from this fixed set only: "
    + ", ".join(INTERIOR_BUCKET_ALLOWLIST)
    + "), interior_guess_text (short human label INCLUDING a color, e.g. 'Black leather' or 'Tan/Black'), confidence (0.0-1.0 float), "
    "evidence (one short phrase; mention the seats if visible). If unsure, use interior_buckets [\"other\"] and low confidence."
)

_SYSTEM_PROMPT_THROUGH_WINDOWS = (
    "You infer **passenger cabin** colors from a vehicle listing image that may be an **exterior** "
    "shot. Use only what is visible **through side or rear windows or windshield** (seats, dash, "
    "door panels, headliner). Prioritize **seat upholstery color** if seats are visible. "
    "Do **not** guess from body paint, wheels, or reflections you cannot "
    "resolve as interior. Reply with STRICT JSON only, no markdown, no prose outside JSON. Keys: "
    "interior_buckets (array of strings from this fixed set only: "
    + ", ".join(INTERIOR_BUCKET_ALLOWLIST)
    + "), interior_guess_text (short human label INCLUDING a color), confidence (0.0-1.0 float), "
    "evidence (one short phrase; mention the seats if visible). If little or no interior is visible, use interior_buckets "
    '[\"other\"], low confidence, and evidence stating visibility limits.'
)

_USER_INTERIOR_CABIN = (
    "Analyze this vehicle interior/cabin image. Focus on the **seat upholstery color** first."
)

_USER_INTERIOR_THROUGH_WINDOWS = (
    "This may be an outside view of the vehicle. Describe **interior upholstery and trim colors** "
    "visible **only through the glass**. Focus on the **seat upholstery color** if you can see seats. "
    "If you cannot see inside, say so and use low confidence."
)


def _guess_text_from_buckets(buckets: list[str]) -> str:
    out = [b for b in buckets if isinstance(b, str) and b and b.lower() != "other"]
    if not out:
        return ""
    # Keep stable order; title-case for display.
    return " / ".join(str(b).strip().title() for b in out)


_GENERIC_GUESS_TOKENS: frozenset[str] = frozenset(
    {
        "leather",
        "cloth",
        "suede",
        "alcantara",
        "vinyl",
        "upholstery",
        "seats",
        "seat",
        "interior",
        "trim",
    }
)

_MATERIAL_TOKENS: tuple[str, ...] = ("leather", "cloth", "suede", "alcantara", "vinyl")

_LISTING_IMAGE_SYSTEM_PROMPT = (
    "You judge one image from an online vehicle listing gallery. Reply with STRICT JSON only, "
    "no markdown, no prose outside JSON.\n"
    "Set keep=true only if the photo is at least one of: (1) the exterior of a specific vehicle, "
    "(2) the interior/cabin of a specific vehicle, (3) a window sticker / Monroney label / "
    "factory equipment label for a vehicle.\n"
    "Set keep=false for dealership buildings, logos, signs, maps, staff, unrelated products, "
    "generic marketing graphics, empty lots, service areas with no clear vehicle, screenshots, "
    "financing widgets, or anything that is not clearly the listed vehicle or its sticker/interior.\n"
    'Keys: keep (boolean), category (string, one of: "exterior", "interior", "window_sticker", '
    '"not_vehicle"), confidence (0.0-1.0 float). If unsure, use not_vehicle and keep=false.'
)

_LISTING_GOOD_CATEGORIES: frozenset[str] = frozenset(
    {
        "exterior",
        "interior",
        "window_sticker",
        "sticker",
        "monroney",
        "monroney_label",
        "window_label",
        "cabin",
    }
)
_LISTING_BAD_CATEGORIES: frozenset[str] = frozenset(
    {
        "not_vehicle",
        "irrelevant",
        "dealership",
        "logo",
        "sign",
        "map",
        "building",
        "staff",
        "marketing",
    }
)


def _extract_json_object(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    t = text.strip()
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{[\s\S]*\}", t)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return None


def _ollama_vision_chat_json(
    *,
    system: str,
    user_text: str,
    image_b64_jpeg: str,
    timeout_s: float | None = None,
) -> str | None:
    global _LAST_OLLAMA_ERROR, _LAST_OLLAMA_RAW_SNIPPET
    payload = {
        "model": OLLAMA_VISION_MODEL,
        "stream": False,
        "messages": [
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": user_text,
                "images": [image_b64_jpeg],
            },
        ],
    }
    t = float(OLLAMA_INTERIOR_VISION_TIMEOUT_S if timeout_s is None else timeout_s)
    url = f"{OLLAMA_HOST}/api/chat"
    _LAST_OLLAMA_ERROR = None
    _LAST_OLLAMA_RAW_SNIPPET = None
    # One retry helps with transient 5xx / connection churn.
    for attempt in (1, 2):
        try:
            resp = requests.post(url, json=payload, timeout=t)
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as e:
            _LAST_OLLAMA_ERROR = f"{type(e).__name__}: {e}"
            if attempt >= 2:
                logger.warning("Ollama LLaVA request failed: %s", e)
                return None
    msg = (data.get("message") or {}) if isinstance(data, dict) else {}
    content = msg.get("content") if isinstance(msg, dict) else None
    if not isinstance(content, str):
        _LAST_OLLAMA_ERROR = "Ollama response missing message.content"
        return None
    _LAST_OLLAMA_RAW_SNIPPET = content[:300]
    return content


def analyze_interior_from_image_url(
    image_url: str,
    *,
    inference_context: str = "cabin",
) -> dict[str, Any] | None:
    """
    Call Ollama chat with images for *image_url*. Returns parsed dict or ``None``.

    ``inference_context``: ``\"cabin\"`` (direct interior photo) or ``\"through_windows\"``
    (exterior / hero shot — model is asked to read cabin only through glass).

    On failure logs and returns ``None`` (callers decide whether to persist).
    """
    b64 = _fetch_image_b64_optimized(image_url)
    if not b64:
        return None
    return analyze_interior_from_image_b64(b64, inference_context=inference_context)


def analyze_interior_from_image_b64(
    image_b64_jpeg: str,
    *,
    inference_context: str = "cabin",
) -> dict[str, Any] | None:
    ctx = (inference_context or "cabin").strip().lower().replace("-", "_")
    if ctx == "through_windows":
        system = _SYSTEM_PROMPT_THROUGH_WINDOWS
        user_text = _USER_INTERIOR_THROUGH_WINDOWS
    else:
        system = _SYSTEM_PROMPT
        user_text = _USER_INTERIOR_CABIN
    content = _ollama_vision_chat_json(
        system=system,
        user_text=user_text,
        image_b64_jpeg=image_b64_jpeg,
    )
    if not content:
        return None
    parsed = _extract_json_object(content)
    if not isinstance(parsed, dict):
        # Model sometimes violates the "STRICT JSON only" instruction, especially on
        # through-windows exterior shots when it cannot see seats. Return a safe low-confidence
        # payload instead of None so CLIs can show a consistent schema and callers can decide.
        low = content.strip().lower()
        allow = set(INTERIOR_BUCKET_ALLOWLIST)
        found: list[str] = []
        for b in INTERIOR_BUCKET_ALLOWLIST:
            if b == "other":
                continue
            if re.search(rf"\b{re.escape(b)}\b", low) and b in allow and b not in found:
                found.append(b)
        buckets = found if found else ["other"]
        guess_s = _guess_text_from_buckets(buckets)
        material: str | None = None
        for m in _MATERIAL_TOKENS:
            if re.search(rf"\b{re.escape(m)}\b", low):
                material = m
                break
        if guess_s and material:
            guess_s = f"{guess_s} {material}".strip()
        # Keep this low: the model didn't follow the schema, but we can still salvage signal.
        confidence = 0.12 if buckets != ["other"] else 0.05

        ev = content.strip().replace("\n", " ")[:160]
        if not ev:
            ev = "model returned non-JSON output"
        return {
            "interior_buckets": buckets,
            "interior_guess_text": guess_s,
            "confidence": confidence,
            "evidence": ev,
            "model": OLLAMA_VISION_MODEL,
            "image_b64_len": len(image_b64_jpeg),
            "inference_context": "through_windows" if ctx == "through_windows" else "cabin",
            "parse_error": "non_json",
        }
    raw_buckets = parsed.get("interior_buckets")
    buckets: list[str] = []
    if isinstance(raw_buckets, list):
        allow = set(INTERIOR_BUCKET_ALLOWLIST)
        for x in raw_buckets:
            b = str(x).strip().lower()
            if b in allow and b not in buckets:
                buckets.append(b)
    if not buckets:
        buckets = ["other"]
    guess = parsed.get("interior_guess_text")
    evidence = parsed.get("evidence")
    conf_raw = parsed.get("confidence")
    try:
        confidence = float(conf_raw)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    guess_s = str(guess).strip() if guess is not None else ""
    # LLaVA sometimes answers material only (e.g. "leather") — force a color label from buckets.
    if guess_s and guess_s.strip().lower() in _GENERIC_GUESS_TOKENS:
        label = _guess_text_from_buckets(buckets)
        if label:
            guess_s = f"{label} {guess_s}".strip()
    elif not guess_s:
        label = _guess_text_from_buckets(buckets)
        if label:
            guess_s = label
    return {
        "interior_buckets": buckets,
        "interior_guess_text": guess_s,
        "confidence": confidence,
        "evidence": str(evidence).strip() if evidence is not None else "",
        "model": OLLAMA_VISION_MODEL,
        "image_b64_len": len(image_b64_jpeg),
        "inference_context": "through_windows" if ctx == "through_windows" else "cabin",
    }


def _listing_image_keep_from_parsed(parsed: dict[str, Any] | None) -> bool:
    """True = keep URL in gallery. On missing/ambiguous model output, keep (conservative)."""
    if not isinstance(parsed, dict):
        return True
    cat_raw = str(parsed.get("category") or "").strip().lower().replace(" ", "_").replace("-", "_")
    if cat_raw in _LISTING_GOOD_CATEGORIES:
        return True
    if cat_raw in _LISTING_BAD_CATEGORIES:
        return False
    rk = parsed.get("keep")
    if isinstance(rk, bool):
        return rk
    if isinstance(rk, str):
        s = rk.strip().lower()
        if s in ("true", "1", "yes"):
            return True
        if s in ("false", "0", "no"):
            return False
    return True


def classify_listing_image_from_url(image_url: str) -> dict[str, Any] | None:
    """
    Returns ``{"keep": bool, "category": str, "confidence": float, ...}`` or ``None`` on failure.
    """
    b64 = _fetch_image_b64_optimized(image_url)
    if not b64:
        return None
    return classify_listing_image_from_image_b64(b64)


def classify_listing_image_from_image_b64(image_b64_jpeg: str) -> dict[str, Any] | None:
    content = _ollama_vision_chat_json(
        system=_LISTING_IMAGE_SYSTEM_PROMPT,
        user_text="Classify this image for a vehicle listing gallery.",
        image_b64_jpeg=image_b64_jpeg,
    )
    if not content:
        return None
    parsed = _extract_json_object(content)
    if not isinstance(parsed, dict):
        return None
    keep = _listing_image_keep_from_parsed(parsed)
    cat = str(parsed.get("category") or "").strip()[:64]
    conf_raw = parsed.get("confidence")
    try:
        confidence = float(conf_raw)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    return {
        "keep": keep,
        "category": cat,
        "confidence": confidence,
        "model": OLLAMA_VISION_MODEL,
        "image_b64_len": len(image_b64_jpeg),
    }


def filter_gallery_urls_for_vehicle_listing(
    urls: list[str],
    *,
    max_workers: int = 1,
) -> list[str]:
    """
    Return URLs in original order that LLaVA classifies as vehicle-related (exterior, interior,
    or window sticker). Duplicate URLs are skipped. On fetch/model failure for a URL, that URL is
    **kept** so transient errors do not wipe galleries.
    """
    seen: set[str] = set()
    ordered: list[str] = []
    for u in urls:
        if not isinstance(u, str):
            continue
        s = u.strip()
        if not s.lower().startswith("http") or s in seen:
            continue
        seen.add(s)
        ordered.append(s)
    if not ordered:
        return []
    mw = max(1, int(max_workers))

    def decide(u: str) -> tuple[str, bool]:
        parsed = classify_listing_image_from_url(u)
        if parsed is None:
            return u, True
        return u, bool(parsed.get("keep"))

    if mw <= 1:
        return [u for u in ordered if decide(u)[1]]

    out_map: dict[str, bool] = {}
    with ThreadPoolExecutor(max_workers=mw, thread_name_prefix="LlavaGallery") as pool:
        futures = {pool.submit(decide, u): u for u in ordered}
        for fut in as_completed(futures):
            u, keep = fut.result()
            out_map[u] = keep
    return [u for u in ordered if out_map.get(u, True)]


_MONRONEY_STICKER_SYSTEM = (
    "You read a vehicle window sticker (Monroney) photo. Reply with STRICT JSON only, no markdown, "
    "no prose outside JSON.\n"
    "Keys: optional_packages (string[] of optional equipment lines you can read), "
    "standard_equipment_summary (string[] short bullets of major standard equipment if legible), "
    "engine_description (string|null), transmission (string|null), drivetrain (string|null), "
    "fuel_type (string|null), cylinders (integer|null), mpg_city (integer|null), mpg_highway (integer|null), "
    "msrp (number|null), total_vehicle_price (number|null), vin_visible (string|null), "
    "confidence (0.0-1.0 number). Use null or [] when not visible. Do not invent unreadable lines."
)

_MONRONEY_TEXT_SYSTEM = (
    "You read text copied from a dealership vehicle listing page; it may include Monroney / window "
    "sticker lines. Reply with STRICT JSON only, same key schema as for a sticker photo: "
    "optional_packages, standard_equipment_summary, engine_description, transmission, drivetrain, "
    "fuel_type, cylinders, mpg_city, mpg_highway, msrp, total_vehicle_price, vin_visible, confidence. "
    "Use null or [] when unknown."
)


def _ollama_chat_text_only(*, system: str, user_text: str) -> str | None:
    payload = {
        "model": OLLAMA_VISION_MODEL,
        "stream": False,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_text},
        ],
    }
    url = f"{OLLAMA_HOST}/api/chat"
    try:
        resp = requests.post(url, json=payload, timeout=float(OLLAMA_INTERIOR_VISION_TIMEOUT_S))
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("Ollama text chat failed: %s", e)
        return None
    msg = (data.get("message") or {}) if isinstance(data, dict) else {}
    content = msg.get("content") if isinstance(msg, dict) else None
    return content if isinstance(content, str) else None


def is_probable_sticker_image_url(url: str) -> bool:
    """Heuristic URL match for Monroney / window-sticker assets (CDK and similar CDNs)."""
    ul = (url or "").lower()
    if not ul.startswith("http"):
        return False
    needles = (
        "sticker",
        "monroney",
        "monrone",
        "windowsticker",
        "window-sticker",
        "window_sticker",
        "label-g",
        "options.jpg",
        "equipment-label",
        "equiplabel",
        "monroneylabel",
    )
    return any(n in ul for n in needles)


def analyze_monroney_sticker_from_image_url(image_url: str) -> dict[str, Any] | None:
    b64 = _fetch_image_b64_optimized(image_url)
    if not b64:
        return None
    return analyze_monroney_sticker_from_image_b64(b64)


def analyze_monroney_sticker_from_image_b64(image_b64_jpeg: str) -> dict[str, Any] | None:
    content = _ollama_vision_chat_json(
        system=_MONRONEY_STICKER_SYSTEM,
        user_text="Read this window sticker / Monroney image.",
        image_b64_jpeg=image_b64_jpeg,
    )
    if not content:
        return None
    parsed = _extract_json_object(content)
    if not isinstance(parsed, dict):
        return None
    parsed["vision_model"] = OLLAMA_VISION_MODEL
    parsed["source"] = "sticker_image"
    return parsed


def analyze_monroney_from_page_texts(text_blocks: list[str]) -> dict[str, Any] | None:
    """Parse combined listing-page sticker text (no image) via the same Ollama model."""
    parts = [t.strip() for t in text_blocks if isinstance(t, str) and t.strip()]
    if not parts:
        return None
    blob = "\n---\n".join(parts)[:8000]
    user = "Listing page excerpt(s) follow:\n" + blob
    content = _ollama_chat_text_only(system=_MONRONEY_TEXT_SYSTEM, user_text=user)
    if not content:
        return None
    parsed = _extract_json_object(content)
    if not isinstance(parsed, dict):
        return None
    parsed["vision_model"] = OLLAMA_VISION_MODEL
    parsed["source"] = "page_text"
    return parsed


def image_bytes_to_b64_jpeg(image_bytes: bytes) -> str | None:
    """Encode raw image bytes as base64 JPEG for Ollama (no resize — caller may resize)."""
    if not image_bytes:
        return None
    try:
        return base64.b64encode(image_bytes).decode("ascii")
    except Exception:
        return None
