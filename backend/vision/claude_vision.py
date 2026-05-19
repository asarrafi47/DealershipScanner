"""
Claude Haiku vision gallery filter — replaces LLaVA when ANTHROPIC_API_KEY is set.

Batches up to BATCH_SIZE images per API call. Reuses URL heuristics from ollama_llava
(heuristic drops, trusted-CDN passthrough, lot-photo scoring) and only replaces the
actual vision classification step.

Set SCANNER_GALLERY_VISION_BATCH to control images per Claude call (default: 8).
"""
from __future__ import annotations

import base64
import json
import logging
import os
from io import BytesIO
from typing import Any

import requests

logger = logging.getLogger(__name__)

_CLAUDE_MODEL = "claude-haiku-4-5-20251001"
_FETCH_TIMEOUT = 12.0
_FETCH_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_CLASSIFY_PROMPT = """\
You are classifying car dealership listing photos. For each image I send, tell me:
- keep: true if it is an actual vehicle photo (exterior shot, interior, engine bay, trunk, wheels, dashboard, seats)
- keep: false if it is a badge, logo, award, CARFAX report, financing offer, promo banner, captcha, QR code, watermark-only slide, or dealership marketing material

Respond ONLY with a JSON array, one entry per image, in order:
[{"idx": 0, "keep": true, "category": "exterior"}, {"idx": 1, "keep": false, "category": "badge"}, ...]

Categories: exterior, interior, engine, wheels, trunk, badge, promo, award, carfax, other_keep, other_drop
"""


def _api_key() -> str:
    return (os.environ.get("ANTHROPIC_API_KEY") or "").strip()


def _batch_size() -> int:
    try:
        return max(1, min(20, int(os.environ.get("SCANNER_GALLERY_VISION_BATCH") or "8")))
    except (TypeError, ValueError):
        return 8


def _fetch_image_b64(url: str, referer: str | None = None) -> str | None:
    headers = {"User-Agent": _FETCH_UA, "Accept": "image/*"}
    if referer:
        headers["Referer"] = referer
    try:
        r = requests.get(url, headers=headers, timeout=_FETCH_TIMEOUT, stream=True)
        r.raise_for_status()
        raw = r.content
        if len(raw) < 800:
            return None
        ct = r.headers.get("content-type", "").lower().split(";")[0].strip()
        if ct not in ("image/jpeg", "image/jpg", "image/png", "image/webp", "image/gif"):
            if not any(url.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp")):
                return None
        try:
            from PIL import Image
            img = Image.open(BytesIO(raw)).convert("RGB")
            w, h = img.size
            if w < 60 or h < 60:
                return None
            # Resize large images to save tokens
            max_dim = 800
            if w > max_dim or h > max_dim:
                img.thumbnail((max_dim, max_dim), Image.LANCZOS)
            buf = BytesIO()
            img.save(buf, format="JPEG", quality=75)
            return base64.b64encode(buf.getvalue()).decode()
        except Exception:
            return base64.b64encode(raw[:300_000]).decode()
    except Exception as e:
        logger.debug("Image fetch failed %s: %s", url[:80], e)
        return None


def _classify_batch(
    indexed_urls: list[tuple[int, str]],
    referer: str | None,
    api_key: str,
) -> list[tuple[int, str, bool, str]]:
    """
    Returns list of (original_index, url, keep, category) for each url in the batch.
    Falls back to keep=True on any error so we never silently drop real photos.
    """
    fallback = [(i, u, True, "unknown") for i, u in indexed_urls]

    images_b64: list[tuple[int, str, str]] = []  # (idx, url, b64)
    for i, u in indexed_urls:
        b64 = _fetch_image_b64(u, referer)
        if b64 is None:
            logger.debug("Could not fetch image for classification: %s", u[:80])
        else:
            images_b64.append((i, u, b64))

    if not images_b64:
        return fallback

    content: list[dict[str, Any]] = []
    for seq, (i, u, b64) in enumerate(images_b64):
        content.append({
            "type": "text",
            "text": f"Image {seq} (idx={i}):"
        })
        content.append({
            "type": "image",
            "source": {"type": "base64", "media_type": "image/jpeg", "data": b64},
        })
    content.append({"type": "text", "text": "Classify each image as described. Return JSON array only."})

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": _CLAUDE_MODEL,
                "max_tokens": 512,
                "system": _CLASSIFY_PROMPT,
                "messages": [{"role": "user", "content": content}],
            },
            timeout=45.0,
        )
        r.raise_for_status()
        data = r.json()
        text = data["content"][0]["text"].strip()
        # Strip markdown code fences if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        parsed: list[dict[str, Any]] = json.loads(text)
    except Exception as e:
        logger.warning("Claude gallery classification failed: %s", e)
        return fallback

    # Build lookup by seq position
    seq_map = {seq: (i, u) for seq, (i, u, _) in enumerate(images_b64)}
    result_map: dict[int, tuple[int, str, bool, str]] = {}
    for entry in parsed:
        try:
            seq_key = int(entry.get("idx", -1))
            # idx in prompt = original index i, but seq = position in images_b64
            # Claude gets "idx=i" text, so look up by i value
            orig_i = seq_key
            orig_u = None
            for _seq, (_i, _u, _) in enumerate(images_b64):
                if _i == orig_i:
                    orig_u = _u
                    break
            if orig_u is None:
                continue
            keep = bool(entry.get("keep", True))
            cat = str(entry.get("category") or "unknown")
            result_map[orig_i] = (orig_i, orig_u, keep, cat)
        except (TypeError, ValueError, KeyError):
            continue

    # Fill in fetched images; keep=True for any that Claude didn't classify
    out: list[tuple[int, str, bool, str]] = []
    for i, u, _ in images_b64:
        if i in result_map:
            out.append(result_map[i])
        else:
            out.append((i, u, True, "unknown"))

    # Any URL that couldn't be fetched: keep by default
    fetched_indices = {i for i, _, _ in images_b64}
    for i, u in indexed_urls:
        if i not in fetched_indices:
            out.append((i, u, True, "unfetchable"))

    return out


def filter_gallery_urls_for_vehicle_listing(
    urls: list[str],
    *,
    page_referer: str | None = None,
    **_kwargs: Any,
) -> list[str]:
    """
    Drop-in replacement for ollama_llava.filter_gallery_urls_for_vehicle_listing.

    Reuses URL heuristics (heuristic_drop, passthrough trusted CDN, lot-score sort),
    then classifies ambiguous images with Claude Haiku vision in batches.
    """
    from backend.vision.url_heuristics import (
        heuristic_drop_gallery_listing_url,
        dealer_listing_gallery_passthrough_vision,
        dealer_lot_photo_score,
    )

    key = _api_key()
    if not key:
        logger.warning("ANTHROPIC_API_KEY not set — skipping Claude gallery filter")
        return [u for u in urls if isinstance(u, str) and u.strip().lower().startswith("http")]

    # Deduplicate
    seen: set[str] = set()
    ordered: list[str] = []
    for u in urls:
        s = (u or "").strip()
        if s.lower().startswith("http") and s not in seen:
            seen.add(s)
            ordered.append(s)
    if not ordered:
        return []

    # Separate: heuristic drop | trusted CDN passthrough | needs vision
    passthrough: list[tuple[int, str]] = []
    needs_vision: list[tuple[int, str]] = []
    for i, u in enumerate(ordered):
        if heuristic_drop_gallery_listing_url(u):
            continue
        if dealer_listing_gallery_passthrough_vision(u):
            passthrough.append((i, u))
        else:
            needs_vision.append((i, u))

    passthrough_results: list[tuple[int, str, bool, str]] = [
        (i, u, True, "cdn_passthrough") for i, u in passthrough
    ]

    vision_results: list[tuple[int, str, bool, str]] = []
    bs = _batch_size()
    for batch_start in range(0, len(needs_vision), bs):
        batch = needs_vision[batch_start: batch_start + bs]
        vision_results.extend(_classify_batch(batch, page_referer, key))

    all_results = passthrough_results + vision_results
    kept = [(i, u, cat) for i, u, keep, cat in all_results if keep]
    if not kept:
        # Fallback: return passthrough + heuristic-passed URLs rather than empty
        return [u for _, u, _ in sorted(passthrough_results, key=lambda t: (-dealer_lot_photo_score(t[1]), t[0]))]

    _CAT_PRIORITY = {
        "exterior": 10, "wheels": 8, "trunk": 7,
        "interior": 6, "dashboard": 6, "seats": 5,
        "engine": 4, "cdn_passthrough": 3,
        "other_keep": 1, "unknown": 0,
    }

    def _sort_key(t: tuple[int, str, str]) -> tuple:
        i, u, cat = t
        return (-(dealer_lot_photo_score(u) + _CAT_PRIORITY.get(cat, 0)), i)

    kept.sort(key=_sort_key)
    return [u for _, u, _ in kept]
