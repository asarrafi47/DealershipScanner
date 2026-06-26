"""
Extract vehicle history badge strings from dealer-published text (description, VDP badges).

Does not scrape Carfax report pages — only phrases the dealer shows on their listing.
"""
from __future__ import annotations

import json
import re
from typing import Any

from backend.utils.json_column_storage import nullable_json_array_text

# More specific patterns first; each match maps to a canonical dealer-claim label.
_CANONICAL_RULES: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"carfax\s*one[\s-]*owner", re.I), "CARFAX One-Owner"),
    (re.compile(r"clean\s+carfax|carfax\s+clean", re.I), "Clean CARFAX"),
    (re.compile(r"no\s+accidents?\s+(?:or\s+damage\s+)?reported", re.I), "No Accidents Reported"),
    (re.compile(r"no\s+accidents?\s+reported", re.I), "No Accidents Reported"),
    (re.compile(r"accident[\s-]*free", re.I), "Accident Free"),
    (re.compile(r"personal\s+use\s+only", re.I), "Personal Use Only"),
    (re.compile(r"clean\s+title", re.I), "Clean Title"),
    (re.compile(r"^\s*1[\s-]*owner\s*$", re.I), "1 Owner"),
    (re.compile(r"\bone[\s-]*owner\b", re.I), "One Owner"),
    (re.compile(r"\bautocheck\b", re.I), "AutoCheck"),
)

_HISTORY_BADGE_HINT = re.compile(
    r"carfax|autocheck|owner|accident|title|salvage|lemon|fleet|personal\s+use|damage\s+reported",
    re.I,
)

# Marketing / trim noise that sometimes lands in badge selectors.
_BADGE_NOISE = re.compile(
    r"certified|pre[\s-]*owned|used|new|sale|price|\$|awd|fwd|rwd|4wd|"
    r"financ|warranty|contact|call\s+us|schedule|test\s+drive|kbb|jd\s+power",
    re.I,
)


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _canonical_from_text(text: str) -> str | None:
    t = _norm_space(text)
    if not t or len(t) > 160:
        return None
    for rx, label in _CANONICAL_RULES:
        if rx.search(t):
            return label
    if _HISTORY_BADGE_HINT.search(t) and not _BADGE_NOISE.search(t) and len(t) <= 80:
        return t
    return None


def _text_segments(text: str) -> list[str]:
    """Split prose / badge blobs into phrase-sized chunks."""
    t = _norm_space(text)
    if not t:
        return []
    parts = re.split(r"[.\n|;]+", t)
    out = [t]
    for p in parts:
        p = _norm_space(p)
        if p and len(p) >= 4:
            out.append(p)
    return out


def _as_highlight_list(value: Any) -> list[str]:
    if value is None or value == "" or value == "[]":
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except (json.JSONDecodeError, TypeError):
            pass
        one = _canonical_from_text(value)
        return [one] if one else []
    return []


def _prune_redundant_highlights(labels: list[str]) -> list[str]:
    """Drop generic labels when a more specific Carfax/owner phrase is already present."""
    lower = {x.lower() for x in labels}
    drop: set[str] = set()
    if "carfax one-owner" in lower:
        drop.update({"one owner", "1 owner", "carfax"})
    if "clean carfax" in lower:
        drop.add("carfax")
    out: list[str] = []
    seen: set[str] = set()
    for label in labels:
        if label.lower() in drop or label.lower() in seen:
            continue
        seen.add(label.lower())
        out.append(label)
    return out


def extract_history_highlights_from_dealer_text(*texts: str | None) -> list[str]:
    """Return deduped canonical highlights found in dealer description / badge text."""
    seen: set[str] = set()
    out: list[str] = []

    def add(label: str | None) -> None:
        if not label:
            return
        key = label.lower()
        if key in seen:
            return
        seen.add(key)
        out.append(label)

    for raw in texts:
        if not raw or not str(raw).strip():
            continue
        body = str(raw)
        for segment in _text_segments(body):
            add(_canonical_from_text(segment))
        for rx, label in _CANONICAL_RULES:
            if rx.search(body):
                add(label)
    return _prune_redundant_highlights(out)


def extract_history_highlights_from_badges(badges: list[Any] | None) -> list[str]:
    if not badges:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in badges:
        if not isinstance(item, str):
            continue
        label = _canonical_from_text(item)
        if not label:
            continue
        key = label.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(label)
    return _prune_redundant_highlights(out)


def merge_history_highlights(*sources: Any) -> list[str] | None:
    """Union highlight lists / text sources; return None when empty."""
    seen: set[str] = set()
    out: list[str] = []

    def absorb(value: Any) -> None:
        if value is None:
            return
        if isinstance(value, str):
            if value.strip() in ("", "[]", "null"):
                return
            items = extract_history_highlights_from_dealer_text(value)
        elif isinstance(value, list):
            items = []
            for entry in value:
                if isinstance(entry, str):
                    items.extend(extract_history_highlights_from_dealer_text(entry))
                    canon = _canonical_from_text(entry)
                    if canon:
                        items.append(canon)
                elif entry is not None:
                    items.extend(extract_history_highlights_from_dealer_text(str(entry)))
        else:
            return
        for label in items:
            key = label.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(label)

    for src in sources:
        absorb(src)
    return _prune_redundant_highlights(out) or None


def coalesce_history_highlights_for_storage(vehicle: dict[str, Any]) -> list[str] | None:
    """Fill ``history_highlights`` from inventory JSON, description, and optional VDP badges."""
    existing = _as_highlight_list(vehicle.get("history_highlights"))
    badges = vehicle.get("_vdp_dom_badges")
    badge_list = badges if isinstance(badges, list) else None
    merged = merge_history_highlights(
        existing,
        extract_history_highlights_from_badges(badge_list),
        vehicle.get("description"),
    )
    return merged


def history_highlights_json(highlights: list[str] | None) -> str | None:
    return nullable_json_array_text(highlights)
