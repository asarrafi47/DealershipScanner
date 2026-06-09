"""Draft trim overlays and ladder steps from ``derived/brochure_text`` JSON."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from backend.enrichment.dictionary_catalog import catalog_key
from backend.enrichment.dictionary_paths import BROCHURE_TEXT_DIR, BROCHURE_TRIM_CANDIDATES_DIR
from backend.enrichment.trim_ladder import _is_valid_trim_name
from backend.enrichment.trim_ladder_knowledge import extract_trims_from_text, merge_trim_names

_MODELS_HEADING = re.compile(
    r"\b([A-Z][A-Z0-9®™'\-/&]{1,24}(?:\s+[A-Z][A-Z0-9®™'\-/&]{1,24}){0,4})\s+MODELS\b",
    re.I,
)
_MODELS_LINE = re.compile(r"^\s*MODELS\s*$", re.I)
_SPECS_HEADER = re.compile(
    r"\bSPECIFICATIONS\b|\bMechanical/Performance\b",
    re.I,
)
_TRIM_TOKEN = re.compile(r"^[A-Z][A-Z0-9®™'\-]{1,20}$")
_ADDS_TO = re.compile(
    r"Adds to or replaces features (?:offered|on)\s+(?:on\s+)?([A-Za-z0-9®™'\-\s]{2,24})",
    re.I,
)
_INCLUDES_KEY = re.compile(r"Includes these key features", re.I)
_BULLET = re.compile(r"^[\-\u2022•]\s+(.+)$")
_JUNK_TRIM = re.compile(
    r"predecessor|wikipedia|wheelbase|sourced from|unveiled|launched in|"
    r"disclosure|page \d+$|mechanical/performance",
    re.I,
)


def brochure_text_path_for_key(catalog_key_str: str) -> Path:
    return BROCHURE_TEXT_DIR / f"{catalog_key_str.replace('|', '__')}.json"


def candidate_path_for_key(catalog_key_str: str) -> Path:
    return BROCHURE_TRIM_CANDIDATES_DIR / f"{catalog_key_str.replace('|', '__')}.json"


def load_brochure_text_json(catalog_key_str: str) -> dict[str, Any] | None:
    path = brochure_text_path_for_key(catalog_key_str)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def brochure_status_from_payload(data: dict[str, Any]) -> str:
    warnings = data.get("warnings") or []
    if "no_text_extracted" in warnings:
        return "no_text"
    pages = data.get("pages") or []
    if not pages:
        return "empty"
    if "no_trim_hint_pages_used_all_text_pages" in warnings:
        return "weak_hints"
    if data.get("trim_hint_pages"):
        return "ok"
    return "ok"


def _clean_trim_token(tok: str) -> str:
    t = tok.strip().strip("®™")
    if not t or _JUNK_TRIM.search(t):
        return ""
    if len(t) > 28:
        return ""
    return t


def _tokens_from_models_line(line: str) -> list[str]:
    line = re.sub(r"\bMODELS\b", "", line, flags=re.I).strip()
    if not line:
        return []
    parts = re.split(r"\s{2,}|\s+", line)
    out: list[str] = []
    for p in parts:
        p = _clean_trim_token(p)
        if not p:
            continue
        if _TRIM_TOKEN.match(p) or (len(p) <= 24 and p[0].isupper()):
            out.append(p)
    return out


def filter_spurious_brochure_trims(
    trims: list[str],
    *,
    make: str,
    model: str,
) -> list[str]:
    """Drop cross-brand tokens (e.g. Limited/Premium on Honda EX/LX ladders)."""
    mk = (make or "").strip().lower()
    mod = re.sub(r"[^a-z0-9]+", "", (model or "").lower())
    if mk == "jeep" and "renegade" in mod:
        jeep_renegade = re.compile(
            r"^(?:Trailhawk|Limited|Latitude|Sport|Altitude|Upland|"
            r"High Altitude|80th Anniversary|75th Anniversary)$",
            re.I,
        )
        kept = [t for t in trims if jeep_renegade.match((t or "").strip())]
        if len(kept) >= 2:
            return kept
    if mk not in {"honda", "acura"}:
        return trims
    honda_core = re.compile(
        r"^(?:LX|LX-P|LX-S|EX|EX-L|SE|Si|Sport|Touring|Type\s*R|"
        r"DX|VP|Hybrid|Plug-?In|Touring|Elite|Technology|A-Spec|"
        r"SH-AWD|Advance|Type\s*S)$",
        re.I,
    )
    has_core = any(honda_core.match(t.strip()) for t in trims)
    if not has_core:
        return trims
    drop = {"limited", "premium", "platinum", "touring", "base", "standard"}
    return [t for t in trims if t.strip().lower() not in drop]


def parse_trim_names_from_text(text: str, *, make: str, model: str) -> list[str]:
    """Extract marketing trim names from brochure comparison/spec pages."""
    if not text or not text.strip():
        return []

    names: list[str] = []
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    for i, line in enumerate(lines):
        m = _MODELS_HEADING.search(line)
        if m:
            names.extend(_tokens_from_models_line(m.group(1)))
            continue
        if _MODELS_LINE.match(line):
            for j in range(i + 1, min(i + 4, len(lines))):
                names.extend(_tokens_from_models_line(lines[j]))
            continue
        if _SPECS_HEADER.search(line) and i + 1 < len(lines):
            header = lines[i + 1]
            if "Mechanical" not in header and len(header) < 120:
                names.extend(_tokens_from_models_line(header))

    knowledge = extract_trims_from_text(make, text, model=model, limit=16)
    merged = merge_trim_names(names, knowledge, make=make, model=model, limit=12)
    out: list[str] = []
    seen: set[str] = set()
    for n in merged:
        if not _is_valid_trim_name(n, make=make, model=model):
            continue
        key = n.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(n)
    return filter_spurious_brochure_trims(out, make=make, model=model)


def _draft_adds_from_text(text: str, trims: list[str]) -> dict[str, list[str]]:
    """Lightweight 'adds over lower trim' bullets for human review."""
    if len(trims) < 2:
        return {}

    adds: dict[str, list[str]] = {t: [] for t in trims}
    current: str | None = None
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        for t in trims[1:]:
            if _ADDS_TO.search(line) and t.lower() in line.lower():
                current = t
                break
        m = _ADDS_TO.search(line)
        if m:
            base = _clean_trim_token(m.group(1))
            for t in trims:
                if base and t.lower().startswith(base.lower()):
                    for u in trims:
                        if u != t and u.lower() > t.lower():
                            current = u
                            break
        if _INCLUDES_KEY.search(line) and trims:
            current = trims[0]
        bm = _BULLET.match(line)
        if bm and current and current in adds:
            bullet = bm.group(1).strip()
            if 12 <= len(bullet) <= 220 and not _JUNK_TRIM.search(bullet):
                adds[current].append(bullet)
        elif current and 20 <= len(line) <= 200 and not _JUNK_TRIM.search(line):
            if any(ch.islower() for ch in line) and line[0].isupper():
                adds[current].append(line)

    return {k: v[:12] for k, v in adds.items() if v}


def build_trim_candidate(
    data: dict[str, Any],
    *,
    make: str,
    model: str,
    year: int,
) -> dict[str, Any]:
    text = str(data.get("combined_trim_pages_text") or "")
    if not text:
        for pg in data.get("pages") or []:
            if isinstance(pg, dict):
                text += "\n" + str(pg.get("text") or "")

    trims = parse_trim_names_from_text(text, make=make, model=model)
    adds = _draft_adds_from_text(text, trims) if len(trims) >= 2 else {}

    quality = "high" if len(trims) >= 3 and any(adds.values()) else "medium" if len(trims) >= 2 else "low"
    if brochure_status_from_payload(data) == "no_text":
        quality = "none"

    return {
        "catalog_key": catalog_key(year, make, model),
        "year": year,
        "make": make,
        "model": model,
        "status": "draft",
        "quality": quality,
        "trims_available": trims,
        "adds_by_trim": adds,
        "brochure_status": brochure_status_from_payload(data),
        "source": "brochure_text_auto",
        "warnings": list(data.get("warnings") or []),
    }


def persist_trim_candidate(payload: dict[str, Any]) -> Path:
    BROCHURE_TRIM_CANDIDATES_DIR.mkdir(parents=True, exist_ok=True)
    ck = str(payload.get("catalog_key") or "")
    out = candidate_path_for_key(ck)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out
