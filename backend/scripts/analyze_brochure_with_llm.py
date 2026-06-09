#!/usr/bin/env python3
"""
Use Claude Haiku to extract detailed trim-level feature adds from brochure text.

Reads ``derived/brochure_text_slim/*.json``, injects the pre-discovered trim
hierarchy from ``curated/trim_ladders_brochure.json``, sends cleaned text to
Claude, and writes ``adds_by_trim`` output to ``derived/trim_adds_by_year/{ck}.json``.

Rule-based extraction (no API key) runs first when ``--rule-first`` is enabled.

Usage::

    python -m backend.scripts.analyze_brochure_with_llm
    python -m backend.scripts.analyze_brochure_with_llm --limit 50
    python -m backend.scripts.analyze_brochure_with_llm --overwrite
    python -m backend.scripts.analyze_brochure_with_llm --dry-run
    python -m backend.scripts.analyze_brochure_with_llm --rule-only
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.brochure_overlay_backfill import enrich_brochure_overlay  # noqa: E402
from backend.enrichment.brochure_promote import extract_promotable_overlay  # noqa: E402
from backend.enrichment.dictionary_paths import (  # noqa: E402
    BROCHURE_TEXT_SLIM_DIR,
    TRIM_ADDS_BY_YEAR_DIR,
    trim_ladders_brochure_path,
)
from backend.utils.project_env import load_project_dotenv  # noqa: E402

logger = logging.getLogger(__name__)

_MODEL = "claude-haiku-4-5-20251001"
_MAX_BROCHURE_CHARS = 8_000
_MAX_LLM_TOKENS = 2048
_SOURCE = "brochure_llm"
_RULE_SOURCE = "promoted_brochure_auto"
_MIN_TRIMS = 2
_MAX_BULLETS = 10
_MIN_TEXT_CHARS = 200
_DEFAULT_MIN_YEAR = 2010
_CURATED_SOURCES = frozenset(
    {"manual", "curated", "manual_brochure_review", "manual_review"}
)

_SYSTEM = """\
You are an advanced automotive normalization engine parsing data for a premium car platform.
Return only valid JSON matching the requested schema. Never add commentary outside the JSON object.\
"""

_PROMPT_TEMPLATE = """\
Your sole task is to extract feature upgrades from the provided text snippet and map them directly to a pre-defined trim ladder hierarchy.

Vehicle: {year} {make} {model}

Pre-Defined Trim Hierarchy for this Vehicle (position 1 = base tier, ascending = higher trims):
{discovered_trim_ladder_array}

Raw Brochure Fragment:
{slim_text_payload}

Strict Schema Instructions:
1. For each trim listed in the pre-defined hierarchy, identify the explicit additions or upgrades featured in the text.
2. The absolute base tier trim (ladder_position 1) must list its primary standard equipment groups (Infotainment, Seating surfaces, Wheels, Powertrain, Core Safety features).
3. For EVERY trim level above the base tier, you must execute a strict feature subtraction check. You must ONLY list features that are additions, premium upgrades, or structural replacements over the lower-level trims.
4. CRITICAL: Never repeat a specification that was standard on a lower-level tier. If a higher trim has the same screen or engine as a lower trim, omit it. If it upgrades a feature, use clear, action-oriented text.
5. Strip out all technical dimension metrics (overall length, wheelbase, cargo volume, ground clearance weights, towing maximum capacities) and footnotes/citations (e.g., [1], [2]).
6. Every list string inside the upgrades array must start with an active verb: 'Adds...', 'Upgraded to...', 'Replaces...', 'Swaps...'.
7. Use trim_name values exactly as listed in the pre-defined hierarchy. Include every hierarchy trim in overlays.
8. Max {max_bullets} upgrade strings per trim.

Return your output in a valid JSON object matching this schema:
{{
  "make": "string",
  "model": "string",
  "year": integer,
  "overlays": [
    {{
      "trim_name": "string",
      "ladder_position": integer,
      "upgrades": [
        "string",
        "string"
      ]
    }}
  ]
}}\
"""

# Sentences/lines containing raw dimension or weight specs (removed before LLM call).
_DIMENSION_INDICATOR = re.compile(
    r"\b(?:"
    r"wheelbase|ground\s+clearance|overall\s+length|cargo\s+volume|"
    r"towing\s+capacity|maximum\s+towing|curb\s+weight|gross\s+vehicle\s+weight|"
    r"headroom|legroom|shoulder\s+room|hip\s+room|track\s*\(|"
    r"\d[\d,.]*\s*(?:in\.|lbs?\.|lb-ft|cu\.?\s*ft\.|kg|mm|cm)\b"
    r")",
    re.I,
)

_FOOTNOTE = re.compile(r"\[\d+\]")

_UPGRADE_VERB_PREFIX = re.compile(
    r"^(Adds|Upgraded to|Replaces|Swaps)\b",
    re.I,
)

_ladder_index: dict[str, dict[str, Any]] | None = None


class AnthropicAuthError(RuntimeError):
    """Raised when Anthropic returns 401 / invalid API key."""


def _get_client() -> Any:
    import anthropic

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")
    return anthropic.Anthropic(api_key=api_key)


def load_brochure_ladder_index(*, refresh: bool = False) -> dict[str, dict[str, Any]]:
    """catalog_key -> ladder record from trim_ladders_brochure.json."""
    global _ladder_index
    if _ladder_index is not None and not refresh:
        return _ladder_index

    index: dict[str, dict[str, Any]] = {}
    path = trim_ladders_brochure_path()
    if path.is_file():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            for lad in data.get("ladders") or []:
                if not isinstance(lad, dict):
                    continue
                ck = str(lad.get("catalog_key") or "").strip()
                if ck:
                    index[ck] = lad
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Could not load brochure ladders from %s: %s", path, exc)

    _ladder_index = index
    return index


def discovered_trim_hierarchy_for_ck(catalog_key: str) -> list[dict[str, Any]] | None:
    """
    Return trim hierarchy base → top with ladder_position (1 = base).

    Brochure ladder steps are stored luxury → base; this reverses for the prompt.
    """
    lad = load_brochure_ladder_index().get(catalog_key)
    if not lad:
        return None

    steps = lad.get("steps") or []
    names = [
        str(step.get("name") or "").strip()
        for step in steps
        if isinstance(step, dict) and str(step.get("name") or "").strip()
    ]
    if len(names) < _MIN_TRIMS:
        return None

    base_to_top = list(reversed(names))
    top_index = len(base_to_top) - 1
    hierarchy: list[dict[str, Any]] = []
    for i, name in enumerate(base_to_top):
        if i == 0:
            tier = "base"
        elif i == top_index:
            tier = "top"
        else:
            tier = "mid"
        hierarchy.append(
            {
                "ladder_position": i + 1,
                "trim_name": name,
                "tier": tier,
            }
        )
    return hierarchy


def brochure_ladder_id_for_ck(catalog_key: str) -> str | None:
    lad = load_brochure_ladder_index().get(catalog_key)
    if not lad:
        return None
    lid = str(lad.get("id") or "").strip()
    return lid or None


def _strip_dimension_sentences(text: str) -> str:
    """Drop lines/sentences that are mostly dimensional spec noise."""
    kept: list[str] = []
    for block in text.splitlines():
        line = block.strip()
        if not line:
            kept.append("")
            continue
        sentences = re.split(r"(?<=[.!?])\s+", line)
        for sentence in sentences:
            part = sentence.strip()
            if not part:
                continue
            if _DIMENSION_INDICATOR.search(part):
                continue
            kept.append(part)
    return "\n".join(kept)


def _strip_footnotes(text: str) -> str:
    return _FOOTNOTE.sub("", text)


def _truncate_at_sentence_boundary(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    chunk = text[:max_chars]
    ends = list(re.finditer(r"[.!?](?:\s|$)", chunk))
    if ends:
        return chunk[: ends[-1].end()].strip()
    nl = chunk.rfind("\n")
    if nl > max_chars // 2:
        return chunk[:nl].strip()
    return chunk.strip()


def _trim_text(text: str, *, max_chars: int = _MAX_BROCHURE_CHARS) -> str:
    """Prefer trim-comparison pages; never truncate mid-sentence."""
    t = text.strip()
    if len(t) <= max_chars:
        return t

    chunks: list[tuple[int, str]] = []
    for page in re.split(r"---\s*page\s+\d+\s*---", t, flags=re.I):
        page = page.strip()
        if not page:
            continue
        low = page.lower()
        score = (
            low.count("adds to or replaces")
            + low.count("models")
            + low.count("standard")
            + low.count("• ")
            + low.count("upgraded")
        )
        chunks.append((score, page))
    chunks.sort(key=lambda item: item[0], reverse=True)

    selected_parts: list[str] = []
    total = 0
    for _, page in chunks:
        if total + len(page) + 2 > max_chars:
            remaining = max_chars - total - 2
            if remaining > 200:
                selected_parts.append(_truncate_at_sentence_boundary(page, remaining))
            break
        selected_parts.append(page)
        total += len(page) + 2

    if not selected_parts:
        return _truncate_at_sentence_boundary(t, max_chars)

    joined = "\n\n".join(selected_parts)
    if len(joined) > max_chars:
        return _truncate_at_sentence_boundary(joined, max_chars)
    return joined


def prepare_slim_payload(text: str, *, max_chars: int = _MAX_BROCHURE_CHARS) -> str:
    """Strip dimensions/footnotes, then fit brochure text without mid-sentence cuts."""
    cleaned = _strip_footnotes(_strip_dimension_sentences(text))
    return _trim_text(cleaned, max_chars=max_chars)


def build_llm_user_prompt(
    *,
    year: int,
    make: str,
    model: str,
    slim_text: str,
    trim_hierarchy: list[dict[str, Any]],
    max_bullets: int = _MAX_BULLETS,
) -> str:
    hierarchy_json = json.dumps(trim_hierarchy, indent=2, ensure_ascii=False)
    prepared = prepare_slim_payload(slim_text)
    return _PROMPT_TEMPLATE.format(
        year=year,
        make=make,
        model=model,
        discovered_trim_ladder_array=hierarchy_json,
        slim_text_payload=prepared,
        max_bullets=max_bullets,
    )


def _call_claude(client: Any, prompt: str) -> str:
    from backend.vision.claude_rate_limit import anthropic_messages_create

    msg = anthropic_messages_create(
        client,
        model=_MODEL,
        max_tokens=_MAX_LLM_TOKENS,
        system=_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text.strip()


def _is_auth_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "401" in text
        or "authentication_error" in text
        or "invalid x-api-key" in text
        or "invalid api key" in text
    )


def _has_substantive_adds(adds_by_trim: dict[str, list[str]]) -> bool:
    return any(len(bullets) >= 1 for bullets in adds_by_trim.values())


def _load_json_from_response(raw: str) -> dict[str, Any] | None:
    text = raw.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.I)
    text = re.sub(r"\s*```$", "", text)
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            return None
        try:
            data = json.loads(match.group(0))
        except json.JSONDecodeError:
            return None
    return data if isinstance(data, dict) else None


def _sanitize_upgrade_line(line: str) -> str | None:
    cleaned = _strip_footnotes(str(line or "")).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if len(cleaned) < 12 or len(cleaned) > 240:
        return None
    if _DIMENSION_INDICATOR.search(cleaned):
        return None
    if not _UPGRADE_VERB_PREFIX.match(cleaned):
        low = cleaned.lower()
        if low.startswith(("standard ", "includes ", "features ")):
            cleaned = f"Adds {cleaned}"
        else:
            return None
    return cleaned


def _parse_legacy_shape(data: dict[str, Any]) -> dict[str, Any] | None:
    trims = data.get("trims")
    adds = data.get("adds")
    if not isinstance(trims, list) or not isinstance(adds, dict):
        return None
    if len(trims) < _MIN_TRIMS:
        return None

    clean_trims = [str(t).strip() for t in trims if str(t).strip()]
    clean_adds: dict[str, list[str]] = {}
    for key, bullets in adds.items():
        trim_name = str(key).strip()
        if not trim_name or not isinstance(bullets, list):
            continue
        normalized = [
            line
            for b in bullets
            if (line := _sanitize_upgrade_line(str(b))) is not None
        ]
        if normalized:
            clean_adds[trim_name] = normalized[:_MAX_BULLETS]

    for trim_name in clean_trims:
        clean_adds.setdefault(trim_name, [])

    if len(clean_trims) < _MIN_TRIMS or not _has_substantive_adds(clean_adds):
        return None
    return {"trims": clean_trims, "adds": clean_adds}


def _parse_overlays_shape(
    data: dict[str, Any],
    *,
    expected_hierarchy: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    overlays = data.get("overlays")
    if not isinstance(overlays, list) or len(overlays) < _MIN_TRIMS:
        return None

    expected_names: list[str] = []
    if expected_hierarchy:
        expected_names = [str(row.get("trim_name") or "").strip() for row in expected_hierarchy]
        expected_names = [n for n in expected_names if n]

    by_trim: dict[str, tuple[int, list[str]]] = {}
    for entry in overlays:
        if not isinstance(entry, dict):
            continue
        trim_name = str(entry.get("trim_name") or "").strip()
        if not trim_name:
            continue
        try:
            position = int(entry.get("ladder_position") or 0)
        except (TypeError, ValueError):
            position = 0
        upgrades_raw = entry.get("upgrades")
        if not isinstance(upgrades_raw, list):
            upgrades_raw = []
        upgrades = [
            line
            for item in upgrades_raw
            if (line := _sanitize_upgrade_line(str(item))) is not None
        ]
        by_trim[trim_name] = (position, upgrades[:_MAX_BULLETS])

    if expected_names:
        missing = [n for n in expected_names if n not in by_trim]
        if missing:
            return None
        ordered = expected_names
    else:
        ordered = sorted(by_trim.keys(), key=lambda name: by_trim[name][0])

    adds_by_trim: dict[str, list[str]] = {}
    for trim_name in ordered:
        adds_by_trim[trim_name] = by_trim.get(trim_name, (0, []))[1]

    if not _has_substantive_adds(adds_by_trim):
        return None

    # trims_available: most luxurious first (highest ladder_position).
    trims_most_to_least = sorted(
        ordered,
        key=lambda name: by_trim.get(name, (0, []))[0],
        reverse=True,
    )
    return {"trims": trims_most_to_least, "adds": adds_by_trim}


def _parse_response(
    raw: str,
    *,
    expected_hierarchy: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    data = _load_json_from_response(raw)
    if not data:
        return None

    if "overlays" in data:
        return _parse_overlays_shape(data, expected_hierarchy=expected_hierarchy)

    return _parse_legacy_shape(data)


def _existing_source(ck: str) -> str:
    path = TRIM_ADDS_BY_YEAR_DIR / f"{ck.replace('|', '__')}.json"
    if not path.is_file():
        return ""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return str(data.get("source") or "").lower()
    except (OSError, json.JSONDecodeError):
        return ""


def _should_skip_existing(existing_src: str, *, overwrite: bool) -> str | None:
    if any(curated in existing_src for curated in _CURATED_SOURCES):
        return "skipped:curated"
    if existing_src == _SOURCE and not overwrite:
        return "skipped:already_done"
    if existing_src == _RULE_SOURCE and not overwrite:
        return "skipped:rule_done"
    return None


def _write_overlay(
    ck: str,
    year: int,
    make: str,
    model: str,
    parsed: dict[str, Any],
    *,
    ladder_id: str,
    source: str = _SOURCE,
) -> Path:
    trims_most_to_least = parsed["trims"]
    adds_by_trim = parsed["adds"]

    TRIM_ADDS_BY_YEAR_DIR.mkdir(parents=True, exist_ok=True)
    out = TRIM_ADDS_BY_YEAR_DIR / f"{ck.replace('|', '__')}.json"
    payload = {
        "catalog_key": ck,
        "year": year,
        "make": make,
        "model": model,
        "ladder_id": ladder_id,
        "source": source,
        "trims_available": trims_most_to_least,
        "adds_by_trim": adds_by_trim,
    }
    payload = enrich_brochure_overlay(payload)
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return out


def _empty_reason(
    *,
    ck: str,
    year: int,
    make: str,
    model: str,
    text: str,
    min_year: int,
) -> str | None:
    if not ck:
        return "empty:missing_meta"
    if not year:
        return "empty:missing_meta"
    if year < min_year:
        return f"empty:pre_{min_year}"
    if not make or not model:
        return "empty:missing_meta"
    if len(text) < _MIN_TEXT_CHARS:
        return "empty:short_text"
    return None


def _try_rule_based(
    meta: dict[str, Any],
    ck: str,
    *,
    dry_run: bool = False,
) -> str | None:
    payload = extract_promotable_overlay(meta)
    if not payload:
        return None
    adds = payload.get("adds_by_trim") or {}
    if not isinstance(adds, dict) or not _has_substantive_adds(adds):
        return None
    if dry_run:
        return "dry_run:rule"
    year = int(payload.get("year") or meta.get("year") or 0)
    make = str(payload.get("make") or meta.get("make") or "")
    model = str(payload.get("model") or meta.get("model") or "")
    ladder_id = str(payload.get("ladder_id") or f"brochure_rule_{ck.replace('|', '__')}")
    parsed = {
        "trims": list(payload.get("trims_available") or []),
        "adds": dict(adds),
    }
    if len(parsed["trims"]) < _MIN_TRIMS:
        return None
    out = _write_overlay(
        ck,
        year,
        make,
        model,
        parsed,
        ladder_id=ladder_id,
        source=_RULE_SOURCE,
    )
    return f"written:rule:{out.name}"


def analyze_one(
    slim_path: Path,
    client: Any | None,
    *,
    dry_run: bool = False,
    overwrite: bool = False,
    min_year: int = _DEFAULT_MIN_YEAR,
    rule_first: bool = True,
    rule_only: bool = False,
) -> str:
    """Return status string: 'written', 'skipped', 'empty:*', 'error:<msg>'."""
    try:
        meta = json.loads(slim_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        return f"error:read:{e}"

    ck = str(meta.get("catalog_key") or "")
    year = int(meta.get("year") or 0)
    make = str(meta.get("make") or "").strip()
    model = str(meta.get("model") or "").strip()
    text = str(meta.get("combined_trim_pages_text") or "").strip()

    empty = _empty_reason(
        ck=ck, year=year, make=make, model=model, text=text, min_year=min_year
    )
    if empty:
        return empty

    existing_src = _existing_source(ck)
    skip = _should_skip_existing(existing_src, overwrite=overwrite)
    if skip:
        return skip

    if rule_first or rule_only:
        rule_status = _try_rule_based(meta, ck, dry_run=dry_run)
        if rule_status:
            return rule_status
        if rule_only:
            return "empty:rule_failed"

    trim_hierarchy = discovered_trim_hierarchy_for_ck(ck)
    if not trim_hierarchy:
        return "empty:no_ladder"

    prompt = build_llm_user_prompt(
        year=year,
        make=make,
        model=model,
        slim_text=text,
        trim_hierarchy=trim_hierarchy,
    )

    if dry_run:
        return "dry_run:llm"

    if client is None:
        return "error:api:no_client"

    try:
        raw = _call_claude(client, prompt)
    except Exception as e:
        if _is_auth_error(e):
            raise AnthropicAuthError(str(e)) from e
        return f"error:api:{e}"

    parsed = _parse_response(raw, expected_hierarchy=trim_hierarchy)
    if not parsed or len(parsed.get("trims") or []) < _MIN_TRIMS:
        return "empty:parse_failed"

    ladder_id = brochure_ladder_id_for_ck(ck) or f"brochure_llm_{ck.replace('|', '__')}"
    out = _write_overlay(ck, year, make, model, parsed, ladder_id=ladder_id)
    return f"written:llm:{out.name}"


def main() -> int:
    load_project_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Analyze brochures with Claude Haiku.")
    parser.add_argument("--limit", type=int, default=0, help="Max files to process (0=all)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing LLM results")
    parser.add_argument("--dry-run", action="store_true", help="Do not write or call API")
    parser.add_argument("--make", default="", help="Filter by make (case-insensitive)")
    parser.add_argument("--year", type=int, default=0, help="Filter by year")
    parser.add_argument(
        "--min-year",
        type=int,
        default=_DEFAULT_MIN_YEAR,
        help=f"Skip brochures before this model year (default {_DEFAULT_MIN_YEAR})",
    )
    parser.add_argument(
        "--rule-first",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Try rule-based extraction before calling Claude (default: on)",
    )
    parser.add_argument(
        "--rule-only",
        action="store_true",
        help="Only run rule-based extraction (no ANTHROPIC_API_KEY required)",
    )
    args = parser.parse_args()

    slim_files = sorted(BROCHURE_TEXT_SLIM_DIR.glob("*.json"))
    if args.make:
        mk = args.make.lower().replace(" ", "")
        slim_files = [f for f in slim_files if mk in f.stem.replace("_", "")]
    if args.year:
        slim_files = [f for f in slim_files if f.stem.startswith(str(args.year))]

    if args.limit:
        slim_files = slim_files[: args.limit]

    if not slim_files:
        print("No brochure files to process.")
        return 0

    client = None
    need_api = not args.dry_run and not args.rule_only
    if need_api:
        try:
            client = _get_client()
        except RuntimeError as e:
            print(f"Error: {e}")
            print("Use --rule-only to extract without an API key, or set ANTHROPIC_API_KEY.")
            return 1

    stats: dict[str, int] = {}
    for i, path in enumerate(slim_files):
        try:
            status = analyze_one(
                path,
                client,
                dry_run=args.dry_run,
                overwrite=args.overwrite,
                min_year=args.min_year,
                rule_first=args.rule_first,
                rule_only=args.rule_only,
            )
        except AnthropicAuthError as e:
            logger.error(
                "Anthropic authentication failed (invalid ANTHROPIC_API_KEY). "
                "Stopping batch after %d/%d files.",
                i + 1,
                len(slim_files),
            )
            print(f"\nError: {e}")
            print("Fix ANTHROPIC_API_KEY and re-run, or use --rule-only for free extraction.")
            stats["error:api_auth"] = stats.get("error:api_auth", 0) + 1
            break

        key = status.split(":")[0]
        stats[key] = stats.get(key, 0) + 1
        if (i + 1) % 10 == 0 or not status.startswith("skipped"):
            logger.info("[%d/%d] %s → %s", i + 1, len(slim_files), path.stem, status)

    print("\nDone.")
    for k, v in sorted(stats.items()):
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
