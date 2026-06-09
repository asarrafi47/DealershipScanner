"""
Compute trim-ladder ``adds`` bullets from offline ``trim_spec_sheets/*.json``.

Walks each ladder baseline → top (entry trim first), compares structured spec rows
against the cumulative feature map from lower rungs, and writes human-readable deltas
into ``trim_ladders_generated.json`` step ``adds`` arrays.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

from backend.enrichment.trim_ladder import _LADDERS_GENERATED_JSON, _read_ladders_file
from backend.enrichment.trim_spec_extractor import STANDARD_LABELS

logger = logging.getLogger(__name__)

from backend.enrichment.dictionary_paths import DICTIONARY_ROOT, trim_spec_sheets_dir

_SHEETS_DIR = trim_spec_sheets_dir()

_GENERIC_ADDS_RE = re.compile(
    r"^equipment and features typical of the .+ trim\.?$",
    re.I,
)


def _norm_token(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())


def _norm_label_key(label: str) -> str:
    return re.sub(r"\s+", " ", (label or "").strip().lower())


def _norm_value(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _parse_trim_rows(raw: Any) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if isinstance(raw, dict):
        for label, value in raw.items():
            lbl = str(label or "").strip()
            val = str(value or "").strip()
            if lbl and val:
                rows.append({"label": lbl, "value": val})
        return rows
    if isinstance(raw, list):
        for row in raw:
            if not isinstance(row, dict):
                continue
            lbl = str(row.get("label") or "").strip()
            val = str(row.get("value") or "").strip()
            if lbl and val:
                rows.append({"label": lbl, "value": val})
    return rows


def _sanitize_sheet_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Keep structured sheet label/value pairs (no prose-splitting heuristics)."""
    out: list[dict[str, str]] = []
    for row in rows:
        label = str(row.get("label") or "").strip()
        value = re.sub(r"\s+", " ", str(row.get("value") or "").strip())
        if not label or not value or len(value) < 2:
            continue
        out.append({"label": label, "value": value})
    return out


def load_spec_sheet(ladder_id: str, *, sheets_dir: Path | None = None) -> dict[str, Any] | None:
    """Load ``trim_spec_sheets/{ladder_id}.json`` or return None."""
    lid = (ladder_id or "").strip()
    if not lid:
        return None
    root = sheets_dir or _SHEETS_DIR
    path = root / f"{lid}.json"
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logger.exception("Failed to read trim spec sheet %s", path)
        return None
    return raw if isinstance(raw, dict) else None


def resolve_trim_spec_rows(
    sheet: dict[str, Any],
    step: dict[str, Any],
) -> list[dict[str, str]]:
    """Match a ladder step to sheet rows using ``name`` and ``aliases``."""
    trims = sheet.get("trims") or {}
    if not isinstance(trims, dict):
        return []

    names: list[str] = []
    primary = str(step.get("name") or "").strip()
    if primary:
        names.append(primary)
    for alias in step.get("aliases") or []:
        a = str(alias or "").strip()
        if a and a not in names:
            names.append(a)

    for candidate in names:
        raw = trims.get(candidate)
        if raw is not None:
            return _sanitize_sheet_rows(_parse_trim_rows(raw))

    for candidate in names:
        want = _norm_token(candidate)
        if not want:
            continue
        for key, raw in trims.items():
            if _norm_token(str(key)) == want:
                return _sanitize_sheet_rows(_parse_trim_rows(raw))
    return []


def feature_map_from_rows(rows: list[dict[str, str]]) -> dict[str, tuple[str, str]]:
    """Map normalized label key → (display label, value). Last row wins per key."""
    out: dict[str, tuple[str, str]] = {}
    for row in rows:
        display = str(row.get("label") or "").strip()
        value = str(row.get("value") or "").strip()
        if not display or not value:
            continue
        out[_norm_label_key(display)] = (display, value)
    return out


def _ordered_feature_items(features: dict[str, tuple[str, str]]) -> list[tuple[str, str]]:
    """Stable label order: STANDARD_LABELS first, then alpha by display label."""
    if not features:
        return []
    standard_index = { _norm_label_key(lbl): i for i, lbl in enumerate(STANDARD_LABELS) }
    items = list(features.values())

    def sort_key(item: tuple[str, str]) -> tuple[int, str]:
        label = item[0]
        return (standard_index.get(_norm_label_key(label), 999), label.lower())

    return sorted(items, key=sort_key)


def compute_step_adds(
    features: dict[str, tuple[str, str]],
    *,
    cumulative_baseline: dict[str, tuple[str, str]],
    is_base_rung: bool,
) -> list[str]:
    """Build ``adds`` strings for one rung."""
    adds: list[str] = []
    for display_label, value in _ordered_feature_items(features):
        if is_base_rung:
            adds.append(f"{display_label}: {value}")
            continue

        key = _norm_label_key(display_label)
        prior = cumulative_baseline.get(key)
        if prior is None:
            adds.append(f"Adds {display_label}: {value}")
            continue
        prior_label, prior_value = prior
        if _norm_value(prior_value) == _norm_value(value):
            continue
        adds.append(f"Upgrades {display_label}: {value} (was {prior_value})")
        _ = prior_label
    return adds


def compute_ladder_adds_by_step_name(
    ladder: dict[str, Any],
    sheet: dict[str, Any],
) -> dict[str, list[str]]:
    """
    Return ``{step_name: [adds...]}`` using baseline → top order (entry trim first).

    Steps without resolvable spec rows are omitted from the result (caller keeps prior adds).
    """
    steps = ladder.get("steps") or []
    if not isinstance(steps, list) or len(steps) < 2:
        return {}

    baseline_order = list(reversed(steps))
    cumulative: dict[str, tuple[str, str]] = {}
    out: dict[str, list[str]] = {}

    for idx, step in enumerate(baseline_order):
        if not isinstance(step, dict):
            continue
        name = str(step.get("name") or "").strip()
        if not name:
            continue
        rows = resolve_trim_spec_rows(sheet, step)
        features = feature_map_from_rows(rows)
        if not features:
            continue
        adds = compute_step_adds(
            features,
            cumulative_baseline=cumulative,
            is_base_rung=(idx == 0),
        )
        if adds:
            out[name] = adds
        cumulative.update(features)

    return out


def apply_adds_to_ladder(ladder: dict[str, Any], adds_by_name: dict[str, list[str]]) -> int:
    """Inject computed adds into ladder steps; returns count of steps updated."""
    if not adds_by_name:
        return 0
    updated = 0
    for step in ladder.get("steps") or []:
        if not isinstance(step, dict):
            continue
        name = str(step.get("name") or "").strip()
        new_adds = adds_by_name.get(name)
        if not new_adds:
            continue
        step["adds"] = new_adds
        updated += 1
    return updated


def apply_trim_diffs_to_generated_ladders(
    *,
    generated_path: Path | None = None,
    sheets_dir: Path | None = None,
    ladder_ids: set[str] | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """
    Read ``trim_ladders_generated.json``, compute adds from spec sheets, optionally save.

    Returns stats dict with keys: ladders_seen, sheets_found, ladders_updated, steps_updated, skipped_no_sheet.
    """
    path = generated_path or _LADDERS_GENERATED_JSON
    if not path.is_file():
        raise FileNotFoundError(f"Generated ladders file not found: {path}")

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Failed to parse {path}") from exc

    ladders = payload.get("ladders")
    if not isinstance(ladders, list):
        raise RuntimeError(f"Invalid ladders payload in {path}")

    stats = {
        "ladders_seen": 0,
        "sheets_found": 0,
        "ladders_updated": 0,
        "steps_updated": 0,
        "skipped_no_sheet": 0,
    }

    for ladder in ladders:
        if not isinstance(ladder, dict):
            continue
        lid = str(ladder.get("id") or "").strip()
        if not lid:
            continue
        if ladder_ids and lid not in ladder_ids:
            continue
        stats["ladders_seen"] += 1

        sheet = load_spec_sheet(lid, sheets_dir=sheets_dir)
        if sheet is None:
            stats["skipped_no_sheet"] += 1
            continue
        stats["sheets_found"] += 1

        adds_by_name = compute_ladder_adds_by_step_name(ladder, sheet)
        if not adds_by_name:
            continue

        n = apply_adds_to_ladder(ladder, adds_by_name)
        if n:
            stats["ladders_updated"] += 1
            stats["steps_updated"] += n

    if not dry_run and stats["steps_updated"]:
        payload["ladder_count"] = len(ladders)
        path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    return stats


def strip_generic_placeholder_adds(ladder: dict[str, Any]) -> None:
    """Remove auto-generated placeholder adds before recomputing (optional helper)."""
    for step in ladder.get("steps") or []:
        if not isinstance(step, dict):
            continue
        adds = step.get("adds") or []
        if not isinstance(adds, list):
            continue
        filtered = [
            str(a).strip()
            for a in adds
            if str(a).strip() and not _GENERIC_ADDS_RE.match(str(a).strip())
        ]
        if len(filtered) != len(adds):
            step["adds"] = filtered
