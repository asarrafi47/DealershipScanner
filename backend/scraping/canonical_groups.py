"""
Canonical dealer-group names: alias table, lookup keys, and batch sibling reinforcement.
Does not change confidence thresholds.
"""
from __future__ import annotations

import json
import re
from pathlib import Path


def _strip_leading_articles(s: str) -> str:
    t = (s or "").strip()
    while True:
        low = t.lower()
        for art in ("the ", "a ", "an "):
            if low.startswith(art):
                t = t[len(art) :].lstrip()
                break
        else:
            break
    return t.strip()


# Normalized lookup key: lowercase, collapse space, hyphens -> space for matching
def normalize_lookup_key(name: str) -> str:
    s = _strip_leading_articles((name or "").strip())
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


# Exact keys after normalize_lookup_key -> final display string
GROUP_ALIAS_EXACT: dict[str, str] = {
    "tuttle-click": "Tuttle-Click Automotive Group",
    "tuttle click": "Tuttle-Click Automotive Group",
    "tuttle-click automotive group": "Tuttle-Click Automotive Group",
    "tuttle click automotive group": "Tuttle-Click Automotive Group",
    "tuttle-click automotive": "Tuttle-Click Automotive Group",
    "tuttle click automotive": "Tuttle-Click Automotive Group",
    "tuttle-click auto group": "Tuttle-Click Automotive Group",
    "tuttle click auto group": "Tuttle-Click Automotive Group",
    "sonic automotive": "Sonic Automotive",
    "sonic automotive group": "Sonic Automotive",
    "sonic": "Sonic Automotive",
}


def canonical_group_display(name: str) -> str:
    """
    Final public-facing dealer group label: alias expansion + known family cleanup.
    """
    if not (name or "").strip():
        return name
    key = normalize_lookup_key(name)
    if key in GROUP_ALIAS_EXACT:
        return GROUP_ALIAS_EXACT[key]
    if "tuttle" in key and "click" in key:
        return "Tuttle-Click Automotive Group"
    if "sonic" in key and "automotive" in key:
        return "Sonic Automotive"
    return name.strip()


def merge_canonical_key(name: str) -> str:
    """Stable key for deduping extractions that refer to the same org."""
    return normalize_lookup_key(canonical_group_display(name))


def family_key(name: str) -> str | None:
    """Cluster id for sibling-site reinforcement; None if unknown family."""
    k = normalize_lookup_key(canonical_group_display(name))
    if "tuttle" in k and "click" in k:
        return "tuttle_click"
    if "sonic" in k:
        return "sonic"
    if "hendrick" in k:
        return "hendrick"
    if "penske" in k:
        return "penske"
    return None


def apply_sibling_canonical_reinforcement(batch: list) -> None:
    """
    When 2+ sites in the same run resolve to the same family with assigned status,
    align canonical labels to the longest / richest agreed form (no score changes).
    """
    by_fk: dict[str, list] = {}
    for sr in batch:
        if not getattr(sr, "homepage_loaded", False):
            continue
        label = sr.best_candidate_canonical or sr.best_candidate_normalized or sr.best_candidate
        if not label:
            continue
        fk = family_key(label)
        if not fk:
            continue
        by_fk.setdefault(fk, []).append(sr)

    for _fk, group in by_fk.items():
        assigned = [s for s in group if s.final_status == "assigned"]
        if len(assigned) < 2:
            continue
        canons = [s.best_candidate_canonical for s in assigned if s.best_candidate_canonical]
        if len(canons) < 2:
            continue
        preferred = max(canons, key=len)
        for s in assigned:
            if s.best_candidate_canonical != preferred:
                s.best_candidate_canonical = preferred
                s.best_candidate = preferred
                s.flags.append("sibling_canonical_reinforcement")


def load_alias_table_from_json(path: str | Path | None) -> int:
    """
    Merge JSON `aliases` and `canonical_groups` into GROUP_ALIAS_EXACT.
    Safe to call multiple times; later merges override keys.
    Returns number of entries applied.
    """
    if not path:
        return 0
    p = Path(path)
    if not p.is_file():
        return 0
    with p.open(encoding="utf-8") as f:
        data = json.load(f)
    n = 0
    for k, v in (data.get("aliases") or {}).items():
        if not isinstance(v, str) or not str(k).strip():
            continue
        nk = normalize_lookup_key(str(k))
        GROUP_ALIAS_EXACT[nk] = v.strip()
        n += 1
    for cg in data.get("canonical_groups") or []:
        if isinstance(cg, str) and cg.strip():
            key = normalize_lookup_key(cg)
            GROUP_ALIAS_EXACT[key] = cg.strip()
            n += 1
    return n
