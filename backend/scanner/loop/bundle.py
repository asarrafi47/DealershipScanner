"""Run-bundle helpers. This module so far holds the shard loader and merger (card L6).

``load_shard`` reads the files of one shard directory (that is its job); ``merge_shards`` is pure: it
takes loaded shards, never mutates them and returns a new dict that does not depend on shard order.
Dealer-derived text passes through verbatim; whoever writes it to a file or table cleans it first.
"""

from __future__ import annotations

import copy
import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

COUNT_KEYS = ("ok", "thin", "inaccurate", "no_rows", "error", "no_recipe")
LIFECYCLE_COUNTERS = ("routed", "resynth_ok", "capture_ok", "failed", "resynth_failed")
_LIST_KEYS = ("needs_discovery", "thin", "inaccurate")


def _read_lines(path: Path) -> list:
    if not path.is_file():
        return []
    text = path.read_text(encoding="utf-8", errors="replace")
    return [line.rstrip() for line in text.splitlines() if line.strip()]


def load_shard(shard_dir) -> dict:
    """Load one shard directory: ``triage.json`` or ``triage.json.gz`` plus the two optional text lists.

    A missing triage file raises ``FileNotFoundError`` and an unreadable or malformed one raises
    ``ValueError``; both messages name the path.
    """
    directory = Path(shard_dir)
    plain = directory / "triage.json"
    zipped = directory / "triage.json.gz"
    if plain.is_file():
        path = plain
    elif zipped.is_file():
        path = zipped
    else:
        raise FileNotFoundError("no triage.json or triage.json.gz in " + str(directory))
    try:
        if path.suffix == ".gz":
            with gzip.open(path, "rb") as handle:
                raw = handle.read()
        else:
            raw = path.read_bytes()
        triage = json.loads(raw.decode("utf-8"))
    except (OSError, EOFError, ValueError, UnicodeError, RecursionError) as exc:
        raise ValueError("malformed triage file " + str(path) + ": " + type(exc).__name__) from None
    if not isinstance(triage, dict) or not isinstance(triage.get("dealers"), list):
        raise ValueError("malformed triage file " + str(path) + ": no dealers list")
    return {
        "name": directory.name,
        "triage": triage,
        "needs_discovery_lines": _read_lines(directory / "needs_discovery.txt"),
        "slow_dealers_lines": _read_lines(directory / "slow_dealers.txt"),
    }


def _parse_started(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _canon(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _as_dict(triage: dict, key: str, shard_name) -> dict:
    value = triage.get(key, {})
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("shard " + str(shard_name) + ": " + key + " is not an object")
    return value


def _as_list(triage: dict, key: str, shard_name) -> list:
    value = triage.get(key, [])
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("shard " + str(shard_name) + ": " + key + " is not a list")
    return value


def _number(value) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0
    return value


def _merge_maps(maps: list) -> dict:
    merged = {}
    for mapping in maps:
        for key, value in mapping.items():
            if key not in merged or _canon(value) > _canon(merged[key]):
                merged[key] = value
    return {key: merged[key] for key in sorted(merged)}


def merge_shards(shards) -> dict:
    """Merge loaded shards into one fleet-level dict; the result does not depend on shard order."""
    shards = list(shards)
    if not shards:
        raise ValueError("no shards to merge")
    triages = []
    for index, shard in enumerate(shards):
        if not isinstance(shard, dict) or not isinstance(shard.get("triage"), dict):
            raise ValueError("shard " + str(index) + " is not a loaded shard")
        triages.append((shard.get("name", index), shard["triage"], shard))

    dealers = []
    seen = {}
    duplicates = set()
    for name, triage, _ in triages:
        for row in _as_list(triage, "dealers", name):
            dealer_id = row.get("dealer_id") if isinstance(row, dict) else None
            if not isinstance(dealer_id, str) or not dealer_id:
                raise ValueError("shard " + str(name) + ": a dealer row has no dealer_id")
            if dealer_id in seen:
                duplicates.add(dealer_id)
            seen[dealer_id] = True
            dealers.append(row)
    if duplicates:
        raise ValueError("dealers in more than one shard (or twice): " + ", ".join(sorted(duplicates)))
    dealers.sort(key=lambda row: row["dealer_id"].encode("utf-8"))

    counts = dict.fromkeys(COUNT_KEYS, 0)
    coverage = {}
    lifecycle_counts = dict.fromkeys(LIFECYCLE_COUNTERS, 0)
    enabled = True
    retried_maps = []
    by_dealer_maps = []
    clusters = {}
    starts = []
    lists = {key: [] for key in _LIST_KEYS}
    for name, triage, _ in triages:
        for key in COUNT_KEYS:
            counts[key] += _number(_as_dict(triage, "counts", name).get(key))
        for key, value in _as_dict(triage, "recipe_coverage", name).items():
            coverage[key] = coverage.get(key, 0) + _number(value)
        life = _as_dict(triage, "lifecycle", name)
        for key in LIFECYCLE_COUNTERS:
            lifecycle_counts[key] += _number(life.get(key))
        if life.get("enabled") is False:
            enabled = False
        retried_maps.append(_as_dict(life, "retried", name))
        by_dealer_maps.append(_as_dict(life, "by_dealer", name))
        for item in _as_list(triage, "platform_clusters", name):
            clusters.setdefault(_canon(item), item)
        parsed = _parse_started(triage.get("started"))
        if parsed is not None:
            starts.append((parsed, triage["started"]))
        for key in _LIST_KEYS:
            lists[key].extend(_as_list(triage, key, name))

    started = min(starts, key=lambda pair: (pair[0], pair[1]))[1] if starts else None
    lifecycle = dict(lifecycle_counts)
    lifecycle["enabled"] = enabled
    lifecycle["retried"] = _merge_maps(retried_maps)
    lifecycle["by_dealer"] = _merge_maps(by_dealer_maps)

    merged = {
        "started": started,
        "dealers": dealers,
        "counts": counts,
        "needs_discovery": sorted(lists["needs_discovery"]),
        "thin": sorted(lists["thin"]),
        "inaccurate": sorted(lists["inaccurate"]),
        "recipe_coverage": {key: coverage[key] for key in sorted(coverage)},
        "lifecycle": lifecycle,
        "platform_clusters": [clusters[key] for key in sorted(clusters)],
        "needs_discovery_lines": sorted(line for _, _, s in triages for line in s.get("needs_discovery_lines", [])),
        "slow_dealers_lines": sorted(line for _, _, s in triages for line in s.get("slow_dealers_lines", [])),
    }
    return copy.deepcopy(merged)
