"""
Dictionary catalog: manifest + SQLite index for EPA and Complete_Options CSVs.

Lookups prefer the catalog DB (O(1) by normalized key); fall back to legacy glob.
"""

from __future__ import annotations

import csv
import json
import logging
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterator

from backend.enrichment.dictionary_paths import (
    CANONICAL_CSV_COLUMNS,
    CATALOG_DB_PATH,
    DICTIONARY_ROOT,
    INDEX_DIR,
    MAKE_ALIASES_PATH,
    MANIFEST_PATH,
    iter_search_roots,
)

logger = logging.getLogger(__name__)

_STUB_MAX_BYTES = 300
_OPTIONS_SUFFIX = "_Complete_Options.csv"
_EPA_SUFFIX = "_EPA.csv"
_DT_OPTIONS_SUFFIX = "_DT_Complete_Options.csv"


def _norm_token(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())


def _filename_token(s: str) -> str:
    return re.sub(r"[^\w]+", "_", (s or "").strip()).strip("_")


@lru_cache(maxsize=1)
def load_make_aliases() -> dict[str, str]:
    if not MAKE_ALIASES_PATH.is_file():
        return {}
    try:
        raw = json.loads(MAKE_ALIASES_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {str(k).lower(): str(v) for k, v in raw.items()}


def canonical_make(make: str) -> str:
    key = (make or "").strip().lower()
    if not key:
        return make or ""
    aliases = load_make_aliases()
    return aliases.get(key, make.strip())


def catalog_key(year: int | None, make: str, model: str) -> str:
    y = str(year) if year is not None else "*"
    return f"{y}|{_norm_token(canonical_make(make))}|{_norm_token(model)}"


def parse_csv_filename(path: Path) -> dict[str, Any] | None:
    name = path.name
    if name.endswith(_DT_OPTIONS_SUFFIX):
        stem = name[: -len(_DT_OPTIONS_SUFFIX)]
        return {
            "kind": "options_dt",
            "year": None,
            "make": stem.replace("_", " "),
            "model": "DT",
            "filename": name,
        }
    if name.endswith(_OPTIONS_SUFFIX):
        stem = name[: -len(_OPTIONS_SUFFIX)]
        kind = "options"
    elif name.endswith(_EPA_SUFFIX):
        stem = name[: -len(_EPA_SUFFIX)]
        kind = "epa"
    else:
        return None
    m = re.match(r"^(\d{4})_(.+)$", stem)
    if not m:
        return None
    year = int(m.group(1))
    rest = m.group(2)
    parts = rest.split("_", 1)
    if len(parts) < 2:
        return {"kind": kind, "year": year, "make": rest.replace("_", " "), "model": "", "filename": name}
    make_raw, model_raw = parts
    return {
        "kind": kind,
        "year": year,
        "make": make_raw.replace("_", " "),
        "model": model_raw.replace("_", " "),
        "filename": name,
    }


def _count_csv_rows(path: Path) -> int:
    try:
        with path.open(encoding="utf-8", newline="", errors="replace") as fh:
            return max(0, sum(1 for _ in csv.DictReader(fh)))
    except OSError:
        return 0


def options_status(path: Path, row_count: int | None = None) -> str:
    try:
        size = path.stat().st_size
    except OSError:
        return "missing"
    rows = row_count if row_count is not None else _count_csv_rows(path)
    if rows <= 0 and size <= _STUB_MAX_BYTES:
        return "stub"
    if rows <= 0:
        return "empty"
    return "rich"


def _rel_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(DICTIONARY_ROOT.resolve()))
    except ValueError:
        return str(path.resolve())


def _abs_from_rel(rel: str) -> Path:
    p = DICTIONARY_ROOT / rel
    if p.is_file():
        return p.resolve()
    return (DICTIONARY_ROOT / rel).resolve()


def iter_dictionary_csv_paths(kind: str) -> Iterator[Path]:
    seen: set[str] = set()
    suffix = _EPA_SUFFIX if kind == "epa" else _OPTIONS_SUFFIX
    for root in iter_search_roots(kind=kind):
        if not root.is_dir():
            continue
        for path in sorted(root.rglob(f"*{suffix}")):
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            yield path


def iter_dt_options_paths() -> Iterator[Path]:
    seen: set[str] = set()
    for root in iter_search_roots(kind="options"):
        if not root.is_dir():
            continue
        for path in sorted(root.rglob(f"*{_DT_OPTIONS_SUFFIX}")):
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            yield path
    for path in sorted(DICTIONARY_ROOT.glob(f"*{_DT_OPTIONS_SUFFIX}")):
        key = str(path.resolve())
        if key not in seen:
            yield path


def build_manifest_entries() -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}

    def bucket(key: str, year: int | None, make: str, model: str) -> dict[str, Any]:
        if key not in grouped:
            grouped[key] = {
                "catalog_key": key,
                "year": year,
                "make": canonical_make(make),
                "model": model,
                "epa_path": None,
                "options_path": None,
                "options_status": "missing",
                "epa_row_count": 0,
                "options_row_count": 0,
            }
        return grouped[key]

    for path in iter_dictionary_csv_paths("epa"):
        meta = parse_csv_filename(path)
        if not meta or meta.get("kind") != "epa":
            continue
        year = meta["year"]
        make = meta["make"]
        model = meta["model"]
        key = catalog_key(year, make, model)
        entry = bucket(key, year, make, model)
        rows = _count_csv_rows(path)
        entry["epa_path"] = _rel_path(path)
        entry["epa_row_count"] = rows

    for path in iter_dictionary_csv_paths("options"):
        meta = parse_csv_filename(path)
        if not meta or meta.get("kind") != "options":
            continue
        year = meta["year"]
        make = meta["make"]
        model = meta["model"]
        key = catalog_key(year, make, model)
        entry = bucket(key, year, make, model)
        rows = _count_csv_rows(path)
        status = options_status(path, rows)
        if status == "stub" and entry.get("options_status") == "rich":
            continue
        entry["options_path"] = _rel_path(path)
        entry["options_row_count"] = rows
        entry["options_status"] = status

    return sorted(grouped.values(), key=lambda e: (e.get("year") or 0, e.get("make") or "", e.get("model") or ""))


def write_manifest(entries: list[dict[str, Any]] | None = None) -> Path:
    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    entries = entries if entries is not None else build_manifest_entries()
    payload = {
        "version": 1,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "dictionary_root": str(DICTIONARY_ROOT),
        "entry_count": len(entries),
        "entries": entries,
    }
    MANIFEST_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return MANIFEST_PATH


def init_catalog_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS dictionary_entries (
            catalog_key TEXT PRIMARY KEY,
            year INTEGER,
            make TEXT NOT NULL,
            make_norm TEXT NOT NULL,
            model TEXT NOT NULL,
            model_norm TEXT NOT NULL,
            epa_path TEXT,
            options_path TEXT,
            options_status TEXT NOT NULL DEFAULT 'missing',
            epa_row_count INTEGER NOT NULL DEFAULT 0,
            options_row_count INTEGER NOT NULL DEFAULT 0,
            brochure_text_path TEXT,
            brochure_status TEXT,
            brochure_pages_saved INTEGER NOT NULL DEFAULT 0,
            trim_overlay_path TEXT,
            trim_overlay_status TEXT,
            trim_candidate_path TEXT,
            ladder_id TEXT,
            brochure_trim_count INTEGER NOT NULL DEFAULT 0,
            epa_trim_count INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_dict_make_model
            ON dictionary_entries(make_norm, model_norm);
        CREATE INDEX IF NOT EXISTS idx_dict_year
            ON dictionary_entries(year);
        """
    )
    for ddl in (
        "ALTER TABLE dictionary_entries ADD COLUMN brochure_text_path TEXT",
        "ALTER TABLE dictionary_entries ADD COLUMN brochure_status TEXT",
        "ALTER TABLE dictionary_entries ADD COLUMN brochure_pages_saved INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE dictionary_entries ADD COLUMN trim_overlay_path TEXT",
        "ALTER TABLE dictionary_entries ADD COLUMN trim_overlay_status TEXT",
        "ALTER TABLE dictionary_entries ADD COLUMN trim_candidate_path TEXT",
        "ALTER TABLE dictionary_entries ADD COLUMN ladder_id TEXT",
        "ALTER TABLE dictionary_entries ADD COLUMN brochure_trim_count INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE dictionary_entries ADD COLUMN epa_trim_count INTEGER NOT NULL DEFAULT 0",
    ):
        try:
            conn.execute(ddl)
        except sqlite3.OperationalError:
            pass


def rebuild_catalog_db(entries: list[dict[str, Any]] | None = None) -> Path:
    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    entries = entries if entries is not None else build_manifest_entries()
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(str(CATALOG_DB_PATH))
    try:
        init_catalog_db(conn)
        conn.execute("DELETE FROM dictionary_entries")
        for e in entries:
            make = e.get("make") or ""
            model = e.get("model") or ""
            conn.execute(
                """
                INSERT INTO dictionary_entries (
                    catalog_key, year, make, make_norm, model, model_norm,
                    epa_path, options_path, options_status,
                    epa_row_count, options_row_count,
                    brochure_text_path, brochure_status, brochure_pages_saved,
                    trim_overlay_path, trim_overlay_status, trim_candidate_path,
                    ladder_id, brochure_trim_count, epa_trim_count,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    e["catalog_key"],
                    e.get("year"),
                    make,
                    _norm_token(canonical_make(make)),
                    model,
                    _norm_token(model),
                    e.get("epa_path"),
                    e.get("options_path"),
                    e.get("options_status") or "missing",
                    int(e.get("epa_row_count") or 0),
                    int(e.get("options_row_count") or 0),
                    e.get("brochure_text_path"),
                    e.get("brochure_status"),
                    int(e.get("brochure_pages_saved") or 0),
                    e.get("trim_overlay_path"),
                    e.get("trim_overlay_status"),
                    e.get("trim_candidate_path"),
                    e.get("ladder_id"),
                    len(e.get("brochure_trim_names") or []),
                    len(e.get("epa_trim_names") or []),
                    now,
                ),
            )
        conn.commit()
    finally:
        conn.close()
    return CATALOG_DB_PATH


def rebuild_catalog(
    entries: list[dict[str, Any]] | None = None,
    *,
    enrich_derived: bool = True,
) -> tuple[Path, Path]:
    entries = entries if entries is not None else build_manifest_entries()
    if enrich_derived:
        from backend.enrichment.dictionary_derived import enrich_manifest_entries

        entries = enrich_manifest_entries(entries)
    manifest = write_manifest(entries)
    db = rebuild_catalog_db(entries)
    return manifest, db


@lru_cache(maxsize=1)
def _catalog_db_available() -> bool:
    return CATALOG_DB_PATH.is_file()


def _catalog_lookup_candidates(
    make: str,
    model: str,
    year: Any,
    *,
    kind: str,
) -> list[Path]:
    if not _catalog_db_available():
        return []
    make_norm = _norm_token(canonical_make(make))
    model_norm = _norm_token(model)
    try:
        target_year = int(year)
    except (TypeError, ValueError):
        target_year = None

    conn = sqlite3.connect(str(CATALOG_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT * FROM dictionary_entries
            WHERE make_norm = ? AND (model_norm = ? OR model_norm LIKE ? OR ? LIKE model_norm || '%')
            """,
            (make_norm, model_norm, f"{model_norm}%", model_norm),
        ).fetchall()
    finally:
        conn.close()

    paths: list[tuple[int, Path]] = []
    for row in rows:
        rel = row["epa_path"] if kind == "epa" else row["options_path"]
        if not rel:
            continue
        if kind == "options" and row["options_status"] == "stub":
            continue
        path = _abs_from_rel(rel)
        if not path.is_file():
            continue
        row_year = row["year"]
        if target_year is not None and row_year is not None:
            dist = abs(int(row_year) - target_year)
        else:
            dist = 0
        paths.append((dist, path))

    if not paths:
        return []
    paths.sort(key=lambda item: (item[0], item[1].name))
    return [p for _, p in paths]


@lru_cache(maxsize=1)
def _epa_paths_by_make_norm() -> dict[str, list[tuple[int, str, Path]]]:
    """One-time scan: make_norm -> [(year, model_norm, path)]."""
    out: dict[str, list[tuple[int, str, Path]]] = defaultdict(list)
    for path in iter_dictionary_csv_paths("epa"):
        meta = parse_csv_filename(path)
        if not meta:
            continue
        make_norm = _norm_token(canonical_make(meta["make"]))
        model_norm = _norm_token(meta["model"])
        try:
            file_year = int(meta["year"] or 0)
        except (TypeError, ValueError):
            file_year = 0
        out[make_norm].append((file_year, model_norm, path))
    return dict(out)


def _legacy_glob_find(
    make: str,
    model: str,
    year: Any,
    *,
    kind: str,
    epa_model_search_name_fn,
) -> Path | None:
    """Fallback glob search (legacy flat + sharded roots)."""
    model_label = epa_model_search_name_fn(make, model) if kind == "epa" else model
    make_t = _filename_token(canonical_make(make))
    model_t = _filename_token(model_label)
    suffix = _EPA_SUFFIX if kind == "epa" else _OPTIONS_SUFFIX

    found: list[Path] = []
    patterns = [
        f"*_{make_t}_{model_t}{suffix}",
        f"*_{make_t}_{model_t.replace('_', '')}{suffix}",
    ]
    if kind == "epa":
        patterns.append(f"*_{make_t}_{model.replace(' ', ' ')}*{suffix}")

    for root in iter_search_roots(kind=kind):
        for pat in patterns:
            found.extend(root.glob(pat))

    try:
        target_year = int(year)
    except (TypeError, ValueError):
        target_year = None

    def year_from_path(p: Path) -> int | None:
        m = re.match(r"^(\d{4})_", p.name)
        return int(m.group(1)) if m else None

    if found:
        if target_year is not None:
            found.sort(key=lambda p: (abs((year_from_path(p) or target_year) - target_year), p.name))
        else:
            found.sort(key=lambda p: p.name, reverse=True)
        if kind == "options":
            for p in found:
                if options_status(p) != "stub":
                    return p
            return found[0]
        return found[0]

    if kind != "epa":
        return None

    car_model_norm = _norm_token(model_label)
    make_norm = _norm_token(canonical_make(make))
    fuzzy: list[tuple[int, Path]] = []
    for file_year, file_model_norm, path in _epa_paths_by_make_norm().get(make_norm, []):
        if not (
            car_model_norm == file_model_norm
            or car_model_norm.startswith(file_model_norm)
            or file_model_norm.startswith(car_model_norm)
        ):
            continue
        year_dist = abs(file_year - target_year) if target_year is not None else 0
        name_dist = 0 if car_model_norm == file_model_norm else 1
        fuzzy.append((year_dist + name_dist, path))

    if not fuzzy:
        return None
    fuzzy.sort(key=lambda item: (item[0], item[1].name))
    return fuzzy[0][1]


def _find_epa_csv_uncached(make: str, model: str, year: int) -> Path | None:
    from backend.enrichment.trim_ladder_knowledge import epa_model_search_name

    for path in _catalog_lookup_candidates(make, model, year, kind="epa"):
        return path
    return _legacy_glob_find(
        make, model, year, kind="epa", epa_model_search_name_fn=epa_model_search_name
    )


@lru_cache(maxsize=4096)
def _find_epa_csv_cached(make: str, model: str, year: int) -> Path | None:
    return _find_epa_csv_uncached(make, model, year)


def find_epa_csv(make: str, model: str, year: Any) -> Path | None:
    try:
        y = int(year)
    except (TypeError, ValueError):
        return None
    mk = (make or "").strip()
    md = (model or "").strip()
    if not mk or not md or not y:
        return None
    return _find_epa_csv_cached(mk, md, y)


def find_complete_options_csv(make: str, model: str, year: Any) -> Path | None:
    from backend.enrichment.trim_ladder_knowledge import epa_model_search_name

    for path in _catalog_lookup_candidates(make, model, year, kind="options"):
        return path
    path = _legacy_glob_find(
        make, model, year, kind="options", epa_model_search_name_fn=epa_model_search_name
    )
    if path and options_status(path) == "stub":
        return None
    return path


def normalize_csv_columns(path: Path, *, dry_run: bool = False) -> bool:
    """Ensure Complete_Options / EPA CSV has canonical column order (non-destructive)."""
    if not path.name.endswith((".csv",)):
        return False
    canonical = list(CANONICAL_CSV_COLUMNS)
    try:
        with path.open(encoding="utf-8", newline="", errors="replace") as fh:
            header_line = fh.readline().rstrip("\r\n")
    except OSError:
        return False

    if not header_line:
        return False

    old_fields = header_line.split(",")
    if old_fields == canonical:
        return False

    try:
        with path.open(encoding="utf-8", newline="", errors="replace") as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)
    except OSError:
        return False

    out_rows: list[dict[str, str]] = []
    for row in rows:
        out_rows.append({col: (row.get(col) or "") for col in canonical})

    if dry_run:
        return True

    tmp = path.with_suffix(".csv.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=canonical, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(out_rows)
    tmp.replace(path)
    return True


def invalidate_catalog_cache() -> None:
    _catalog_db_available.cache_clear()
    load_make_aliases.cache_clear()
    _find_epa_csv_cached.cache_clear()
    _epa_paths_by_make_norm.cache_clear()
    from backend.enrichment.knowledge_engine import clear_epa_trim_lookup_cache

    clear_epa_trim_lookup_cache()
