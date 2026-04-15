#!/usr/bin/env python3
"""
Download EPA Fuel Economy vehicle data, build canonical description strings,
and index them into a dedicated ChromaDB store (sentence-transformers locally).

Data source (official EPA / fueleconomy.gov epadata):
  https://www.fueleconomy.gov/feg/epadata/vehicles.csv.zip

Per-model-year ZIPs (e.g. 24data.zip) currently ship Excel workbooks only; this
script therefore uses the consolidated ``vehicles.csv`` and **filters** rows where
``year`` ∈ {2024, 2025, 2026}, which matches the EPA "all model years" dataset.

Usage:
  python -m backend.vector.ingest_master_specs --reindex
  python -m backend.vector.ingest_master_specs --reindex --batch-size 100

Environment:
  MASTER_CATALOG_BATCH (optional) default batch size for Chroma upserts.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import sys
import zipfile
from collections import defaultdict
from typing import Any

import requests

from backend.vector.catalog_service import (
    _COLLECTION,
    known_packages_for_row,
    master_catalog_persist_dir,
)

logger = logging.getLogger("ingest_master_specs")

EPA_VEHICLES_ZIP_URL = "https://www.fueleconomy.gov/feg/epadata/vehicles.csv.zip"
# Legacy / supplemental: yearly bundles are often XLSX-only; kept for transparency.
EPA_YEAR_ZIP_TEMPLATE = "https://www.fueleconomy.gov/feg/epadata/{yy}data.zip"

TARGET_YEARS = ("2024", "2025", "2026")
MODEL_NAME = "all-MiniLM-L6-v2"

# Prefer "typical" gas configurations before alt-fuel / PHEV duplicates of the same nameplate.
_FUEL_TIER = [
    "Regular Gasoline",
    "Midgrade Gasoline",
    "Premium Gasoline",
    "Diesel",
    "Gasoline or E85",
    "Regular Gas and Electricity",
    "Premium and Electricity",
    "Electricity and Hydrogen",
    "Electricity",
    "Hydrogen",
]


def _fuel_tier_rank(fuel_type1: str) -> int:
    ft = (fuel_type1 or "").strip()
    try:
        return _FUEL_TIER.index(ft)
    except ValueError:
        return len(_FUEL_TIER) + 1


def _safe_float(x: str | None, default: float = 0.0) -> float:
    if x is None or not str(x).strip():
        return default
    try:
        return float(str(x).strip())
    except ValueError:
        return default


def _safe_int(x: str | None, default: int = 0) -> int:
    if x is None or not str(x).strip():
        return default
    try:
        return int(round(float(str(x).strip())))
    except ValueError:
        return default


def _drive_readable(drive: str) -> str:
    d = (drive or "").strip()
    if not d:
        return "Unknown drivetrain"
    # vehicles.csv already uses phrases like "Front-Wheel Drive"
    if "wheel" in d.lower() or "drive" in d.lower():
        return d
    m = {"F": "Front-Wheel Drive (FWD)", "R": "Rear-Wheel Drive (RWD)", "4": "4-Wheel Drive (4WD)"}
    return m.get(d[:1].upper(), d)


def _trans_readable(trany: str, trans_dscr: str) -> str:
    t = (trany or "").strip()
    if not t and (trans_dscr or "").strip():
        return trans_dscr.strip()
    return t or "Unknown transmission"


def _engine_phrase(displ: float, cyl: int, eng_dscr: str) -> str:
    parts: list[str] = []
    if displ > 0:
        parts.append(f"{displ:.1f}L")
    if cyl > 0:
        parts.append(f"{cyl}-cylinder")
    ed = (eng_dscr or "").strip()
    if ed and ed.lower() not in " ".join(parts).lower():
        parts.append(ed)
    if not parts:
        return "Engine details per EPA test vehicle"
    return ", ".join(parts)


def _trim_hint(base_model: str, model: str) -> str:
    b = (base_model or "").strip()
    m = (model or "").strip()
    if not b or not m or m == b:
        return m
    if m.lower().startswith(b.lower()):
        rest = m[len(b) :].strip(" -–")
        return rest or m
    return m


def download_vehicles_csv_zip(timeout: int = 120) -> bytes:
    logger.info("Downloading %s", EPA_VEHICLES_ZIP_URL)
    r = requests.get(EPA_VEHICLES_ZIP_URL, timeout=timeout)
    r.raise_for_status()
    return r.content


def try_download_year_guide_zip(year: int, timeout: int = 60) -> list[str]:
    """Return list of CSV member names if any exist inside YYdata.zip; else []."""
    yy = str(year)[-2:]
    url = EPA_YEAR_ZIP_TEMPLATE.format(yy=yy)
    try:
        resp = requests.get(url, timeout=timeout)
        if resp.status_code != 200:
            return []
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not names:
            logger.info("Year bundle %s contains no CSV (EPA may ship XLSX only).", url)
        return names
    except Exception as e:
        logger.debug("Year bundle fetch skipped: %s", e)
        return []


def extract_vehicles_csv(zip_bytes: bytes) -> list[dict[str, str]]:
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    member = None
    for name in zf.namelist():
        lower = name.lower()
        if lower.endswith("vehicles.csv") or lower.endswith("/vehicles.csv"):
            member = name
            break
    if member is None:
        csv_members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_members:
            raise FileNotFoundError("No CSV found in vehicles.csv.zip")
        member = csv_members[0]
    with zf.open(member) as f:
        text = f.read().decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def row_to_unified(raw: dict[str, str]) -> dict[str, Any]:
    """Map EPA vehicles.csv columns to unified schema + extras for descriptions."""
    return {
        "make": (raw.get("make") or "").strip(),
        "model": (raw.get("model") or "").strip(),
        "base_model": (raw.get("baseModel") or "").strip(),
        "year": str(raw.get("year") or "").strip(),
        "displ": _safe_float(raw.get("displ")),
        "cylinders": _safe_int(raw.get("cylinders")),
        "trany": (raw.get("trany") or "").strip(),
        "trans_dscr": (raw.get("trans_dscr") or "").strip(),
        "drive": (raw.get("drive") or "").strip(),
        "city08": _safe_int(raw.get("city08"), 0),
        "highway08": _safe_int(raw.get("highway08"), 0),
        "comb08": _safe_int(raw.get("comb08"), 0),
        "fuel_type1": (raw.get("fuelType1") or "").strip(),
        "fuel_type": (raw.get("fuelType") or "").strip(),
        "vehicle_class": (raw.get("VClass") or "").strip(),
        "eng_dscr": (raw.get("eng_dscr") or "").strip(),
        "epa_id": str(raw.get("id") or "").strip(),
    }


def dedupe_standard_config(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Collapse multiple EPA rows (fuel variants, AWD packages) for the same
    make + model + year by choosing a single "standard" configuration:

    1. Lowest fuel-type tier rank (Regular Gasoline preferred over PHEV/EV).
    2. Smallest displacement (often the volume base engine).
    3. Higher combined MPG as a weak tie-break among same-displacement gas rows.
    4. Lowest numeric EPA id for stability.
    """
    buckets: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        key = (r["make"].lower(), r["model"].lower(), r["year"])
        buckets[key].append(r)

    out: list[dict[str, Any]] = []
    for key, group in buckets.items():
        def sort_key(r: dict[str, Any]):
            tier = _fuel_tier_rank(r.get("fuel_type1") or "")
            displ = float(r.get("displ") or 0.0)
            comb = int(r.get("comb08") or 0)
            epa_id = int(r.get("epa_id") or "0") if str(r.get("epa_id") or "").isdigit() else 0
            return (tier, displ, -comb, epa_id)

        group_sorted = sorted(group, key=sort_key)
        chosen = group_sorted[0]
        out.append(chosen)
    return out


def build_canonical_description(r: dict[str, Any], package_line: str) -> str:
    year = r["year"]
    make = r["make"]
    model = r["model"]
    title = f"{year} {make} {model}".strip()

    eng = _engine_phrase(float(r.get("displ") or 0), int(r.get("cylinders") or 0), r.get("eng_dscr") or "")
    tr = _trans_readable(r.get("trany") or "", r.get("trans_dscr") or "")
    drv = _drive_readable(r.get("drive") or "")
    city = int(r.get("city08") or 0)
    hwy = int(r.get("highway08") or 0)
    vclass = (r.get("vehicle_class") or "").strip()

    parts = [
        f"{title}, {eng}, {tr}. {drv}.",
        f"Fuel Economy: {city} City / {hwy} Highway MPG (EPA).",
    ]
    if vclass:
        parts.append(f"EPA class: {vclass}.")
    if package_line:
        parts.append(package_line)
    return " ".join(parts)


def row_chroma_metadata(r: dict[str, Any], package_line: str) -> dict[str, Any]:
    """Chroma metadata: str/int/float only (flatten packages)."""
    trim = _trim_hint(r.get("base_model") or "", r.get("model") or "")
    return {
        "make": (r.get("make") or "")[:120],
        "model": (r.get("model") or "")[:200],
        "base_model": (r.get("base_model") or "")[:120],
        "trim_hint": trim[:120],
        "year": int(r["year"]) if str(r.get("year") or "").isdigit() else 0,
        "displ": float(r.get("displ") or 0.0),
        "cylinders": int(r.get("cylinders") or 0),
        "trany": (r.get("trany") or "")[:200],
        "drive": (r.get("drive") or "")[:120],
        "city08": int(r.get("city08") or 0),
        "highway08": int(r.get("highway08") or 0),
        "comb08": int(r.get("comb08") or 0),
        "fuel_type1": (r.get("fuel_type1") or "")[:80],
        "vehicle_class": (r.get("vehicle_class") or "")[:120],
        "known_packages": (package_line or "")[:2000],
    }


def _clear_collection(client: Any, name: str) -> None:
    try:
        client.delete_collection(name)
    except Exception:
        pass


def ingest(
    *,
    reindex: bool,
    batch_size: int,
    dry_run: bool = False,
) -> int:
    for y in (2024, 2025, 2026):
        csvs = try_download_year_guide_zip(y)
        if csvs:
            logger.info("MY%s year-zip advertises CSV members: %s", y, csvs)
        else:
            logger.info(
                "MY%s: no CSV in %s (expected if EPA ships XLSX-only); using vehicles.csv filter.",
                y,
                EPA_YEAR_ZIP_TEMPLATE.format(yy=str(y)[-2:]),
            )

    zip_bytes = download_vehicles_csv_zip()
    raw_rows = extract_vehicles_csv(zip_bytes)
    unified: list[dict[str, Any]] = []
    for raw in raw_rows:
        yr = str(raw.get("year") or "").strip()
        if yr not in TARGET_YEARS:
            continue
        u = row_to_unified(raw)
        if not u["make"] or not u["model"]:
            continue
        unified.append(u)

    logger.info("Loaded %s EPA rows for years %s", len(unified), ", ".join(TARGET_YEARS))
    deduped = dedupe_standard_config(unified)
    logger.info("After standard-config dedupe: %s vehicles", len(deduped))

    if dry_run:
        for sample in deduped[:3]:
            pl = known_packages_for_row(sample["make"], sample.get("base_model") or "", sample["model"])
            print(build_canonical_description(sample, pl))
        return len(deduped)

    from sentence_transformers import SentenceTransformer
    import chromadb
    from chromadb.config import Settings

    persist = master_catalog_persist_dir()
    client = chromadb.PersistentClient(
        path=str(persist),
        settings=Settings(anonymized_telemetry=False),
    )
    if reindex:
        _clear_collection(client, _COLLECTION)

    col = client.get_or_create_collection(
        _COLLECTION,
        metadata={"source": "epa_vehicles_csv", "model": MODEL_NAME},
    )

    model = SentenceTransformer(MODEL_NAME)
    n_indexed = 0
    batch_ids: list[str] = []
    batch_docs: list[str] = []
    batch_meta: list[dict[str, Any]] = []

    def flush() -> None:
        nonlocal n_indexed, batch_ids, batch_docs, batch_meta
        if not batch_ids:
            return
        emb = model.encode(
            batch_docs,
            show_progress_bar=False,
            batch_size=min(32, len(batch_docs)),
            convert_to_numpy=True,
        )
        vectors = emb.tolist()
        col.add(ids=batch_ids, documents=batch_docs, metadatas=batch_meta, embeddings=vectors)
        n_indexed += len(batch_ids)
        batch_ids, batch_docs, batch_meta = [], [], []

    for r in deduped:
        pkg = known_packages_for_row(r["make"], r.get("base_model") or "", r["model"])
        doc = build_canonical_description(r, pkg)
        meta = row_chroma_metadata(r, pkg)
        rid = f"epa-{r.get('epa_id')}" if r.get("epa_id") else f"epa-{r['year']}-{hash(doc) & 0xFFFFFFFF:x}"
        batch_ids.append(rid)
        batch_docs.append(doc)
        batch_meta.append(meta)
        if len(batch_ids) >= batch_size:
            flush()
    flush()

    logger.info("Indexed %s documents into %s / %s", n_indexed, persist, _COLLECTION)
    return n_indexed


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    p = argparse.ArgumentParser(description="EPA Master Spec Catalog → ChromaDB")
    p.add_argument(
        "--reindex",
        action="store_true",
        help="Delete existing master_spec_catalog collection and rebuild from EPA data.",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=int(os.environ.get("MASTER_CATALOG_BATCH", "100")),
        help="Records per Chroma add / embedding batch (default 100).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse, filter, and dedupe only; print a few canonical strings; no Chroma writes.",
    )
    args = p.parse_args(argv)

    if not args.reindex and not args.dry_run:
        print("Nothing to do. Pass --reindex to rebuild the catalog, or --dry-run to validate.", file=sys.stderr)
        return 2

    n = ingest(
        reindex=bool(args.reindex) and not args.dry_run,
        batch_size=max(1, args.batch_size),
        dry_run=args.dry_run,
    )
    print(f"Done. Records: {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
