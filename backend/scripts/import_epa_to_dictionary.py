#!/usr/bin/env python3
"""
Import EPA fueleconomy.gov vehicles.csv into ``backend/dictionary/``.

That directory is the canonical EPA-based reference for enrichment
(``backend/dictionary/enrich_from_dictionary.py``). Wikipedia / NHTSA ``*Complete_Options.csv`` output
from ``car_data_scraper.py`` lives under ``csv_out*`` and is a different dataset.

Downloads all make/model/trim/engine data for model years 1950+
(default ``--min-year``) and writes one CSV per Year+Make+Model, matching the DICTIONARY format.

Usage:
  python import_epa_to_dictionary.py
  python import_epa_to_dictionary.py --epa-csv /path/to/vehicles.csv
  python import_epa_to_dictionary.py --min-year 2020
  python import_epa_to_dictionary.py --years 2019,2020
  python import_epa_to_dictionary.py --patch-induction-only            # dry run
  python import_epa_to_dictionary.py --patch-induction-only --apply
  python import_epa_to_dictionary.py --verify

``--patch-induction-only`` rewrites forcedInduction and engineDisplay in the existing
``*_EPA.csv`` files from the pinned vehicles.csv and fills epaId/atvType/cityE/highwayE;
every other cell stays byte-identical (D-EPA1). ``--verify`` re-reads the CSVs and exits
non-zero on any label that disagrees with EPA's own flags.
"""
import argparse
import csv
import hashlib
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

# Repo root → backend/dictionary (canonical EPA CSV dir; same folder ``enrich_from_dictionary.py`` uses).
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from backend.enrichment.dictionary_paths import DICTIONARY_ROOT, EPA_CSV_COLUMNS  # noqa: E402
from backend.utils.forced_induction import (  # noqa: E402
    NA_LABEL,
    classify_forced_induction_from_car_row,
    epa_twin_hint,
    forced_induction_from_epa,
)

DICTIONARY = DICTIONARY_ROOT
DEFAULT_EPA_CSV = Path("/tmp/vehicles.csv")

# 21 canonical columns plus epaId, atvType, cityE, highwayE (D-EPA1)
DICT_COLUMNS = list(EPA_CSV_COLUMNS)

DRIVE_MAP = {
    "Rear-Wheel Drive": "Rear-Wheel Drive",
    "Front-Wheel Drive": "Front-Wheel Drive",
    "All-Wheel Drive": "All-Wheel Drive",
    "4-Wheel Drive": "Four-Wheel Drive",
    "4-Wheel or All-Wheel Drive": "All-Wheel Drive",
    "Part-time 4-Wheel Drive": "Four-Wheel Drive",
    "2-Wheel Drive": "Front-Wheel Drive",
}


def normalize_drive(drive: str) -> str:
    return DRIVE_MAP.get(drive.strip(), drive.strip())


def _cylinder_count(cyl: str) -> int | None:
    """EPA sometimes uses 'NA', blanks, or non-numeric placeholders for cylinders."""
    if not cyl:
        return None
    s = cyl.strip()
    if s in ("0", "", "NA", "N/A", "-", "—", "null"):
        return None
    try:
        n = int(float(s))
    except (ValueError, TypeError):
        return None
    return n if n > 0 else None


def build_engine_desc(row: dict) -> str:
    parts = []
    displ = row.get("displ", "").strip()
    cyl = (row.get("cylinders") or "").strip()
    eng = row.get("eng_dscr", "").strip()
    atv = row.get("atvType", "").strip()
    fuel = row.get("fuelType1", "").strip()

    if atv and atv.lower() not in ("", "null"):
        parts.append(atv)  # "EV", "Plug-in Hybrid", etc.

    if displ and displ not in ("0", "0.0", ""):
        parts.append(f"{displ}L")
    cyl_n = _cylinder_count(cyl)
    if cyl_n is not None:
        parts.append(f"I{cyl_n}" if cyl_n <= 4 else f"V{cyl_n}")
    if eng:
        # strip noisy tokens
        eng = re.sub(r'\s+', ' ', eng).strip()
        parts.append(f"({eng})")
    if fuel and fuel not in ("Regular Gasoline", "Premium Gasoline"):
        parts.append(fuel)

    return " ".join(parts)


def safe_filename(s: str) -> str:
    return re.sub(r'[^\w\-. ]', '_', s).strip()


def _text(row: dict, key: str) -> str:
    return (row.get(key) or "").strip()


def build_dict_row(row: dict) -> dict | None:
    """One vehicles.csv row as a dictionary row (no induction fields), or None if unusable."""
    try:
        year = int(row.get("year", 0))
    except (ValueError, TypeError):
        return None
    make = _text(row, "make")
    model = _text(row, "model")
    base_model = (row.get("baseModel") or model).strip()
    if not make or not model:
        return None

    fuel_type = _text(row, "fuelType1")
    fuel2 = _text(row, "fuelType2")
    if fuel2:
        fuel_type = f"{fuel_type} / {fuel2}"

    # Trim: use full model name minus base_model prefix (approximation)
    trim = model
    if base_model and model.startswith(base_model):
        trim = model[len(base_model):].strip()
    if not trim:
        trim = model

    return {
        "Year": year,
        "Make": make,
        "Model": base_model,
        "Trim": trim,
        "engineOptions": build_engine_desc(row),
        "transmissionOptions": _text(row, "trany"),
        "drivetrainOptions": normalize_drive(row.get("drive") or ""),
        "fuelType": fuel_type,
        "bodyStyle": _text(row, "VClass"),
        "cylinders": _text(row, "cylinders"),
        "displacement": _text(row, "displ"),
        "mpg_city": _text(row, "city08"),
        "mpg_highway": _text(row, "highway08"),
        "mpg_combined": _text(row, "comb08"),
        "exteriorColors": "",
        "Packages": "",
        "packageDetails": "",
        "Options": "",
        "optionDetails": "",
        "epaId": _text(row, "id"),
        "atvType": _text(row, "atvType"),
        "cityE": _text(row, "cityE"),
        "highwayE": _text(row, "highwayE"),
    }


def _classifier_label(dict_row: dict) -> str | None:
    """The make/model classifier's answer for a row that carries no label of its own."""
    from backend.dictionary.epa_engine import _epa_row_car_context

    bare = {k: v for k, v in dict_row.items() if k != "forcedInduction"}
    try:
        return classify_forced_induction_from_car_row(_epa_row_car_context(bare))
    except Exception:
        return None


def epa_label(dict_row: dict, source: dict, existing_label: str | None = None) -> str:
    """EPA-flag label for a row, twin decision shared with ``import_epa_master`` (D-ET5)."""
    twin = epa_twin_hint(_classifier_label(dict_row), existing_label)
    return forced_induction_from_epa(
        source.get("tCharger"), source.get("sCharger"), source.get("eng_dscr"),
        source.get("model"), source.get("atvType"), twin_hint=twin,
    )


def engine_fields_for_label(dict_row: dict, label: str) -> dict[str, str]:
    """engineDisplay and forcedInduction for ``dict_row`` under an explicit label.

    A blank label (EV, or unknown) is final: the classifier must not put a suffix back,
    so the display is the base alone.
    """
    from backend.dictionary.epa_engine import _format_epa_engine_base, catalog_engine_fields

    row = {k: v for k, v in dict_row.items() if k not in ("forcedInduction", "engineDisplay")}
    if not label:
        return {"engineDisplay": _format_epa_engine_base(row), "forcedInduction": ""}
    # the row now holds an explicit label, so catalog_engine_fields never reclassifies it
    out = catalog_engine_fields({**row, "forcedInduction": label})
    return {"engineDisplay": out.get("engineDisplay", ""), "forcedInduction": label}


def _existing_labels(dictionary_dir: Path, min_year: int, years: set[int] | None) -> dict[tuple, str]:
    """Label already on disk per importer key, where every row of the key agrees.

    Feeds the twin decision (D-ET5): a pre-2016 row the classifier cannot call keeps
    the twin label it already has, so a full regeneration cannot demote it.
    """
    seen: dict[tuple, set[str]] = defaultdict(set)
    if not dictionary_dir.is_dir():
        return {}
    for path in _epa_files(dictionary_dir, min_year, years):
        try:
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row in csv.DictReader(f):
                    seen[row_key(row)].add((row.get("forcedInduction") or "").strip())
        except OSError:
            continue
    return {k: next(iter(v)) for k, v in seen.items() if len(v) == 1}


def process_epa_csv(
    epa_csv: Path,
    min_year: int,
    dictionary_dir: Path,
    years: set[int] | None = None,
    stats: Counter | None = None,
):
    """Regenerate EPA dictionary CSVs; ``stats['failed']`` counts rows that raised."""
    stats = stats if stats is not None else Counter()
    print(f"Reading {epa_csv}...")
    rows_by_file: dict[str, list[dict]] = defaultdict(list)
    existing = _existing_labels(dictionary_dir, min_year, years)

    with open(epa_csv, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        total = 0
        kept = 0
        for row in reader:
            total += 1
            dict_row = build_dict_row(row)
            if dict_row is None:
                continue
            year = dict_row["Year"]
            if year < min_year or (years is not None and year not in years):
                continue
            try:
                # label first (EPA flags), then the display built around it
                label = epa_label(dict_row, row, existing.get(row_key(dict_row)))
                dict_row.update(engine_fields_for_label(dict_row, label))
            except Exception as exc:
                stats["failed"] += 1
                print(f"  ERROR row id={_text(row, 'id')} {dict_row['Make']} {dict_row['Model']}: {exc!r}",
                      file=sys.stderr)
                continue

            file_key = f"{year}_{safe_filename(dict_row['Make'])}_{safe_filename(dict_row['Model'])}"
            rows_by_file[file_key].append(dict_row)
            kept += 1

    print(f"  Total rows: {total}, kept: {kept}")
    print(f"  Unique year+make+model files: {len(rows_by_file)}")
    if stats["failed"]:
        print(f"  FAILED rows: {stats['failed']} (left out of the output)", file=sys.stderr)

    dictionary_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    for file_key, file_rows in sorted(rows_by_file.items()):
        make_label = (file_rows[0].get("Make") if file_rows else "") or ""
        make_dir = re.sub(r"[^\w\-. ]", "_", make_label.strip()).replace(" ", "_") or "Unknown"
        out_dir = dictionary_dir / "epa" / make_dir if dictionary_dir.resolve() == DICTIONARY_ROOT.resolve() else dictionary_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        fname = out_dir / f"{file_key}_EPA.csv"
        with open(fname, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=DICT_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(file_rows)
        written += 1

    print(f"  Wrote {written} EPA CSV files under {dictionary_dir}/")
    return written


# --- --patch-induction-only / --verify ---------------------------------------------

_KEY_FIELDS = (
    "Year", "Make", "Model", "Trim", "engineOptions", "transmissionOptions",
    "drivetrainOptions", "fuelType", "bodyStyle", "cylinders", "displacement",
    "mpg_city", "mpg_highway", "mpg_combined",
)
_DISPLAY_SUFFIXES = (" Twin Supercharged", " Twin Turbo", " Supercharged", " Turbo")


def row_key(row: dict) -> tuple[str, ...]:
    """The importer key: Year, Make, baseModel, Trim, engineOptions, transmission, drive,
    fuel, VClass, cylinders, displ, city08, highway08, comb08."""
    return tuple(str(row.get(k) if row.get(k) is not None else "").strip() for k in _KEY_FIELDS)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_source_index(epa_csv: Path) -> tuple[dict[tuple, list[tuple[dict, dict]]], int]:
    """vehicles.csv grouped by importer key: ``{key: [(dict_row, source_row), ...]}``."""
    index: dict[tuple, list[tuple[dict, dict]]] = defaultdict(list)
    total = 0
    with open(epa_csv, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            total += 1
            dict_row = build_dict_row(row)
            if dict_row is not None:
                index[row_key(dict_row)].append((dict_row, row))
    return index, total


def _flag_signature(source: dict) -> tuple[str, str]:
    return (_text(source, "tCharger").upper(), _text(source, "sCharger").upper())


def classify_match(matches: list[tuple[dict, dict]]) -> str:
    """'unmatched', 'unique', 'ambiguous_multi' (several rows) or 'ambiguous_flags'."""
    if not matches:
        return "unmatched"
    if len(matches) > 1:
        flags = {_flag_signature(src) for _, src in matches}
        return "ambiguous_flags" if len(flags) > 1 else "ambiguous_multi"
    return "unique"


def _strip_suffix(display: str) -> str:
    for suf in _DISPLAY_SUFFIXES:
        if display.endswith(suf):
            return display[: -len(suf)]
    return display


def _marker_label(row: dict) -> str:
    """Label from an explicit text marker in the row alone; '' when there is none."""
    label = forced_induction_from_epa(
        "", "", row.get("engineOptions"), row.get("Model"), row.get("atvType"),
        twin_hint=epa_twin_hint(_classifier_label(row), row.get("forcedInduction")),
    )
    return "" if label == NA_LABEL else label


def patched_row(row: dict, matches: list[tuple[dict, dict]]) -> tuple[dict, str]:
    """``(new row, status)`` for one existing CSV row. Only the fields this patch owns change."""
    status = classify_match(matches)
    existing = (row.get("forcedInduction") or "").strip()
    new = dict(row)
    if status == "unique":
        dict_row, source = matches[0]
        label = epa_label(dict_row, source, existing)
        for col in ("epaId", "atvType", "cityE", "highwayE"):
            new[col] = dict_row[col]
    else:
        label = _marker_label(row)
        for col in ("epaId", "atvType", "cityE", "highwayE"):
            new[col] = row.get(col) or ""
    new.update(engine_fields_for_label(row, label))
    return new, status


def _epa_files(dictionary_dir: Path, min_year: int, years: set[int] | None) -> list[Path]:
    root = dictionary_dir
    if root.resolve() == DICTIONARY_ROOT.resolve():
        root = root / "epa"
    out = []
    for path in sorted(root.rglob("*_EPA.csv")):
        m = re.match(r"(\d{4})_", path.name)
        year = int(m.group(1)) if m else None
        if year is not None and (year < min_year or (years is not None and year not in years)):
            continue
        out.append(path)
    return out


def _write_rows(path: Path, rows: list[dict]) -> None:
    tmp = path.with_suffix(".csv.tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=DICT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)


def patch_induction(
    epa_csv: Path,
    dictionary_dir: Path,
    *,
    min_year: int = 1950,
    years: set[int] | None = None,
    apply: bool = False,
) -> Counter:
    """Patch forcedInduction/engineDisplay and the four EPA columns in place.

    Dry run unless ``apply``. Returns counts; ``stats['failed']`` is non-zero if any
    file could not be read or any row raised.
    """
    stats: Counter = Counter()
    print(f"Source: {epa_csv}")
    print(f"  sha256: {sha256_of(epa_csv)}")
    index, total = load_source_index(epa_csv)
    print(f"  rows: {total}")
    transitions: Counter = Counter()
    base_changes: Counter = Counter()

    for path in _epa_files(dictionary_dir, min_year, years):
        try:
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                rows = list(csv.DictReader(f))
            header = list(rows[0].keys()) if rows else None
        except OSError as exc:
            stats["failed"] += 1
            print(f"  ERROR {path}: {exc!r}", file=sys.stderr)
            continue
        out_rows: list[dict] = []
        changed = header is not None and header != DICT_COLUMNS
        for row in rows:
            try:
                new, status = patched_row(row, index.get(row_key(row), []))
            except Exception as exc:
                stats["failed"] += 1
                print(f"  ERROR {path.name}: {exc!r}", file=sys.stderr)
                new, status = row, "failed"
            stats[status] += 1
            old_label = (row.get("forcedInduction") or "").strip()
            new_label = (new.get("forcedInduction") or "").strip()
            transitions[(old_label or "(blank)", new_label or "(blank)")] += 1
            old_base = _strip_suffix((row.get("engineDisplay") or "").strip())
            new_base = _strip_suffix((new.get("engineDisplay") or "").strip())
            if old_base != new_base:
                base_changes[(old_base, new_base)] += 1
            if any((new.get(c) or "") != (row.get(c) or "") for c in DICT_COLUMNS):
                changed = True
            out_rows.append(new)
        stats["rows"] += len(rows)
        if changed:
            stats["files_changed"] += 1
            if apply:
                _write_rows(path, out_rows)
        stats["files"] += 1

    print(f"  files: {stats['files']}  rows: {stats['rows']}  files to change: {stats['files_changed']}")
    print(f"  matched (unique): {stats['unique']}  unmatched: {stats['unmatched']}")
    print(f"  ambiguous: several rows {stats['ambiguous_multi']}, conflicting T/S flags {stats['ambiguous_flags']}")
    print("  label transitions:")
    for (old, new), n in sorted(transitions.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"    {old} -> {new}: {n}")
    if base_changes:
        print(f"  base-display changes (separate from labels): {sum(base_changes.values())}")
        for (old, new), n in sorted(base_changes.items(), key=lambda kv: (-kv[1], kv[0]))[:20]:
            print(f"    {old!r} -> {new!r}: {n}")
    print("  APPLIED" if apply else "  DRY RUN: nothing written (pass --apply)")
    return stats


def verify_induction(
    epa_csv: Path,
    dictionary_dir: Path,
    *,
    min_year: int = 1950,
    years: set[int] | None = None,
) -> Counter:
    """Re-read the CSVs; count labels that disagree with EPA's flags on matched rows."""
    stats: Counter = Counter()
    print(f"Source: {epa_csv}  sha256: {sha256_of(epa_csv)}")
    index, total = load_source_index(epa_csv)
    print(f"  rows: {total}")
    shown = 0
    for path in _epa_files(dictionary_dir, min_year, years):
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            rows = list(csv.DictReader(f))
        for row in rows:
            matches = index.get(row_key(row), [])
            status = classify_match(matches)
            stats[status] += 1
            if status != "unique":
                continue
            dict_row, source = matches[0]
            have = (row.get("forcedInduction") or "").strip()
            want = epa_label(dict_row, source, have)
            if have != want or (row.get("epaId") or "").strip() != dict_row["epaId"]:
                stats["disagreements"] += 1
                if shown < 20:
                    shown += 1
                    print(f"  DISAGREE {path.name} id={dict_row['epaId']}: "
                          f"label {have!r} want {want!r}; epaId {row.get('epaId')!r}")
    print(f"  matched: {stats['unique']}  unmatched: {stats['unmatched']}  "
          f"ambiguous: {stats['ambiguous_multi'] + stats['ambiguous_flags']}")
    print(f"  label disagreements: {stats['disagreements']}")
    return stats


def _parse_years(text: str | None) -> set[int] | None:
    if not text:
        return None
    return {int(p) for p in text.split(",") if p.strip()}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Import EPA vehicles.csv into DICTIONARY/")
    ap.add_argument("--epa-csv", default=str(DEFAULT_EPA_CSV), help="Path to EPA vehicles.csv")
    ap.add_argument("--min-year", type=int, default=1950, help="Minimum model year (default 1950)")
    ap.add_argument("--years", default=None, help="Only these model years, e.g. 2019,2020")
    ap.add_argument("--dictionary", default=str(DICTIONARY), help="Output DICTIONARY directory")
    ap.add_argument("--patch-induction-only", action="store_true",
                    help="Patch forcedInduction/engineDisplay and the EPA columns in the existing CSVs "
                         "(dry run unless --apply)")
    ap.add_argument("--apply", action="store_true", help="Write the --patch-induction-only changes")
    ap.add_argument("--verify", action="store_true",
                    help="Check every matched CSV row's label against the EPA flags; non-zero on any miss")
    args = ap.parse_args(argv)

    epa_csv = Path(args.epa_csv)
    if not epa_csv.exists():
        print(f"ERROR: EPA CSV not found at {epa_csv}")
        print("Download with: curl -L -o /tmp/vehicles.csv.zip https://www.fueleconomy.gov/feg/epadata/vehicles.csv.zip && unzip /tmp/vehicles.csv.zip -d /tmp/")
        return 1
    years = _parse_years(args.years)
    dictionary = Path(args.dictionary)

    if args.apply and not args.patch_induction_only:
        print("ERROR: --apply only goes with --patch-induction-only", file=sys.stderr)
        return 2
    if args.verify:
        stats = verify_induction(epa_csv, dictionary, min_year=args.min_year, years=years)
        return 1 if stats["disagreements"] else 0
    if args.patch_induction_only:
        stats = patch_induction(epa_csv, dictionary, min_year=args.min_year, years=years, apply=args.apply)
        return 1 if stats["failed"] else 0

    stats: Counter = Counter()
    written = process_epa_csv(epa_csv, args.min_year, dictionary, years=years, stats=stats)
    print(f"\nDone. {written} EPA files added to DICTIONARY.")
    if stats["failed"]:
        print(f"FAILED: {stats['failed']} rows could not be processed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
