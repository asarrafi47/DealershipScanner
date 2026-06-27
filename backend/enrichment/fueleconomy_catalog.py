"""Download and match official EPA FuelEconomy.gov vehicle records."""

from __future__ import annotations

import csv
import logging
import re
import tempfile
import urllib.request
from dataclasses import dataclass
from typing import Any, Iterable

log = logging.getLogger(__name__)

EPA_VEHICLES_CSV_URL = "https://www.fueleconomy.gov/feg/epadata/vehicles.csv"
EPA_VEHICLES_ZIP_URL = "https://www.fueleconomy.gov/feg/epadata/vehicles.csv.zip"
DEFAULT_UA = "SarrafiCollection/1.0 (+local; epa_master sync)"

# Extra make labels to try when matching (same nameplate, different EPA-era branding).
_MAKE_MATCH_ALIASES: dict[str, list[str]] = {
    "ram": ["Ram", "Dodge"],
    "dodge": ["Dodge", "Ram"],
    "mini": ["MINI", "Mini"],
}

_DRIVE_MAP = {
    "rear-wheel drive": "rwd",
    "front-wheel drive": "fwd",
    "all-wheel drive": "awd",
    "4-wheel drive": "4wd",
    "four-wheel drive": "4wd",
    "4-wheel or all-wheel drive": "awd",
    "part-time 4-wheel drive": "4wd",
    "2-wheel drive": "fwd",
}


@dataclass(frozen=True)
class FueleconomyRecord:
    epa_vehicle_id: int
    year: int
    make: str
    model: str
    base_model: str
    trim_hint: str
    cylinders: int | None
    displacement: float | None
    trany: str | None
    drive: str | None
    fuel_type: str | None
    fuel_type2: str | None
    city08: float | None
    highway08: float | None
    city_e: float | None
    highway_e: float | None
    atv_type: str | None
    body_style: str | None

    @property
    def fuel_type_combined(self) -> str | None:
        f1 = (self.fuel_type or "").strip()
        f2 = (self.fuel_type2 or "").strip()
        if f1 and f2:
            return f"{f1} / {f2}"
        return f1 or f2 or None


def _safe_int(val: Any) -> int | None:
    if val is None or str(val).strip() == "":
        return None
    try:
        return int(float(str(val).strip()))
    except (TypeError, ValueError):
        return None


def _safe_float(val: Any) -> float | None:
    if val is None or str(val).strip() == "":
        return None
    try:
        v = float(str(val).strip())
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _norm_token(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())


def normalize_drive_key(drive: str | None) -> str:
    d = (drive or "").strip().lower()
    if not d:
        return ""
    if d in _DRIVE_MAP:
        return _DRIVE_MAP[d]
    for phrase, key in _DRIVE_MAP.items():
        if phrase in d:
            return key
    if "awd" in d or "all-wheel" in d:
        return "awd"
    if "4wd" in d or "4-wheel" in d or "four-wheel" in d:
        return "4wd"
    if "fwd" in d or "front-wheel" in d:
        return "fwd"
    if "rwd" in d or "rear-wheel" in d:
        return "rwd"
    return _norm_token(d)


def normalize_trany_key(trany: str | None) -> str:
    t = (trany or "").strip().lower()
    if not t:
        return ""
    t = re.sub(r"\s+", "", t)
    t = t.replace("automatic", "auto").replace("manual", "man")
    return t


def _parse_fueleconomy_row(row: dict[str, str]) -> FueleconomyRecord | None:
    year = _safe_int(row.get("year"))
    make = (row.get("make") or "").strip()
    model = (row.get("model") or "").strip()
    if year is None or not make or not model:
        return None
    vid = _safe_int(row.get("id"))
    if vid is None:
        return None
    base = (row.get("baseModel") or model).strip()
    trim_hint = model[len(base) :].strip() if model.startswith(base) else model
    f1 = (row.get("fuelType1") or row.get("fuelType") or row.get("fuel_type") or "").strip() or None
    f2 = (row.get("fuelType2") or "").strip() or None
    atv = (row.get("atvType") or "").strip() or None
    if not atv:
        atv = _infer_atv_type(f1, f2)
    return FueleconomyRecord(
        epa_vehicle_id=vid,
        year=year,
        make=make,
        model=model,
        base_model=base,
        trim_hint=trim_hint or model,
        cylinders=_safe_int(row.get("cylinders")),
        displacement=_safe_float(row.get("displ")),
        trany=(row.get("trany") or "").strip() or None,
        drive=(row.get("drive") or "").strip() or None,
        fuel_type=f1,
        fuel_type2=f2,
        city08=_safe_float(row.get("city08")),
        highway08=_safe_float(row.get("highway08")),
        city_e=_safe_float(row.get("cityE")),
        highway_e=_safe_float(row.get("highwayE")),
        atv_type=atv,
        body_style=(row.get("VClass") or "").strip() or None,
    )


def _infer_atv_type(fuel_type: str | None, fuel_type2: str | None) -> str | None:
    blob = " ".join(x for x in (fuel_type, fuel_type2) if x).lower()
    if not blob:
        return None
    if "electricity" in blob and ("gasoline" in blob or "premium" in blob or "regular" in blob):
        return "Plug-in Hybrid"
    if "electricity" in blob or blob.strip() == "electric":
        return "EV"
    if "plug-in" in blob or "phev" in blob:
        return "Plug-in Hybrid"
    if "hybrid" in blob:
        return "Hybrid"
    return None


def download_vehicles_csv(*, dest_path: str | None = None, timeout: int = 120) -> str:
    """Download official ``vehicles.csv``; return local path."""
    if dest_path:
        path = dest_path
    else:
        tmp = tempfile.NamedTemporaryFile(mode="w+b", suffix=".csv", delete=False)
        tmp.close()
        path = tmp.name
    req = urllib.request.Request(EPA_VEHICLES_CSV_URL, headers={"User-Agent": DEFAULT_UA})
    with urllib.request.urlopen(req, timeout=timeout) as resp, open(path, "wb") as out:
        out.write(resp.read())
    return path


def load_fueleconomy_records(csv_path: str) -> list[FueleconomyRecord]:
    out: list[FueleconomyRecord] = []
    with open(csv_path, newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)
        for raw in reader:
            rec = _parse_fueleconomy_row(raw)
            if rec:
                out.append(rec)
    log.info("Loaded %d fueleconomy.gov records from %s", len(out), csv_path)
    return out


def build_fueleconomy_id_index(
    records: Iterable[FueleconomyRecord],
) -> dict[int, FueleconomyRecord]:
    return {rec.epa_vehicle_id: rec for rec in records}


def infer_atv_type_from_fuel(fuel_type: str | None) -> str | None:
    """Best-effort ``atv_type`` when fueleconomy.gov left it blank."""
    fl = (fuel_type or "").lower()
    if not fl:
        return None
    if "diesel" in fl:
        return "Diesel"
    if "e85" in fl or "flex" in fl:
        return "FFV"
    if "natural gas" in fl or fl.strip() == "cng":
        return "CNG"
    if "hydrogen" in fl:
        return "FCV"
    if "electricity" in fl and ("gasoline" in fl or "premium" in fl or "regular" in fl):
        return "Plug-in Hybrid"
    if "electricity" in fl or fl.strip() == "electric":
        return "EV"
    if "plug-in" in fl or "phev" in fl:
        return "Plug-in Hybrid"
    if "hybrid" in fl:
        return "Hybrid"
    return None


def _make_lookup_variants(make: str) -> list[str]:
    mk = (make or "").strip()
    if not mk:
        return []
    key = _norm_token(mk)
    out = [mk]
    for alias in _MAKE_MATCH_ALIASES.get(key, []):
        if alias not in out:
            out.append(alias)
    return out


def build_fueleconomy_index(records: Iterable[FueleconomyRecord]) -> dict[tuple[int, str, str], list[FueleconomyRecord]]:
    """Index by (year, make_norm, base_model_norm)."""
    index: dict[tuple[int, str, str], list[FueleconomyRecord]] = {}
    for rec in records:
        key = (rec.year, _norm_token(rec.make), _norm_token(rec.base_model or rec.model))
        index.setdefault(key, []).append(rec)
    return index


def _trim_tokens(trim: str | None) -> set[str]:
    t = (trim or "").strip().lower()
    if not t:
        return set()
    parts = re.split(r"[\s/\-+]+", t)
    return {p for p in parts if len(p) >= 2}


def _model_tokens(model: str | None) -> set[str]:
    return _trim_tokens(model)


def score_fueleconomy_match(db_row: dict[str, Any], fe: FueleconomyRecord) -> int:
    """Higher is better; -1 means incompatible."""
    try:
        year = int(db_row.get("year") or 0)
    except (TypeError, ValueError):
        return -1
    if year != fe.year:
        return -1
    make = (db_row.get("make") or "").strip()
    if _norm_token(make) != _norm_token(fe.make):
        return -1

    db_model = (db_row.get("model") or "").strip()
    db_model_norm = _norm_token(db_model)
    fe_base_norm = _norm_token(fe.base_model or fe.model)
    fe_model_norm = _norm_token(fe.model)
    if db_model_norm not in fe_base_norm and fe_base_norm not in db_model_norm:
        if db_model_norm not in fe_model_norm:
            return -1

    score = 10
    db_trim = (db_row.get("trim") or "").strip()
    trim_toks = _trim_tokens(db_trim)
    fe_blob = f"{fe.model} {fe.trim_hint}".lower()
    if db_trim and db_trim.lower() in fe.model.lower():
        score += 30
    elif trim_toks:
        overlap = sum(1 for tok in trim_toks if tok in fe_blob)
        score += min(20, overlap * 8)
    elif db_model.lower() in fe.model.lower():
        score += 5

    db_trany = normalize_trany_key(db_row.get("trany"))
    fe_trany = normalize_trany_key(fe.trany)
    if db_trany and fe_trany:
        score += 15 if db_trany == fe_trany else (8 if db_trany[:4] == fe_trany[:4] else -5)

    db_drive = normalize_drive_key(db_row.get("drive"))
    fe_drive = normalize_drive_key(fe.drive)
    if db_drive and fe_drive:
        score += 12 if db_drive == fe_drive else -3

    db_cyl = _safe_int(db_row.get("cylinders"))
    if db_cyl and fe.cylinders and db_cyl == fe.cylinders:
        score += 6

    db_disp = _safe_float(db_row.get("displacement"))
    if db_disp and fe.displacement and abs(db_disp - fe.displacement) <= 0.2:
        score += 6

    db_fuel = (db_row.get("fuel_type") or "").lower()
    fe_fuel = (fe.fuel_type_combined or "").lower()
    if db_fuel and fe_fuel:
        if "electric" in db_fuel and "electric" in fe_fuel:
            score += 8
        elif "diesel" in db_fuel and "diesel" in fe_fuel:
            score += 8
        elif "premium" in db_fuel and "premium" in fe_fuel:
            score += 4
        elif "regular" in db_fuel and "regular" in fe_fuel:
            score += 4

    return score


def best_fueleconomy_match(
    db_row: dict[str, Any],
    index: dict[tuple[int, str, str], list[FueleconomyRecord]],
    *,
    min_score: int = 18,
) -> FueleconomyRecord | None:
    try:
        year = int(db_row.get("year") or 0)
    except (TypeError, ValueError):
        return None
    make = (db_row.get("make") or "").strip()
    model = (db_row.get("model") or "").strip()
    if not year or not make or not model:
        return None

    best: FueleconomyRecord | None = None
    best_score = min_score - 1
    seen_ids: set[int] = set()

    for make_variant in _make_lookup_variants(make):
        keys = [
            (year, _norm_token(make_variant), _norm_token(model)),
        ]
        first = model.split()[0] if model.split() else model
        keys.append((year, _norm_token(make_variant), _norm_token(first)))
        for key in keys:
            for fe in index.get(key, []):
                if fe.epa_vehicle_id in seen_ids:
                    continue
                seen_ids.add(fe.epa_vehicle_id)
                sc = score_fueleconomy_match({**db_row, "make": make_variant}, fe)
                if sc > best_score:
                    best_score = sc
                    best = fe
    return best


def patch_from_fueleconomy(db_row: dict[str, Any], fe: FueleconomyRecord) -> dict[str, Any]:
    """Return epa_master column updates — only keys that should overwrite nulls."""
    out: dict[str, Any] = {}
    if db_row.get("epa_vehicle_id") is None:
        out["epa_vehicle_id"] = fe.epa_vehicle_id
    if not (db_row.get("atv_type") or "").strip() and (fe.atv_type or "").strip():
        out["atv_type"] = fe.atv_type
    if db_row.get("city_e") is None and fe.city_e is not None:
        out["city_e"] = fe.city_e
    if db_row.get("highway_e") is None and fe.highway_e is not None:
        out["highway_e"] = fe.highway_e
    if db_row.get("city08") is None and fe.city08 is not None:
        out["city08"] = fe.city08
    if db_row.get("highway08") is None and fe.highway08 is not None:
        out["highway08"] = fe.highway08
    if db_row.get("cylinders") is None and fe.cylinders is not None:
        out["cylinders"] = fe.cylinders
    if db_row.get("displacement") is None and fe.displacement is not None:
        out["displacement"] = fe.displacement
    if not (db_row.get("trany") or "").strip() and fe.trany:
        out["trany"] = fe.trany
    if not (db_row.get("drive") or "").strip() and fe.drive:
        out["drive"] = fe.drive
    if not (db_row.get("fuel_type") or "").strip() and fe.fuel_type_combined:
        out["fuel_type"] = fe.fuel_type_combined
    if not (db_row.get("body_style") or "").strip() and fe.body_style:
        out["body_style"] = fe.body_style
    return out


def resolve_fueleconomy_via_api(
    *,
    year: int,
    make: str,
    model: str,
    trim: str | None = None,
    trany: str | None = None,
    drive: str | None = None,
    sleep_s: float = 0.2,
) -> FueleconomyRecord | None:
    """REST fallback: menu/options then vehicle record (one YMM at a time)."""
    from backend.oem.vehicle_reference.sources.epa_client import (
        fetch_with_retries,
        menu_models,
        menu_options,
        vehicle_record,
    )

    db_row = {
        "year": year,
        "make": make,
        "model": model,
        "trim": trim or "",
        "trany": trany or "",
        "drive": drive or "",
    }
    models = fetch_with_retries(menu_models, year, make, sleep_s=sleep_s)
    model_norm = _norm_token(model)
    model_value = None
    for label, value in models:
        if _norm_token(label) == model_norm or model_norm in _norm_token(label):
            model_value = value
            break
    if not model_value:
        for label, value in models:
            if _norm_token(model) in _norm_token(label):
                model_value = value
                break
    if not model_value:
        return None

    options = fetch_with_retries(menu_options, year, make, model_value, sleep_s=sleep_s)
    if not options:
        return None

    best_id: int | None = None
    best_score = 0
    trim_l = (trim or "").lower()
    for label, value in options:
        try:
            vid = int(value)
        except (TypeError, ValueError):
            continue
        sc = 5
        lbl = label.lower()
        if trim_l and trim_l in lbl:
            sc += 20
        elif trim_l:
            sc += sum(4 for tok in _trim_tokens(trim) if tok in lbl)
        if sc > best_score:
            best_score = sc
            best_id = vid
    if best_id is None:
        try:
            best_id = int(options[0][1])
        except (TypeError, ValueError, IndexError):
            return None

    raw = fetch_with_retries(vehicle_record, best_id, sleep_s=sleep_s)
    if not raw:
        return None
    return _parse_fueleconomy_row({k: str(v) for k, v in raw.items()})
