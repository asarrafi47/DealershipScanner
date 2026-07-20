"""
The ONE resolver that links a listing row to its canonical ``epa_master``
catalog row.

Every previous enrichment path did its own fuzzy (year, make, model, trim)
lookup with ``LIMIT 1`` and no engine cross-check — which is how a 2011 E350
(NA 3.5L V6) picked up specs from the modern turbo-four generation. This
resolver scores every candidate row for the (year, make, model) instead, using
trim agreement PLUS engine agreement (displacement, cylinders, drivetrain,
fuel), and refuses to link below a confidence floor: an unlinked car shows
dealer-observed data only, which is always safer than a mislink.

Writes go to ``cars.epa_master_id`` / ``epa_match_confidence`` /
``epa_match_method`` — the only model-level values ever persisted on a
listing row (they are a *link*, not facts).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backend.db.inventory_db import get_conn
from backend.utils.engine_consistency import cylinders_from_engine_text

# Link only when we're at least this confident (0-1 scale).
MIN_CONFIDENCE = 0.45

_CANDIDATE_COLS = (
    "id", "year", "make", "model", "trim", "cylinders", "displacement",
    "trany", "drive", "fuel_type", "engine_description", "forced_induction",
    "atv_type",
)


@dataclass
class CatalogMatch:
    epa_master_id: int
    confidence: float
    method: str


def _norm(s: Any) -> str:
    return "".join(ch for ch in str(s or "").lower() if ch.isalnum())


def _drive_bucket(s: Any) -> str:
    t = str(s or "").upper()
    if "ALL" in t or "AWD" in t or "4MATIC" in t or "XDRIVE" in t or "QUATTRO" in t:
        return "AWD"
    if "4" in t and ("WD" in t or "WHEEL" in t):
        return "4WD"
    if "FRONT" in t or "FWD" in t:
        return "FWD"
    if "REAR" in t or "RWD" in t:
        return "RWD"
    return ""


def _fuel_bucket(s: Any) -> str:
    t = str(s or "").lower()
    if "electric" in t and "gas" not in t and "hybrid" not in t:
        return "ev"
    if "hybrid" in t or "phev" in t:
        return "hybrid"
    if "diesel" in t:
        return "diesel"
    if "gas" in t or "petrol" in t or "premium" in t or "regular" in t:
        return "gas"
    return ""


# Powertrain suffixes dealers append to model names that EPA folds into trims
_MODEL_SUFFIXES = (
    " plug-in hybrid", " i-force max", " hybrid max", " hybrid", " prime",
    " phev", " ev",
)


def _model_variants(make: str, model: str) -> list[str]:
    out = [model.strip()]
    low = model.lower()
    for suf in _MODEL_SUFFIXES:
        if low.endswith(suf):
            base = model[: len(model) - len(suf)].strip()
            if base and base not in out:
                out.append(base)
    try:
        from backend.enrichment.knowledge_engine import _model_epa_fallbacks

        for fb in _model_epa_fallbacks(make, model):
            if fb and fb not in out:
                out.append(fb)
    except ImportError:
        pass
    return out


def _query_year_make(cur, year: int, make: str) -> list[dict[str, Any]]:
    cols = ", ".join(_CANDIDATE_COLS)
    try:
        cur.execute(
            f"SELECT {cols} FROM epa_master WHERE year=? AND lower(make)=lower(?)",
            (year, make.strip()),
        )
    except Exception:
        # SQLite dev DBs may lack the extended epa_master columns — a car that
        # can't be resolved just stays unlinked (dealer data only).
        return []
    return [dict(zip(_CANDIDATE_COLS, r)) for r in cur.fetchall()]


def _match_models(rows: list[dict[str, Any]], make: str, model: str) -> list[dict[str, Any]]:
    """
    Match listing model against EPA model naming ("F-150" vs "F150",
    "GLE" vs "GLE-Class", "Corolla Hybrid" vs "Corolla") without ever
    letting a base model absorb a distinct nameplate (Mustang ≠ Mach-E).
    """
    targets = [_norm(v) for v in _model_variants(make, model)]
    targets += [_norm(v + "-Class") for v in _model_variants(make, model)]
    by_norm: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        by_norm.setdefault(_norm(r.get("model")), []).append(r)

    # Tier 1: exact normalized match on any variant
    for t in targets:
        if t and t in by_norm:
            return by_norm[t]
    # Tier 2: listing model EXTENDS an EPA base name (extra powertrain/series
    # words on the listing side only — the safe direction).
    car_norm = _norm(model)
    hits: list[dict[str, Any]] = []
    best_len = 0
    for epa_norm, group in by_norm.items():
        if epa_norm and car_norm.startswith(epa_norm) and len(epa_norm) >= 4:
            if len(epa_norm) > best_len:
                hits, best_len = list(group), len(epa_norm)
    return hits


def _candidates(cur, year: int, make: str, model: str) -> list[dict[str, Any]]:
    rows = _match_models(_query_year_make(cur, year, make), make, model)
    if rows:
        return rows
    # New model years lag the EPA import; one prior year of the same nameplate
    # is almost always the same generation. Bounded to exactly -1 and penalized
    # in scoring + tagged in the match method (unlike the unbounded nearest-year
    # fallback that caused the 2026-07-19 incident).
    rows = _match_models(_query_year_make(cur, year - 1, make), make, model)
    for r in rows:
        r["_prev_year"] = True
    return rows


def _electrification(text: str) -> str:
    """'phev' | 'hybrid' | 'ev' | '' from free text."""
    t = text.lower()
    if "plug-in" in t or "plugin" in t or "phev" in t or "prime" in t:
        return "phev"
    if "hybrid" in t or "i-force max" in t or "powerboost" in t or "etorque hybrid" in t:
        return "hybrid"
    if ("electric" in t and "hybrid" not in t) or " bev" in t or t.strip() == "ev":
        return "ev"
    return ""


def score_candidate(car: dict[str, Any], cand: dict[str, Any]) -> tuple[float, str]:
    """
    (score 0-1, method label) for one epa_master candidate row.

    Dealer trim names ("LE", "Platinum") rarely match EPA variant labels
    ("Hybrid SE", "4WD"), so trim agreement is a *bonus* — the link signal is
    engine agreement: cylinders, displacement, drivetrain, fuel, and above all
    electrification variant (a gas Corolla must never get hybrid MPG/hp).
    """
    score = 0.0
    reasons: list[str] = []

    car_trim = _norm(car.get("trim"))
    cand_trim = _norm(cand.get("trim"))
    if car_trim and cand_trim:
        if car_trim == cand_trim:
            score += 0.30
            reasons.append("trim_exact")
        elif car_trim in cand_trim or cand_trim in car_trim:
            score += 0.15
            reasons.append("trim_partial")

    # Electrification variant agreement (decisive either way)
    car_blob = " ".join(
        str(car.get(k) or "") for k in ("model", "trim", "title", "fuel_type", "engine_description")
    )
    cand_blob = f"{cand.get('trim') or ''} {cand.get('atv_type') or ''} {cand.get('fuel_type') or ''}"
    car_e = _electrification(car_blob)
    cand_e = _electrification(cand_blob)
    if car_e == cand_e:
        score += 0.15 if car_e else 0.10
        reasons.append(f"variant_{car_e or 'conventional'}")
    else:
        score -= 0.35
        reasons.append("variant_conflict")

    # Engine agreement — dealer-observed values are the anchor.
    try:
        car_el = float(str(car.get("engine_l") or "").split("L")[0])
    except (TypeError, ValueError):
        car_el = None
    try:
        cand_el = float(cand.get("displacement"))
    except (TypeError, ValueError):
        cand_el = None
    if car_el and cand_el:
        if abs(car_el - cand_el) <= 0.15:
            score += 0.25
            reasons.append("engine_l")
        else:
            score -= 0.30
            reasons.append("engine_l_conflict")

    car_cyl = None
    try:
        car_cyl = int(car.get("cylinders")) if car.get("cylinders") else None
    except (TypeError, ValueError):
        pass
    if car_cyl is None:
        car_cyl = cylinders_from_engine_text(car.get("engine_description"))
    try:
        cand_cyl = int(cand.get("cylinders")) if cand.get("cylinders") else None
    except (TypeError, ValueError):
        cand_cyl = None
    if car_cyl and cand_cyl:
        if car_cyl == cand_cyl:
            score += 0.25
            reasons.append("cylinders")
        else:
            score -= 0.35
            reasons.append("cylinders_conflict")

    db = _drive_bucket(car.get("drivetrain"))
    cb = _drive_bucket(cand.get("drive") or cand.get("trim"))
    if db and cb:
        if db == cb or {db, cb} == {"AWD", "4WD"}:
            score += 0.10
            reasons.append("drive")
        else:
            score -= 0.10
            reasons.append("drive_conflict")

    # Fuel bucket comparison only for conventional cars: EPA stores hybrids'
    # fuel_type as "Regular"/"Premium" (bucket "gas") while dealers say
    # "Hybrid", so hybrids would ALWAYS fuel-conflict and sink below the link
    # floor. Electrification agreement above already scored that dimension.
    if not car_e and not cand_e:
        fb_ = _fuel_bucket(car.get("fuel_type"))
        cf = _fuel_bucket(cand.get("fuel_type"))
        if fb_ and cf:
            if fb_ == cf:
                score += 0.10
                reasons.append("fuel")
            else:
                score -= 0.25
                reasons.append("fuel_conflict")

    if cand.get("_prev_year"):
        score -= 0.08
        reasons.append("prev_year")

    return max(0.0, min(1.0, score)), "+".join(reasons) or "no_signal"


def candidates_for(cur, year: int, make: str, model: str) -> list[dict[str, Any]]:
    """Public candidate fetch (see ``_candidates``) for callers batching many cars."""
    return _candidates(cur, year, make, model)


def resolve_from_candidates(car: dict[str, Any], cands: list[dict[str, Any]]) -> CatalogMatch | None:
    """Score pre-fetched candidates; None below the confidence floor."""
    if not cands:
        return None
    scored = [(score_candidate(car, c), c) for c in cands]
    scored.sort(key=lambda item: item[0][0], reverse=True)
    (best_score, method), best = scored[0]
    if best_score < MIN_CONFIDENCE:
        return None
    return CatalogMatch(
        epa_master_id=int(best["id"]),
        confidence=round(best_score, 3),
        method=method,
    )


def resolve_car(car: dict[str, Any]) -> CatalogMatch | None:
    """Best catalog row for a listing, or None below the confidence floor."""
    year = car.get("year")
    make = (car.get("make") or "").strip()
    model = (car.get("model") or "").strip()
    try:
        y = int(year)
    except (TypeError, ValueError):
        return None
    if not make or not model or not y:
        return None

    conn = get_conn()
    try:
        cur = conn.cursor()
        cands = _candidates(cur, y, make, model)
    finally:
        conn.close()
    if not cands:
        return None

    scored = [(score_candidate(car, c), c) for c in cands]
    scored.sort(key=lambda item: item[0][0], reverse=True)
    (best_score, method), best = scored[0]
    if best_score < MIN_CONFIDENCE:
        return None
    return CatalogMatch(
        epa_master_id=int(best["id"]),
        confidence=round(best_score, 3),
        method=method,
    )
