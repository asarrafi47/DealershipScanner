"""
Deterministic natural-language query parser for inventory search (no LLM).
"""
from __future__ import annotations

import re
from typing import Any

from backend.db.inventory_db import get_conn

# Spoken / slang -> canonical token before make/model fuzzy match
MAKE_SYNONYMS: dict[str, str] = {
    "beamer": "bmw",
    "bimmer": "bmw",
    "chevy": "chevrolet",
    "vw": "volkswagen",
    "merc": "mercedes-benz",
    "benz": "mercedes-benz",
    "mercedes": "mercedes-benz",
}

# Drivetrain-related phrases -> DB drivetrain values we OR together
_AWD_GROUP = ("AWD", "4WD")


def _fuzz_module():
    try:
        from thefuzz import fuzz as fuzz_mod  # type: ignore
        from thefuzz import process as process_mod  # type: ignore
        return fuzz_mod, process_mod
    except ImportError:
        return None, None


def _extract_one(query: str, choices: list[str], threshold: int = 76) -> str | None:
    if not query.strip() or not choices:
        return None
    _, process_mod = _fuzz_module()
    if process_mod is not None:
        m = process_mod.extractOne(query, choices)
        if m and m[1] >= threshold:
            return m[0]
    from difflib import get_close_matches

    q = query.lower().strip()
    lowered = [(c, c.lower()) for c in choices]
    best = get_close_matches(q, [x[1] for x in lowered], n=1, cutoff=0.55)
    if not best:
        return None
    for c, low in lowered:
        if low == best[0]:
            return c
    return None


def _extract_best_token(tokens: list[str], choices: list[str], threshold: int = 80) -> str | None:
    """Best match scanning individual tokens and 2-grams."""
    if not choices:
        return None
    _, process_mod = _fuzz_module()
    best_val = None
    best_score = 0
    candidates: list[str] = []
    for t in tokens:
        if len(t) >= 2 and not t.isdigit():
            candidates.append(t)
    for i in range(len(tokens) - 1):
        a, b = tokens[i], tokens[i + 1]
        if a.isdigit() or b.isdigit():
            continue
        candidates.append(f"{a} {b}")
    for cand in candidates:
        if process_mod is not None:
            m = process_mod.extractOne(cand, choices)
            if m and m[1] > best_score:
                best_score = m[1]
                best_val = m[0]
        else:
            hit = _extract_one(cand, choices, threshold=60)
            if hit:
                return hit
    if process_mod is not None and best_score >= threshold:
        return best_val
    return None


def _load_inventory_keywords() -> tuple[list[tuple[str, str]], list[str], list[str], list[str]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT make, model FROM cars
        WHERE make IS NOT NULL AND model IS NOT NULL
        ORDER BY make, model
        """
    )
    pairs = [(r[0], r[1]) for r in cur.fetchall()]
    cur.execute(
        "SELECT DISTINCT exterior_color FROM cars WHERE exterior_color IS NOT NULL ORDER BY exterior_color"
    )
    ext_colors = [r[0] for r in cur.fetchall()]
    cur.execute(
        "SELECT DISTINCT interior_color FROM cars WHERE interior_color IS NOT NULL ORDER BY interior_color"
    )
    int_colors = [r[0] for r in cur.fetchall()]
    cur.execute(
        """
        SELECT DISTINCT body_style FROM cars
        WHERE body_style IS NOT NULL AND TRIM(body_style) != ''
        ORDER BY body_style
        """
    )
    body_styles = [r[0] for r in cur.fetchall()]
    conn.close()
    return pairs, ext_colors, int_colors, body_styles


# Phrases in user text → fuzzy hint against DISTINCT body_style values
_BODY_STYLE_CUES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b(suv|crossovers?|cuv|sport\s+utility)\b", re.I), "SUV sport utility"),
    (re.compile(r"\b(sedan|saloon)\b", re.I), "Sedan"),
    (re.compile(r"\b(coupe)\b", re.I), "Coupe"),
    (re.compile(r"\b(convertible|roadster|spyder|cabrio|cabriolet)\b", re.I), "Convertible"),
    (re.compile(r"\b(hatchback|hatch)\b", re.I), "Hatchback"),
    (re.compile(r"\b(wagon|estate|avant)\b", re.I), "Wagon"),
    (re.compile(r"\b(truck|pickup|pick-up|crew\s+cab)\b", re.I), "Truck"),
    (re.compile(r"\b(minivan|mini\s*van|mpv)\b", re.I), "Minivan"),
    (re.compile(r"\bvan\b", re.I), "Van"),
    (re.compile(r"\b(gran\s+coupe|4[-\s]?door\s+coupe)\b", re.I), "Gran Coupe"),
]


def _match_body_style_filters(text: str, distinct: list[str]) -> list[str] | None:
    """Map natural-language body cues + exact facet tokens to DB ``body_style`` values (OR list)."""
    if not distinct or not (text or "").strip():
        return None
    hits: list[str] = []
    seen: set[str] = set()
    low = text.lower()
    for d in distinct:
        if not d or not str(d).strip():
            continue
        dlow = str(d).strip().lower()
        if len(dlow) >= 2 and re.search(rf"(?i)\b{re.escape(dlow)}\b", low):
            if d not in seen:
                seen.add(d)
                hits.append(d)
    for rx, hint in _BODY_STYLE_CUES:
        if not rx.search(text):
            continue
        pick = _extract_one(hint, distinct, threshold=60)
        if not pick:
            pick = _extract_best_token(_tokenize(f"{hint} {text}"), distinct, threshold=68)
        if pick and pick not in seen:
            seen.add(pick)
            hits.append(pick)
    return hits or None


def _apply_make_synonyms(text: str) -> str:
    t = text.lower()
    for slang, canon in MAKE_SYNONYMS.items():
        t = re.sub(rf"\b{re.escape(slang)}\b", canon, t)
    return t


def _strip_price_and_year_segments(text: str) -> str:
    """Remove segments already interpreted so tokens are not double-counted."""
    t = text
    # price patterns (same as extraction pass)
    t = re.sub(
        r"\b(?:under|below|less\s+than|max(?:imum)?|at\s+most)\s*"
        r"(?:\$?\s*)?([\d,]+(?:\.\d+)?)\s*(k|thousand)?\b",
        " ",
        t,
        flags=re.I,
    )
    t = re.sub(r"\$\s*([\d,]+(?:\.\d+)?)\s*(k|thousand)?\b", " ", t, flags=re.I)
    t = re.sub(r"\b([\d,]+)\s*k\b(?!\s*miles)", " ", t, flags=re.I)
    t = re.sub(
        r"\bunder\s+([\d,]+(?:\.\d+)?)\s*miles?\b",
        " ",
        t,
        flags=re.I,
    )
    t = re.sub(r"\b(?:19|20)\d{2}\s*(?:or\s+newer|\+)?\b", " ", t, flags=re.I)
    t = re.sub(r"\b(?:19|20)\d{2}\b", " ", t)
    return re.sub(r"\s+", " ", t).strip()


_COLOR_HINTS = frozenset(
    """
    white black silver gray grey red blue green brown beige gold orange yellow tan charcoal
    pearl metallic midnight sonic oxford rapid polar mythos jet granite carbon frozen
    mineral bernina atomic midnight
    """.split()
)


def _tokenize(s: str) -> list[str]:
    return [x for x in re.split(r"[^\w]+", s.lower()) if x]


def _detect_drivetrain(text: str) -> list[str] | None:
    t = text.lower()
    if re.search(
        r"\b(?:awd|4wd|4x4|four[\s-]?wheel|all[\s-]?wheel|xdrive|quattro|4matic)\b",
        t,
    ):
        return list(_AWD_GROUP)
    if re.search(r"\b(?:fwd|front[\s-]?wheel)\b", t):
        return ["FWD"]
    if re.search(r"\b(?:rwd|rear[\s-]?wheel)\b", t):
        return ["RWD"]
    return None


def _interior_color_cue(text: str) -> bool:
    low = text.lower()
    return any(
        w in low
        for w in ("interior", "inside", "cabin", "upholstery", "seats", "leather trim")
    )


def _match_interior_color(text: str, distinct_colors: list[str]) -> str | None:
    if not distinct_colors or not _interior_color_cue(text):
        return None
    tokens = _tokenize(text)
    _, process_mod = _fuzz_module()
    for tok in tokens:
        if tok in _COLOR_HINTS or len(tok) >= 4:
            if process_mod is not None:
                m = process_mod.extractOne(tok, distinct_colors)
                if m and m[1] >= 82:
                    return m[0]
            else:
                hit = _extract_one(tok, distinct_colors, threshold=60)
                if hit:
                    return hit
    return None


def _match_exterior_color(text: str, distinct_colors: list[str]) -> str | None:
    if not distinct_colors:
        return None
    tokens = _tokenize(text)
    _, process_mod = _fuzz_module()
    for tok in tokens:
        if tok in _COLOR_HINTS or len(tok) >= 4:
            if process_mod is not None:
                m = process_mod.extractOne(tok, distinct_colors)
                if m and m[1] >= 82:
                    return m[0]
            else:
                hit = _extract_one(tok, distinct_colors, threshold=60)
                if hit:
                    return hit
    return None


def _extract_max_price(text: str) -> tuple[float | None, str]:
    low = text.lower()
    max_price: float | None = None

    def consider(val: float) -> None:
        nonlocal max_price
        if max_price is None or val < max_price:
            max_price = val

    for m in re.finditer(
        r"\b(?:under|below|less\s+than|max(?:imum)?|at\s+most)\s*"
        r"(?:\$?\s*)?([\d,]+(?:\.\d+)?)\s*(k|thousand)?\b",
        low,
    ):
        raw, suffix = m.group(1), m.group(2) or ""
        num = float(raw.replace(",", ""))
        if suffix in ("k", "thousand"):
            num *= 1000
        consider(num)
    for m in re.finditer(r"\$\s*([\d,]+(?:\.\d+)?)\s*(k|thousand)?\b", low):
        raw, suffix = m.group(1), m.group(2) or ""
        num = float(raw.replace(",", ""))
        if suffix in ("k", "thousand"):
            num *= 1000
        consider(num)
    for m in re.finditer(r"\b([\d,]+(?:\.\d+)?)\s*k\b(?!\s*miles)", low):
        num = float(m.group(1).replace(",", "")) * 1000
        consider(num)
    return max_price, text


def _extract_mileage_cap(text: str) -> int | None:
    low = text.lower()
    m = re.search(
        r"\b(?:under|below|less\s+than|max)\s*([\d,]+)\s*k\s*miles?\b",
        low,
    )
    if m:
        raw = m.group(1).replace(",", "")
        try:
            return int(float(raw)) * 1000
        except ValueError:
            return None
    m2 = re.search(
        r"\b(?:under|below|less\s+than|max)\s*([\d,]+)\s*miles?\b",
        low,
    )
    if not m2:
        return None
    raw = m2.group(1).replace(",", "")
    try:
        return int(float(raw))
    except ValueError:
        return None


def _model_appears_in_text(model: str, raw_query: str) -> bool:
    """Require a real model cue in the user text (avoid spurious fuzzy pair matches)."""
    if not model:
        return False
    rq = raw_query.lower()
    m = model.lower().strip()
    if len(m) < 2:
        return False
    if re.search(rf"(?i)\b{re.escape(m)}\b", rq):
        return True
    for part in re.split(r"[\s\-]+", m):
        if len(part) >= 2 and re.search(rf"(?i)\b{re.escape(part)}\b", rq):
            return True
    return False


def _extract_years(text: str) -> tuple[int | None, int | None]:
    """Returns (min_year, max_year) for inventory filters."""
    low = text.lower()
    m = re.search(r"\b((?:19|20)\d{2})\s*or\s*newer\b", low)
    if m:
        y = int(m.group(1))
        return y, None
    years = [int(x) for x in re.findall(r"\b((?:19|20)\d{2})\b", low)]
    if not years:
        return None, None
    if len(years) >= 2:
        return min(years), max(years)
    y = years[0]
    return y, y


def parse_natural_query(query_text: str) -> dict[str, Any]:
    """
    Parse a free-text vehicle query into structured filters.
    Returns a dict with only applicable keys (omit unset/null).
    """
    raw = (query_text or "").strip()
    if not raw:
        return {}

    pairs, ext_colors, int_colors, body_styles = _load_inventory_keywords()
    makes = sorted({p[0] for p in pairs}, key=len, reverse=True)
    models_by_make: dict[str, list[str]] = {}
    for mk, md in pairs:
        models_by_make.setdefault(mk, []).append(md)
    all_models = sorted({p[1] for p in pairs}, key=len, reverse=True)

    out: dict[str, Any] = {}

    max_price, _ = _extract_max_price(raw)
    if max_price is not None:
        out["max_price"] = int(round(max_price))

    miles = _extract_mileage_cap(raw)
    if miles is not None:
        out["max_mileage"] = miles

    y_min, y_max = _extract_years(raw)
    if y_min is not None:
        out["min_year"] = y_min
    if y_max is not None:
        out["max_year"] = y_max

    dt = _detect_drivetrain(raw)
    if dt:
        out["drivetrain"] = dt

    bs = _match_body_style_filters(raw, body_styles)
    if bs:
        out["body_style"] = bs

    ic = _match_interior_color(raw, int_colors)
    if ic:
        out["interior_color"] = [ic]

    ec = _match_exterior_color(raw, ext_colors)
    if ec:
        out["exterior_color"] = [ec]

    working = _apply_make_synonyms(raw)
    working = _strip_price_and_year_segments(working)
    working = re.sub(
        r"\b(?:awd|4wd|4x4|fwd|rwd|xdrive|quattro|4matic)\b",
        " ",
        working,
        flags=re.I,
    )
    for rx, _hint in _BODY_STYLE_CUES:
        working = rx.sub(" ", working)
    for tok in _tokenize(working):
        if tok in _COLOR_HINTS:
            working = re.sub(rf"\b{re.escape(tok)}\b", " ", working, flags=re.I)
    working = re.sub(r"\s+", " ", working).strip()

    tokens = _tokenize(working)

    # Prefer full "Make Model" pair match (WRatio handles multi-word makes/models)
    pair_labels = [f"{a} {b}" for a, b in pairs]
    fuzz_mod, process_mod = _fuzz_module()
    best_make, best_model = None, None
    if working:
        if process_mod is not None and fuzz_mod is not None:
            pm = process_mod.extractOne(
                working, pair_labels, scorer=fuzz_mod.WRatio, score_cutoff=75
            )
            if pm:
                label = pm[0]
                try:
                    idx = pair_labels.index(label)
                    best_make, best_model = pairs[idx]
                except ValueError:
                    pass
        if best_make is None:
            for mk, md in sorted(pairs, key=lambda x: len(x[1]), reverse=True):
                if re.search(rf"(?i)\b{re.escape(md)}\b", working):
                    best_make, best_model = mk, md
                    break

    if best_make is None and tokens:
        best_make = _extract_best_token(tokens, makes, threshold=82)
        if best_make:
            mdl_list = models_by_make.get(best_make, all_models)
            best_model = _extract_best_token(tokens, mdl_list, threshold=82)

    if best_make is None and tokens:
        best_model = _extract_best_token(tokens, all_models, threshold=85)
        if best_model:
            for mk, md in pairs:
                if md == best_model:
                    best_make = mk
                    break

    if best_make:
        out["make"] = best_make
    if best_model and _model_appears_in_text(best_model, raw):
        out["model"] = best_model

    return out
