"""
Deterministic natural-language query parser for inventory search (no LLM).
"""
from __future__ import annotations

import re
from functools import lru_cache
from typing import Any

from backend.db.inventory_db import get_conn
from backend.utils.field_clean import is_effectively_empty
from backend.utils.interior_color_buckets import (
    infer_paint_color_buckets,
    parse_stored_buckets,
    sort_paint_family_ids,
)

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


def _load_inventory_keywords() -> tuple[list[tuple[str, str]], list[str], list[str], list[str], list[str]]:
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
        """
        SELECT exterior_color, interior_color, interior_color_buckets
        FROM cars
        WHERE (COALESCE(listing_active, 1) = 1)
        """
    )
    ext_ids: set[str] = set()
    int_ids: set[str] = set()
    for ext_raw, int_raw, int_bucks in cur.fetchall():
        if not is_effectively_empty(ext_raw):
            ext_ids.update(infer_paint_color_buckets(ext_raw, None))
        ib = parse_stored_buckets(int_bucks)
        if ib:
            int_ids.update(ib)
        elif not is_effectively_empty(int_raw):
            int_ids.update(infer_paint_color_buckets(int_raw, None))
    ext_colors = sort_paint_family_ids(ext_ids)
    int_colors = sort_paint_family_ids(int_ids)
    cur.execute(
        """
        SELECT DISTINCT body_style FROM cars
        WHERE body_style IS NOT NULL AND TRIM(body_style) != ''
        ORDER BY body_style
        """
    )
    body_styles = [r[0] for r in cur.fetchall()]
    cur.execute(
        """
        SELECT DISTINCT TRIM(trim) FROM cars
        WHERE trim IS NOT NULL AND LENGTH(TRIM(trim)) >= 3
        ORDER BY LENGTH(TRIM(trim)) DESC, TRIM(trim)
        """
    )
    trims = [r[0] for r in cur.fetchall() if r[0]]
    conn.close()
    return pairs, ext_colors, int_colors, body_styles, trims


# Phrases in user text → body type cue, with keywords to match against DB values
_BODY_STYLE_CUES: list[tuple[re.Pattern[str], str, list[str]]] = [
    (re.compile(r"\b(suv|crossovers?|cuv|sport\s+utility|family\s+(?:car|vehicle|suv))\b", re.I), "SUV sport utility",
     ["suv", "sport utility", "crossover", "cuv", "utility vehicle"]),
    (re.compile(r"\b(sedan|saloon|4[-\s]?door(?!\s+coupe))\b", re.I), "Sedan",
     ["sedan", "saloon"]),
    (re.compile(r"\b(coupe|sports?\s+car|2[-\s]?door)\b", re.I), "Coupe",
     ["coupe"]),
    (re.compile(r"\b(convertible|roadster|spyder|cabrio|cabriolet|drop\s*top|open\s+top)\b", re.I), "Convertible",
     ["convertible", "cabriolet", "roadster"]),
    (re.compile(r"\b(hatchback|hatch)\b", re.I), "Hatchback",
     ["hatchback", "liftback", "sportshatch"]),
    (re.compile(r"\b(wagon|estate|avant)\b", re.I), "Wagon",
     ["wagon", "estate"]),
    (re.compile(r"\b(truck|pickup|pick-up|crew\s+cab)\b", re.I), "Truck",
     ["truck", "pickup", "crew cab", "supercrew", "supercab", "regular cab",
      "double cab", "quad cab", "flatbed"]),
    (re.compile(r"\b(minivan|mini\s*van|mpv|family\s+van)\b", re.I), "Minivan",
     ["minivan", "mpv"]),
    (re.compile(r"\bvan\b", re.I), "Van",
     ["van"]),
    (re.compile(r"\b(gran\s+coupe|4[-\s]?door\s+coupe)\b", re.I), "Gran Coupe",
     ["gran coupe"]),
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
        # Skip body_style values that are really trim names (e.g. "LIMITED", "XLE", "LE")
        if dlow in _TRIM_SKIP_ALONE:
            continue
        if len(dlow) >= 2 and re.search(rf"(?i)\b{re.escape(dlow)}\b", low):
            if d not in seen:
                seen.add(d)
                hits.append(d)
    for rx, _hint, keywords in _BODY_STYLE_CUES:
        m = rx.search(text)
        if not m:
            continue
        # Only apply keywords tied to what the user actually said (e.g. "crew cab" → crew cab,
        # not every DB value containing "pickup" or "truck").
        phrase = (m.group(0) or "").lower()
        active_kws = [
            kw
            for kw in keywords
            if kw in phrase or phrase in kw or re.search(rf"\b{re.escape(kw)}\b", phrase)
        ]
        if not active_kws:
            active_kws = keywords[:2]
        for d in distinct:
            if not d:
                continue
            dlow = str(d).strip().lower()
            if any(kw in dlow for kw in active_kws) and d not in seen:
                seen.add(d)
                hits.append(d)
    if len(hits) > 10:
        hits = hits[:10]
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
        # Only consider tokens that are themselves known color words — prevents
        # model names like "silverado" fuzzy-matching to "silver"
        if tok not in _COLOR_HINTS:
            continue
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
        # Only consider tokens that are themselves known color words — prevents
        # model names like "silverado", "mustang" fuzzy-matching to colors
        if tok not in _COLOR_HINTS:
            continue
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


def _make_appears_in_text(make: str, raw_query: str) -> bool:
    """Make name (or its synonym) must appear literally in query to prevent fuzzy false positives."""
    if not make:
        return False
    expanded = _apply_make_synonyms(raw_query).lower()
    original = raw_query.lower()
    make_lower = make.lower().strip()
    make_words = [w for w in re.split(r"[\s\-]+", make_lower) if len(w) >= 3]
    for w in make_words:
        if re.search(rf"\b{re.escape(w)}\b", expanded) or re.search(rf"\b{re.escape(w)}\b", original):
            return True
    return False


# Common automotive/English words that appear in model names but are NOT distinctive model identifiers.
# Matching these alone does not confirm the user named the model.
_MODEL_GENERIC_WORDS = frozenset(
    "hybrid electric plug plug-in in phev sport turbo premium limited touring luxury base type "
    "plus max pro se le xle xse awd fwd rwd van sedan coupe truck suv car auto "
    "new used certified performance edition special gt rs si type-r".split()
)
_FUEL_TYPE_BODY_STYLES = frozenset(
    "hybrid electric plug-in plug-in hybrid fhev phev hev".split()
)


def _model_appears_in_text(model: str, raw_query: str) -> bool:
    """Require a distinctive model token in the user text (avoid spurious fuzzy pair matches)."""
    if not model:
        return False
    rq = raw_query.lower()
    m = model.lower().strip()
    if len(m) < 2:
        return False
    # Full model name match is always valid
    if re.search(rf"(?i)\b{re.escape(m)}\b", rq):
        return True
    # Require at least one DISTINCTIVE (non-generic) word from model name to appear
    parts = [p for p in re.split(r"[\s\-/]+", m) if len(p) >= 2]
    distinctive = [p for p in parts if p not in _MODEL_GENERIC_WORDS]
    if not distinctive:
        return False
    # Long variant names (4+ parts, e.g. "Silverado 2500HD Built After Aug 14") require at
    # least one secondary distinctive word to also appear — "silverado" alone is not enough.
    if len(parts) >= 4:
        secondary = [p for p in distinctive[1:] if not p.isdigit()]
        if secondary and not any(re.search(rf"(?i)\b{re.escape(p)}\b", rq) for p in secondary):
            return False
    return any(re.search(rf"(?i)\b{re.escape(p)}\b", rq) for p in distinctive)


_CYLINDER_RE = re.compile(
    r"\b(?:"
    r"v\s*(\d{1,2})"                       # v6, v8, v12
    r"|(\d{1,2})\s*[-\s]?cyl(?:inder)?s?"  # 6-cyl, 8cylinder, 6 cylinders
    r"|(?:inline|straight|i)\s*[-\s]?(\d{1,2})"  # inline-4, i6
    r"|(?:four|4)[-\s]?cyl"               # four-cyl → 4
    r"|(?:six|6)[-\s]?cyl"               # six-cyl → 6
    r"|(?:eight|8)[-\s]?cyl"             # eight-cyl → 8
    r")\b",
    re.I,
)
_WORD_CYLINDERS = {"four": 4, "six": 6, "eight": 8, "ten": 10, "twelve": 12}


def _extract_cylinders(text: str) -> int | None:
    low = text.lower()
    # Word form: "six cylinder", "four cylinders"
    for word, n in _WORD_CYLINDERS.items():
        if re.search(rf"\b{word}\s+cyl", low):
            return n
    m = _CYLINDER_RE.search(low)
    if not m:
        return None
    val = m.group(1) or m.group(2) or m.group(3)
    if val:
        try:
            n = int(val)
            return n if 3 <= n <= 16 else None
        except ValueError:
            return None
    # Named word forms (four-cyl, six-cyl, eight-cyl) captured by alternation
    if re.search(r"\bfour[-\s]?cyl", low):
        return 4
    if re.search(r"\bsix[-\s]?cyl", low):
        return 6
    if re.search(r"\beight[-\s]?cyl", low):
        return 8
    return None


_FUEL_RE: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(electric|ev|bev|battery|zero[\s-]?emission|no[\s-]?gas|all[\s-]?electric)\b", re.I), "electric"),
    (re.compile(r"\b(plug.?in|phev)\b", re.I), "plug-in hybrid"),
    (re.compile(r"\b(hybrid|hev|fhev|good[\s-]?on[\s-]?gas|fuel[\s-]?efficient|gas[\s-]?saver|save[\s-]?gas|mpg)\b", re.I), "hybrid"),
    (re.compile(r"\bdiesel\b", re.I), "diesel"),
    (re.compile(r"\b(gas|gasoline|petrol)\b", re.I), "gasoline"),
]


# Feature/package keyword extraction — maps user phrases to package search terms
_FEATURE_MAP: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(panoramic|pano\s*roof)\b", re.I), "panoramic"),
    (re.compile(r"\b(sun\s*roof|moon\s*roof)\b", re.I), "sunroof"),
    (re.compile(r"\b(heated\s+seats?|seat\s+warmer|warm\s+seats?)\b", re.I), "heated"),
    (re.compile(r"\b(ventilated\s+seats?|cooled\s+seats?)\b", re.I), "ventilated"),
    (re.compile(r"\bmassage\s+seats?\b", re.I), "massage"),
    (re.compile(r"\b(nav(?:igation)?|gps)\b", re.I), "navigation"),
    (re.compile(r"\b(apple\s+car\s*play|carplay)\b", re.I), "carplay"),
    (re.compile(r"\bandroid\s+auto\b", re.I), "android auto"),
    (re.compile(r"\bleather\b", re.I), "leather"),
    (re.compile(r"\b(blind\s+spot|bsd|bsm)\b", re.I), "blind spot"),
    (re.compile(r"\b(back(?:up)?\s+cam(?:era)?|rear\s+cam(?:era)?|reverse\s+cam(?:era)?)\b", re.I), "backup camera"),
    (re.compile(r"\bremote\s+start\b", re.I), "remote start"),
    (re.compile(r"\bwireless\s+charg\w*\b", re.I), "wireless"),
    (re.compile(r"\b(third\s+row|3rd\s+row|[78][-\s]?seat(?:er)?|[78]\s+passenger)\b", re.I), "third row"),
    (re.compile(r"\blane\s+(?:assist|departure|keep|warning)\b", re.I), "lane"),
    (re.compile(r"\badaptive\s+cruise\b", re.I), "adaptive cruise"),
    (re.compile(r"\b(heads?\s*[\s-]?up\s*display|hud)\b", re.I), "head-up"),
]


def _extract_feature_keyword(text: str) -> str | None:
    """Return the first matching feature keyword for packages_json_contains, or None."""
    for rx, keyword in _FEATURE_MAP:
        if rx.search(text):
            return keyword
    return None


def _extract_fuel_type(text: str) -> str | None:
    for rx, label in _FUEL_RE:
        if rx.search(text):
            return label
    return None


_ENGINE_L_RE = re.compile(
    r"\b(\d+\.\d+)\s*[lL](?:iter)?s?\b"               # 5.0L, 3.5 liter
    r"|\b(\d+\.\d+)\s*(?=\s+(?:engine|liter|motor))"  # 5.0 engine
    r"|\b([1-9]\.\d)\b",                               # bare 5.0 / 3.5 (displacement shorthand)
    re.I,
)


def _extract_engine_liters(text: str) -> float | None:
    m = _ENGINE_L_RE.search(text)
    if not m:
        return None
    val = m.group(1) or m.group(2) or m.group(3)
    if val:
        try:
            v = float(val)
            return v if 0.5 <= v <= 10.0 else None
        except ValueError:
            return None
    return None


# Generic trim words — don't use these alone as trim filters
_TRIM_SKIP_ALONE = frozenset(
    "sport se le xle xse lx ex s premium base limited touring "
    "plus max pro platinum signature gt rs type r edition special "
    "ultra advance prestige elite luxury value standard entry".split()
)


def _extract_trim_hint(working_stripped: str, trims: list[str], make: str | None) -> str | None:
    """Find a trim keyword from the cleaned query. Returns the DB trim substring to search for."""
    if not working_stripped or not trims:
        return None
    low = working_stripped.lower().strip()
    if not low:
        return None

    # Build fast lookup: lowercased trim → original trim (longest first to prefer specific matches)
    seen: set[str] = set()
    candidates: list[str] = []
    for t in trims:  # already sorted longest-first from DB query
        tl = t.strip().lower()
        if tl not in seen:
            seen.add(tl)
            candidates.append(t)

    # Try multi-word matches first (longest-first order handles this naturally)
    for t in candidates:
        tl = t.strip().lower()
        tlen = len(tl)
        if tlen < 3:
            continue
        if re.search(rf"\b{re.escape(tl)}\b", low):
            # Skip generic single-word trims unless make context narrows it
            if tlen < 5 and tl in _TRIM_SKIP_ALONE and not make:
                continue
            return tl  # return lowercase for INSTR search

    # Reverse pass: check if any query token appears as a substring of a DB trim.
    # Handles BMW-style shorthand: "40i" ⊂ "xDrive40i", "50i" ⊂ "M50i", etc.
    # Skip plain decimals (e.g. "5.0") — those are engine displacements, not trim names.
    trim_lowers = {t.strip().lower() for t in candidates}
    for tok in re.split(r"\s+", low):
        if len(tok) < 3 or tok in _TRIM_SKIP_ALONE:
            continue
        if re.match(r"^\d+\.?\d*$", tok):  # pure numeric / decimal → skip
            continue
        if any(tok in tl for tl in trim_lowers):
            return tok
    return None


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

    pairs, ext_colors, int_colors, body_styles, trims = _load_inventory_keywords()
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

    cyl = _extract_cylinders(raw)
    if cyl is not None:
        out["cylinders"] = cyl

    ft = _extract_fuel_type(raw)
    if ft:
        out["fuel_type"] = ft

    eng_l = _extract_engine_liters(raw)
    if eng_l is not None:
        out["engine_displacement_l_min"] = round(eng_l - 0.15, 2)
        out["engine_displacement_l_max"] = round(eng_l + 0.15, 2)

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
    # Strip cylinder tokens so "v6" / "v8" don't fuzzy-match make/model names
    working = _CYLINDER_RE.sub(" ", working)
    # Strip engine displacement (e.g. "5.0L", "3.5 liter") so it doesn't skew model matching
    working = _ENGINE_L_RE.sub(" ", working)
    # Strip fuel type cues
    for rx, _ in _FUEL_RE:
        working = rx.sub(" ", working)
    for rx, *_ in _BODY_STYLE_CUES:
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

    # Prefer the model whose non-numeric distinctive words ALL appear in query.
    # "x5 40i" → pair picks "X5 sDrive40i"; "sdrive40i" ∉ query → downgrade to "X5".
    # "silverado high country" → pair picks "Silverado MD"; "md" ∉ query → "Silverado 1500".
    if best_make and best_model:
        def _all_nonnum_dist_in_query(mdl: str, rq: str) -> bool:
            ml = mdl.lower().strip()
            pts = [p for p in re.split(r"[\s\-/]+", ml) if len(p) >= 2]
            dist = [p for p in pts if p not in _MODEL_GENERIC_WORDS]
            nonnum = [p for p in dist if not p.isdigit()]
            if not nonnum:
                return True
            return all(re.search(rf"(?i)\b{re.escape(p)}\b", rq) for p in nonnum)

        if not _all_nonnum_dist_in_query(best_model, raw):
            mdl_list = models_by_make.get(best_make, [])
            valid = [
                md for md in mdl_list
                if _all_nonnum_dist_in_query(md, raw) and _model_appears_in_text(md, raw)
            ]
            best_model = min(valid, key=len) if valid else None

    if best_make and _make_appears_in_text(best_make, raw):
        out["make"] = best_make
        if best_model and _model_appears_in_text(best_model, raw):
            out["model"] = best_model
    elif best_model and _model_appears_in_text(best_model, raw):
        out["model"] = best_model
        out["make"] = best_make  # keep inferred make when model is confirmed

    # Detect trim keyword from remaining working string (after stripping known signals)
    # Strip make/model names so they don't re-match as trim keywords
    trim_working = working
    if out.get("make"):
        trim_working = re.sub(rf"\b{re.escape(out['make'])}\b", " ", trim_working, flags=re.I)
    if out.get("model"):
        trim_working = re.sub(rf"\b{re.escape(out['model'])}\b", " ", trim_working, flags=re.I)
    trim_working = re.sub(r"\s+", " ", trim_working).strip()
    trim_hint = _extract_trim_hint(trim_working, trims, out.get("make"))
    if trim_hint:
        out["trim_contains"] = trim_hint

    feature = _extract_feature_keyword(raw)
    if feature:
        out["packages_json_contains"] = feature

    # Drop body_style values that are really fuel-type labels (e.g. "PLUG-IN HYBRID" as body_style)
    if out.get("fuel_type") and out.get("body_style"):
        cleaned = [
            b for b in out["body_style"]
            if b.lower().strip() not in _FUEL_TYPE_BODY_STYLES
            and "hybrid" not in b.lower()
            and "electric" not in b.lower()
            and "plug" not in b.lower()
        ]
        if cleaned:
            out["body_style"] = cleaned
        else:
            out.pop("body_style", None)

    return out


_AI_SYSTEM_PROMPT = """Extract car inventory search filters from a user query. Return ONLY a JSON object.

STRICT RULE: Only include "make" or "model" if the user EXPLICITLY names a car brand or model (e.g. "Honda", "Accord", "Toyota Camry"). Do NOT infer make from body style, engine, or other attributes.

Available fields (omit any not mentioned):
- "make": explicit brand name only (e.g. "Honda", "Toyota", "BMW")
- "model": explicit model name only (e.g. "Accord", "Camry", "F-150")
- "min_year": integer
- "max_year": integer
- "max_price": integer dollars ("30k"=30000, "under 35"=35000 treating bare numbers as thousands)
- "max_mileage": integer miles ("50k miles"=50000)
- "drivetrain": list, values only from ["AWD","4WD","FWD","RWD"]
- "body_style": list, values only from ["Sedan","SUV","Truck","Coupe","Hatchback","Convertible","Minivan","Wagon"]
- "fuel_type": one of ["Gasoline","Hybrid","Electric","Plug-In Hybrid","Diesel"]
- "exterior_color": list of color families ["red","white","black","silver","blue","gray","green","brown","gold","orange","yellow"]
- "cylinders": integer (V8→8, V6→6, "4-cylinder"/"inline-4"/"I4"→4, "6-cylinder"→6)
- "engine_displacement_l_min": float liters
- "engine_displacement_l_max": float liters
- "packages_json_contains": MOST important single feature word the user mentions (sunroof, heated seats, navigation, carplay, leather, blind spot, backup camera, panoramic)

Mappings:
- AWD/4x4/all-wheel/quattro/xDrive/4MATIC → drivetrain:["AWD","4WD"]
- FWD/front-wheel → drivetrain:["FWD"]
- RWD/rear-wheel → drivetrain:["RWD"]
- hybrid → fuel_type:"Hybrid"
- plug-in hybrid/PHEV → fuel_type:"Plug-In Hybrid"
- electric/EV/battery → fuel_type:"Electric"
- SUV/crossover/CUV → body_style:["SUV"]
- truck/pickup → body_style:["Truck"]
- van/minivan → body_style:["Minivan"]
- hatchback/hatch → body_style:["Hatchback"]
- "2.0L" → engine_displacement_l_min:1.9, engine_displacement_l_max:2.1
- "turbo 4" → cylinders:4
- sporty/performance with no explicit engine → cylinders:6

Return valid JSON only. No explanation."""


def ai_parse_natural_query(query_text: str) -> dict[str, Any]:
    """Parse query with Claude Haiku; falls back to parse_natural_query on error."""
    import json
    import os

    raw = (query_text or "").strip()
    if not raw:
        return {}

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return parse_natural_query(raw)

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            system=_AI_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": raw}],
        )
        text = resp.content[0].text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\n?", "", text)
            text = re.sub(r"\n?```$", "", text)
        filters = json.loads(text)
        if not isinstance(filters, dict):
            return parse_natural_query(raw)

        _, _ext, _int, body_styles_distinct, _trims = _load_inventory_keywords()

        # Drop make/model from AI output — the regex parser handles these reliably.
        # AI is only trusted for signals it does better: cylinders, fuel_type, features, engine, body_style.
        filters.pop("make", None)
        filters.pop("model", None)

        # Expand canonical body_style values to actual DB variants via existing matcher
        if filters.get("body_style"):
            canonical = filters["body_style"]
            hint = " ".join(canonical) if isinstance(canonical, list) else str(canonical)
            expanded = _match_body_style_filters(hint, body_styles_distinct)
            if expanded:
                filters["body_style"] = expanded
            else:
                filters.pop("body_style", None)

        # Merge: regex parser is authoritative for structured fields
        regex = parse_natural_query(raw)
        if regex.get("make"):
            filters["make"] = regex["make"]
        if regex.get("model"):
            filters["model"] = regex["model"]
        # Regex values take precedence over AI; AI fills in if regex found nothing
        for k in ("min_year", "max_year", "max_price", "max_mileage", "drivetrain",
                  "cylinders", "fuel_type"):
            if regex.get(k) is not None:
                filters[k] = regex[k]  # regex wins
            # else: AI value (if any) stays in filters
        # body_style: prefer expanded AI value if present, otherwise use regex
        if not filters.get("body_style") and regex.get("body_style"):
            filters["body_style"] = regex["body_style"]
        # Remove fuel-type-labeled DB body_style values (e.g. "PLUG-IN HYBRID" stored as body_style)
        # when we already have a fuel_type filter for the same concept.
        if filters.get("fuel_type") and filters.get("body_style"):
            cleaned = [
                b for b in filters["body_style"]
                if b.lower().strip() not in _FUEL_TYPE_BODY_STYLES
                and "hybrid" not in b.lower()
                and "electric" not in b.lower()
                and "plug" not in b.lower()
            ]
            if cleaned:
                filters["body_style"] = cleaned
            else:
                filters.pop("body_style", None)

        return filters
    except Exception:
        return parse_natural_query(raw)
