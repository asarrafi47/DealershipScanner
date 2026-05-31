"""
Deterministic natural-language query parser for inventory search.

Used by the listings search bar and ``POST /api/search/smart`` — local regex/fuzzy
only; no Claude or other LLM calls. Premium chat/compare features use separate modules.
"""
from __future__ import annotations

import os
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


def _inventory_db_cache_key() -> str:
    """Cache token so parser reloads when inventory DB path or file changes."""
    from backend.db import inventory_db as inv_db

    path = os.environ.get("INVENTORY_DB_PATH") or getattr(inv_db, "DB_PATH", "") or ""
    try:
        st = os.stat(path)
        return f"{path}:{st.st_mtime_ns}:{st.st_size}"
    except OSError:
        return str(path)


def clear_query_parser_caches() -> None:
    """Drop cached inventory keywords (tests, DB path changes)."""
    _load_inventory_keywords.cache_clear()
    _load_package_names.cache_clear()


@lru_cache(maxsize=4)
def _load_inventory_keywords(cache_key: str) -> tuple[tuple[tuple[str, str], ...], tuple[str, ...], tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    import sqlite3

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT DISTINCT make, model FROM cars
            WHERE make IS NOT NULL AND model IS NOT NULL
            ORDER BY make, model
            """
        )
    except sqlite3.OperationalError:
        conn.close()
        return (), (), (), (), ()
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
    return tuple(pairs), tuple(ext_colors), tuple(int_colors), tuple(body_styles), tuple(trims)


@lru_cache(maxsize=4)
def _load_package_names(cache_key: str) -> tuple[str, ...]:
    """Distinct OEM package / option names from active inventory (longest first for substring match)."""
    import json

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT packages FROM cars
        WHERE (COALESCE(listing_active, 1) = 1)
          AND packages IS NOT NULL
          AND packages NOT IN ('{}', '[]', 'null', '')
        """
    )
    seen: set[str] = set()
    names: list[str] = []
    for (pkg_raw,) in cur.fetchall():
        try:
            pj = json.loads(pkg_raw)
        except Exception:
            continue
        if not isinstance(pj, dict):
            continue
        row_names: list[str] = []
        for entry in pj.get("packages_normalized") or []:
            if isinstance(entry, dict):
                n = (entry.get("canonical_name") or entry.get("name") or "").strip()
                if n:
                    row_names.append(n)
        for n in pj.get("possible_packages") or []:
            if isinstance(n, str) and n.strip():
                row_names.append(n.strip())
        for priced in pj.get("sticker_options_priced") or []:
            if isinstance(priced, dict):
                n = (priced.get("name") or priced.get("label") or "").strip()
                if n:
                    row_names.append(n)
        for n in pj.get("sticker_options") or []:
            if isinstance(n, str) and n.strip():
                row_names.append(n.strip())
        for n in row_names:
            low = n.lower()
            if low not in seen:
                seen.add(low)
                names.append(n)
    conn.close()
    names.sort(key=lambda s: len(s), reverse=True)
    return tuple(names)


# Phrases in user text → body type cue, with keywords to match against DB values
_BODY_STYLE_CUES: list[tuple[re.Pattern[str], str, list[str]]] = [
    (re.compile(r"\b(suv|sport\s+utility|family\s+(?:car|vehicle|suv))\b", re.I), "SUV sport utility",
     ["suv", "sport utility", "utility vehicle"]),
    (re.compile(r"\b(crossovers?|cuv)\b", re.I), "Crossover",
     ["crossover", "cuv"]),
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


def _exterior_color_cue(text: str) -> bool:
    low = text.lower()
    return any(w in low for w in ("exterior", "outside", "paint", "body color", "body colour"))


def _resolve_color_token(token: str, distinct_colors: list[str]) -> str | None:
    tok = (token or "").strip().lower()
    if not tok or tok not in _COLOR_HINTS:
        return None
    _, process_mod = _fuzz_module()
    if process_mod is not None:
        m = process_mod.extractOne(tok, distinct_colors)
        if m and m[1] >= 82:
            return m[0]
    return _extract_one(tok, distinct_colors, threshold=60)


_CONTEXTUAL_INTERIOR_COLOR_RE = re.compile(
    r"\b(?:(?:interior|inside|cabin|upholstery)\s+(?:is\s+)?(\w+)|(\w+)\s+(?:interior|inside|cabin|upholstery))\b",
    re.I,
)
_CONTEXTUAL_EXTERIOR_COLOR_RE = re.compile(
    r"\b(?:(?:exterior|outside|paint)\s+(?:is\s+)?(\w+)|(\w+)\s+(?:exterior|outside|paint))\b",
    re.I,
)


def _extract_contextual_color(text: str, pattern: re.Pattern[str], distinct_colors: list[str]) -> str | None:
    for m in pattern.finditer(text):
        raw = (m.group(1) or m.group(2) or "").strip()
        hit = _resolve_color_token(raw, distinct_colors)
        if hit:
            return hit
    return None


def _match_interior_color(text: str, distinct_colors: list[str]) -> str | None:
    if not distinct_colors:
        return None
    hit = _extract_contextual_color(text, _CONTEXTUAL_INTERIOR_COLOR_RE, distinct_colors)
    if hit:
        return hit
    if not _interior_color_cue(text):
        return None
    tokens = _tokenize(text)
    _, process_mod = _fuzz_module()
    for tok in tokens:
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


def _match_exterior_color(text: str, distinct_colors: list[str], *, interior_color: str | None = None) -> str | None:
    if not distinct_colors:
        return None
    hit = _extract_contextual_color(text, _CONTEXTUAL_EXTERIOR_COLOR_RE, distinct_colors)
    if hit:
        return hit
    interior_tok = (interior_color or "").strip().lower()
    tokens = [t for t in _tokenize(text) if t in _COLOR_HINTS and t != interior_tok]
    if _interior_color_cue(text) and not _exterior_color_cue(text):
        return None
    _, process_mod = _fuzz_module()
    for tok in tokens:
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
        r"(?:\$?\s*)?([\d,]+(?:\.\d+)?)\s*(k|thousand)?\b(?!\s*miles?\b)",
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
    (re.compile(r"\bbowers[\s&-]*(?:and[\s-]*)?wilkins\b", re.I), "bowers"),
    (re.compile(r"\b(?:harman[\s/-]*kardon|harman)\b", re.I), "harman"),
    (re.compile(r"\b(?:360\s*°?\s*cam(?:era)?s?|surround[\s-]?view(?:\s+cam(?:era)?s?)?)\b", re.I), "360"),
    (re.compile(r"\bpremium\s+audio\b", re.I), "premium audio"),
]


_FULLY_LOADED_RE = re.compile(
    r"\b(?:fully[\s-]?loaded|fully[\s-]?equipped|every\s+option|all\s+the\s+options|max(?:imum)?\s+options)\b",
    re.I,
)


def _extract_feature_keywords(text: str) -> list[str]:
    """Return all matching feature keywords for packages_json_contains_list."""
    hits: list[str] = []
    seen: set[str] = set()
    for rx, keyword in _FEATURE_MAP:
        if rx.search(text):
            low = keyword.lower()
            if low not in seen:
                seen.add(low)
                hits.append(keyword)
    return hits


def _extract_feature_keyword(text: str) -> str | None:
    """Return the first matching feature keyword for packages_json_contains, or None."""
    hits = _extract_feature_keywords(text)
    return hits[0] if hits else None


_WITH_EQUIPMENT_RE = re.compile(
    r"\b(?:with|w/)\s+(?:the\s+)?([\w\s/&.+-]{2,48}?)(?:\s+package)?(?:\s|$|,|·)",
    re.I,
)
_NAMED_PACKAGE_RE = re.compile(
    r"\b([\w\s/&.+-]{2,48}?\s+package)\b",
    re.I,
)


def _extract_all_package_search_terms(text: str, package_names: tuple[str, ...] | list[str]) -> list[str]:
    """Collect every equipment / package substring mentioned in the query."""
    needles: list[str] = []
    seen: set[str] = set()

    def add(raw: str) -> None:
        needle = (raw or "").strip().lower()
        if not needle or len(needle) < 2 or needle in seen:
            return
        if len(needle) > 200:
            needle = needle[:200]
        seen.add(needle)
        needles.append(needle)

    for feature in _extract_feature_keywords(text):
        add(feature)

    low = text.lower()
    for name in package_names:
        nlow = name.lower().strip()
        if len(nlow) >= 4 and nlow in low:
            core = re.sub(r"\s+package$", "", nlow).strip()
            add(core if len(core) >= 3 else nlow)

    for m in _NAMED_PACKAGE_RE.finditer(text):
        phrase = m.group(1).strip().lower()
        core = re.sub(r"\s+package$", "", phrase).strip()
        add(core if len(core) >= 3 else phrase)

    for m in _WITH_EQUIPMENT_RE.finditer(text):
        phrase = m.group(1).strip().lower()
        if phrase and phrase not in {"a", "the", "an"}:
            add(phrase)

    return needles


def _extract_package_search_term(text: str, package_names: tuple[str, ...] | list[str]) -> str | None:
    """Map free text to a single packages_json_contains needle (legacy helper)."""
    hits = _extract_all_package_search_terms(text, package_names)
    return hits[0] if hits else None


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
    "ultra advance prestige elite luxury value standard entry "
    "and or with inside outside under loaded fully miles mile camera display".split()
)

# Mercedes-Benz line codes: S580, E 350, GLC300 (inventory uses S-Class + trim "S 580", etc.)
_MB_LINE_PREFIXES = (
    "GLC", "GLE", "GLS", "CLA", "CLE", "CLS", "GLA", "GLB", "SL", "A", "C", "E", "G", "S"
)
_MB_PREFIX_TO_MODEL = {
    "A": "A-Class",
    "C": "C-Class",
    "E": "E-Class",
    "G": "G-Class",
    "S": "S-Class",
    "GLC": "GLC",
    "GLE": "GLE",
    "GLS": "GLS",
    "CLA": "CLA",
    "CLE": "CLE",
    "CLS": "CLS",
    "GLA": "GLA",
    "GLB": "GLB",
    "SL": "SL",
}
_MB_LINE_RE = re.compile(
    r"(?i)\b(" + "|".join(re.escape(p) for p in _MB_LINE_PREFIXES) + r")\s*(\d{2,3})\b"
)


def _resolve_mercedes_inventory_model(prefix: str, models: set[str]) -> str | None:
    """Map line prefix to the model name used in inventory."""
    base = _MB_PREFIX_TO_MODEL.get(prefix.upper())
    if not base:
        return None
    if not models or base in models:
        return base
    for alt in (f"{prefix.upper()}-Class", prefix.upper(), f"{prefix.upper()} Class"):
        if alt in models:
            return alt
    return base


def _extract_mercedes_benz_line(
    raw: str, models_by_make: dict[str, list[str]]
) -> tuple[str | None, str | None, str | None]:
    """
    Parse Mercedes shorthand (S580, E 350, GLC300) into make, model, trim digits.
    Returns (make, model, trim_contains) with unset fields as None.
    """
    m = _MB_LINE_RE.search(raw or "")
    if not m:
        return None, None, None
    prefix = m.group(1).upper()
    digits = m.group(2)
    mb_models = set(models_by_make.get("Mercedes-Benz", []))
    model = _resolve_mercedes_inventory_model(prefix, mb_models)
    if not model:
        return None, None, None
    return "Mercedes-Benz", model, digits


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
        if any(re.search(rf"(?<![a-z0-9]){re.escape(tok)}(?![a-z0-9])", tl) for tl in trim_lowers):
            return tok
    return None


_VEHICLE_SEGMENT_SPLIT_RE = re.compile(r"\s+(?:or|\|)\s+", re.I)


def _strip_vehicle_identity_noise(text: str) -> str:
    """Remove price/year/color/drivetrain tokens before make/model matching."""
    working = _apply_make_synonyms(text)
    working = _strip_price_and_year_segments(working)
    working = re.sub(
        r"\b(?:awd|4wd|4x4|fwd|rwd|xdrive|quattro|4matic)\b",
        " ",
        working,
        flags=re.I,
    )
    working = _CYLINDER_RE.sub(" ", working)
    working = _ENGINE_L_RE.sub(" ", working)
    for rx, _ in _FUEL_RE:
        working = rx.sub(" ", working)
    for rx, *_ in _BODY_STYLE_CUES:
        working = rx.sub(" ", working)
    for rx, _ in _FEATURE_MAP:
        working = rx.sub(" ", working)
    working = _FULLY_LOADED_RE.sub(" ", working)
    working = _CONTEXTUAL_INTERIOR_COLOR_RE.sub(" ", working)
    working = _CONTEXTUAL_EXTERIOR_COLOR_RE.sub(" ", working)
    for tok in _tokenize(working):
        if tok in _COLOR_HINTS:
            working = re.sub(rf"\b{re.escape(tok)}\b", " ", working, flags=re.I)
    return re.sub(r"\s+", " ", working).strip()


def _segment_looks_vehicle(segment: str, makes: list[str], all_models: list[str]) -> bool:
    seg = (segment or "").strip()
    if not seg:
        return False
    for mk in makes:
        if _make_appears_in_text(mk, seg):
            return True
    for md in all_models:
        if _model_appears_in_text(md, seg):
            return True
    return bool(re.search(r"\b(?:19|20)\d{2}\b", seg))


def _split_vehicle_segments(raw: str, makes: list[str], all_models: list[str]) -> list[str]:
    """Split a query into OR vehicle clauses (e.g. BMW X5 or Honda Accord)."""
    text = (raw or "").strip()
    if not text:
        return []
    parts = [p.strip() for p in _VEHICLE_SEGMENT_SPLIT_RE.split(text) if p.strip()]
    if len(parts) > 1:
        return parts
    if ";" in text:
        parts = [p.strip() for p in text.split(";") if p.strip()]
        if len(parts) > 1:
            return parts
    if "," in text:
        parts = [p.strip() for p in text.split(",") if p.strip()]
        if len(parts) >= 2 and all(_segment_looks_vehicle(p, makes, all_models) for p in parts):
            return parts
    return [text]


def _inherit_make_across_segments(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Carry make forward: 'BMW X5 or X3' → second segment inherits BMW."""
    last_make: str | None = None
    out: list[dict[str, Any]] = []
    for hit in hits:
        row = dict(hit)
        if row.get("make"):
            last_make = str(row["make"])
        elif last_make and (row.get("model") or row.get("trim_contains")):
            row["make"] = last_make
        out.append(row)
    return out


def _merge_vehicle_hits(hits: list[dict[str, Any]]) -> dict[str, Any]:
    """Combine per-segment make/model/trim into single- or multi-value filters."""
    hits = [h for h in hits if h.get("make") or h.get("model") or h.get("trim_contains")]
    if not hits:
        return {}
    if len(hits) == 1:
        return dict(hits[0])

    hits = _inherit_make_across_segments(hits)
    all_makes = list(dict.fromkeys(str(h["make"]) for h in hits if h.get("make")))
    all_models = list(dict.fromkeys(str(h["model"]) for h in hits if h.get("model")))
    all_trims = list(dict.fromkeys(str(h["trim_contains"]) for h in hits if h.get("trim_contains")))
    unique_pairs = list(
        dict.fromkeys((str(h["make"]), str(h["model"])) for h in hits if h.get("make") and h.get("model"))
    )

    out: dict[str, Any] = {}

    if len(unique_pairs) >= 2:
        out["vehicle_or"] = [{"make": mk, "model": md} for mk, md in unique_pairs]
        for h in hits:
            if h.get("make") and not h.get("model"):
                branch = {"make": str(h["make"])}
                if h.get("trim_contains"):
                    branch["trim_contains"] = str(h["trim_contains"])
                if branch not in out["vehicle_or"]:
                    out["vehicle_or"].append(branch)
    elif len(all_makes) == 1 and len(all_models) >= 2:
        out["make"] = all_makes[0]
        out["model"] = all_models
    elif len(all_makes) >= 2 and not all_models:
        out["make"] = all_makes
    elif len(all_models) >= 2 and not all_makes:
        out["model"] = all_models
    elif len(hits) >= 2:
        branches: list[dict[str, str]] = []
        for h in hits:
            branch: dict[str, str] = {}
            if h.get("make"):
                branch["make"] = str(h["make"])
            if h.get("model"):
                branch["model"] = str(h["model"])
            if h.get("trim_contains"):
                branch["trim_contains"] = str(h["trim_contains"])
            if branch and branch not in branches:
                branches.append(branch)
        if len(branches) >= 2:
            out["vehicle_or"] = branches
        elif len(branches) == 1:
            out.update(branches[0])
    else:
        out.update(hits[0])

    if all_trims:
        out["trim_contains"] = all_trims if len(all_trims) > 1 else all_trims[0]
    return out


def _parse_single_vehicle_identity(
    segment_raw: str,
    *,
    pairs: list[tuple[str, str]],
    makes: list[str],
    models_by_make: dict[str, list[str]],
    all_models: list[str],
    trims: list[str],
) -> dict[str, Any]:
    """Extract make/model/trim from one vehicle clause."""
    raw = (segment_raw or "").strip()
    if not raw:
        return {}

    working = _strip_vehicle_identity_noise(raw)
    tokens = _tokenize(working)
    out: dict[str, Any] = {}

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

    mb_make, mb_model, mb_trim = _extract_mercedes_benz_line(raw, models_by_make)
    if mb_make:
        out["make"] = mb_make
        if mb_model:
            out["model"] = mb_model
        if mb_trim:
            out["trim_contains"] = mb_trim
    elif best_make and _make_appears_in_text(best_make, raw):
        out["make"] = best_make
        if best_model and _model_appears_in_text(best_model, raw):
            out["model"] = best_model
    elif best_model and _model_appears_in_text(best_model, raw):
        out["model"] = best_model
        if best_make:
            out["make"] = best_make

    if not out.get("trim_contains"):
        trim_working = working
        if out.get("make"):
            trim_working = re.sub(rf"\b{re.escape(str(out['make']))}\b", " ", trim_working, flags=re.I)
        if out.get("model"):
            trim_working = re.sub(rf"\b{re.escape(str(out['model']))}\b", " ", trim_working, flags=re.I)
        trim_working = re.sub(r"\s+", " ", trim_working).strip()
        trim_hint = _extract_trim_hint(trim_working, trims, out.get("make"))
        if trim_hint:
            out["trim_contains"] = trim_hint
    return out


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

    pairs, ext_colors, int_colors, body_styles, trims = _load_inventory_keywords(_inventory_db_cache_key())
    package_names = _load_package_names(_inventory_db_cache_key())
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

    ec = _match_exterior_color(raw, ext_colors, interior_color=ic)
    if ec:
        out["exterior_color"] = [ec]

    if _FULLY_LOADED_RE.search(raw):
        out["fully_loaded"] = True

    segments = _split_vehicle_segments(raw, makes, all_models)
    vehicle_hits = [
        _parse_single_vehicle_identity(
            seg,
            pairs=pairs,
            makes=makes,
            models_by_make=models_by_make,
            all_models=all_models,
            trims=trims,
        )
        for seg in segments
    ]
    out.update(_merge_vehicle_hits(vehicle_hits))

    pkg_terms = _extract_all_package_search_terms(raw, package_names)
    if pkg_terms:
        out["packages_json_contains_list"] = pkg_terms

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
