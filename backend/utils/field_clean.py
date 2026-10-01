"""
Normalize scraped / legacy placeholder strings to None for SQLite + prompts.

Single source of truth for "empty" text so parsers, upserts, embeddings, and LLM
context do not propagate N/A poison.
"""
from __future__ import annotations

import json
import re
from typing import Any

from backend.vehicle_facts.drivetrain import drivetrain_storage_value

# Lowercased set of junk tokens → NULL in DB / omit from embeddings
# Dealer DMS / VDP boilerplate (same idea as ``car_serialize._MANUFACTURER_SPEC_RE``).
_MANUFACTURER_SPEC_JUNK_RE = re.compile(
    r"see\s+manufacturer|manufacturer\s+specifications|refer\s+to\s+manufacturer",
    re.IGNORECASE,
)

# Drivetrain vocabulary (schema.org URLs, abbreviations, EPA/vPIC long forms,
# 4x2/2WD) lives in ``backend.vehicle_facts.drivetrain`` -- the one normalizer.

# Buyer-facing fuel presets (facet order + storage normalization target).
FUEL_TYPE_PRESETS: tuple[str, ...] = (
    "Gasoline",
    "Hybrid",
    "Plug-In Hybrid",
    "Diesel",
    "Electric",
    "Hydrogen",
)

# Any free-text fuel_type value → canonical display name (exact match, lowercased key).
_CANONICAL_FUEL_TYPE: dict[str, str] = {
    "gasoline": "Gasoline",
    "gas": "Gasoline",
    "gasoline fuel": "Gasoline",
    "regular gasoline": "Gasoline",
    "premium gasoline": "Gasoline",
    "premium unleaded": "Gasoline",
    "regular unleaded": "Gasoline",
    "midgrade gasoline": "Gasoline",
    "midgrade unleaded": "Gasoline",
    "unleaded": "Gasoline",
    "regular unleaded fuel": "Gasoline",
    "premium unleaded fuel": "Gasoline",
    "regular gasoline / e85": "Gasoline",
    "flex fuel": "Gasoline",
    "flex fuel capability": "Gasoline",
    "flexible": "Gasoline",
    "flexfuel": "Gasoline",
    "flex-fuel": "Gasoline",
    "ffv": "Gasoline",
    "e85": "Gasoline",
    # feed abbreviations (7 dealers send 'G', 1,054 rows; 'UNL' 94; F13 2026-09-28)
    "g": "Gasoline",
    "unl": "Gasoline",
    "unleaded gas": "Gasoline",
    "unleaded gasoline": "Gasoline",
    "regular": "Gasoline",
    "premium": "Gasoline",
    "hybrid": "Hybrid",
    "h": "Hybrid",
    "hyb": "Hybrid",
    "hybr": "Hybrid",
    "hev": "Hybrid",
    "d": "Diesel",
    "e": "Electric",
    "ele": "Electric",
    "elec": "Electric",
    "electricity": "Electric",
    "hybrid fuel": "Hybrid",
    "gas / mild hybrid": "Hybrid",
    "gasoline/mild electric hybrid": "Hybrid",
    "gasoline / electric": "Hybrid",
    "full hybrid electric (fhev)": "Hybrid",
    "mild hybrid": "Hybrid",
    "plug-in hybrid": "Plug-In Hybrid",
    "plug in hybrid": "Plug-In Hybrid",
    "phev": "Plug-In Hybrid",
    "performance plug-in hybrid": "Plug-In Hybrid",
    "plug-in electric/gas": "Plug-In Hybrid",
    "electric": "Electric",
    "battery electric": "Electric",
    "bev": "Electric",
    "diesel": "Diesel",
    "diesel fuel": "Diesel",
    "hydrogen": "Hydrogen",
    "hydrogen fuel cell": "Hydrogen",
    "fcev": "Hydrogen",
}

# Buyer-facing body-style presets (facet order + storage normalization target).
BODY_STYLE_PRESETS: tuple[str, ...] = (
    "Sedan",
    "SUV",
    "Truck",
    "Coupe",
    "Convertible",
    "Hatchback",
    "Wagon",
    "Crossover",
    "Minivan",
    "Van",
    "Roadster",
)

_CANONICAL_BODY_STYLE: dict[str, str] = {
    "sedan": "Sedan",
    "4dr sedan": "Sedan",
    "4-door sedan": "Sedan",
    "coupe": "Coupe",
    "2dr coupe": "Coupe",
    "gran coupe": "Coupe",
    "convertible": "Convertible",
    "cabriolet": "Convertible",
    "hatchback": "Hatchback",
    "5dr hatchback": "Hatchback",
    "wagon": "Wagon",
    "sport wagon": "Wagon",
    "suv": "SUV",
    "sport utility": "SUV",
    "sport utility vehicle": "SUV",
    "sport utility vehicle (suv)/multi-purpose vehicle (mpv)": "SUV",
    "crossover": "Crossover",
    "crossover utility vehicle (cuv)": "Crossover",
    "cuv": "Crossover",
    "pickup": "Truck",
    "pickup truck": "Truck",
    "truck": "Truck",
    "crew cab": "Truck",
    "double cab": "Truck",
    "regular cab": "Truck",
    "supercrew": "Truck",
    "chassis cab": "Truck",
    "minivan": "Minivan",
    "van": "Van",
    "cargo van": "Van",
    "passenger van": "Van",
    "roadster": "Roadster",
}


_PLACEHOLDER_LOWER: frozenset[str] = frozenset(
    {
        "",
        "n/a",
        "na",
        "null",
        "none",
        "unknown",
        "undefined",
        "-",
        "—",
        "--",
        "---",
        "tbd",
        "not specified",
        "unspecified",
    }
)


def is_effectively_empty(val: Any) -> bool:
    """True for None, whitespace-only, or known placeholder strings."""
    if val is None:
        return True
    if isinstance(val, bool):
        # bool is a subclass of int; ``False`` must not look like a numeric 0
        return not val
    if isinstance(val, (int, float)):
        return False
    s = str(val).strip()
    if not s:
        return True
    return s.lower() in _PLACEHOLDER_LOWER


def is_spec_overlay_junk(val: Any) -> bool:
    """True for empty/placeholder strings or manufacturer-spec boilerplate (not a real listing value)."""
    if is_effectively_empty(val):
        return True
    s = str(val).strip()
    return bool(_MANUFACTURER_SPEC_JUNK_RE.search(s))


# Feed values that name no drivetrain / transmission / fuel at all. dealer.com
# and Dealer eProcess send "Other" for these; stored verbatim it survived the
# upsert's COALESCE and overwrote vPIC-healed drivetrains on every rescan
# (1,464 rows, F10 2026-09-28).
_SPEC_PLACEHOLDER_LOWER: frozenset[str] = frozenset(
    {"other", "others", "unspecified", "not specified", "not available", "tbd", "see dealer", "call",
     "n/a", "na", "unknown", "none", "null", "-", "--", "---", "\u2014", ""}
)


def is_spec_placeholder(val: Any) -> bool:
    """True when a drivetrain / transmission / fuel value is a feed placeholder
    ("Other", "Unspecified", "N/A"...) rather than a specification."""
    if is_effectively_empty(val):
        return True
    return str(val).strip().lower() in _SPEC_PLACEHOLDER_LOWER


def coerce_drivetrain_stored(val: Any) -> str | None:
    """
    The ``cars.drivetrain`` storage value for any drivetrain text: FWD / RWD /
    AWD / 4WD (``vehicle_facts.normalize_drivetrain``), the literal "2WD" for a
    two-wheel-drive-of-unknown-end claim (4x2 / 2WD: never FWD or RWD), None for
    empty / placeholder input, and the stripped text when nothing is recognised.
    """
    if is_spec_placeholder(val):
        return None
    return drivetrain_storage_value(val)


def _fuel_type_key(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


# Injection / delivery descriptors and "Others" name no fuel at all: store None
# and let the vPIC FuelTypePrimary + ElectrificationLevel heal fill it (F13).
_FUEL_TYPE_NOT_A_FUEL: frozenset[str] = frozenset(
    {"direct injection", "port/direct injection", "port injection", "sequential mpi", "mpi", "gdi",
     "other", "others", "n/a", "na", "unknown", "unspecified", "not specified", "none", "-", ""}
)


def coerce_fuel_type_stored(val: Any) -> str | None:
    """
    Map any fuel type text to one of the canonical values:
    Gasoline / Hybrid / Plug-In Hybrid / Diesel / Electric / Hydrogen.
    Returns None for empty / placeholder / not-a-fuel inputs ("Direct
    Injection", "Others"); unknown non-empty strings are returned as-is.
    """
    if is_effectively_empty(val):
        return None
    s = str(val).strip()
    k = _fuel_type_key(s)
    if k in _FUEL_TYPE_NOT_A_FUEL:
        return None
    if k in _CANONICAL_FUEL_TYPE:
        return _CANONICAL_FUEL_TYPE[k]
    if s in FUEL_TYPE_PRESETS:
        return s
    if "hydrogen" in k or "fuel cell" in k or "fcev" in k:
        return "Hydrogen"
    if ("plug" in k and "hybrid" in k) or "phev" in k or "plug-in electric" in k:
        return "Plug-In Hybrid"
    if "diesel" in k:
        return "Diesel"
    if "gasoline" in k and "electric" in k:
        if "plug" in k:
            return "Plug-In Hybrid"
        return "Hybrid"
    if "hybrid" in k or "fhev" in k or "hev" in k or "mild electric" in k:
        return "Hybrid"
    if (
        "electric" in k
        and "gasoline" not in k
        and "gas" not in k
        and "diesel" not in k
        and "hybrid" not in k
    ):
        return "Electric"
    if any(
        tok in k
        for tok in (
            "gasoline",
            "unleaded",
            "petrol",
            "flex fuel",
            "e85",
            "regular fuel",
            "premium fuel",
        )
    ):
        return "Gasoline"
    if re.search(r"\bgas\b", k) and "hybrid" not in k and "electric" not in k:
        return "Gasoline"
    return s


def coerce_body_style_stored(val: Any) -> str | None:
    """
    Map free-text body style to a buyer-facing preset (see ``BODY_STYLE_PRESETS``).
    Returns None for empty inputs; unknown non-empty strings are returned as-is.
    """
    if is_effectively_empty(val):
        return None
    s = str(val).strip()
    k = _fuel_type_key(s)
    if k in _CANONICAL_BODY_STYLE:
        return _CANONICAL_BODY_STYLE[k]
    if s in BODY_STYLE_PRESETS:
        return s
    if any(tok in k for tok in ("pickup", "crew cab", "double cab", "super cab", "chassis cab")):
        return "Truck"
    if "truck" in k:
        return "Truck"
    if any(
        tok in k
        for tok in (
            "sport utility",
            "suv",
            "multi-purpose",
            "mpv",
            "4wd",
            "4x4",
        )
    ) and "pickup" not in k:
        return "SUV"
    if "crossover" in k or "cuv" in k:
        return "Crossover"
    if "minivan" in k:
        return "Minivan"
    if re.search(r"\bvan\b", k) and "suv" not in k:
        return "Van"
    if "convertible" in k or "cabriolet" in k or "roadster" in k:
        if "roadster" in k:
            return "Roadster"
        return "Convertible"
    if "hatchback" in k or "hatch back" in k:
        return "Hatchback"
    if "wagon" in k:
        return "Wagon"
    if "coupe" in k or re.search(r"\b2\s*dr\b", k):
        return "Coupe"
    if "sedan" in k or re.search(r"\b4\s*dr\b", k):
        return "Sedan"
    return s


def sort_fuel_type_presets(values: set[str] | list[str]) -> list[str]:
    """Order facet fuel values using ``FUEL_TYPE_PRESETS`` then any extras."""
    seen: dict[str, str] = {}
    for v in values:
        c = coerce_fuel_type_stored(v)
        if c:
            seen[c.lower()] = c
    ordered = [p for p in FUEL_TYPE_PRESETS if p.lower() in seen]
    extras = sorted(v for k, v in seen.items() if v not in ordered)
    return ordered + extras


def sort_body_style_presets(values: set[str] | list[str]) -> list[str]:
    """Order facet body-style values using ``BODY_STYLE_PRESETS`` then any extras."""
    seen: dict[str, str] = {}
    for v in values:
        c = coerce_body_style_stored(v)
        if c:
            seen[c.lower()] = c
    ordered = [p for p in BODY_STYLE_PRESETS if p.lower() in seen]
    extras = sorted(v for k, v in seen.items() if v not in ordered)
    return ordered + extras


def body_styles_for_filter(canonical: str) -> list[str]:
    """Expand a canonical body-style filter to DB values that should match."""
    c = coerce_body_style_stored(canonical) or canonical
    out: list[str] = []
    for v in (c, canonical):
        if v and v not in out:
            out.append(v)
    _legacy_by_preset: dict[str, list[str]] = {
        "SUV": ["Sport Utility Vehicle", "Sport Utility", "4WD"],
        "Truck": ["Pickup", "Pickup Truck", "Crew Cab", "Double Cab", "Truck Double Cab"],
        "Sedan": ["4dr Sedan", "4-Door Sedan"],
        "Coupe": ["2dr Coupe", "Gran Coupe"],
        "Crossover": ["Crossover Utility Vehicle (CUV)", "CUV"],
    }
    for leg in _legacy_by_preset.get(c, []):
        if leg not in out:
            out.append(leg)
    return out


def fuel_types_for_filter(canonical: str) -> list[str]:
    """
    Expand a canonical fuel filter to DB values that should match (canonical + common legacy).
    """
    c = coerce_fuel_type_stored(canonical) or canonical
    out: list[str] = []
    for v in (c, canonical):
        if v and v not in out:
            out.append(v)
    _legacy_by_preset: dict[str, list[str]] = {
        "Gasoline": [
            "Gasoline Fuel",
            "Premium Unleaded",
            "Regular Unleaded",
            "Gas",
            "Gasoline/Mild Electric Hybrid",
        ],
        "Hybrid": [
            "Hybrid Fuel",
            "Full Hybrid Electric (FHEV)",
            "Gasoline / Electric",
        ],
        "Plug-In Hybrid": [
            "Performance Plug-In Hybrid",
            "Plug-In Electric/Gas",
        ],
        "Diesel": ["Diesel Fuel"],
    }
    for leg in _legacy_by_preset.get(c, []):
        if leg not in out:
            out.append(leg)
    return out


def is_jeep_wrangler_car(
    make: str | None,
    model: str | None,
    trim: str | None = None,
    title: str | None = None,
) -> bool:
    """True for Jeep Wrangler and Wrangler Unlimited (not Gladiator)."""
    mk = (make or "").strip().lower()
    mo = (model or "").strip().lower()
    blob = " ".join(
        x.strip().lower() for x in (model, trim, title) if x and str(x).strip()
    )

    if mk == "wrangler" and (not mo or mo == "wrangler"):
        return True

    if "wrangler" not in blob and mo != "wrangler" and not mo.startswith("wrangler "):
        return False

    if mk not in ("jeep", "wrangler", ""):
        return False

    if "gladiator" in blob and "wrangler" not in blob:
        return False

    return bool(
        mo == "wrangler"
        or mo.startswith("wrangler ")
        or re.search(r"\bwrangler\b", blob)
    )


def normalize_body_style_for_car(
    body_style: str | None,
    *,
    make: str | None = None,
    model: str | None = None,
    trim: str | None = None,
    title: str | None = None,
) -> str | None:
    """
    Buyer-facing body style corrections (e.g. Wrangler → SUV, not Convertible).
    """
    if is_jeep_wrangler_car(make, model, trim, title):
        return "SUV"
    if body_style is None or is_effectively_empty(body_style):
        return None
    return coerce_body_style_stored(body_style)


# Dealer DMS stock codes: 1-3 leading letters, a run of 3-6 digits, optional
# 1-2 letter suffix (e.g. R1111, FR040, T0386, UK0005, U0113A). Requiring a
# 3+ digit run keeps legit spec values ('4WD', '4x4', '6MT', 'PW7') unmatched.
_STOCK_CODE_SHAPE_RE = re.compile(r"^[A-Za-z]{1,3}\d{3,6}[A-Za-z]{0,2}$")

# Values that must NEVER be treated as stock codes even if a future shape
# change would match them (drivetrain abbreviations and axle configs).
_STOCK_CODE_EXEMPT_UPPER: frozenset[str] = frozenset(
    {"4WD", "AWD", "FWD", "RWD", "2WD", "4X4", "4X2", "6X2", "6X4", "6X6", "8X4"}
)

# cars.* spec fields a leaked stock code contaminates (PixelMotion-style bugs).
_STOCK_CODE_GUARDED_FIELDS: tuple[str, ...] = (
    "drivetrain",
    "transmission",
    "exterior_color",
    "interior_color",
    "trim",
)

# The identical-token recovery heuristic stays keyed to the four fields the
# PixelMotion leak actually stamps; ``trim`` is guarded for per-field rejection
# only (a code stamped into trim alone is rejected, not moved to stock_number).
_STOCK_CODE_IDENTICAL_HEAL_FIELDS: tuple[str, ...] = (
    "drivetrain",
    "transmission",
    "exterior_color",
    "interior_color",
)


def looks_like_stock_code(val: Any) -> bool:
    """
    True when ``val`` is a single stock-code-shaped token (letters+digits, no
    real word), e.g. ``R1111``, ``FR040``, ``T0386`` — never a valid
    drivetrain/transmission/color.

    Deliberately conservative: multi-word values (paint codes WITH color words
    like ``PW7 Bright White``), hyphenated values (``9-Speed``), and canonical
    drivetrain values (``4WD``, ``AWD``, ``4x4``, ``FWD``) are never matched.
    """
    if is_effectively_empty(val):
        return False
    s = str(val).strip()
    if re.search(r"[\s/\-]", s):
        return False
    if s.upper() in _STOCK_CODE_EXEMPT_UPPER:
        return False
    return bool(_STOCK_CODE_SHAPE_RE.match(s))


def _apply_stock_code_guard(out: dict[str, Any]) -> None:
    """
    Defense-in-depth for scraper leaks that stamp the vehicle's stock code
    into spec fields (see backend/scanner/scrapers/pixel_motion.py history).

    1. When all four guarded fields carry the IDENTICAL stock-code token,
       treat it as the stock number: clear the four fields and fill
       ``stock_number`` if it is empty.
    2. Any remaining stock-code-shaped value in a guarded field is rejected.
    """
    present = [k for k in _STOCK_CODE_GUARDED_FIELDS if k in out]
    vals = [out.get(k) for k in _STOCK_CODE_IDENTICAL_HEAL_FIELDS]
    tokens = [str(v).strip() for v in vals if isinstance(v, str) and str(v).strip()]
    if len(tokens) == len(_STOCK_CODE_IDENTICAL_HEAL_FIELDS) and len({t.upper() for t in tokens}) == 1:
        tok = tokens[0]
        if looks_like_stock_code(tok):
            for k in _STOCK_CODE_IDENTICAL_HEAL_FIELDS:
                out[k] = None
            if is_effectively_empty(out.get("stock_number")):
                out["stock_number"] = tok
            # No return: a stock code stamped into ``trim`` too is still
            # handled by the per-field rejection below.
    for k in present:
        v = out.get(k)
        if not looks_like_stock_code(v):
            continue
        if k == "trim":
            # Real trims can be stock-code-shaped (GT350, P100D, E350). Only
            # reject a trim the listing's own text does not name — same
            # corroboration rule as the vPIC trim guard.
            from backend.utils.spec_field_normalize import (
                _trim_norm_words,
                trim_named_in_listing,
            )

            # A trim equal to the row's own stock number IS the leaked code —
            # including the token the identical-field heal above just filed
            # there — and needs no listing-text corroboration to reject.
            stock = str(out.get("stock_number") or "").strip()
            if stock and str(v).strip().upper() == stock.upper():
                out[k] = None
                continue
            # Absence of corroboration is only meaningful when the text we
            # would search is actually loaded. Grid reads clean partial rows
            # (LISTINGS_GRID_CAR_COLUMNS carries neither description nor
            # model_full_raw); nulling there makes the card disagree with the
            # VDP about the same stored trim. A row missing BOTH text-heavy
            # columns abstains — full-row paths are where rejection happens.
            if "description" not in out and "model_full_raw" not in out:
                continue
            if trim_named_in_listing(out, v):
                continue
            # Mercedes-style grades are stock-code-shaped and often not
            # repeated in listing text ('E350' under an 'E-Class' title): a
            # trim whose leading alpha token echoes the model's own leading
            # token (E350/E-Class, GLC300/GLC, C300/C-Class) is the model's
            # grade, not a stock code.
            model_tok = (_trim_norm_words(out.get("model")).split() or [""])[0]
            trim_tok = (_trim_norm_words(v).split() or [""])[0]
            if model_tok and trim_tok and model_tok == trim_tok:
                continue
        out[k] = None


def normalize_optional_str(val: Any, *, max_len: int | None = None) -> str | None:
    """
    Return a clean string or None. Never returns placeholder tokens.
    """
    if is_effectively_empty(val):
        return None
    s = str(val).strip()
    if is_spec_overlay_junk(s):
        return None
    if max_len is not None and len(s) > max_len:
        s = s[:max_len]
    return s


def normalize_optional_url(val: Any) -> str | None:
    """HTTP(S) URLs and local /car-images/ paths; empty or placeholders → None."""
    u = normalize_optional_str(val)
    if not u:
        return None
    low = u.lower()
    if low.startswith("http://") or low.startswith("https://"):
        return u
    # Local images served by Flask /car-images/ route (image_downloader.py)
    if low.startswith("/car-images/"):
        return u
    return None


# Drivetrain descriptors some feeds mis-file into the trim field — never a grade.
_DRIVETRAIN_AS_TRIM = {
    "all-wheel drive", "awd", "front-wheel drive", "fwd",
    "rear-wheel drive", "rwd", "4wd", "2wd", "4x4",
}


def split_bz_woodland_model_trim(out: dict[str, Any]) -> None:
    """Recover the trim for Toyota's ``bZ Woodland`` (model absorbed the grade).

    Toyota's 2026 ``bZ`` (formerly bZ4X) has a rugged **Woodland** grade. Many
    dealer feeds pack the grade into the MODEL field ("bZ Woodland") and leave
    ``trim`` blank — or duplicate it ("bZ Woodland" / "bZ Woodland Premium") —
    which then flags the car incomplete for a missing trim. Split it back so
    ``model="bZ"`` and ``trim="Woodland"`` (plus any real sub-grade like
    "Premium"). Drivetrain tokens ("All-Wheel Drive") a feed mis-files into trim
    are dropped — they are not a grade. Mutates *out* in place.
    """
    if str(out.get("make") or "").strip().lower() != "toyota":
        return
    model = str(out.get("model") or "").strip()
    if not re.match(r"(?i)^bz\s+woodland\b", model):
        return
    out["model"] = "bZ"
    t = str(out.get("trim") or "").strip()
    # Strip a redundant model prefix the feed may have copied into the trim.
    t = re.sub(r"(?i)^bz(\s+woodland)?\s*", "", t).strip()
    if t.lower() in _DRIVETRAIN_AS_TRIM:
        t = ""
    out["trim"] = f"Woodland {t}".strip() if t else "Woodland"


# Raw ``condition`` spellings seen in cars (2026-09-28, F14): New, Used, Certified,
# Pre-Owned, Pre-owned, Certified Pre-Owned, CTP, CarBravo, Demo. Stored as-is,
# CPO filters and the certified / used split were unreliable (is_cpo NULL on
# 2,210 certified rows). Program labels are kept under ``_condition_program``.
_CONDITION_PROGRAMS: dict[str, tuple[str, str]] = {
    # raw (lower)          -> (canonical, program tag)
    "ctp": ("New", "CTP"),                    # GM Courtesy Transportation loaner, titled new
    "courtesy transportation": ("New", "CTP"),
    "demo": ("New", "Demo"),
    "demonstrator": ("New", "Demo"),
    "loaner": ("New", "Loaner"),
    "carbravo": ("Used", "CarBravo"),         # GM used-car program; certified only when the row says so
}


def canonicalize_condition(row: dict[str, Any]) -> None:
    """Canonicalise ``row['condition']`` to New / Used / Certified in place and
    set ``is_cpo`` from it (1 for Certified, 0 for New / Used when unset). A
    program spelling (CTP, Demo, CarBravo) is kept in ``_condition_program``."""
    raw = row.get("condition")
    if raw is None:
        return
    s = str(raw).strip()
    if not s:
        row["condition"] = None
        return
    low = s.lower()
    is_cpo_flag = row.get("is_cpo") in (1, True, "1")
    canonical: str | None = None
    program: str | None = None
    if low in _CONDITION_PROGRAMS:
        canonical, program = _CONDITION_PROGRAMS[low]
        if canonical == "Used" and is_cpo_flag:
            canonical = "Certified"
    elif "certif" in low or low == "cpo":
        canonical = "Certified"
    elif low == "new" or low.startswith("new "):
        canonical = "New"
    elif low in ("used", "pre-owned", "preowned", "pre owned") or low.startswith(("used ", "pre-owned ")):
        canonical = "Used"
    if canonical is None:
        return  # unknown spelling: leave it for the display-side normaliser
    if canonical == "Used" and is_cpo_flag:
        canonical = "Certified"
    row["condition"] = canonical
    if program:
        row["_condition_program"] = program
    if canonical == "Certified":
        row["is_cpo"] = 1
    elif row.get("is_cpo") is None:
        row["is_cpo"] = 0


def clean_car_row_dict(d: dict[str, Any]) -> dict[str, Any]:
    """
    Apply normalization to typical cars.* string columns in-place copy.
    Used before SQLite upsert and before semantic indexing.
    """
    out = dict(d)
    string_cols = (
        "trim",
        "zip_code",
        "transmission",
        "drivetrain",
        "exterior_color",
        "interior_color",
        "fuel_type",
        "dealer_url",
        "carfax_url",
        "title",
        "make",
        "model",
        "dealer_name",
        "stock_number",
        "dealer_id",
        "image_url",
        "source_url",
        "body_style",
        "engine_description",
        "transmission_type",
        "condition",
        "description",
        "model_full_raw",
    )
    for k in string_cols:
        if k not in out:
            continue
        if k in ("dealer_url", "carfax_url", "image_url", "source_url"):
            out[k] = normalize_optional_url(out.get(k))
        elif k == "drivetrain":
            out[k] = normalize_optional_str(coerce_drivetrain_stored(out.get(k)))
        elif k == "fuel_type":
            out[k] = normalize_optional_str(coerce_fuel_type_stored(out.get(k)))
        elif k == "transmission":
            out[k] = None if is_spec_placeholder(out.get(k)) else normalize_optional_str(out.get(k))
        elif k == "body_style":
            out[k] = normalize_optional_str(
                normalize_body_style_for_car(
                    out.get(k),
                    make=out.get("make"),
                    model=out.get("model"),
                    trim=out.get("trim"),
                    title=out.get("title"),
                )
            )
        else:
            out[k] = normalize_optional_str(out.get(k))

    # Split known compound models where the feed packed the grade into `model`.
    split_bz_woodland_model_trim(out)

    # Reject leaked stock codes in spec fields (and recover stock_number).
    _apply_stock_code_guard(out)

    # Some feeds store boolean-ish junk in ``condition`` (not a real listing value).
    _cv = out.get("condition")
    if _cv is not None and str(_cv).strip().lower() in ("0", "false", "no", "off"):
        out["condition"] = None
    canonicalize_condition(out)

    for num_key in ("mpg_city", "mpg_highway", "cylinders"):
        if num_key not in out:
            continue
        v = out.get(num_key)
        if v is None or v == "":
            out[num_key] = None
            continue
        try:
            n = int(float(str(v).replace(",", "").strip()))
        except (TypeError, ValueError):
            out[num_key] = None
            continue
        if num_key == "cylinders":
            # 0 is valid (battery-electric). Counts above 16 are feed sentinels
            # (GM feeds send 99 for EVs); no production engine has more than 16
            # cylinders, so they must never reach the column or a display.
            out[num_key] = n if 0 <= n <= 16 else None
        else:
            out[num_key] = n if n > 0 else None

    pkg = out.get("packages")
    if pkg is not None:
        if isinstance(pkg, dict):
            try:
                out["packages"] = json.dumps(pkg, ensure_ascii=False)
            except (TypeError, ValueError):
                out["packages"] = None
        elif isinstance(pkg, str):
            s = pkg.strip()
            if not s or s.lower() in ("{}", "[]", "null"):
                out["packages"] = None
            else:
                out["packages"] = s[:800000]
        else:
            out["packages"] = None
    return out


def display_str(val: Any, *, fallback: str = "unknown") -> str:
    """For LLM prompts: never show N/A; use fallback for missing."""
    if is_effectively_empty(val):
        return fallback
    return str(val).strip()


def build_compact_listing_document(car: dict[str, Any]) -> str:
    """
    Human-readable summary for embedding; skips null/junk fields.
    """
    c = clean_car_row_dict(car)
    parts: list[str] = []
    for label, key in (
        ("VIN", "vin"),
        ("Year", "year"),
        ("Make", "make"),
        ("Model", "model"),
        ("Trim", "trim"),
        ("Title", "title"),
        ("Dealer", "dealer_name"),
        ("ZIP", "zip_code"),
        ("Transmission", "transmission"),
        ("Drivetrain", "drivetrain"),
        ("Fuel", "fuel_type"),
        ("Exterior", "exterior_color"),
        ("Interior", "interior_color"),
        ("Body", "body_style"),
        ("Engine", "engine_description"),
        ("Condition", "condition"),
    ):
        v = c.get(key)
        if v is None or (isinstance(v, str) and not v.strip()):
            continue
        parts.append(f"{label}: {v}")
    price = c.get("price")
    if price is not None:
        try:
            p = float(price)
            if p > 0:
                parts.append(f"Price: {int(round(p))}")
        except (TypeError, ValueError):
            pass
    mil = c.get("mileage")
    if mil is not None:
        try:
            mi = int(mil)
            if mi > 0:
                parts.append(f"Mileage: {mi}")
        except (TypeError, ValueError):
            pass
    cyl = c.get("cylinders")
    if cyl is not None:
        try:
            if int(cyl) > 0:
                parts.append(f"Cylinders: {int(cyl)}")
        except (TypeError, ValueError):
            pass
    eng = c.get("engine_l")
    if eng is not None and str(eng).strip() and not is_effectively_empty(eng):
        parts.append(f"Engine L: {eng}")
    desc = c.get("description")
    if desc and len(str(desc)) > 20:
        parts.append(str(desc)[:500])
    pkg = c.get("packages")
    if pkg and str(pkg).strip() not in ("{}", "[]", "null"):
        try:
            if isinstance(pkg, str):
                pj = json.loads(pkg)
            else:
                pj = pkg
            if isinstance(pj, dict):
                obs = pj.get("observed_features") or []
                if isinstance(obs, list) and obs:
                    parts.append("Observed: " + "; ".join(str(x) for x in obs[:8]))
        except (json.JSONDecodeError, TypeError):
            pass
    text = " ".join(parts)
    return text[:8000] if text else "vehicle"


def format_mpg_city_highway_display(mpg_city: Any, mpg_highway: Any) -> str | None:
    """Build ``19 city / 26 highway MPG`` from DB integers when EPA aggregate string is absent."""
    try:
        c = int(mpg_city) if mpg_city is not None and str(mpg_city).strip() != "" else None
    except (TypeError, ValueError):
        c = None
    try:
        h = int(mpg_highway) if mpg_highway is not None and str(mpg_highway).strip() != "" else None
    except (TypeError, ValueError):
        h = None
    if c is not None and h is not None and c > 0 and h > 0:
        return f"{c} City / {h} Hwy"
    if c is not None and c > 0:
        return f"{c} City MPG"
    if h is not None and h > 0:
        return f"{h} Hwy MPG"
    return None


def compute_data_quality_score(car: dict[str, Any]) -> float:
    """
    0–100 heuristic: completeness + no junk strings + image + price.
    """
    c = clean_car_row_dict(car)
    pts = 0.0
    max_pts = 0.0

    def add(w: float, ok: bool) -> None:
        nonlocal pts, max_pts
        max_pts += w
        if ok:
            pts += w

    add(12, bool(c.get("vin")) and not str(c.get("vin", "")).lower().startswith("unknown"))
    add(10, bool(c.get("title")))
    add(8, c.get("year") is not None)
    add(8, bool(c.get("make")))
    add(8, bool(c.get("model")))
    add(6, bool(c.get("trim")))
    add(8, bool(c.get("price")) and float(c.get("price") or 0) > 0)
    add(5, c.get("mileage") is not None and int(c.get("mileage") or 0) >= 0)
    add(6, bool(c.get("transmission")))
    add(6, bool(c.get("drivetrain")))
    add(5, bool(c.get("fuel_type")))
    add(4, bool(c.get("exterior_color")))
    add(4, bool(c.get("interior_color")))
    add(5, bool(c.get("zip_code")))
    add(5, _has_real_image(c))
    add(4, bool(c.get("engine_l")) or c.get("cylinders") not in (None, 0))
    add(4, c.get("mpg_city") is not None and c.get("mpg_highway") is not None)
    if max_pts <= 0:
        return 0.0
    return round(100.0 * pts / max_pts, 2)


def _has_real_image(car: dict) -> bool:
    u = car.get("image_url")
    if u and str(u).strip().startswith("http"):
        return True
    g = car.get("gallery")
    if isinstance(g, list):
        return any(isinstance(x, str) and x.startswith("http") for x in g)
    if isinstance(g, str) and "http" in g:
        return True
    return False


_JUNK_URL = re.compile(r"^(n/?a|none|null|unknown|[-—]+)$", re.IGNORECASE)


def clean_url_for_db(val: Any) -> str | None:
    u = normalize_optional_str(val)
    if not u:
        return None
    if _JUNK_URL.match(u):
        return None
    return normalize_optional_url(u)
