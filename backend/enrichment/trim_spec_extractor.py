"""Build structured trim spec rows (label + value) from dictionary CSVs and ladder adds."""

from __future__ import annotations

import csv
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

from backend.enrichment.dictionary_catalog import find_complete_options_csv as _catalog_find_co
from backend.enrichment.dictionary_catalog import find_epa_csv as _catalog_find_epa

STANDARD_LABELS: tuple[str, ...] = (
    "Engine Options",
    "Maximum Towing Capacity",
    "Screen Size",
    "Interior Materials",
    "Audio System Layout",
    "Speaker Count & Breakdown",
    "Subwoofer",
    "Amplifier Wattage",
)

_LABEL_KEYWORDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Engine Options", ("engine", "v6", "v8", "v4", "turbo", "hemi", "ecoboost", "powerstroke", "pentastar", "motor", "hybrid", "ev", "kwh", "horsepower", "hp ", "liter", "displacement")),
    ("Maximum Towing Capacity", ("tow", "towing", "trailer", " payload", "lbs towing")),
    ("Screen Size", ("screen", "display", "uconnect", "sync", "infotainment", "touchscreen", " inch", "nav ", "navigation")),
    ("Interior Materials", ("leather", "interior", "seats", "cloth", "nappa", "wood", "dashboard", "suede", "trimmed", "upholstery", "headliner")),
    ("Audio System Layout", ("audio", "speaker", "harman", "kardon", "alpine", "bose", "beats", "sound system", "premium sound", "stereo")),
    ("Speaker Count & Breakdown", ("speaker", "tweeter", "woofer", "mid-range", "midrange")),
    ("Subwoofer", ("subwoofer", "sub-woofer", "sub woofer")),
    ("Amplifier Wattage", ("amplifier", " watt", "watts", "wattage", "channel dsp")),
    ("Exterior Styling", ("exterior styling", "fascia", "grille", "hood", "headlamp", "wheels", "srt-inspired", "body-color")),
)

_TRIM_CONTEXT_RE = re.compile(
    r"(?:standard|optional|available|exclusive)\s+(?:on|to|for|with)\s+(?:the\s+)?([A-Za-z0-9][\w\s\-/®™']{1,40}?)(?:\s+trim|\s+and|\s*,|\s+models|\s+model|\s+package|\.)",
    re.I,
)
_SCREEN_RE = re.compile(
    r"(\d+\.?\d*[-\s]?inch(?:\s+[\w\-]+){0,8}\s*(?:touch\s*screen|touchscreen|display|screen|cluster))",
    re.I,
)
_UCONNECT_SYNC_RE = re.compile(
    r"((?:Uconnect|Sync|SYNC|iDrive|MMI|MBUX|COMAND|Entune|MyLink|CUE|UVO)\s*[\w\s\-]*\d+\.?\d*(?:\s*inch)?[\w\s\-]*)",
    re.I,
)
_ENGINE_RE = re.compile(
    r"(\d+\.?\d*[-\s]?L(?:iter)?\s+(?:"
    r"Pentastar|HEMI|EcoBoost|PowerBoost|Power\s*Stroke|Coyote|Hurricane|TwinPower|"
    r"V6|V8|I4|I6|I3|turbo|supercharged|Hybrid|eTorque|"
    r"[\w\-]+"
    r")(?:\s*[\w\-/]+){0,6})",
    re.I,
)
_TOWING_RE = re.compile(
    r"((?:maximum\s+)?(?:trailer\s+)?tow(?:ing)?(?:\s+capacity)?[^.;]{0,40}?\d[\d,]*\s*lbs?(?:\s*\([^)]+\))?)",
    re.I,
)
_SPEAKER_LAYOUT_RE = re.compile(
    r"(\d+[-\s]speaker(?:\s+[\w\-/]+){0,12}(?:audio|system|premium|setup)?)",
    re.I,
)
_SPEAKER_DETAIL_RE = re.compile(
    r"((?:\d+\s+)?(?:instrument panel|front door|rear door|tweeter|woofer|mid[- ]range)[^.;]{10,220})",
    re.I,
)
_SUBWOOFER_RE = re.compile(
    r"((?:\d+\s+)?(?:rear\s+)?(?:enclosed\s+)?sub(?:-|\s)?woofer[^.;]{0,80})",
    re.I,
)
_AMP_RE = re.compile(
    r"(\d+[-\s]?watt(?:\s+[\w\-]+){0,20}(?:amplifier|amp|channel|dsp)[^.;]{0,60})",
    re.I,
)


def _norm_token(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())


def _norm_make(s: str) -> str:
    return (s or "").strip().lower()


def _norm_model(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())


def _filename_token(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", (s or "").strip()).strip("_")


_JUNK_VALUE_RE = re.compile(
    r"(show vte|road car timeline|wikipedia|click here|see also|previous —|next —|"
    r"product warnings for a wide variety|important product information|"
    r"everything you need to know|order guide pdf|dealer installed options \(dio\)|"
    r"it includes codes for|jump to latest|\d+k views|kelley blue|"
    r"ultimate driving machine|bmw fa option codes|forum|enjoy\])",
    re.I,
)
_INCLUDES_CODES_RE = re.compile(r"\bit includes codes for\b", re.I)
_PROSE_JUNK_RE = re.compile(
    r"\b(?:in addition(?: to)?|adds the following features|as well as safety features|"
    r"standard on .{0,100}?(?:and )?optional on|is the most luxurious|"
    r"which provides subscription-based|depends on the trim level selected|"
    r"available in its class|turns heads with)\b",
    re.I,
)
_OTHER_TRIM_RE = re.compile(
    r"\b(?:Limited|Laramie Longhorn|Laramie|Big Horn|Lone Star|Rebel|Tradesman|"
    r"Platinum|King Ranch|Denali|High Country|LTZ|RST|Z71)\b",
    re.I,
)
_MARKET_PROSE_RE = re.compile(
    r"\b(?:"
    r"australia|canada|europe|japan|new zealand|united kingdom|brazil|mexico|"
    r"right-hand drive|rhd\b|left-hand drive|lhd\b|"
    r"re-?introduced|discontinuation in|went on sale in|on sale in|sales began|"
    r"grey import|gray import|marketed in|assembled in|exported to|import presence|"
    r"outside of the (?:us|united states)|not available in the united states|"
    r"buyers in the u\.s\.|right-hand drive conversion|"
    r"model model years engine|engine production configuration"
    r")\b",
    re.I,
)
_TRIM_LEVEL_DESCRIPTION_RE = re.compile(
    r"\bis the (?:mid-level|entry-level|top|most luxurious|base|flagship)\b.*\btrim\b|"
    r"\btrim level is the most luxurious\b",
    re.I,
)


def _is_plausible_engine_field(text: str) -> bool:
    """True when a CSV engineOptions cell looks like a powertrain spec, not wiki market prose."""
    t = re.sub(r"\s+", " ", (text or "").strip())
    if not t or is_junk_spec_text(t):
        return False
    if _ENGINE_RE.search(t):
        return True
    if re.search(
        r"\b\d+\.?\d\s*L\b|\b(?:V6|V8|I4|I6|HEMI|Pentastar|Hurricane|EcoDiesel|turbo|supercharged|"
        r"automatic|manual|eTorque|hybrid|kWh)\b",
        t,
        re.I,
    ):
        return True
    return len(t) <= 48


def _is_cross_trim_prose(text: str, trim_name: str) -> bool:
    """True when scraped prose describes a different trim than *trim_name*."""
    if not text or not trim_name:
        return False
    if _sentence_applies_to_trim(text, trim_name):
        return False
    if _trim_in_text(trim_name, text):
        return False
    if _OTHER_TRIM_RE.search(text):
        return True
    std = re.search(r"\bstandard on\s+([^.;]{3,80})", text, re.I)
    if std and not _trim_in_text(trim_name, std.group(1)):
        return True
    opt = re.search(r"\boptional on\s+([^.;]{3,80})", text, re.I)
    if opt and not _trim_in_text(trim_name, opt.group(1)):
        return True
    return False


def _salvage_feature_list(text: str) -> str:
    """Pull the first concrete feature from wiki-style 'adds the following features…:' prose."""
    if ":" not in text:
        return ""
    tail = text.rsplit(":", 1)[-1].strip()
    if not tail or len(tail) < 8:
        return ""
    first = re.split(r",|\band\b", tail, maxsplit=1)[0].strip()
    if len(first) < 8 or _PROSE_JUNK_RE.search(first):
        return ""
    return first[:120]


def is_stale_listing_trim_prose(
    text: str,
    *,
    make: str = "",
    model: str = "",
    year: Any = None,
) -> bool:
    """Drop OEM prose that is wrong for the listing's model year (e.g. Uconnect 4C on 2025+ Ram)."""
    t = re.sub(r"\s+", " ", (text or "").strip())
    if not t:
        return False
    mk = _norm_make(make)
    mod = _norm_model(model)
    try:
        y = int(year)
    except (TypeError, ValueError):
        return False
    if mk == "ram" and "1500" in mod and y >= 2025 and re.search(r"\buconnect\s*4c?\b", t, re.I):
        return True
    return False


def compact_trim_bullet(
    text: str,
    *,
    trim_name: str = "",
    make: str = "",
    model: str = "",
    year: Any = None,
) -> str:
    """Return a short feature line suitable for the trim ladder UI, or empty if prose/junk."""
    t = re.sub(r"\s+", " ", (text or "").strip())
    if not t or is_junk_spec_text(t) or is_stale_listing_trim_prose(t, make=make, model=model, year=year):
        return ""
    if trim_name and _is_cross_trim_prose(t, trim_name):
        return ""
    if _PROSE_JUNK_RE.search(t):
        salvaged = _salvage_feature_list(t)
        if salvaged and not is_junk_spec_text(salvaged):
            return salvaged
        m = _SCREEN_RE.search(t) or _UCONNECT_SYNC_RE.search(t)
        if m:
            return _clean_text(m.group(1), limit=120)
        m = _ENGINE_RE.search(t)
        if m:
            return _clean_text(m.group(1), limit=120)
        return ""
    if len(t) <= 100:
        return _clean_text(t, limit=120)
    m = _SCREEN_RE.search(t) or _UCONNECT_SYNC_RE.search(t)
    if m:
        return _clean_text(m.group(1), limit=120)
    return ""


def is_junk_spec_text(text: str) -> bool:
    """True for placeholder CSV/wiki/forum scrapes that must never appear in the UI."""
    t = re.sub(r"\s+", " ", (text or "").strip())
    if not t or len(t) < 8:
        return True
    if re.fullmatch(r"automatic\s+\d{1,2}[- ]?spd\.?", t, re.I):
        return True
    if _INCLUDES_CODES_RE.search(t):
        return True
    if t.lower().count("it includes codes for") >= 1:
        return True
    if _JUNK_VALUE_RE.search(t):
        return True
    if _MARKET_PROSE_RE.search(t):
        return True
    if _TRIM_LEVEL_DESCRIPTION_RE.search(t):
        return True
    if re.search(r"\b(?:19|20)\d{2}\s+\d+\.?\d*\s*L\b", t):
        return True
    if re.search(r"lb⋅ft|N⋅m|kg⋅m|Years Engine Displacement|Torque Notes", t):
        return True
    if re.search(r"\bstandard was the\b|\bbeing optio\b|\bwith the 5\.2 L V8\b", t, re.I):
        return True
    if re.search(r"package groups with various|optional engine, available on all", t, re.I):
        return True
    if re.search(r",\s as w$|,\s aside from t$", t, re.I):
        return True
    if re.search(r"\bby 19\d{2},\s*the options list\b", t, re.I):
        return True
    if _PROSE_JUNK_RE.search(t):
        return True
    if re.search(r"\bstandard on\b.+\boptional on\b", t, re.I):
        return True
    if re.search(r"\bthe .+ system,\s*standard on\b", t, re.I):
        return True
    # Dense scraped prose with no real feature signal (keep long curated OEM bullets)
    if len(t) > 200 and not _classify_text(t) and ";" in t:
        return True
    return False


# --- fail-closed gate: a rung bullet is a QUOTED SPEC or it does not render ---
#
# Everything upstream of the trim ladder (Complete_Options CSVs, the derived
# trim_spec_sheets built from them, trim_ladders_generated.json) is scraped, and
# a large share of what it holds is encyclopedia narrative rather than spec text
# — "In the United States, the 4Runner carried over the same engine options from
# the". A shopper reads a rung bullet as a fact about the car in front of them,
# so anything that reads as prose, is cut off mid-sentence, or states a
# comparison whose baseline WE picked (the ``Upgrades X: Y (was Z)`` shape that
# ``trim_diff_engine`` computes by walking the ladder) must be dropped.
#
# These predicates only ever DELETE a bullet: the failure mode is a short rung,
# never a false claim.

# Curated OEM trim-walk lines really do run long — the WK2 Grand Cherokee
# Limited X styling bullet is 189 characters of pure equipment list. The cap is
# therefore set above that and prose is caught by the shape rules below, not by
# length alone.
_MAX_TRIM_BULLET_CHARS = 240

# Brochure-quoted engine step-ups carry the OEM's own baseline in a fixed
# suffix ("… — added over the GT"). Strip it before the prose tests so the one
# comparison the OEM printed itself is not mistaken for narrative.
_ADDED_OVER_SUFFIX_RE = re.compile(r"\s*[—–-]\s*added over the\s+\S[^—–]{0,40}$", re.I)

_DERIVED_COMPARISON_RE = re.compile(
    r"^(?:adds|upgrades)\s+[A-Za-z][A-Za-z0-9 &/]{2,40}:\s",
    re.I,
)
_DERIVED_BASELINE_RE = re.compile(r"\(\s*was\s", re.I)

_STANDARD_LABEL_PREFIX_RE = re.compile(
    r"^(?:%s)\s*:\s*" % "|".join(re.escape(lbl) for lbl in STANDARD_LABELS),
    re.I,
)

# A bullet that opens like a sentence is a sentence, not a spec line.
_NARRATIVE_LEAD_RE = re.compile(
    r"^(?:the|a|an|in|since|following|based|according|however|most|also|by|this|that|these|those|"
    r"it|its|there|while|although|during|after|before|both|when|european|american|additionally|"
    r"meanwhile|unlike|compared|originally|later|initially|subsequently)\s+",
    re.I,
)

_NARRATIVE_PHRASE_RE = re.compile(
    r"\bcarried over\b|\bin the united states\b|\baccording to\b|\bwent on sale\b|"
    r"\bcam(?:e|es) standard\b|\bcome standard\b|\bcompetes with\b|\bbecame (?:available|standard)\b|"
    r"\bwas (?:equipped|offered|introduced|launched|unveiled|added|replaced|discontinued|available|"
    r"reinstated|standard|hampered|modified|slightly|the|a|an)\b|"
    r"\bwere (?:equipped|offered|introduced|launched|available|standard|hampered|full-time|the|a|an)\b|"
    r"\bhas (?:noticed|been)\b|\bhave been\b|\bowner's manual\b|\bsummary of contents\b|"
    r"\bfor the \d{4} model\b|\bin order to\b|\bturbo tru\b",
    re.I,
)

# Legal / footnote text lifted out of brochures. It is quotable, but it is not a
# spec, and a rung bullet is read as a statement about the vehicle.
_BOILERPLATE_RE = re.compile(
    r"\bterms of use\b|\bprivacy (?:policy|notice|statement)\b|\bdata charges\b|"
    r"\bmaster data consent\b|\bdata collection\b|\bsubscription (?:required|service|term)\b|"
    r"\bsee (?:your )?(?:dealer|retailer) for (?:details|complete)\b|\bconsult your (?:owner|dealer)\b|"
    r"\bfor (?:more )?details,? (?:visit|see|go to)\b|\bnot available in all\b|"
    r"\balways (?:drive|pay attention|obey)\b|\bdriver'?s? responsib|\bcharges may apply\b",
    re.I,
)

# Cut-off scrapes: a dangling function word, a bare connector, or an ellipsis.
_TRUNCATED_TAIL_RE = re.compile(
    r"(?:…|\.\.\.|[,;:]|[-–—]"
    r"|\b(?:the|a|an|and|or|of|with|for|from|to|in|on|at|is|are|was|were|by|as|but|that|which|"
    r"while|when|its|it|their|this|these|than|per|into|over|under|plus)\b)\s*$",
    re.I,
)


# Short but well-formed spec values ("2.0L I4", "12-speaker").
_SHORT_SPEC_RE = re.compile(
    r"^(?:\d+\.?\d*\s?L\b|\d+(?:\.\d+)?[- ]?(?:inch|in\.)\b|\d+[- ]speaker\b|\d+\s?kWh\b)",
    re.I,
)

# --- three leaks measured against the live overlays in trim_adds_by_year ---
#
# Every rule below was written after finding bullets that ALREADY passed
# ``is_displayable_trim_bullet`` and would therefore have rendered on a car
# page: "(cid:2) All-weather floor mats (front)", "is a registered trademark of
# Harman International Industries, Inc. Quiet Steel", "river and front passenger
# lumbar control, memory package…" (a mid-word truncation of "driver"), and
# "3rd row) Vehicles shown may contain optional equipment. Features shown…".

# pdfplumber emits unmapped glyphs as (cid:NNN); a spec line never contains one.
_CID_ARTIFACT_RE = re.compile(r"\(cid:\d+\)")

# Copyright/trademark footers scraped out of a brochure's small print.
_LEGAL_FOOTER_RE = re.compile(
    r"\bregistered trademark\b|\bword mark\b|\ball rights reserved\b|"
    r"\bvehicles? shown\b|\bfeatures shown\b|\bsimulated screen\b",
    re.I,
)

# Two sentences in one bullet means prose was scraped, not a spec quoted. The
# period must be a real sentence end: "4.2-in. TFT Multi-Information Display"
# and "5570 lb. Gross Vehicle Weight Rating" are single spec lines whose period
# closes a unit abbreviation, so only words of three or more letters count.
_SENTENCE_BREAK_RE = re.compile(r"\b([A-Za-z]{3,})\.\s+[A-Z]")
_UNIT_ABBREVIATIONS = frozenset(
    {"approx", "est", "max", "min", "incl", "avg", "std", "opt", "qty", "etc", "mfg", "mpg", "cyl"}
)

# A spec line starts with a capital or a digit. A leading all-lowercase word is
# either a mid-word truncation ("river and front passenger…") or the tail of a
# sentence. OEM names that legitimately start lowercase carry an interior
# capital — iPod, iDrive, i-MID, xDrive, eTorque, i-Activ, i-FORCE.
_LOWERCASE_LEAD_RE = re.compile(r"^[a-z][a-z'’]*(?:\s|$)")


def _has_sentence_break(text: str) -> bool:
    for m in _SENTENCE_BREAK_RE.finditer(text):
        if m.group(1).lower() not in _UNIT_ABBREVIATIONS:
            return True
    return False


def is_derived_comparison_bullet(text: str) -> bool:
    """True for ``Adds/Upgrades <Label>: … (was …)`` lines.

    ``trim_diff_engine`` builds these by diffing a rung against the cumulative
    feature map of the rungs BELOW it. Both the direction and the baseline come
    from our own ordering of the ladder, not from anything the OEM printed, so
    the comparison is not quotable and must not reach a shopper.
    """
    t = re.sub(r"\s+", " ", (text or "").strip())
    if not t:
        return False
    return bool(_DERIVED_COMPARISON_RE.match(t) or _DERIVED_BASELINE_RE.search(t))


def is_narrative_prose(text: str) -> bool:
    """True when *text* reads as encyclopedia/marketing prose rather than a spec line."""
    t = re.sub(r"\s+", " ", (text or "").strip())
    if not t:
        return False
    core = _ADDED_OVER_SUFFIX_RE.sub("", t).strip()
    body = _STANDARD_LABEL_PREFIX_RE.sub("", core).strip()
    if not body:
        return True
    if " the " in body.lower():
        return True
    if _NARRATIVE_LEAD_RE.match(body):
        return True
    if _NARRATIVE_PHRASE_RE.search(body):
        return True
    if _BOILERPLATE_RE.search(body):
        return True
    if _TRUNCATED_TAIL_RE.search(body):
        return True
    if _CID_ARTIFACT_RE.search(body):
        return True
    if _LEGAL_FOOTER_RE.search(body):
        return True
    if _LOWERCASE_LEAD_RE.match(body):
        return True
    if _has_sentence_break(body):
        return True
    return False


def is_displayable_trim_bullet(text: str) -> bool:
    """Gate every trim-rung bullet: quotable spec text, or it does not render."""
    t = re.sub(r"\s+", " ", (text or "").strip())
    if not t:
        return False
    if len(t) > _MAX_TRIM_BULLET_CHARS:
        return False
    if is_derived_comparison_bullet(t):
        return False
    if is_narrative_prose(t):
        return False
    # Re-run the junk screen on the value alone: a "Engine Options: " prefix
    # otherwise hides placeholder values such as "Automatic 8-spd".
    body = _STANDARD_LABEL_PREFIX_RE.sub("", _ADDED_OVER_SUFFIX_RE.sub("", t).strip()).strip()
    if not body:
        return False
    if is_junk_spec_text(body):
        # ``is_junk_spec_text`` treats anything under 8 characters as junk, which
        # would also discard well-formed short specs like "2.0L I4".
        return bool(len(body) < 8 and _SHORT_SPEC_RE.match(body))
    return True


def _clean_text(s: str, *, limit: int = 280) -> str:
    t = re.sub(r"\s+", " ", (s or "").strip())
    if is_junk_spec_text(t):
        return ""
    if len(t) > limit:
        t = t[: limit - 1].rsplit(" ", 1)[0] + "…"
    return t


def split_spec_value_parts(value: str, *, trim_name: str = "") -> list[str]:
    """Split a spec value into short bullet lines, dropping junk fragments."""
    raw = re.sub(r"\s+", " ", (value or "").strip())
    if not raw:
        return []
    parts = re.split(r"\s*;\s*", raw)
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        compact = compact_trim_bullet(part.strip(), trim_name=trim_name)
        if not compact:
            p = _clean_text(part.strip(), limit=200)
            if not p or (trim_name and _is_cross_trim_prose(p, trim_name)):
                continue
            compact = p
        if not is_displayable_trim_bullet(compact):
            continue
        key = re.sub(r"[^a-z0-9]+", "", compact.lower())
        if len(key) < 8 or key in seen:
            continue
        prefix = key[:40]
        if any(existing.startswith(prefix) or prefix.startswith(existing[:40]) for existing in seen):
            continue
        seen.add(key)
        out.append(compact)
    return out


def sanitize_trim_specs(specs: list[dict[str, str]] | None) -> list[dict[str, str]]:
    """Drop junk rows/values from structured trim specs."""
    out: list[dict[str, str]] = []
    for row in specs or []:
        label = str(row.get("label") or "").strip()
        parts = split_spec_value_parts(str(row.get("value") or ""))
        if not label or not parts:
            continue
        out.append({"label": label, "value": "; ".join(parts)})
    return out


def trim_specs_to_bullets(
    specs: list[dict[str, str]] | None,
    *,
    trim_name: str = "",
) -> list[str]:
    """Convert structured specs to concise feature bullets (no label prefixes or prose)."""
    bullets: list[str] = []
    seen: set[str] = set()
    for row in sanitize_trim_specs(specs):
        for part in split_spec_value_parts(str(row.get("value") or ""), trim_name=trim_name):
            key = part.lower()
            if key in seen:
                continue
            seen.add(key)
            bullets.append(part)
    return bullets[:8]


def _classify_text(text: str) -> str | None:
    low = (text or "").lower()
    best_label: str | None = None
    best_score = 0
    for label, keywords in _LABEL_KEYWORDS:
        score = sum(1 for kw in keywords if kw in low)
        if score > best_score:
            best_score = score
            best_label = label
    return best_label if best_score > 0 else None


def _trim_tokens(trim_name: str) -> set[str]:
    raw = (trim_name or "").strip()
    tokens = {_norm_token(raw)}
    for part in re.split(r"[\s/\-]+", raw):
        if len(part) >= 2:
            tokens.add(_norm_token(part))
    return {t for t in tokens if t}


def _trim_in_text(trim_name: str, text: str) -> bool:
    if not trim_name or not text:
        return False
    tokens = _trim_tokens(trim_name)
    hay = _norm_token(text)
    return any(tok and tok in hay for tok in tokens)


def _sentence_applies_to_trim(sentence: str, trim_name: str) -> bool:
    if not sentence or not trim_name:
        return False
    s = sentence.strip()
    if not _trim_in_text(trim_name, s):
        return False
    low = s.lower()
    trim_low = trim_name.lower()
    if re.search(rf"\b(?:standard|optional|available|exclusive)\s+(?:on|to|for|with)\s+(?:the\s+)?{re.escape(trim_low)}\b", low):
        return True
    if re.search(rf"\b{re.escape(trim_low)}\b(?:\s+trim|\s+is the|\s+features|\s+includes|\s+adds)", low):
        return True
    if re.search(rf"\bthe\s+{re.escape(trim_low)}\b", low):
        return True
    return False


def _find_epa_csv(make: str, model: str, year: Any) -> Path | None:
    """Best-match EPA CSV via dictionary catalog."""
    return _catalog_find_epa(make, model, year)


def _extract_epa_trim_fields(
    rows: list[dict[str, str]],
    trim_name: str,
    make: str,
    model: str,
) -> dict[str, list[str]]:
    """Engine text from EPA dictionary rows (no wiki packages).

    ``engineOptions`` is the descriptive string that came in with the row;
    ``engineDisplay`` is a short label OUR importer synthesized from displacement
    plus cylinder count, and it guesses the bank layout — the 2026 Ram 2500
    Laramie row pairs ``engineOptions="6.7L Cummins Turbo Diesel I6"`` with
    ``engineDisplay="6.7L V6 Turbo"``. Quote the source field, never our guess.

    ``transmissionOptions`` / ``drivetrainOptions`` are deliberately NOT emitted:
    they describe the one EPA configuration this row happens to cover, so
    printing "Rear-Wheel Drive" as something a rung adds is a claim the row does
    not support.
    """
    from backend.enrichment.trim_ladder import _normalize_epa_trim

    out: dict[str, list[str]] = {label: [] for label in STANDARD_LABELS}
    trim_key = _norm_token(trim_name)
    for row in rows:
        trim_raw = (row.get("Trim") or "").strip()
        if not trim_raw:
            continue
        row_make = (row.get("Make") or make or "").strip()
        if _norm_make(row_make) != _norm_make(make):
            continue
        name = _normalize_epa_trim(trim_raw, make, model) or trim_raw
        if _norm_token(name) != trim_key and not _trim_in_text(trim_name, trim_raw):
            continue
        for key in ("engineOptions", "engineDisplay"):
            engine = (row.get(key) or "").strip()
            if not engine or not _is_plausible_engine_field(engine):
                continue
            cleaned = _clean_text(engine)
            if cleaned and is_displayable_trim_bullet(cleaned):
                out["Engine Options"].append(cleaned)
                break
    return out


def _find_complete_options_csv(make: str, model: str, year: Any) -> Path | None:
    return _catalog_find_co(make, model, year)


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    try:
        with path.open(encoding="utf-8", newline="") as fh:
            return list(csv.DictReader(fh))
    except OSError:
        return []


def _csv_blob(rows: list[dict[str, str]]) -> str:
    parts: list[str] = []
    for row in rows:
        for key in (
            "Trim",
            "engineOptions",
            "transmissionOptions",
            "Packages",
            "packageDetails",
            "Options",
            "optionDetails",
        ):
            val = (row.get(key) or "").strip()
            if val:
                parts.append(val)
    return "\n".join(parts)


def _extract_trim_from_cell(trim_raw: str, make: str, model: str) -> str | None:
    from backend.enrichment.trim_ladder_knowledge import canonical_trim_name, preserve_trim_label

    t = (trim_raw or "").strip()
    if not t:
        return None
    for prefix in (f"{make} {model}", model, make):
        p = (prefix or "").strip()
        if p and t.lower().startswith(p.lower()):
            t = t[len(p) :].strip(" -–—")
    preserved = preserve_trim_label(t, make, model)
    if preserved:
        return preserved
    return canonical_trim_name(t, make, model)


def _sentence_around(blob: str, start: int, end: int) -> str:
    """Return the sentence in *blob* containing the span [start:end]."""
    left = blob.rfind(".", 0, start)
    left = blob.rfind(";", 0, start) if left < 0 else max(left, blob.rfind(";", 0, start))
    seg_start = left + 1 if left >= 0 else 0
    right = blob.find(".", end)
    right_semi = blob.find(";", end)
    if right < 0 or (0 <= right_semi < right):
        right = right_semi
    seg_end = right if right >= 0 else len(blob)
    return blob[seg_start:seg_end].strip()


def _extract_trim_row_fields(rows: list[dict[str, str]], trim_name: str, make: str, model: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {label: [] for label in STANDARD_LABELS}
    trim_key = _norm_token(trim_name)

    for row in rows:
        trim_raw = (row.get("Trim") or "").strip()
        if not trim_raw:
            continue
        extracted = _extract_trim_from_cell(trim_raw, make, model)
        if extracted and _norm_token(extracted) != trim_key and not _trim_in_text(trim_name, trim_raw):
            continue
        if extracted and _norm_token(extracted) == trim_key:
            pass
        elif not _trim_in_text(trim_name, trim_raw):
            continue

        engine = (row.get("engineOptions") or "").strip()
        if engine and _is_plausible_engine_field(engine):
            cleaned = compact_trim_bullet(engine, trim_name=trim_name) or _clean_text(engine)
            if cleaned:
                out["Engine Options"].append(cleaned)

        for key in ("Packages", "packageDetails", "Options", "optionDetails"):
            val = (row.get(key) or "").strip()
            if not val or len(val) < 12:
                continue
            cleaned = compact_trim_bullet(val, trim_name=trim_name)
            if cleaned:
                label = _classify_text(cleaned) or _classify_text(val)
                if label and label in out:
                    out[label].append(cleaned)

    return out


def _extract_from_blob(blob: str, trim_name: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {label: [] for label in STANDARD_LABELS}
    if not blob:
        return out

    sentences = re.split(r"(?<=[.;])\s+", blob)
    for sentence in sentences:
        if len(sentence) < 16:
            continue
        if not _sentence_applies_to_trim(sentence, trim_name):
            continue
        cleaned = compact_trim_bullet(sentence, trim_name=trim_name)
        if not cleaned:
            continue
        label = _classify_text(cleaned) or _classify_text(sentence)
        if label and label in out:
            out[label].append(cleaned)

    def _match_applies(m: re.Match[str]) -> bool:
        sentence = _sentence_around(blob, m.start(), m.end())
        return bool(sentence) and _sentence_applies_to_trim(sentence, trim_name)

    for m in _SCREEN_RE.finditer(blob):
        if not _match_applies(m):
            continue
        out["Screen Size"].append(_clean_text(m.group(1), limit=120))

    for m in _UCONNECT_SYNC_RE.finditer(blob):
        if not _match_applies(m):
            continue
        out["Screen Size"].append(_clean_text(m.group(1), limit=120))

    for m in _ENGINE_RE.finditer(blob):
        if not _match_applies(m):
            continue
        out["Engine Options"].append(_clean_text(m.group(1), limit=120))

    for m in _TOWING_RE.finditer(blob):
        if not _match_applies(m):
            continue
        out["Maximum Towing Capacity"].append(_clean_text(m.group(1), limit=120))

    for m in _SPEAKER_LAYOUT_RE.finditer(blob):
        if not _match_applies(m):
            continue
        out["Audio System Layout"].append(_clean_text(m.group(1), limit=120))

    for m in _SPEAKER_DETAIL_RE.finditer(blob):
        if not _match_applies(m):
            continue
        out["Speaker Count & Breakdown"].append(_clean_text(m.group(1), limit=120))

    for m in _SUBWOOFER_RE.finditer(blob):
        if not _match_applies(m):
            continue
        out["Subwoofer"].append(_clean_text(m.group(1), limit=120))

    for m in _AMP_RE.finditer(blob):
        if not _match_applies(m):
            continue
        out["Amplifier Wattage"].append(_clean_text(m.group(1), limit=120))

    for match in _TRIM_CONTEXT_RE.finditer(blob):
        mentioned = (match.group(1) or "").strip()
        if not _trim_in_text(trim_name, mentioned):
            continue
        start = max(0, match.start() - 20)
        end = min(len(blob), match.end() + 220)
        sentence = blob[start:end]
        if not _sentence_applies_to_trim(sentence, trim_name):
            continue
        cleaned = compact_trim_bullet(sentence, trim_name=trim_name)
        if not cleaned:
            continue
        label = _classify_text(cleaned) or _classify_text(sentence)
        if label and label in out:
            out[label].append(cleaned)

    return out


def _merge_values(values: list[str], *, max_items: int = 2) -> str:
    seen: set[str] = set()
    ordered: list[str] = []
    for val in values:
        v = _clean_text(val)
        key = v.lower()
        if not v or key in seen:
            continue
        if not is_displayable_trim_bullet(v):
            continue
        seen.add(key)
        ordered.append(v)
    return "; ".join(ordered[:max_items])


def _specs_from_adds(adds: list[str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {label: [] for label in STANDARD_LABELS}
    for item in adds or []:
        text = re.sub(r"\s+", " ", str(item or "").strip())
        if not text or len(text) < 8:
            continue
        # A ladder add already carrying one of OUR labels is a previous run's
        # output being re-ingested; re-classifying it re-launders whatever junk
        # was in it (e.g. "Engine Options: Automatic 8-spd; Rear-Wheel Drive").
        if _STANDARD_LABEL_PREFIX_RE.match(text) or is_derived_comparison_bullet(text):
            continue
        if not is_displayable_trim_bullet(text):
            continue
        label = _classify_text(text)
        if label and label in out:
            out[label].append(_clean_text(text))
    return out


def _is_truckish(make: str, model: str) -> bool:
    mk = _norm_make(make)
    mod = _norm_model(model)
    if mk in {"ram", "ford", "chevrolet", "gmc"}:
        if any(tok in mod for tok in ("1500", "2500", "3500", "f150", "f250", "f350", "silverado", "sierra", "tundra", "titan", "frontier", "ranger", "colorado", "canyon", "maverick", "ridgeline", "gladiator")):
            return True
    if mk == "jeep" and "gladiator" in mod:
        return True
    if mk == "toyota" and "tacoma" in mod:
        return True
    return False


def _finalize_specs(buckets: dict[str, list[str]], *, make: str, model: str) -> list[dict[str, str]]:
    specs: list[dict[str, str]] = []
    for label in STANDARD_LABELS:
        if label == "Maximum Towing Capacity" and not _is_truckish(make, model):
            continue
        val = _merge_values(buckets.get(label) or [])
        if val:
            specs.append({"label": label, "value": val})
    return specs


@lru_cache(maxsize=512)
def extract_trim_specs(
    trim_name: str,
    *,
    make: str,
    model: str,
    year: Any = None,
    adds_key: str = "",
) -> tuple[dict[str, str], ...]:
    """Return structured specs for one trim rung (cached by inputs)."""
    adds = [a for a in (adds_key or "").split("\n") if a.strip()] if adds_key else []
    buckets: dict[str, list[str]] = {label: [] for label in STANDARD_LABELS}

    for label, values in _specs_from_adds(adds).items():
        buckets[label].extend(values)

    epa_path = _find_epa_csv(make, model, year)
    if epa_path:
        epa_rows = _load_csv_rows(epa_path)
        for label, values in _extract_epa_trim_fields(epa_rows, trim_name, make, model).items():
            if values:
                buckets[label].extend(values)

    csv_path = _find_complete_options_csv(make, model, year)
    if csv_path:
        rows = _load_csv_rows(csv_path)
        blob = _csv_blob(rows)
        row_fields = _extract_trim_row_fields(rows, trim_name, make, model)
        blob_fields = _extract_from_blob(blob, trim_name)

        for label in STANDARD_LABELS:
            if buckets[label]:
                continue
            buckets[label].extend(row_fields.get(label) or [])

        for label in STANDARD_LABELS:
            if buckets[label]:
                continue
            buckets[label].extend(blob_fields.get(label) or [])

        # Never scrape Packages/Options prose from Complete_Options — EPA + curated only.
        for label in ("Screen Size", "Maximum Towing Capacity", "Subwoofer", "Amplifier Wattage"):
            if len(buckets[label]) >= 1:
                continue
            extra = blob_fields.get(label) or row_fields.get(label) or []
            buckets[label].extend(extra)

    return tuple(_finalize_specs(buckets, make=make, model=model))


def build_spec_sheet(ladder: dict[str, Any]) -> dict[str, Any] | None:
    """Build a trim spec sheet JSON object for a ladder definition."""
    steps = ladder.get("steps") or []
    if len(steps) < 2:
        return None

    make = str(ladder.get("make") or "").strip()
    models = [str(m).strip() for m in (ladder.get("models") or []) if str(m).strip()]
    model = models[0] if models else ""
    if not make or not model:
        return None

    year = ladder.get("year_min") or ladder.get("year_max")
    trims: dict[str, list[dict[str, str]]] = {}
    for step in steps:
        name = str(step.get("name") or "").strip()
        if not name:
            continue
        adds = [str(a).strip() for a in (step.get("adds") or []) if str(a).strip()]
        adds_key = "\n".join(adds)
        specs = list(
            extract_trim_specs(
                name,
                make=make,
                model=model,
                year=year,
                adds_key=adds_key,
            )
        )
        if specs:
            trims[name] = specs

    if len(trims) < 2:
        return None

    return {
        "ladder_id": ladder.get("id"),
        "make": make,
        "models": models or [model],
        "year_min": int(ladder.get("year_min") or 0),
        "year_max": int(ladder.get("year_max") or 9999),
        "source": "generated",
        "trims": trims,
    }
