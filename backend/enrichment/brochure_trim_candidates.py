"""Draft trim overlays and ladder steps from ``derived/brochure_text`` JSON."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

from backend.enrichment.dictionary_catalog import catalog_key
from backend.enrichment.dictionary_paths import BROCHURE_TEXT_DIR, BROCHURE_TRIM_CANDIDATES_DIR
from backend.enrichment.trim_ladder import _is_valid_trim_name
from backend.enrichment.trim_ladder_knowledge import extract_trims_from_text, merge_trim_names

_MODELS_HEADING = re.compile(
    r"\b([A-Z][A-Z0-9®™'\-/&]{1,24}(?:\s+[A-Z][A-Z0-9®™'\-/&]{1,24}){0,4})\s+MODELS\b",
    re.I,
)
_MODELS_LINE = re.compile(r"^\s*MODELS\s*$", re.I)
_SPECS_HEADER = re.compile(
    r"\bSPECIFICATIONS\b|\bMechanical/Performance\b",
    re.I,
)
_TRIM_TOKEN = re.compile(r"^[A-Z][A-Z0-9®™'\-]{1,20}$")
_JUNK_TRIM = re.compile(
    r"predecessor|wikipedia|wheelbase|sourced from|unveiled|launched in|"
    r"disclosure|page \d+$|mechanical/performance",
    re.I,
)


def brochure_text_path_for_key(catalog_key_str: str) -> Path:
    return BROCHURE_TEXT_DIR / f"{catalog_key_str.replace('|', '__')}.json"


def candidate_path_for_key(catalog_key_str: str) -> Path:
    return BROCHURE_TRIM_CANDIDATES_DIR / f"{catalog_key_str.replace('|', '__')}.json"


def load_brochure_text_json(catalog_key_str: str) -> dict[str, Any] | None:
    path = brochure_text_path_for_key(catalog_key_str)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def brochure_status_from_payload(data: dict[str, Any]) -> str:
    warnings = data.get("warnings") or []
    if "no_text_extracted" in warnings:
        return "no_text"
    pages = data.get("pages") or []
    if not pages:
        return "empty"
    if "no_trim_hint_pages_used_all_text_pages" in warnings:
        return "weak_hints"
    if data.get("trim_hint_pages"):
        return "ok"
    return "ok"


def _clean_trim_token(tok: str) -> str:
    t = tok.strip().strip("®™")
    if not t or _JUNK_TRIM.search(t):
        return ""
    if len(t) > 28:
        return ""
    return t


def _tokens_from_models_line(line: str) -> list[str]:
    line = re.sub(r"\bMODELS\b", "", line, flags=re.I).strip()
    if not line:
        return []
    parts = re.split(r"\s{2,}|\s+", line)
    out: list[str] = []
    for p in parts:
        p = _clean_trim_token(p)
        if not p:
            continue
        if _TRIM_TOKEN.match(p) or (len(p) <= 24 and p[0].isupper()):
            out.append(p)
    return out


def filter_spurious_brochure_trims(
    trims: list[str],
    *,
    make: str,
    model: str,
) -> list[str]:
    """Drop cross-brand tokens (e.g. Limited/Premium on Honda EX/LX ladders)."""
    mk = (make or "").strip().lower()
    mod = re.sub(r"[^a-z0-9]+", "", (model or "").lower())
    if mk == "jeep" and "renegade" in mod:
        jeep_renegade = re.compile(
            r"^(?:Trailhawk|Limited|Latitude|Sport|Altitude|Upland|"
            r"High Altitude|80th Anniversary|75th Anniversary)$",
            re.I,
        )
        kept = [t for t in trims if jeep_renegade.match((t or "").strip())]
        if len(kept) >= 2:
            return kept
    if mk not in {"honda", "acura"}:
        return trims
    honda_core = re.compile(
        r"^(?:LX|LX-P|LX-S|EX|EX-L|SE|Si|Sport|Touring|Type\s*R|"
        r"DX|VP|Hybrid|Plug-?In|Touring|Elite|Technology|A-Spec|"
        r"SH-AWD|Advance|Type\s*S)$",
        re.I,
    )
    has_core = any(honda_core.match(t.strip()) for t in trims)
    if not has_core:
        return trims
    drop = {"limited", "premium", "platinum", "touring", "base", "standard"}
    return [t for t in trims if t.strip().lower() not in drop]


def parse_trim_names_from_text(text: str, *, make: str, model: str) -> list[str]:
    """Extract marketing trim names from brochure comparison/spec pages."""
    if not text or not text.strip():
        return []

    names: list[str] = []
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    for i, line in enumerate(lines):
        m = _MODELS_HEADING.search(line)
        if m:
            names.extend(_tokens_from_models_line(m.group(1)))
            continue
        if _MODELS_LINE.match(line):
            for j in range(i + 1, min(i + 4, len(lines))):
                names.extend(_tokens_from_models_line(lines[j]))
            continue
        if _SPECS_HEADER.search(line) and i + 1 < len(lines):
            header = lines[i + 1]
            if "Mechanical" not in header and len(header) < 120:
                names.extend(_tokens_from_models_line(header))

    knowledge = extract_trims_from_text(make, text, model=model, limit=16)
    merged = merge_trim_names(names, knowledge, make=make, model=model, limit=12)
    out: list[str] = []
    seen: set[str] = set()
    for n in merged:
        if not _is_valid_trim_name(n, make=make, model=model):
            continue
        key = n.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(n)
    return filter_spurious_brochure_trims(out, make=make, model=model)


def build_trim_candidate(
    data: dict[str, Any],
    *,
    make: str,
    model: str,
    year: int,
) -> dict[str, Any]:
    """Draft a trim candidate for one brochure.

    ``adds_by_trim`` comes from :func:`extract_trim_walk`, which only quotes
    lines it can attribute to a named trim on a named page. The previous drafter
    (``_draft_adds_from_text``) set a "current trim" on any line mentioning a
    trim and then appended every following line of 20-200 characters, which is
    how bullets like ``"(cid:2) All-weather floor mats (front)"``, ``"is a
    registered trademark of Harman International Industries, Inc. Quiet Steel"``
    and ``"river and front passenger, d"`` reached
    ``derived/trim_adds_by_year``. It has been removed rather than tuned: a page
    whose layout is unreadable now yields nothing.
    """
    text = str(data.get("combined_trim_pages_text") or "")
    if not text:
        for pg in data.get("pages") or []:
            if isinstance(pg, dict):
                text += "\n" + str(pg.get("text") or "")

    trims = parse_trim_names_from_text(text, make=make, model=model)
    walk = extract_trim_walk(data, make=make, model=model, year=year)
    adds = dict(walk.adds_by_trim)
    for name in walk.trims_available:
        if name not in trims:
            trims.append(name)

    quality = "high" if len(trims) >= 3 and any(adds.values()) else "medium" if len(trims) >= 2 else "low"
    if brochure_status_from_payload(data) == "no_text":
        quality = "none"

    return {
        "catalog_key": catalog_key(year, make, model),
        "year": year,
        "make": make,
        "model": model,
        "status": "draft",
        "quality": quality,
        "trims_available": trims,
        "adds_by_trim": adds,
        "adds_provenance": walk.provenance,
        "adds_layouts": walk.layouts,
        "brochure_status": brochure_status_from_payload(data),
        "source": "brochure_text_quoted",
        "warnings": list(data.get("warnings") or []),
    }


def persist_trim_candidate(payload: dict[str, Any]) -> Path:
    BROCHURE_TRIM_CANDIDATES_DIR.mkdir(parents=True, exist_ok=True)
    ck = str(payload.get("catalog_key") or "")
    out = candidate_path_for_key(ck)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out


# ---------------------------------------------------------------------------
# Quoted trim-walk extraction from derived/brochure_text JSON
# ---------------------------------------------------------------------------
#
# The PDFs these JSON files were rendered from are no longer on disk, so every
# claim below has to be traceable to a page of ``derived/brochure_text/<key>.json``.
# Each emitted bullet therefore carries the brochure_text file name and the
# 1-based PDF page it was quoted from; a bullet with no page is never emitted.
#
# Two layouts are supported, both of which state the trim walk in the OEM's own
# words:
#
#   ``adds_to_block``  — a page headed "<TRIM> / Adds to <LOWER TRIM>" followed by
#                        a SINGLE-COLUMN bullet list (FCA buyer's-guide style).
#   ``marker_grid``    — an equipment grid whose header row is a run of trim names
#                        and whose rows end in one marker per column.
#
# Multi-column bullet pages (Toyota's "<TRIM> / Adds to or replaces features
# offered on <LOWER>" comparison spreads, Nissan's "INCLUDES <LOWER> EQUIPMENT
# PLUS" spreads) are rejected on purpose — see ``_bullet_columns_are_ambiguous``.

_BULLET_GLYPHS = "•●▪▸‣⁃◦"
_BULLET_GLYPH_RE = re.compile(f"[{_BULLET_GLYPHS}]")
_BULLET_LINE_RE = re.compile(f"^\\s*[{_BULLET_GLYPHS}]\\s*(.+)$")

# Grid markers whose meaning is unambiguous across OEM legends.
_GRID_STANDARD_GLYPHS = "●•▪◆✓"        # ● • ▪ ◆ ✓
_GRID_NEGATIVE_GLYPHS = "○◇—–−-"        # ○ ◇ — – − -
_GRID_LETTER_MARKS = {"S": "std", "O": "neg", "P": "neg", "N": "neg", "A": "neg"}
_GRID_MARK_RE = re.compile(
    "(?:[%s%s]|(?<![A-Za-z0-9])[SOPNA](?![A-Za-z0-9]))"
    % (re.escape(_GRID_STANDARD_GLYPHS), re.escape(_GRID_NEGATIVE_GLYPHS))
)
# A grid cell label is short. Anything longer is the neighbouring text column
# bleeding in — the 2023 IONIQ 5 page 6 grid produced "Wheels IONIQ 5 Heat pump
# (heater) 20˝alloy wheels (Limited AWD only) row air conditioning vents".
_MAX_GRID_FEATURE_CHARS = 90

_HDR_WORD_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9®™'’\.\-+/]*")

_ADDS_TO_RE = re.compile(
    r"^\s*adds\s+to\s+(?:the\s+)?(?P<lower>[A-Za-z0-9][A-Za-z0-9®™'\-/ ]{0,28})\s*$",
    re.I,
)
_TRIM_THEN_ADDS_RE = re.compile(
    r"^\s*(?P<trim>[A-Za-z0-9][A-Za-z0-9®™'\-/ ]{0,28}?)\s+"
    r"adds\s+to\s+(?:the\s+)?(?P<lower>[A-Za-z0-9][A-Za-z0-9®™'\-/ ]{0,28})\s*$",
    re.I,
)
_INCLUDES_PLUS_RE = re.compile(
    r"^\s*(?P<trim>[A-Za-z0-9][A-Za-z0-9®™'\-/ ]{0,28}?)\s+includes\s+"
    r"(?P<lower>[A-Za-z0-9][A-Za-z0-9®™'\-/ ]{0,28}?)\s+equipment\s+plus\s*:?\s*$",
    re.I,
)

# Anything below one of these headers is optional/extra-cost content, not what
# the rung adds as standard equipment.
_BULLET_STOP_RE = re.compile(
    r"^\s*(?:options?\s*/?\s*packages?|packages?\s*/?\s*options?|options?|packages?|"
    r"available(?:\s+(?:equipment|features|packages?))?|optional(?:\s+equipment)?|"
    r"accessories|standard\s+on\s+all|also\s+available|"
    r"\d{4}\s+\w+|see\s+(?:your\s+)?dealer)\b",
    re.I,
)

# PDF kerning artefacts pdfplumber leaves in the text layer: a lone leading
# capital split from its word ("B rass Monkey"), or a split leading numeral
# ("2 0 by 10-inch"). Both are extraction damage, not brochure copy.
_SPLIT_LEAD_ALPHA_RE = re.compile(r"^([A-Z]) (?=[a-z]{2})")
_SPLIT_LEAD_DIGIT_RE = re.compile(r"^(\d) (?=\d)")
_TRAILING_FOOTNOTE_RE = re.compile(r"(?<=[a-z]{3})\d{1,2}$")
# The negative lookbehind keeps digit-letter-digit designations intact: in
# "4x4", "4x2", or "245/70r18" the trailing digits sit after a digit+letter
# pair, which is a designation, not a word with a glued footnote call.
_INLINE_FOOTNOTE_RE = re.compile(r"(?<=[a-z])(?<!\d[a-z])\d{1,2}(?=[\s,;)])")
# Bracketed footnote calls ("SiriusXM® Radio[1]") and the ™ glyph that the PDF
# text layer renders as the literal letters TM ("Blu-rayTM/DVD").
_BRACKET_FOOTNOTE_RE = re.compile(r"\[\d{1,3}(?:\s*,\s*\d{1,3})*\]")
# FCA prints a job number in the page gutter ("… headlamps DDU22US4_021").
_PRINT_CODE_RE = re.compile(r"\s+[A-Z]{2,}\d[A-Z0-9]*_\d+\b")
# "Front- Passenger" is a wrap artefact; "1st- and 2nd-row" is not.
_SPLIT_HYPHEN_RE = re.compile(r"(\w)-\s+(?=[A-Z])")
_LITERAL_TM_RE = re.compile(r"(?<=[a-z])TM(?![A-Za-z])")

# Big-ticket first: the user's standing requirement is that a rung leads with
# what actually costs money (engine, screen, suspension, drive hardware) rather
# than equipment every modern car carries.
_BIG_TICKET_PATTERNS: tuple[tuple[int, re.Pattern[str]], ...] = (
    (100, re.compile(r"\b(?:\d\.\d\s*L\b|hemi|ecoboost|pentastar|turbo|supercharged|"
                     r"v[68]\b|i[46]\b|cylinder|engine|hybrid|kwh|horsepower|\bhp\b|diesel|"
                     r"electric motor|powertrain)", re.I)),
    (90, re.compile(r"\b(?:all-wheel drive|awd|4x4|four-wheel drive|4wd|rear-wheel|"
                    r"limited-slip|locking differential|transfer case|torque vectoring)\b", re.I)),
    (85, re.compile(r"\b(?:suspension|air ride|adaptive damp|magnetic ride|bilstein|"
                    r"load-level|shock|sway bar|brembo|\bbrakes?\b|axle ratio|tow(?:ing)?\b)", re.I)),
    (80, re.compile(r"\b(?:\d+(?:\.\d+)?[- ]?inch(?:es)?\b|touchscreen|touch-screen|"
                    r"uconnect|infotainment|head-up display|instrument cluster|navigation|"
                    r"digital gauge)", re.I)),
    (70, re.compile(r"\b(?:harman|bose|alpine|bang\s*&\s*olufsen|burmester|mark levinson|"
                    r"jbl|meridian|revel|\d+[- ]speaker|subwoofer|amplifier|premium audio)\b", re.I)),
    (60, re.compile(r"\b(?:leather|nappa|suede|alcantara|panoramic|moonroof|sunroof|"
                    r"ventilated|heated (?:and cooled|steering|front|second|rear)|"
                    r"massag|memory seat|captain'?s chairs|\d+-way power)\b", re.I)),
    (50, re.compile(r"\b(?:\d{2}[- ]?(?:by|x)[- ]?\d|\d{2}-inch\s+(?:alloy|wheels)|"
                    r"alloy wheels|wheels\b|led (?:headlamp|headlight|projector)|"
                    r"adaptive cruise|hitch)\b", re.I)),
)
# Equipment so universal that calling it out as what a trim adds reads as filler.
_UNIVERSAL_EQUIPMENT_RE = re.compile(
    r"^(?:.*\b(?:security alarm|theft[- ]deterrent|anti-theft|tire pressure monitor|"
    r"child seat anchor|latch\b|cup ?holders?|floor mats?|rear window defroster|"
    r"12-volt(?: power)? outlet|power windows|power door locks|remote keyless entry|"
    r"cargo (?:net|cover)|first aid|jack\b|owner'?s manual|sun visor|"
    r"passenger (?:air ?bag|airbag)|driver air ?bag|seat belt)\b.*)$",
    re.I,
)


@dataclass
class QuotedTrimBullet:
    """One equipment line, plus the exact place it was quoted from."""

    text: str
    page: int
    layout: str
    source: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "text": self.text,
            "source": self.source,
            "page": self.page,
            "layout": self.layout,
        }


@dataclass
class QuotedRungEdge:
    """One ORDERING fact, plus the exact place it was quoted from.

    A brochure trim-walk page is headed "<TRIM> / Adds to <LOWER>". That heading
    is the OEM stating, in its own words and on a page we hold, that ``trim``
    sits directly above ``below``. It is the only ordering evidence in this
    codebase that meets the same standard the bullets are held to, so it is
    recorded with the same fields: the file, the page, and the printed text.

    ``trim_quote`` and ``below_quote`` are the two verbatim printed lines the
    edge was read off. On an FCA page the trim name is set on its own line above
    "Adds to <LOWER>", so the pair is two lines; on a page that prints
    "<TRIM> ADDS TO <LOWER>" as one line both fields hold that same line. Either
    way each is verbatim on ``page``, so a reviewer can re-open the PDF and find
    it without knowing which layout produced it.
    """

    trim: str
    below: str
    page: int
    layout: str
    source: str
    trim_quote: str
    below_quote: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "trim": self.trim,
            "below": self.below,
            "source": self.source,
            "page": self.page,
            "layout": self.layout,
            "trim_quote": self.trim_quote,
            "below_quote": self.below_quote,
        }


@dataclass
class TrimWalkExtract:
    catalog_key: str
    year: int
    make: str
    model: str
    layouts: list[str] = field(default_factory=list)
    trims_available: list[str] = field(default_factory=list)
    adds_by_trim: dict[str, list[str]] = field(default_factory=dict)
    standard_by_trim: dict[str, list[str]] = field(default_factory=dict)
    provenance: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    pages_used: list[int] = field(default_factory=list)
    reject_reasons: list[str] = field(default_factory=list)
    gate_rejected: int = 0
    #: The quoted "<TRIM> adds to <LOWER>" edges, in the order they were read.
    rung_edges: list[dict[str, Any]] = field(default_factory=list)
    #: Per rung, WHY it sits where it sits in ``trims_available``. See
    #: :func:`order_rungs_with_basis` for the vocabulary.
    order_basis: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def usable(self) -> bool:
        return len([t for t, v in self.adds_by_trim.items() if v]) >= 2


@lru_cache(maxsize=4096)
def _known_trim_vocabulary(make: str, model: str) -> frozenset[str]:
    """Trim names this make/model is actually known to use, lower-cased.

    ``trim_ladder._is_valid_trim_name`` is a shape/blacklist test, not a
    membership test — it answers True for ``TSEB``, ``B52`` and ``CHOS``, which
    is exactly the kind of noise a grid header row is full of once pdfplumber
    has mangled a rotated column label. Column attribution has to be exact, so
    the header parser uses the knowledge base's own trim list instead.
    """
    from backend.enrichment.trim_ladder_knowledge import ordered_trim_candidates

    return frozenset(
        n.strip().lower() for n in ordered_trim_candidates(make, model) if str(n).strip()
    )


@lru_cache(maxsize=16384)
def _valid_trim(token: str, make: str, model: str) -> bool:
    """True when *token* names a trim this model is known to offer.

    A grid column headed ``LX/SE`` covers two trims at once; it counts as a
    header token only when every slash-separated part is itself a trim name.
    """
    tok = (token or "").strip().strip("®™.,:")
    if not tok or len(tok) > 24:
        return False
    vocab = _known_trim_vocabulary(make, model)
    if tok.lower() in vocab:
        return bool(_is_valid_trim_name(tok, make=make, model=model))
    if "/" in tok:
        parts = [p.strip() for p in tok.split("/") if len(p.strip()) >= 2]
        return len(parts) >= 2 and all(
            p.lower() in vocab and _is_valid_trim_name(p, make=make, model=model)
            for p in parts
        )
    return False


@lru_cache(maxsize=16384)
def _valid_walk_trim(label: str, make: str, model: str) -> bool:
    """Looser trim test for a printed "<TRIM> / Adds to <LOWER>" heading.

    Here the OEM has already told us the line is a trim heading, so the label
    only has to survive the shape/blacklist test — unlike a grid header, where
    the token itself is the only evidence that a column belongs to a trim.
    """
    lbl = (label or "").strip().strip("®™.,:")
    if not lbl or len(lbl) > 34:
        return False
    if lbl.lower() in _known_trim_vocabulary(make, model):
        return True
    if re.fullmatch(r"[\d\W_]+", lbl):
        return False
    return bool(_is_valid_trim_name(lbl, make=make, model=model))


def _expand_column_label(label: str) -> list[str]:
    """``"LX/SE"`` -> ``["LX", "SE"]``; a plain label stays a single trim."""
    parts = [p.strip() for p in (label or "").split("/") if p.strip()]
    return parts if len(parts) >= 2 else [(label or "").strip()]


def _clean_trim_label(raw: str, *, make: str = "", model: str = "") -> str:
    """Tidy a printed trim heading. Casing only — never a different trim.

    FCA prints headings in caps ("CITADEL", "SCAT PACK WIDEBODY"). The knowledge
    base's canonical spelling is adopted ONLY when it is the same name in
    different case; ``canonical_trim_name`` happily maps "GT PLUS" to "GT" and
    "SRT HELLCAT REDEYE WIDEBODY" to "SRT Hellcat", which would merge two rungs.
    """
    t = re.sub(r"[®™]", "", re.sub(r"\s+", " ", (raw or "").strip()))
    t = re.sub(r"\s+", " ", t).strip(".,:;• ").strip()
    if not t or not make or not model:
        return t
    from backend.enrichment.trim_ladder_knowledge import canonical_trim_name

    canon = canonical_trim_name(t, make, model) or ""
    if canon and re.sub(r"[^a-z0-9]", "", canon.lower()) == re.sub(r"[^a-z0-9]", "", t.lower()):
        return canon
    return t


def normalize_quoted_line(raw: str) -> str:
    """Collapse whitespace and undo pdfplumber kerning artefacts. No word is invented."""
    t = re.sub(r"[   ]", " ", (raw or ""))
    t = re.sub(r"\s+", " ", t).strip()
    t = t.lstrip("".join(_BULLET_GLYPHS) + " \t")
    t = t.strip()
    prev = None
    while prev != t:
        prev = t
        t = _SPLIT_LEAD_ALPHA_RE.sub(r"\1", t)
        t = _SPLIT_LEAD_DIGIT_RE.sub(r"\1", t)
    t = _BRACKET_FOOTNOTE_RE.sub("", t)
    t = _LITERAL_TM_RE.sub("™", t)
    t = _TRAILING_FOOTNOTE_RE.sub("", t)
    t = _INLINE_FOOTNOTE_RE.sub("", t)
    t = _PRINT_CODE_RE.sub(" ", t)
    t = _SPLIT_HYPHEN_RE.sub(r"\1-", t)
    t = re.sub(r"\s+", " ", t).strip(" .;,•-–—")
    return t


def _acceptable_equipment_line(text: str) -> bool:
    """Positive test: does this read like a quoted equipment line?

    Deliberately conservative. ``is_displayable_trim_bullet`` is the downstream
    display gate; if IT has to reject something this function accepted, the
    extraction is wrong, so ``extract_trim_walk`` counts those disagreements
    instead of quietly relying on the gate.
    """
    t = (text or "").strip()
    if not (8 <= len(t) <= 200):
        return False
    if not re.search(r"[A-Za-z]{3}", t):
        return False
    # A feature line starts with a capital or a digit. A leading all-lowercase
    # word means we are looking at the tail of a wrapped row ("trimmed
    # upholstery" under "Leather-"), not the whole feature. OEM names that
    # legitimately start lowercase carry an interior capital (iPod, xDrive,
    # i-Activ, eTorque).
    if re.match(r"^[a-z][a-z'’]*(?:[)\]}.,;:]|\s|$)", t):
        return False
    # A row that restates the grid's own column list ("RWD SE/SEL/Limited AWD
    # SE/SEL/Limited Ventilated front seats") is header text bleeding into the
    # body, not a feature.
    if re.search(r"\b\w+/\w+/\w+\b", t):
        return False
    # FCA order-guide "Delete" lines say what a rung REMOVES.
    if re.match(r"^delete\b", t, re.I):
        return False
    # An unclosed bracket means the quote is a fragment. A table cell that runs
    # past the bottom of a page is re-emitted on the next page cut in half — the
    # 2026 RAV4 steering-wheel cell continues onto page 11 as "… Multi-
    # Information Display (MID, Bluetooth® hands-free phone" — and half a feature
    # list read as a whole one is a false claim.
    if t.count("(") != t.count(")") or t.count("[") != t.count("]"):
        return False
    # Sentences, not spec lines.
    if t.count(".") >= 2 or re.search(r"\b(?:is|are|was|were|has|have|will|can)\b\s+\w+ed\b", t, re.I):
        return False
    if re.search(r"\b(?:see|visit|call|refer to|consult|read)\b", t, re.I):
        return False
    if t.endswith((",", ";", ":")) or re.search(
        r"\b(?:and|or|with|the|a|an|of|for|to|in|on|plus|than)$", t, re.I
    ):
        return False
    # Page furniture / disclaimers.
    if re.search(r"\b(?:mpg|epa|msrp|warranty|©|all rights reserved|disclaimer|"
                 r"model year|page \d|www\.|\.com)\b", t, re.I):
        return False
    if t.isupper() and len(t.split()) <= 4:
        return False
    letters = sum(1 for c in t if c.isalpha())
    if letters < len(t) * 0.45:
        return False
    return True


def _bullet_columns_are_ambiguous(lines: list[str]) -> bool:
    """True when a page's bullets run in more than one column.

    pdfplumber flattens a multi-column bullet spread row by row, so a wrapped
    bullet in the left column is emitted immediately before the continuation of
    a DIFFERENT bullet in the right column. There is no way to re-associate the
    fragments without the x-positions, which the derived JSON does not carry, so
    such pages yield nothing rather than a merged bullet.
    """
    single = 0
    multi = 0
    for ln in lines:
        n = len(_BULLET_GLYPH_RE.findall(ln))
        if n == 1:
            single += 1
        elif n > 1:
            multi += 1
    if single < 5:
        return True
    # Zero tolerance: one line carrying two bullets means two columns were
    # flattened together, and the 2022 Challenger page 41 spread showed what
    # that produces — "Uconnect® 4C Navigation with lightweight aluminum wheels
    # 8.4-inch touchscreen display", two features welded into one claim.
    return multi > 0


def _collect_single_column_bullets(lines: list[str], start: int) -> tuple[list[str], int]:
    """Read a single-column bullet run starting at *start*; return (bullets, next_index)."""
    bullets: list[str] = []
    current: list[str] = []
    i = start
    while i < len(lines):
        raw = lines[i]
        stripped = raw.strip()
        if not stripped:
            i += 1
            continue
        if _BULLET_STOP_RE.match(stripped):
            break
        if _ADDS_TO_RE.match(stripped) or _TRIM_THEN_ADDS_RE.match(stripped) or _INCLUDES_PLUS_RE.match(stripped):
            break
        m = _BULLET_LINE_RE.match(raw)
        if m:
            if current:
                bullets.append(" ".join(current))
            current = [m.group(1).strip()]
            i += 1
            continue
        if current:
            # Wrapped continuation of the bullet above. Unambiguous only because
            # the caller has already established this page is single-column.
            if len(stripped) <= 90 and not stripped.isupper():
                current.append(stripped)
                i += 1
                continue
            break
        if bullets:
            break
        i += 1
        if i - start > 3:
            break
    if current:
        bullets.append(" ".join(current))
    return bullets, i


_PACKAGE_BULLET_RE = re.compile(r"\b(?:Package|Group)\s*$", re.I)


def _options_column_is_interleaved(bullets: list[str]) -> bool:
    """True when a trim page's OPTIONS/PACKAGES column was flattened into the adds list.

    Some FCA pages set the standard-equipment list and the options list as two
    columns with one bullet glyph per physical line, so the two alternate line
    by line and a wrapped standard bullet is completed by the NEXT column's
    text: the 2022 Challenger page 41 spread produced "Painted Black Satin
    Graphics Package Damping Suspension" and "Uconnect® 4C Navigation with
    lightweight aluminum wheels 8.4-inch touchscreen display". The tell is
    option-package names sitting inside what should be the standard list, so
    such a block is refused whole.
    """
    packages = sum(1 for b in bullets if _PACKAGE_BULLET_RE.search(b or ""))
    return packages >= 3


def _extract_adds_to_page(
    page_no: int,
    text: str,
    *,
    make: str,
    model: str,
    source: str,
) -> tuple[dict[str, list[QuotedTrimBullet]], dict[str, QuotedRungEdge], list[str]]:
    """Parse an FCA-style "<TRIM> / Adds to <LOWER>" page.

    Returns ``(adds, edges, reasons)``. ``edges`` maps a trim to the
    :class:`QuotedRungEdge` read off this page's heading — the OEM's own
    statement of what this grade sits above. It used to be a bare
    ``{trim: lower}`` dict that the caller threw away after sorting with it; it
    now carries the page and the printed lines so the ordering can be cited.
    """
    lines = [ln.rstrip() for ln in (text or "").splitlines()]
    non_empty = [ln for ln in lines if ln.strip()]
    reasons: list[str] = []
    if not any(
        _ADDS_TO_RE.match(ln.strip())
        or _TRIM_THEN_ADDS_RE.match(ln.strip())
        or _INCLUDES_PLUS_RE.match(ln.strip())
        for ln in non_empty
    ):
        return {}, {}, reasons
    if _bullet_columns_are_ambiguous(non_empty):
        return {}, {}, ["adds_to_page_multi_column_bullets"]

    adds: dict[str, list[QuotedTrimBullet]] = {}
    below: dict[str, QuotedRungEdge] = {}
    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        if not stripped:
            i += 1
            continue
        trim = lower = ""
        # The verbatim printed line(s) the trim name and the "adds to" phrase
        # were read off, kept so the edge can be re-checked against the PDF.
        trim_quote = below_quote = ""
        consumed = i + 1
        m = _TRIM_THEN_ADDS_RE.match(stripped) or _INCLUDES_PLUS_RE.match(stripped)
        if m:
            trim = _clean_trim_label(m.group("trim"), make=make, model=model)
            lower = _clean_trim_label(m.group("lower"), make=make, model=model)
            trim_quote = below_quote = stripped
        else:
            m2 = _ADDS_TO_RE.match(stripped)
            if m2:
                # The trim name is the nearest preceding non-empty line.
                j = i - 1
                while j >= 0 and not lines[j].strip():
                    j -= 1
                if j >= 0:
                    trim = _clean_trim_label(lines[j], make=make, model=model)
                    lower = _clean_trim_label(m2.group("lower"), make=make, model=model)
                    trim_quote = lines[j].strip()
                    below_quote = stripped
        if not trim or not lower:
            i += 1
            continue
        if not _valid_walk_trim(trim, make, model) or not _valid_walk_trim(lower, make, model):
            reasons.append("adds_to_header_trim_not_recognised")
            i = consumed
            continue
        if trim.lower() == lower.lower():
            i = consumed
            continue
        bullets, nxt = _collect_single_column_bullets(lines, consumed)
        if _options_column_is_interleaved(bullets):
            reasons.append("adds_to_page_options_column_interleaved")
            i = max(nxt, consumed)
            continue
        kept: list[QuotedTrimBullet] = []
        for b in bullets:
            norm = normalize_quoted_line(b)
            if not _acceptable_equipment_line(norm):
                continue
            kept.append(
                QuotedTrimBullet(text=norm, page=page_no, layout="adds_to_block", source=source)
            )
        if kept:
            adds.setdefault(trim, []).extend(kept)
            below.setdefault(
                trim,
                QuotedRungEdge(
                    trim=trim,
                    below=lower,
                    page=page_no,
                    layout="adds_to_block",
                    source=source,
                    trim_quote=trim_quote,
                    below_quote=below_quote,
                ),
            )
        i = max(nxt, consumed)
    return adds, below, reasons


def _grid_marker_groups(line: str) -> list[list[tuple[int, int, str]]]:
    toks = [(m.start(), m.end(), m.group()) for m in _GRID_MARK_RE.finditer(line)]
    groups: list[list[tuple[int, int, str]]] = []
    cur: list[tuple[int, int, str]] = []
    for t in toks:
        if cur and line[cur[-1][1] : t[0]].strip() == "" and t[0] - cur[-1][1] <= 3:
            cur.append(t)
        else:
            if cur:
                groups.append(cur)
            cur = [t]
    if cur:
        groups.append(cur)
    return groups


def _marker_is_standard(mark: str) -> bool:
    if mark in _GRID_STANDARD_GLYPHS:
        return True
    return _GRID_LETTER_MARKS.get(mark.upper()) == "std"


def _grid_header_candidates(line: str, make: str, model: str) -> list[tuple[str, ...]]:
    """Trim-name column headers on *line*, as a tuple of one label per column.

    Brochure grids print the header either once (``LX S EX SX``), once per
    side-by-side panel on the same physical row (``Engineering LX/SE EX EX-L
    Safety LX/SE EX EX-L``), or repeated end to end (``LX S EX SX LX S EX SX``).
    All three collapse to the same column tuple; anything else is not treated as
    a header.
    """
    raw = [m.group().strip("®™.") for m in _HDR_WORD_RE.finditer(line)]
    raw = [t for t in raw if t]
    if not raw:
        return []
    direct = _grid_header_runs(raw, make, model)
    if direct:
        return direct
    # Grid headers are set in a narrow column, so the OEM abbreviates and
    # pdfplumber truncates: "PALISADE SE SEL Ltd Calli". A token is expanded
    # only when it resolves to exactly ONE trim this model is known to offer.
    resolved = _grid_header_runs([_resolve_header_token(t, make, model) for t in raw], make, model)
    if resolved:
        return resolved
    # GM and FCA spec grids print column labels rotated 90°, which pdfplumber
    # emits character-reversed ("SL1 TL1 TL2 TL3" for "1LS 1LT 2LT 3LT"). A run
    # of reversed tokens that are ALL known trims of this model is not a
    # coincidence, so the reversal is read back rather than guessed at.
    return _grid_header_runs([t[::-1] for t in raw], make, model)


# Column-header abbreviations that appear across OEM grids. An expansion is
# used only when the expanded name is itself a known trim of the model.
_HEADER_ABBREVIATIONS: dict[str, str] = {
    "ltd": "Limited",
    "lim": "Limited",
    "plat": "Platinum",
    "prem": "Premium",
    "pref": "Preferred",
    "tour": "Touring",
    "sig": "Signature",
    "prest": "Prestige",
    "adv": "Advance",
    "ess": "Essence",
}


@lru_cache(maxsize=16384)
def _resolve_header_token(token: str, make: str, model: str) -> str:
    """Expand a truncated/abbreviated grid header token, or return it unchanged."""
    tok = (token or "").strip().strip("®™.,:")
    if not tok or _valid_trim(tok, make, model):
        return token
    vocab = _known_trim_vocabulary(make, model)
    low = tok.lower()
    if len(low) >= 3:
        matches = [v for v in vocab if v.startswith(low)]
        if len(matches) == 1:
            return matches[0].title() if matches[0].islower() else matches[0]
    expanded = _HEADER_ABBREVIATIONS.get(low)
    if expanded and expanded.lower() in vocab:
        return expanded
    return token


def _grid_header_runs(toks: list[str], make: str, model: str) -> list[tuple[str, ...]]:
    if not toks:
        return []
    runs: list[list[str]] = []
    cur: list[str] = []
    for t in toks:
        if _valid_trim(t, make, model):
            cur.append(t)
        else:
            if len(cur) >= 2:
                runs.append(cur)
            cur = []
    if len(cur) >= 2:
        runs.append(cur)
    if not runs:
        return []

    out: list[tuple[str, ...]] = []
    seen: set[tuple[str, ...]] = set()

    def _offer(head: tuple[str, ...]) -> None:
        if len({h.lower() for h in head}) != len(head):
            return  # duplicate column labels — attribution would be a guess
        if all(len(h) == 1 and h.upper() in _GRID_LETTER_MARKS for h in head):
            return  # this is a row of single-letter markers, not a header
        if head in seen:
            return
        seen.add(head)
        out.append(head)

    # Panels: the same trim run printed more than once on the line.
    if len(runs) >= 2:
        first = tuple(runs[0])
        if all(tuple(r) == first for r in runs[1:]):
            _offer(first)

    trim_tokens = sum(len(r) for r in runs)
    last_run_at_eol = toks[-len(runs[-1]) :] == runs[-1]
    for pos, run in enumerate(runs):
        n = len(run)
        # A stray pair of trim-ish words inside a paragraph is not a header row.
        # Real headers either dominate the line or sit at the end of it, after a
        # section title ("AUDIO SYSTEMS (CONTINUED) LS LT LTZ").
        at_eol = last_run_at_eol and pos == len(runs) - 1
        if not at_eol and n < 0.35 * len(toks) and trim_tokens < 0.5 * len(toks):
            continue
        for width in range(2, n + 1):
            if n % width:
                continue
            if all(run[j].lower() == run[j % width].lower() for j in range(n)):
                _offer(tuple(run[:width]))
                break
    return out


def _grid_width_counts(lines: list[str]) -> dict[int, int]:
    from collections import Counter

    counts: Counter[int] = Counter()
    for ln in lines:
        for g in _grid_marker_groups(ln):
            if 2 <= len(g) <= 12:
                counts[len(g)] += 1
    return dict(counts)


def _modal_grid_width(lines: list[str]) -> int:
    counts = _grid_width_counts(lines)
    if not counts:
        return 0
    return max(counts.items(), key=lambda kv: (kv[1], kv[0]))[0]


def _grid_width_is_supported(counts: dict[int, int], width: int) -> bool:
    """True when the page's own marker rows back a header of *width* columns.

    A header whose column count is not what the rows actually carry is a
    mis-tokenised header — ``Engineering LX/SE EX EX-L`` read as four columns
    when the rows carry three markers — and reading it would silently shift
    every feature one column to the left. Only the width the page's rows
    predominantly use is trusted; a merely "supported" runner-up width was
    measured to cost more brochures than it won.
    """
    if not counts or counts.get(width, 0) < 6:
        return False
    return width == max(counts.items(), key=lambda kv: (kv[1], kv[0]))[0]


def _grid_column_order_is_base_to_top(trims: tuple[str, ...], make: str, model: str) -> int:
    """+1 if printed left→top-of-range, -1 if reversed, 0 if not verifiable."""
    from backend.enrichment.trim_ladder_knowledge import luxury_rank

    ranks = [luxury_rank(t, make, model) for t in trims]
    if any(r >= 9_000 for r in ranks) or len(set(ranks)) < len(ranks):
        return 0
    if all(ranks[i] > ranks[i + 1] for i in range(len(ranks) - 1)):
        return 1  # lower rank == more luxurious, so decreasing == base→top
    if all(ranks[i] < ranks[i + 1] for i in range(len(ranks) - 1)):
        return -1
    return 0


def _grid_page_is_single_panel(body_lines: list[str], width: int) -> bool:
    """True when the page prints ONE grid, not several side by side.

    On a two-panel spread pdfplumber flattens both panels onto one physical
    line, so the text preceding the second panel's markers is the tail of the
    first panel's wrapped row glued to the second panel's feature name — e.g.
    "trial, HD Radio™ with iTunes® Tagging, auxiliary audio jack, USB port with
    iPod® Shift lever with silver accents" (2013 RAV4 p10), two unrelated
    features in one string. Column attribution stays right but the quoted text
    does not, so multi-panel pages are refused outright.
    """
    single = 0
    multi = 0
    for row in body_lines:
        n = sum(1 for g in _grid_marker_groups(row) if len(g) == width)
        if n == 1:
            single += 1
        elif n > 1:
            multi += 1
    return single >= 6 and multi <= 0.05 * (single + multi)


def _rejoin_wrapped_feature(body_lines: list[str], row_i: int, feat: str) -> str:
    """Put back the head of a feature name the PDF wrapped onto the line above.

    Only attempted when the row carries a single marker run, i.e. the page has
    one grid panel on this line, so the preceding marker-free line can belong to
    nothing else. ``Leather-`` / ``trimmed upholstery ● ● —`` is the common case.
    """
    if row_i == 0 or not feat or not re.match(r"^[a-z]", feat):
        return feat
    head = body_lines[row_i - 1].strip()
    if not head or _grid_marker_groups(head):
        return feat
    if head.isupper() or len(head) > 90:
        return feat
    hyphenated = head.endswith("-")
    head = normalize_quoted_line(head)
    if not head or not re.match(r"^[A-Z0-9]", head):
        return feat
    return f"{head}-{feat}" if hyphenated else f"{head} {feat}"


def _extract_marker_grid_page(
    page_no: int,
    text: str,
    *,
    make: str,
    model: str,
    source: str,
) -> tuple[list[str], dict[str, list[QuotedTrimBullet]], list[str]]:
    """Parse an equipment grid. Returns (column order base→top, standard_by_trim, reasons)."""
    lines = [ln.rstrip() for ln in (text or "").splitlines() if ln.strip()]
    reasons: list[str] = []
    if len(lines) < 8:
        return [], {}, reasons

    width_counts = _grid_width_counts(lines)
    if not width_counts:
        return [], {}, reasons

    best: tuple[int, tuple[str, ...], int] | None = None  # (rows, header, header_index)
    for idx, ln in enumerate(lines):
        if len(ln) > 240:
            continue
        for head in _grid_header_candidates(ln, make, model):
            width = len(head)
            if not _grid_width_is_supported(width_counts, width):
                # Header column count disagrees with the page's own marker rows.
                reasons.append("grid_header_width_disagrees_with_rows")
                continue
            body = lines[idx + 1 :]
            if not _grid_page_is_single_panel(body, width):
                reasons.append("grid_page_has_side_by_side_panels")
                continue
            rows = 0
            for line_i, row in enumerate(body):
                groups = _grid_marker_groups(row)
                full = [g for g in groups if len(g) == width]
                if len(full) != 1:
                    continue
                g = full[0]
                start = 0
                for prior in groups:
                    if prior is g:
                        break
                    start = prior[-1][1]
                feat = _rejoin_wrapped_feature(
                    body, line_i, normalize_quoted_line(row[start : g[0][0]].strip())
                )
                if _acceptable_equipment_line(feat) and len(feat) <= _MAX_GRID_FEATURE_CHARS:
                    rows += 1
            if rows >= 10 and (best is None or rows > best[0]):
                best = (rows, head, idx)
    if best is None:
        return [], {}, reasons

    rows_found, header, header_idx = best
    # A column headed "LX/SE" covers both trims; both inherit that column.
    columns = [_expand_column_label(label) for label in header]
    expanded: list[str] = [t for col in columns for t in col]
    if len({t.lower() for t in expanded}) != len(expanded):
        return [], {}, ["grid_column_order_unverifiable"]
    direction = _grid_column_order_is_base_to_top(tuple(expanded), make, model)
    if direction == 0:
        return [], {}, ["grid_column_order_unverifiable"]
    order = list(expanded) if direction == 1 else list(reversed(expanded))

    width = len(header)
    std: dict[str, list[QuotedTrimBullet]] = {t: [] for t in expanded}
    seen: dict[str, set[str]] = {t: set() for t in expanded}
    body_lines = lines[header_idx + 1 :]
    for row_i, body in enumerate(body_lines):
        groups = _grid_marker_groups(body)
        full = [g for g in groups if len(g) == width]
        if len(full) != 1:
            continue
        g = full[0]
        start = 0
        for prior in groups:
            if prior is g:
                break
            start = prior[-1][1]
        feat = _rejoin_wrapped_feature(
            body_lines, row_i, normalize_quoted_line(body[start : g[0][0]].strip())
        )
        if not _acceptable_equipment_line(feat) or len(feat) > _MAX_GRID_FEATURE_CHARS:
            continue
        marks = [t[2] for t in g]
        for col, mark in enumerate(marks):
            if not _marker_is_standard(mark):
                continue
            for trim in columns[col]:
                key = feat.lower()
                if key in seen[trim]:
                    continue
                seen[trim].add(key)
                std[trim].append(
                    QuotedTrimBullet(text=feat, page=page_no, layout="marker_grid", source=source)
                )
    if sum(len(v) for v in std.values()) < 6:
        return [], {}, reasons
    reasons.append(f"grid_rows={rows_found}")
    return order, std, reasons


# ---------------------------------------------------------------------------
# Layout 3: ``cell_grid`` — the PDF's OWN table cells
# ---------------------------------------------------------------------------
#
# ``derived/brochure_text`` pages carry a ``table_lines`` list: one string per
# table row, cells joined by " | " and intra-cell line wraps kept as "\n". It is
# pdfplumber's ``extract_tables`` output, i.e. cell boundaries the PDF itself
# declares, so a marker's column is not inferred from character offsets — it IS
# the cell index. 1,932 of the 3,312 brochure_text files carry it and no
# extractor had ever read it.
#
# Two hazards the flattened-text grid parser had are structurally impossible
# here, and both have regression tests:
#   * a "-" cell is a whole cell, so the ASCII hyphen inside "Leather-trimmed"
#     can never be read as a "not available" marker;
#   * a feature and its marker come from the same row of the same table, so two
#     features cannot be welded into one claim by a neighbouring column.

_CELL_SEP = " | "

_CELL_STANDARD_GLYPHS = frozenset("●•▪◆✓■★")
_CELL_NEGATIVE_GLYPHS = frozenset("○◇—–−-□✕✗×")
# Letter legends. "S"/"•" mean standard; everything else in an OEM legend
# ("O"ptional, "P"ackage, "A"vailable, "N"ot available) is NOT standard, so it
# can never turn into a claim about a trim.
_CELL_STANDARD_LETTERS = frozenset({"S", "STD"})
_CELL_NEGATIVE_LETTERS = frozenset({"O", "P", "N", "A", "X", "NA", "OPT"})

# Drivetrain qualifiers OEMs append to a grade column ("LE FWD", "SE AWD", and
# after a rotated-label decode the squashed "LEFWD"). They are stripped so both
# columns resolve to the same grade; a grade then counts as standard only when
# EVERY column carrying its name says standard.
_DRIVETRAIN_SUFFIX_RE = re.compile(
    r"[\s/\-]*(?:FWD|AWD|RWD|4WD|2WD|4X4|4X2|4WHEELDRIVE|E-?4ORCE|4MOTION|QUATTRO|"
    r"XDRIVE|SH-?AWD|HYBRID|HEV|PHEV)\s*$",
    re.I,
)
# Powertrain qualifiers a grade column carries on a Toyota "Product Information"
# sheet: "Gas LE FWD", "Hybrid XLE AWD", "LE 2.0L CVT FWD". They are removed from
# the squashed label so every configuration of one grade resolves to that grade;
# a feature then counts as standard on the grade only when EVERY one of its
# columns says so, which is the conservative reading.
_SQUASHED_POWERTRAIN_PREFIX_RE = re.compile(
    r"^(?:GAS|HYBRIDMAX|PLUGINHYBRID|HYBRID|PHEV|BEV|EV|DIESEL|TURBO)+"
)
# Only tokens no trim name can contain. Bare "AT"/"MT" are deliberately absent:
# stripping them out of a squashed label would eat letters out of real names.
_SQUASHED_ENGINE_TOKEN_RE = re.compile(r"\d(?:\.\d)?L(?=[A-Z]|$)|ECVT|CVT|DCT")

_CELL_TRIM_JUNK_RE = re.compile(
    r"standard|optional|not available|available|package|grade|model|feature|"
    r"^-+$|^=+$|continued",
    re.I,
)
_FOOTNOTE_MARK_RE = re.compile(r"[\d,\s\*†‡§]+$")


def _split_table_row(row: str) -> list[str]:
    return [c.strip() for c in str(row or "").split(_CELL_SEP)]


def _cell_mark(cell: str) -> str | None:
    """``'std'`` / ``'neg'`` / ``'blank'`` for a marker cell, ``None`` if it is not one."""
    c = _CID_ARTIFACT_CELL_RE.sub("", str(cell or "")).strip()
    c = _FOOTNOTE_MARK_RE.sub("", c).strip()
    if not c:
        return "blank"
    if len(c) > 3:
        return None
    if c[0] in _CELL_STANDARD_GLYPHS:
        return "std"
    if c[0] in _CELL_NEGATIVE_GLYPHS:
        return "neg"
    up = c.upper()
    if up in _CELL_STANDARD_LETTERS:
        return "std"
    if up in _CELL_NEGATIVE_LETTERS:
        return "neg"
    return None


_CID_ARTIFACT_CELL_RE = re.compile(r"\(cid:\d+\)")


def _decode_rotated_cell(cell: str) -> str | None:
    """Read back a column label the PDF printed rotated 90°.

    pdfplumber emits a rotated label as one short fragment per glyph row, in
    reverse reading order, each fragment itself reversed:
    ``"D\\nW\\nA\\nd\\nn\\na\\nld\\no\\no\\nW"`` is "Woodland AWD" and
    ``"D\\nW\\nA\\nd\\ne\\nt\\nim\\niL"`` is "Limited AWD". Reversing the
    fragment order and each fragment restores the label exactly — no glyph is
    invented, so a label that does not then match a known trim is simply
    dropped.
    """
    parts = [p.strip() for p in str(cell or "").split("\n")]
    if len(parts) < 3 or not all(parts):
        return None
    if any(len(p) > 2 for p in parts):
        return None
    return "".join(p[::-1] for p in reversed(parts))


def _join_wrapped_cell(cell: str) -> str:
    """Join a cell's wrapped lines. ``"360-\\ndegree"`` -> ``"360-degree"``."""
    parts = [p.strip() for p in str(cell or "").split("\n") if p.strip()]
    if not parts:
        return ""
    out = parts[0]
    for part in parts[1:]:
        out = out + part if out.endswith("-") else f"{out} {part}"
    return out


@lru_cache(maxsize=16384)
def _cell_header_trim(cell: str, make: str, model: str) -> str:
    """Canonical trim named by a header cell, or "" when the cell names no known trim."""
    raw = str(cell or "").strip()
    if not raw or _cell_mark(raw) is not None:
        return ""
    if _CID_ARTIFACT_CELL_RE.search(raw):
        return ""
    vocab = _known_trim_vocabulary(make, model)
    candidates: list[str] = []
    decoded = _decode_rotated_cell(raw)
    if decoded:
        candidates.append(decoded)
    flat = re.sub(r"\s+", " ", raw.replace("\n", " ")).strip()
    candidates.append(flat)
    # A single-line rotated label comes back as one reversed word: GM's Sierra
    # grid heads its columns "ORP", "ELS", "NOITAVELE", "X4TA" for PRO, SLE,
    # ELEVATION, AT4X. The reversal is only accepted when it lands exactly on a
    # trim this model is known to offer.
    if len(flat) >= 2 and " " not in flat:
        candidates.append(flat[::-1])
    for cand in candidates:
        text = re.sub(r"[®™]", "", cand).strip(" .,:;*")
        if not text or len(text) > 32 or _CELL_TRIM_JUNK_RE.search(text):
            continue
        for _ in range(3):
            stripped = _DRIVETRAIN_SUFFIX_RE.sub("", text).strip(" -/")
            if stripped == text:
                break
            text = stripped
        squashed = re.sub(r"[^A-Za-z0-9.]", "", text).upper()
        reduced = _SQUASHED_ENGINE_TOKEN_RE.sub("", squashed)
        reduced = _SQUASHED_POWERTRAIN_PREFIX_RE.sub("", reduced)
        for _ in range(3):
            stripped = _DRIVETRAIN_SUFFIX_RE.sub("", reduced)
            if stripped == reduced:
                break
            reduced = stripped
        for shape in (squashed, reduced):
            key = re.sub(r"[^a-z0-9]", "", shape.lower())
            if not key:
                continue
            for known in vocab:
                if re.sub(r"[^a-z0-9]", "", known) == key:
                    return _canonical_vocab_spelling(known, make, model)
    return ""


@lru_cache(maxsize=16384)
def _canonical_vocab_spelling(lowered: str, make: str, model: str) -> str:
    from backend.enrichment.trim_ladder_knowledge import ordered_trim_candidates

    for name in ordered_trim_candidates(make, model):
        if str(name).strip().lower() == lowered:
            return str(name).strip()
    return lowered


def _cell_grid_direction(
    order: list[str], std_counts: dict[str, int], make: str, model: str
) -> int:
    """+1 when the printed columns run base→top, -1 when reversed, 0 when unprovable.

    Two independent readings have to agree. The first is the grid's own content:
    an equipment grid gives a higher grade at least as much standard equipment as
    the grade below it, so the standard-mark count is monotone along the ladder.
    The second is ``luxury_rank``. Where the ranks disagree with each other they
    are ignored rather than trusted — the RAV4 grid prints LE, XLE, SE, XSE,
    Limited while ``luxury_rank`` puts SE below XLE — but a rank ordering that
    contradicts the counts outright makes the direction unprovable and the page
    yields nothing.
    """
    from backend.enrichment.trim_ladder_knowledge import luxury_rank

    if len(order) < 2:
        return 0
    counts = [std_counts.get(t, 0) for t in order]
    by_counts = 0 if counts[0] == counts[-1] else (1 if counts[-1] > counts[0] else -1)

    ranks = [luxury_rank(t, make, model) for t in order]
    by_rank = 0
    if all(r < 9_000 for r in ranks) and len(set(ranks)) == len(ranks):
        # lower luxury_rank == more luxurious
        if all(ranks[i] > ranks[i + 1] for i in range(len(ranks) - 1)):
            by_rank = 1
        elif all(ranks[i] < ranks[i + 1] for i in range(len(ranks) - 1)):
            by_rank = -1

    if by_counts and by_rank and by_counts != by_rank:
        return 0
    return by_counts or by_rank


def _extract_cell_grid_page(
    page_no: int,
    table_lines: list[Any],
    *,
    make: str,
    model: str,
    source: str,
    carried: tuple[int, tuple[str, ...]] | None = None,
) -> tuple[list[str], dict[str, list[QuotedTrimBullet]], list[str], tuple[int, tuple[str, ...]] | None]:
    """Parse one page's table cells.

    Returns ``(printed column order, standard_by_trim, reasons, header_for_carry)``.
    A brochure sets one equipment grid across a run of pages and prints the trim
    header only on the first of them (the 2026 RAV4 heads pages 5, 9 and 18 and
    continues them on 6-8, 10-15 and 19), so the caller may pass the header from
    the PREVIOUS page as *carried*. It is adopted only for a table with the same
    column count on the immediately following page; anything else re-derives its
    own header or yields nothing.
    """
    reasons: list[str] = []
    rows = [str(r) for r in (table_lines or []) if _CELL_SEP in str(r)]
    if not rows:
        return [], {}, reasons, None

    from collections import Counter

    widths = Counter(len(_split_table_row(r)) for r in rows)
    best: tuple[int, list[str], list[str], dict[str, list[QuotedTrimBullet]]] | None = None
    carry_out: tuple[int, tuple[str, ...]] | None = None
    for ncols, _seen in widths.most_common(3):
        # No minimum row count here on purpose. A brochure often gives the header
        # its own page ahead of the grid — the 2026 RAV4 prints it alone on page
        # 5 and the equipment rows on 6-8 — so a page holding nothing but the
        # header still has to hand it forward.
        if ncols < 3:
            continue
        group = [r for r in rows if len(_split_table_row(r)) == ncols]
        header: list[str] | None = None
        header_at = -1
        for idx, row in enumerate(group):
            cells = _split_table_row(row)[1:]
            if all(_cell_mark(c) is not None for c in cells):
                continue  # a row of markers, not a header
            labels = [_cell_header_trim(c, make, model) for c in cells]
            named = [lbl for lbl in labels if lbl]
            if len(named) >= 2 and len(named) >= 0.6 * len(cells):
                header, header_at = labels, idx
                carry_out = (ncols, tuple(labels))
                break
        if header is None and carried and carried[0] == ncols:
            header, header_at = list(carried[1]), -1
            carry_out = carried
        if header is None:
            reasons.append("cell_grid_no_trim_header_row")
            continue

        trims: list[str] = []
        for lbl in header:
            if lbl and lbl not in trims:
                trims.append(lbl)
        if len(trims) < 2:
            # Every column resolved to the SAME trim. That is what a row of
            # single-letter markers looks like when the letter happens to be a
            # trim name of this model ("S" on an Audi grid, "SS" on a Chevrolet
            # one), and it is not a header.
            reasons.append("cell_grid_header_names_one_trim")
            continue
        std: dict[str, list[QuotedTrimBullet]] = {t: [] for t in trims}
        seen_feature: dict[str, set[str]] = {t: set() for t in trims}
        counted = 0
        for row in group[header_at + 1 :]:
            cells = _split_table_row(row)
            feature = normalize_quoted_line(_join_wrapped_cell(cells[0]))
            marks = [_cell_mark(c) for c in cells[1:]]
            if any(m is None for m in marks) or not any(m == "std" for m in marks):
                continue
            counted += 1
            if not _acceptable_equipment_line(feature) or _CID_ARTIFACT_CELL_RE.search(feature):
                continue
            for trim in trims:
                cols = [i for i, lbl in enumerate(header) if lbl == trim]
                # A grade printed in more than one column (LE FWD / LE AWD) counts
                # as standard only when EVERY one of its columns says so.
                if not all(marks[i] == "std" for i in cols):
                    continue
                key = feature.lower()
                if key in seen_feature[trim]:
                    continue
                seen_feature[trim].add(key)
                std[trim].append(
                    QuotedTrimBullet(text=feature, page=page_no, layout="cell_grid", source=source)
                )
        if counted < 4 or sum(len(v) for v in std.values()) < 3:
            reasons.append("cell_grid_too_few_marker_rows")
            continue
        total = sum(len(v) for v in std.values())
        if best is None or total > best[0]:
            best = (total, trims, header, std)

    if best is None:
        return [], {}, reasons, carry_out
    _total, trims, _header, std = best
    # Direction is NOT decided here: a brochure sets one grid across many pages
    # (the 2026 RAV4 runs pages 5-19) and a single page can be all-standard
    # equipment. The caller merges the pages that print the same column list and
    # resolves base→top once, over the whole grid.
    return trims, std, reasons, carry_out


# ---------------------------------------------------------------------------
# Layout 4: ``column_block`` — "<TRIM> INCLUDES <LOWER> EQUIPMENT PLUS:" spreads
# ---------------------------------------------------------------------------
#
# Nissan sets these as side-by-side columns of comma-separated equipment, which
# the flattened text layer interleaves line for line. They were rejected for
# exactly that reason. The lossless capture keeps per-line cell coordinates, so
# a heading's x-band identifies its own column and the features under it can be
# read without touching the neighbouring one.

_COLUMN_HEADING_RE = re.compile(
    r"^\s*includes\s+(?P<lower>[A-Za-z0-9][A-Za-z0-9®™'\-/ ]{0,28}?)\s+equipment\s+plus\s*:?\s*$",
    re.I,
)
_COLUMN_BLOCK_STOP_RE = re.compile(
    r"^\s*(?:key\s+available\s+package|available\s+packages?|options?|packages?|"
    r"accessories|choose\s+your)\b",
    re.I,
)
# A column can hold several sub-blocks: the Titan XD spread runs "PRO-4X /
# INCLUDES SV EQUIPMENT PLUS:" and then, further down the SAME column, "XD
# PRO-4X Adds: …". Without this stop the second heading is read as part of the
# first block's comma list and comes out welded to the feature beside it —
# "Class IV tow hitch receiver with 4-pin/7-pin wiring harness SL XD Adds: Front
# tow hooks", two claims in one bullet.
_COLUMN_SUBHEADING_RE = re.compile(r"\bAdds\s*:", re.I)
# The x-centre of a wrapped body line may drift from its heading by a few points.
_COLUMN_X_TOLERANCE = 24.0


def _page_cells(page: dict[str, Any]) -> list[tuple[float, float, float, str]]:
    """(top, x0, x1, text) for every cell the capture lane recorded on this page."""
    out: list[tuple[float, float, float, str]] = []
    for line in page.get("lines") or []:
        if not isinstance(line, dict):
            continue
        try:
            top = float(line.get("top"))
        except (TypeError, ValueError):
            continue
        for cell in line.get("cells") or []:
            if not isinstance(cell, (list, tuple)) or len(cell) < 3:
                continue
            try:
                x0, x1 = float(cell[0]), float(cell[1])
            except (TypeError, ValueError):
                continue
            text = str(cell[2] or "").strip()
            if text:
                out.append((top, x0, x1, text))
    out.sort(key=lambda c: (c[0], c[1]))
    return out


def _split_comma_features(text: str) -> list[str]:
    """Split a comma-run equipment list into features. Fragments are dropped, never patched.

    Footnote calls are the hazard. Nissan sets them tight against the comma of
    the feature they belong to ("…Pedestrian Detection,2 Intelligent Lane
    Intervention"), so after the split the digits lead the NEXT fragment and a
    naive strip of leading digits eats real numbers instead — it turns
    "3.5-liter V6 engine" into "5-liter V6 engine" and '18" Machine-finished
    aluminum-alloy wheels' into '" Machine-finished aluminum-alloy wheels'.
    The discriminator is the space: a list separator is printed ", " and leaves
    the fragment starting with a space, a footnote call is printed "," and does
    not.
    """
    out: list[str] = []
    for i, part in enumerate(text.split(",")):
        # Only a fragment that FOLLOWED a comma can start with a footnote call.
        # The first fragment of a block does not, and stripping it turned
        # '20" Machine-finished aluminum-alloy wheels' into '" Machine-finished
        # aluminum-alloy wheels' and "3.5-liter V6 engine" into "5-liter V6".
        if i and re.match(r"^[®™]*\d", part):
            part = re.sub(r"^[®™]*\d+", "", part)
        # A number that is separated from the comma by a space could be either a
        # footnote call left over from a second reference (",6, 7 …") or part of
        # the feature ("4 USB ports"). Nothing here can tell those apart, so the
        # fragment is dropped rather than guessed at. Real leading measurements
        # are unaffected because a unit follows immediately: 12.3", 18", 10-way.
        if i and re.match(r"^\s+\d{1,2}\s", part):
            continue
        cleaned = normalize_quoted_line(part.strip())
        if cleaned:
            out.append(cleaned)
    return out


def _extract_column_block_page(
    page: dict[str, Any],
    *,
    make: str,
    model: str,
    source: str,
) -> tuple[dict[str, list[QuotedTrimBullet]], dict[str, QuotedRungEdge], list[str]]:
    """Parse a coordinate-separated "<TRIM> INCLUDES <LOWER> EQUIPMENT PLUS" spread.

    Second producer of :class:`QuotedRungEdge`; same contract as
    :func:`_extract_adds_to_page`.
    """
    reasons: list[str] = []
    try:
        page_no = int(page.get("page") or 0)
    except (TypeError, ValueError):
        return {}, {}, reasons
    cells = _page_cells(page)
    if not page_no or not cells:
        return {}, {}, reasons

    # A heading is "<TRIM>" immediately left of "INCLUDES <LOWER> EQUIPMENT PLUS:".
    # (top, x0, trim, lower, trim_quote, below_quote)
    headings: list[tuple[float, float, str, str, str, str]] = []
    for i, (top, x0, _x1, text) in enumerate(cells):
        m = _COLUMN_HEADING_RE.match(text)
        if not m:
            continue
        label = ""
        label_text = ""
        for j in range(i - 1, max(-1, i - 4), -1):
            ptop, px0, _px1, ptext = cells[j]
            if abs(ptop - top) > 4.0 or px0 > x0:
                continue
            label = _clean_trim_label(ptext, make=make, model=model)
            label_text = str(ptext).strip()
            break
        lower = _clean_trim_label(m.group("lower"), make=make, model=model)
        if not label or not lower or label.lower() == lower.lower():
            continue
        if not _valid_walk_trim(label, make, model) or not _valid_walk_trim(lower, make, model):
            reasons.append("column_block_header_trim_not_recognised")
            continue
        headings.append(
            (
                top,
                min(x0, cells[max(0, i - 1)][1]),
                label,
                lower,
                label_text,
                str(text).strip(),
            )
        )
    if not headings:
        return {}, {}, reasons

    adds: dict[str, list[QuotedTrimBullet]] = {}
    below: dict[str, QuotedRungEdge] = {}
    for idx, (top, x0, trim, lower, trim_quote, below_quote) in enumerate(headings):
        # The block ends at the next heading in the SAME column, not the next
        # heading on the page: the columns run in parallel down the spread.
        stop_top = float("inf")
        for other_top, other_x0, *_rest in headings[idx + 1 :]:
            if abs(other_x0 - x0) <= _COLUMN_X_TOLERANCE and other_top > top:
                stop_top = min(stop_top, other_top)
        body: list[str] = []
        for ctop, cx0, _cx1, text in cells:
            if ctop <= top or ctop >= stop_top:
                continue
            if abs(cx0 - x0) > _COLUMN_X_TOLERANCE:
                continue
            if (
                _COLUMN_BLOCK_STOP_RE.match(text)
                or _COLUMN_HEADING_RE.match(text)
                or _COLUMN_SUBHEADING_RE.search(text)
            ):
                break
            body.append(text)
        if not body:
            continue
        # Hyphen-aware join: the column wraps '18" Machine-' / 'finished
        # aluminum-alloy wheels' onto two lines and a plain space join would
        # quote it as '18" Machine- finished'.
        joined = body[0]
        for chunk in body[1:]:
            joined = joined + chunk if joined.endswith("-") else f"{joined} {chunk}"
        joined = re.sub(r"\s+", " ", joined).strip()
        kept: list[QuotedTrimBullet] = []
        seen: set[str] = set()
        for feature in _split_comma_features(joined):
            if not _acceptable_equipment_line(feature):
                continue
            key = feature.lower()
            if key in seen:
                continue
            seen.add(key)
            kept.append(
                QuotedTrimBullet(text=feature, page=page_no, layout="column_block", source=source)
            )
        if kept:
            adds.setdefault(trim, []).extend(kept)
            below.setdefault(
                trim,
                QuotedRungEdge(
                    trim=trim,
                    below=lower,
                    page=page_no,
                    layout="column_block",
                    source=source,
                    trim_quote=trim_quote,
                    below_quote=below_quote,
                ),
            )
    return adds, below, reasons


# ---------------------------------------------------------------------------
# Layout 5: ``coord_grid`` — marker columns located by their x position
# ---------------------------------------------------------------------------
#
# GM sets its equipment grids with no ruled feature column, so pdfplumber's
# table extractor returns the marker cells alone ("● | ● | — | —") and the
# feature names never enter ``table_lines`` at all. The lossless capture keeps
# every cell's x range, and on those pages each column's markers sit in a band a
# few points wide, so a marker's column is read off its x position and the
# feature is the text to the left of the first band on the same row. Attribution
# is therefore geometric, not inferred from reading order.

_COORD_MARKER_MAX_WIDTH = 18.0
_COORD_COLUMN_TOLERANCE = 5.0
_COORD_ROW_TOLERANCE = 1.6
_COORD_HEADER_PAD = 8.0


_LEGEND_PAIR_RE = re.compile(
    r"([●•▪◆✓■○◇—–−\-□])\s*(Standard|Available|Optional|Not\s+Available|Package|Included)\b",
    re.I,
)


def brochure_legend_is_ambiguous(text: str) -> bool:
    """True when the document's own legend gives one glyph two meanings.

    GM prints "● Standard ● Available — Not Available": two different discs that
    differ only in fill colour, which the PDF text layer does not carry — both
    come out of pdfplumber as U+25CF. Reading either as "standard" would put a
    3.0L Duramax on an SLE that only OFFERS one. There is no way to tell the two
    apart from the text, so a brochure whose legend collides like this yields no
    grid at all.
    """
    for line in (text or "").splitlines():
        stripped = line.strip()
        if not stripped or len(stripped) > 120:
            continue
        pairs = _LEGEND_PAIR_RE.findall(stripped)
        if len(pairs) < 2:
            continue
        covered = sum(len(g) + len(w) for g, w in pairs)
        if covered < 0.5 * len(stripped):
            continue  # a hyphen inside running prose, not a legend line
        meanings: dict[str, set[str]] = {}
        for glyph, word in pairs:
            meanings.setdefault(glyph, set()).add(re.sub(r"\s+", " ", word).strip().lower())
        if any(len(v) > 1 for v in meanings.values()):
            return True
    return False


def _coord_rows(page: dict[str, Any]) -> list[tuple[float, list[tuple[float, float, str]]]]:
    """Page cells regrouped into rows; lines within 1.6pt of each other are one row."""
    rows: list[tuple[float, list[tuple[float, float, str]]]] = []
    for top, x0, x1, text in _page_cells(page):
        if rows and abs(rows[-1][0] - top) <= _COORD_ROW_TOLERANCE:
            rows[-1][1].append((x0, x1, text))
        else:
            rows.append((top, [(x0, x1, text)]))
    for _top, cells in rows:
        cells.sort(key=lambda c: c[0])
    return rows


def _coord_marker_columns(
    rows: list[tuple[float, list[tuple[float, float, str]]]],
) -> list[tuple[float, float]]:
    """(x0, x1) of every column whose cells are markers on at least six rows."""
    centres: list[list[tuple[float, float]]] = []
    for _top, cells in rows:
        for x0, x1, text in cells:
            if x1 - x0 > _COORD_MARKER_MAX_WIDTH or _cell_mark(text) in (None, "blank"):
                continue
            centre = (x0 + x1) / 2.0
            for bucket in centres:
                if abs((bucket[0][0] + bucket[0][1]) / 2.0 - centre) <= _COORD_COLUMN_TOLERANCE:
                    bucket.append((x0, x1))
                    break
            else:
                centres.append([(x0, x1)])
    cols = [
        (min(x0 for x0, _ in b), max(x1 for _, x1 in b))
        for b in centres
        if len(b) >= 6
    ]
    cols.sort()
    return cols


def _coord_header_label(
    rows: list[tuple[float, list[tuple[float, float, str]]]],
    band: tuple[float, float],
    first_marker_top: float,
    *,
    make: str,
    model: str,
) -> str:
    """Trim named above *band*, read back from a rotated or stacked header."""
    lo, hi = band[0] - _COORD_HEADER_PAD, band[1] + _COORD_HEADER_PAD
    stack: list[tuple[float, str]] = []
    for top, cells in rows:
        if top >= first_marker_top:
            break
        for x0, x1, text in cells:
            if x0 >= lo and x1 <= hi:
                stack.append((top, text))
    if not stack:
        return ""
    down = "".join(t for _y, t in sorted(stack, key=lambda s: s[0]))
    up = "".join(t[::-1] for _y, t in sorted(stack, key=lambda s: -s[0]))
    for candidate in (down, up, down[::-1]):
        trim = _cell_header_trim(candidate, make, model)
        if trim:
            return trim
    return ""


def _extract_coord_grid_page(
    page: dict[str, Any],
    *,
    make: str,
    model: str,
    source: str,
) -> tuple[list[str], dict[str, list[QuotedTrimBullet]], list[str]]:
    """Parse a grid whose columns are identified by x position. Returns (printed order, std, reasons)."""
    reasons: list[str] = []
    try:
        page_no = int(page.get("page") or 0)
    except (TypeError, ValueError):
        return [], {}, reasons
    rows = _coord_rows(page)
    if not page_no or len(rows) < 8:
        return [], {}, reasons
    columns = _coord_marker_columns(rows)
    if len(columns) < 3:
        return [], {}, reasons

    first_marker_top = float("inf")
    for top, cells in rows:
        if any(
            _cell_mark(text) not in (None, "blank") and x1 - x0 <= _COORD_MARKER_MAX_WIDTH
            for x0, x1, text in cells
        ):
            first_marker_top = top
            break
    if first_marker_top == float("inf"):
        return [], {}, reasons

    labels = [
        _coord_header_label(rows, band, first_marker_top, make=make, model=model)
        for band in columns
    ]
    named = [lbl for lbl in labels if lbl]
    if len(set(named)) < 2 or len(named) < 0.6 * len(columns):
        reasons.append("coord_grid_no_trim_header_row")
        return [], {}, reasons

    printed: list[str] = []
    for lbl in labels:
        if lbl and lbl not in printed:
            printed.append(lbl)
    feature_limit = min(band[0] for band in columns) - 4.0

    std: dict[str, list[QuotedTrimBullet]] = {t: [] for t in printed}
    seen: dict[str, set[str]] = {t: set() for t in printed}
    marker_rows = 0
    for top, cells in rows:
        if top < first_marker_top:
            continue
        marks: dict[int, str] = {}
        for x0, x1, text in cells:
            if x1 - x0 > _COORD_MARKER_MAX_WIDTH:
                continue
            mark = _cell_mark(text)
            if mark in (None, "blank"):
                continue
            for i, (bx0, bx1) in enumerate(columns):
                if bx0 - _COORD_COLUMN_TOLERANCE <= x0 and x1 <= bx1 + _COORD_COLUMN_TOLERANCE:
                    marks[i] = mark
                    break
        # Every column has to have spoken on this row. A partial row means the
        # page wrapped one grid row over two lines and half of it is missing.
        if len(marks) != len(columns) or "std" not in marks.values():
            continue
        marker_rows += 1
        feature = normalize_quoted_line(
            " ".join(text for x0, x1, text in cells if x1 <= feature_limit)
        )
        if not _acceptable_equipment_line(feature) or _CID_ARTIFACT_CELL_RE.search(feature):
            continue
        for i, trim in enumerate(labels):
            if not trim or marks.get(i) != "std":
                continue
            # A grade printed in more than one column is standard only when all
            # of its columns say so.
            if not all(marks.get(j) == "std" for j, other in enumerate(labels) if other == trim):
                continue
            key = feature.lower()
            if key in seen[trim]:
                continue
            seen[trim].add(key)
            std[trim].append(
                QuotedTrimBullet(text=feature, page=page_no, layout="coord_grid", source=source)
            )
    if marker_rows < 6 or sum(len(v) for v in std.values()) < 4:
        reasons.append("coord_grid_too_few_marker_rows")
        return [], {}, reasons
    return printed, std, reasons


def _bullet_priority(text: str) -> int:
    """Higher = bigger ticket. Universal equipment sorts last but is not dropped."""
    if _UNIVERSAL_EQUIPMENT_RE.match(text or ""):
        return -10
    for score, pat in _BIG_TICKET_PATTERNS:
        if pat.search(text or ""):
            return score
    return 0


def _rank_bullets(
    bullets: list[QuotedTrimBullet], limit: int | None = 12
) -> list[QuotedTrimBullet]:
    """Big-ticket first, ties broken by printed order. ``limit=None`` ranks all.

    Ranking NEVER drops a bullet for being low-priority — universal equipment
    sinks to the bottom of the list, it is not deleted. Only ``limit`` truncates.
    """
    ordered = sorted(
        enumerate(bullets),
        key=lambda pair: (-_bullet_priority(pair[1].text), pair[0]),
    )
    return [b for _, b in ordered] if limit is None else [b for _, b in ordered[:limit]]


#: How many bullets a rung may show. Applied AFTER the display gate — see
#: ``extract_trim_walk``.
_ADDS_PER_TRIM_LIMIT = 12


# The ordering signals a rung's position may be attributed to, strongest first.
# ``brochure_extract.LADDER_ORDER_STORES`` maps each of these to the store whose
# rule decides whether it may be drawn as a hierarchy at all; that table is where
# the policy lives, this is what the parser is able to emit.
#
#   adds_to_edge      the OEM printed "<TRIM> / Adds to <LOWER>" on a page we
#                     hold. It states the relation itself, in its own words, at a
#                     place we can name. This is the only one that PROVES a
#                     position.
#   page_sequence     the rung's trim-walk page falls at this point in the book.
#                     Brochures do walk a lineup in order, but that is a habit of
#                     the format, not a statement — corroborating only, and it is
#                     what separates two rungs the edges make siblings (2020
#                     Durango: R/T and Citadel BOTH add to GT).
#   printed_sequence  the rung is this column of a printed equipment grid. Same
#                     strength as ``page_sequence``: the column order is the
#                     document's, but which end is the base grade is read off
#                     mark counts by ``_cell_grid_direction`` — our inference,
#                     not the OEM's words.
#   unproven          nothing in the document places this rung.


def order_rungs_with_basis(
    edges: dict[str, QuotedRungEdge],
    adds: dict[str, Any],
    *,
    page_by_trim: dict[str, int] | None = None,
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    """Order trims base→top, and say for each one what put it there.

    The order is built from the quoted "<TRIM> adds to <LOWER>" edges alone.
    Those edges are a FOREST, not a chain, and the difference matters:

    * 2020 Durango — one tree rooted at SXT. R/T and Citadel BOTH "add to GT",
      so the document places each of them above GT and says nothing about which
      of the two is higher. Page order separates them, recorded as
      ``tiebreak="page_sequence"`` on both.
    * 2021 Challenger — TWO trees, rooted at R/T Scat Pack (p.46) and SRT
      Hellcat (p.48). Nothing in the book relates one tree to the other, so
      sorting every rung by its depth alone interleaved them and put SRT Hellcat
      below R/T Scat Pack Widebody. Whole trees are therefore placed by the page
      they start on, and every rung in a document with more than one tree carries
      ``cross_component_order="page_sequence"`` to say so.

    Returns ``(order_base_to_top, basis_by_trim)``.
    """
    pages = dict(page_by_trim or {})
    names: list[str] = []
    for t in adds:
        if t not in names:
            names.append(t)
    for edge in edges.values():
        if edge.below not in names:
            names.append(edge.below)

    def depth(name: str, seen: frozenset[str] = frozenset()) -> int:
        edge = edges.get(name)
        if not edge or edge.below in seen or edge.below == name:
            return 0
        return 1 + depth(edge.below, seen | {name})

    depths = {n: depth(n) for n in names}
    # A rung is placed by an edge if it names a lower rung, or if a higher rung
    # names it. Both readings are the OEM's own sentence about this rung.
    named_by_higher = {e.below for e in edges.values()}

    # Connected components of the UNDIRECTED edge graph: one tree of the walk.
    component: dict[str, str] = {}

    def _root(n: str) -> str:
        seen: set[str] = set()
        cur = n
        while True:
            edge = edges.get(cur)
            if not edge or edge.below == cur or edge.below in seen:
                return cur
            seen.add(cur)
            cur = edge.below

    for n in names:
        component[n] = _root(n)

    def _page(n: str) -> int:
        return pages.get(n, 10**6)

    comp_key: dict[str, tuple[int, int]] = {}
    for n in names:
        root = component[n]
        cand = (_page(n), names.index(n))
        if root not in comp_key or cand < comp_key[root]:
            comp_key[root] = cand

    order = sorted(
        names,
        key=lambda n: (comp_key[component[n]], depths[n], _page(n), names.index(n)),
    )
    multi_component = len({component[n] for n in names}) > 1

    peers: dict[tuple[str, int], list[str]] = {}
    for n in names:
        peers.setdefault((component[n], depths[n]), []).append(n)

    basis: dict[str, dict[str, Any]] = {}
    for n in order:
        edge = edges.get(n)
        entry: dict[str, Any]
        if edge is not None:
            entry = {
                "basis": "adds_to_edge",
                "trim": n,
                # This rung's own page names the grade it sits above.
                "direction": "states_its_baseline",
                "edge": edge.as_dict(),
            }
        elif n in named_by_higher:
            # No outbound edge, but a higher rung's page names it as ITS
            # baseline, which places this rung just as directly.
            higher = next(e for e in edges.values() if e.below == n)
            entry = {
                "basis": "adds_to_edge",
                "trim": n,
                "direction": "named_as_baseline_by",
                "edge": higher.as_dict(),
            }
        elif n in pages:
            entry = {"basis": "page_sequence", "trim": n, "page": pages[n]}
        else:
            entry = {"basis": "unproven", "trim": n}
        if entry["basis"] == "adds_to_edge":
            tied = [m for m in order if m != n and m in peers[(component[n], depths[n])]]
            if tied:
                # Siblings: the edges put them at the same height in the same
                # tree and say nothing about which of them is higher.
                entry["tiebreak"] = "page_sequence" if n in pages else "read_order"
                entry["tied_with"] = tied
            if multi_component:
                # This rung's tree was placed against the other trees by where it
                # starts in the book, which the OEM never stated.
                entry["cross_component_order"] = "page_sequence"
                entry["component_root"] = component[n]
        basis[n] = entry
    return order, basis


def extract_trim_walk(
    data: dict[str, Any],
    *,
    make: str,
    model: str,
    year: int,
) -> TrimWalkExtract:
    """Quote a trim walk out of one ``derived/brochure_text`` payload.

    Every bullet returned carries the brochure_text file and PDF page it came
    from. Pages whose layout cannot be read without guessing yield nothing.
    """
    from backend.enrichment.trim_spec_extractor import is_displayable_trim_bullet

    ck = catalog_key(year, make, model)
    source = f"derived/brochure_text/{ck.replace('|', '__')}.json"
    out = TrimWalkExtract(catalog_key=ck, year=year, make=make, model=model)

    pages = [p for p in (data.get("pages") or []) if isinstance(p, dict)]
    if not pages:
        out.reject_reasons.append("no_pages_in_brochure_text")
        return out

    adds_acc: dict[str, list[QuotedTrimBullet]] = {}
    below_acc: dict[str, QuotedRungEdge] = {}
    # First page each trim's own walk block was printed on. Corroborating
    # evidence only: it separates rungs the quoted edges leave tied, and places
    # rungs no edge covers.
    first_page_by_trim: dict[str, int] = {}
    grid_order: list[str] = []
    grid_std: dict[str, list[QuotedTrimBullet]] = {}
    pages_used: set[int] = set()

    # printed column tuple -> merged standard-equipment map for that grid
    cell_grids: dict[tuple[str, ...], dict[str, list[QuotedTrimBullet]]] = {}
    cell_pages: dict[tuple[str, ...], set[int]] = {}
    cell_layouts: dict[tuple[str, ...], set[str]] = {}

    # One ambiguous legend disqualifies every grid in the document: the same
    # glyph means two things throughout, not only on the page that prints the key.
    grids_allowed = not brochure_legend_is_ambiguous(
        "\n".join(str(p.get("text") or "") for p in pages)
    )
    if not grids_allowed:
        out.reject_reasons.append("legend_glyph_means_both_standard_and_available")

    carried: tuple[int, tuple[str, ...]] | None = None
    prev_page_no = -99
    for pg in pages:
        page_no = int(pg.get("page") or 0)
        if not page_no:
            continue
        if page_no != prev_page_no + 1:
            carried = None  # a header only continues onto the very next page
        prev_page_no = page_no
        # Cell grids first: their column attribution comes from the PDF's own
        # table cells, so it does not depend on reading character offsets.
        printed: list[str] = []
        std: dict[str, list[QuotedTrimBullet]] = {}
        creasons: list[str] = []
        if grids_allowed:
            printed, std, creasons, carried = _extract_cell_grid_page(
                page_no,
                pg.get("table_lines") or [],
                make=make,
                model=model,
                source=source,
                carried=carried,
            )
        out.reject_reasons.extend(creasons)
        layout_name = "cell_grid"
        if grids_allowed and not (printed and std):
            printed, std, coreasons = _extract_coord_grid_page(
                pg, make=make, model=model, source=source
            )
            out.reject_reasons.extend(coreasons)
            layout_name = "coord_grid"
        if printed and std:
            key = tuple(printed)
            merged = cell_grids.setdefault(key, {t: [] for t in printed})
            for trim, bullets in std.items():
                have = {b.text.lower() for b in merged.setdefault(trim, [])}
                merged[trim].extend(b for b in bullets if b.text.lower() not in have)
            cell_pages.setdefault(key, set()).add(page_no)
            cell_layouts.setdefault(key, set()).add(layout_name)
            continue

        cadds, cbelow, colreasons = _extract_column_block_page(
            pg, make=make, model=model, source=source
        )
        out.reject_reasons.extend(colreasons)
        if cadds:
            for trim, bullets in cadds.items():
                adds_acc.setdefault(trim, []).extend(bullets)
            below_acc.update(cbelow)
            for _t in cadds:
                first_page_by_trim.setdefault(_t, page_no)
            pages_used.add(page_no)
            if "column_block" not in out.layouts:
                out.layouts.append("column_block")
            continue

        text = str(pg.get("text") or "")
        if not text.strip():
            continue
        adds, below, reasons = _extract_adds_to_page(
            page_no, text, make=make, model=model, source=source
        )
        out.reject_reasons.extend(reasons)
        if adds:
            for trim, bullets in adds.items():
                adds_acc.setdefault(trim, []).extend(bullets)
            below_acc.update(below)
            for _t in adds:
                first_page_by_trim.setdefault(_t, page_no)
            pages_used.add(page_no)
            if "adds_to_block" not in out.layouts:
                out.layouts.append("adds_to_block")
            continue
        if grid_std:
            continue
        order, std, greasons = _extract_marker_grid_page(
            page_no, text, make=make, model=model, source=source
        )
        out.reject_reasons.extend(greasons)
        if order and std:
            grid_order, grid_std = order, std
            pages_used.add(page_no)
            if "marker_grid" not in out.layouts:
                out.layouts.append("marker_grid")

    if cell_grids and not adds_acc:
        key = max(cell_grids, key=lambda k: sum(len(v) for v in cell_grids[k].values()))
        merged = cell_grids[key]
        direction = _cell_grid_direction(
            list(key), {t: len(v) for t, v in merged.items()}, make, model
        )
        if direction == 0:
            out.reject_reasons.append("cell_grid_column_order_unverifiable")
        else:
            grid_order = list(key) if direction == 1 else list(reversed(key))
            grid_std = merged
            pages_used |= cell_pages.get(key, set())
            out.layouts.extend(sorted(cell_layouts.get(key) or {"cell_grid"}))

    order_basis: dict[str, dict[str, Any]] = {}
    if adds_acc:
        order, order_basis = order_rungs_with_basis(
            below_acc, adds_acc, page_by_trim=first_page_by_trim
        )
        final_adds = adds_acc
    elif grid_std:
        order = grid_order
        # A grid states no relation between its columns; the only ordering fact
        # is that they were PRINTED in this sequence, and which end is the base
        # grade came from ``_cell_grid_direction`` reading mark counts — our
        # inference. Recorded as corroboration, never as a quoted edge.
        for pos, trim in enumerate(order):
            order_basis[trim] = {
                "basis": "printed_sequence",
                "trim": trim,
                "source": source,
                "column_index": pos,
                "pages": sorted(pages_used),
                "layout": sorted(out.layouts)[0] if out.layouts else "cell_grid",
            }
        final_adds = {}
        for i, trim in enumerate(order):
            if i == 0:
                continue
            # Subtract EVERY lower rung, not just the one immediately below.
            # The rungs can still be re-sorted by ``luxury_rank`` downstream —
            # ``resolve_trim_ladder`` does that whenever the overlay carries no
            # document-derived order — and that ordering can differ from the one
            # the brochure printed (the 2026 RAV4 grid runs LE, XLE Premium, SE,
            # XSE, Limited; luxury_rank puts SE below XLE). Subtracting the union
            # means a bullet on this rung is standard on NO grade the grid prints
            # below it, whichever of the two orders the page ends up using.
            lower = {
                b.text.lower()
                for prior in order[:i]
                for b in grid_std.get(prior, [])
            }
            delta = [b for b in grid_std.get(trim, []) if b.text.lower() not in lower]
            if delta:
                final_adds[trim] = delta
                # No ``below_acc`` entry is written here. A grid column's
                # neighbour is not an "adds to" statement, and recording it as
                # one would launder a printed sequence into a quoted edge.
        out.standard_by_trim = {
            t: [b.text for b in _rank_bullets(grid_std.get(t, []), limit=24)] for t in order
        }
    else:
        if not out.reject_reasons:
            out.reject_reasons.append("no_supported_layout_on_any_page")
        return out

    for trim, bullets in final_adds.items():
        # Rank EVERY candidate, then gate, then truncate. Ranking first and
        # truncating to 12 before the gate ran meant a gate rejection inside the
        # top 12 left the rung one bullet short while a perfectly good 13th
        # candidate sat unused. Measured over the 182 brochures whose PDF we
        # still hold, that ordering cost 1 bullet — small, but it was a bullet
        # lost to the order of two lines rather than to any rule.
        kept: list[QuotedTrimBullet] = []
        for b in _rank_bullets(bullets, limit=None):
            if not is_displayable_trim_bullet(b.text):
                out.gate_rejected += 1
                continue
            kept.append(b)
            if len(kept) >= _ADDS_PER_TRIM_LIMIT:
                break
        if kept:
            out.adds_by_trim[trim] = [b.text for b in kept]
            out.provenance[trim] = [b.as_dict() for b in kept]

    # The brochure's own base→top order, not "rungs with bullets first": the
    # overlay's trim list is the only record of the lineup the OEM printed.
    out.trims_available = list(order)
    # The ORDER, and why. Until 2026-08-02 the "<TRIM> adds to <LOWER>" edges
    # were parsed, used to sort this list, and then dropped on the floor: the
    # overlay recorded the sequence but not one word of the OEM sentence that
    # justified it, so every consumer downstream was free to re-sort it by a
    # hand-maintained rank table — and did.
    out.rung_edges = [below_acc[t].as_dict() for t in order if t in below_acc]
    out.order_basis = {t: order_basis[t] for t in order if t in order_basis}
    out.pages_used = sorted(pages_used)
    if len(out.adds_by_trim) < 2:
        out.reject_reasons.append("fewer_than_two_trims_with_quoted_adds")
    return out


_MULTI_COLUMN_WALK_PROBES: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "toyota_style_multi_column_comparison",
        re.compile(r"adds to or replaces features", re.I),
    ),
    (
        "includes_lower_equipment_plus_multi_column",
        re.compile(r"\bincludes\s+\S+\s+equipment\s+plus\b", re.I),
    ),
)


def classify_brochure_failure(
    data: dict[str, Any],
    extract: TrimWalkExtract,
    *,
    make: str,
    model: str,
) -> str:
    """Bucket a brochure that yielded no usable trim walk, for the coverage report."""
    if extract.usable:
        return "ok"
    warnings = set(data.get("warnings") or [])
    pages = [p for p in (data.get("pages") or []) if isinstance(p, dict)]
    if "no_text_extracted" in warnings or not pages:
        return "no_text_layer_in_pdf"
    text = "\n".join(str(p.get("text") or "") for p in pages)
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return "no_text_layer_in_pdf"

    if len(extract.adds_by_trim) == 1:
        return "single_trim_only_no_walk"

    # A grid row is a line carrying a RUN of markers, one per column. Lines with
    # a single stray dash are prose, not a grid.
    grid_rows = sum(1 for ln in lines if any(len(g) >= 2 for g in _grid_marker_groups(ln)))
    if grid_rows >= 6:
        if "grid_column_order_unverifiable" in extract.reject_reasons:
            return "grid_found_but_column_order_unverifiable"
        if "grid_header_width_disagrees_with_rows" in extract.reject_reasons:
            return "grid_header_width_disagrees_with_rows"
        header_lines = sum(1 for ln in lines[:80] if _grid_header_candidates(ln, make, model))
        if header_lines == 0:
            return "grid_rows_present_no_trim_header_row"
        return "grid_header_present_rows_unreadable"

    bullet_lines = [ln for ln in lines if _BULLET_GLYPH_RE.search(ln)]
    if len(bullet_lines) >= 8 and _bullet_columns_are_ambiguous(bullet_lines):
        for name, probe in _MULTI_COLUMN_WALK_PROBES:
            if probe.search(text):
                return name
        return "bullets_present_but_multi_column"
    for name, probe in _MULTI_COLUMN_WALK_PROBES:
        if probe.search(text):
            return name
    if len(bullet_lines) >= 8:
        return "single_column_bullets_but_no_trim_header"

    # The derived corpus only kept pages that tripped the trim-hint regex, so a
    # brochure can be missing its equipment grid entirely.
    page_count = int(data.get("page_count") or 0)
    if page_count >= 8 and len(pages) < 0.4 * page_count:
        return "trim_hint_page_filter_dropped_most_of_pdf"

    if len(lines) < 30:
        return "too_few_text_pages_captured"
    return "prose_and_photography_pages_only"
