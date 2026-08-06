"""Trim-add bullet hygiene: fragments, prose, spec-table artifacts, sanitisation."""
from __future__ import annotations

import re


_GENERIC_TRIM_ADD_RE = re.compile(
    r"(?:equipment and features|factory equipment and features|equipment and packaging)"
    r".*(?:typical of|typical .+ trim\b)",
    re.I,
)


_LADDER_PLACEHOLDER_PROSE_RE = re.compile(
    r"(?:"
    r"mid-level trim between\b"
    r"|rugged or adventure-oriented\b"
    r"|builds on .+ with additional comfort, technology, or appearance upgrades"
    r"|top of this trim lineup\b"
    r"|entry rung on this ladder\b"
    r"|typically the most equipment\b"
    r"|fewer optional upgrades than higher trims\b"
    r"|positioned below .+ on the .+ trim ladder\b"
    r"|positioned above .+ on the .+ trim ladder\b"
    r"|highest trim level offered on the\b"
    r"|base trim level on the\b"
    r"|adds premium audio, larger display, and comfort upgrades over the mid trim\b"
    r"|sits below .+ with fewer premium features\b"
    r")",
    re.I,
)


# Wikipedia model-history prose that the "complete options" CSV scrape shipped
# into per-trim cells. It is narrative about the LINEUP, not equipment on THIS
# trim, and it lands on whichever trim shared a spreadsheet row — so it asserts
# things about the car in front of the shopper that were never true of it.
_LINEUP_NARRATIVE_RE = re.compile(
    r"(?:"
    r"\bin terms of trim-level changes\b"
    r"|\badded to the lineup was\b"
    r"|\btrim (?:level )?(?:has been|was|is) replaced\b"
    r"|\bwas dropped from the\b|\bwas discontinued\b"
    r"|\bfor the first time since \d{4}\b"
    r"|\bis (?:optional|standard) on higher-end trims\b"
    r"|\bonly available (?:with|on) the \w+ trim\b"
    r"|\bthe name derived from\b"
    r"|\bin place of the \w+ logo\b"
    r")",
    re.I,
)

# A spec-table cell that lost its leading displacement number in the scrape
# ("3.5L PowerBoost twin-turbo V6" → "L PowerBoost twin-turbo V6"). What is left
# is an engine claim we cannot even read back, so it never gets shown.
_ORPHANED_DISPLACEMENT_RE = re.compile(
    r"^L\s+\S+.*\b(?:V-?\d\b|I-?\d\b|turbo\w*|supercharged|hybrid|diesel|engine)\b",
    re.I,
)

# "Gasoline Hybrid:" — a section heading from a spec table, not a feature.
_SECTION_HEADING_RE = re.compile(r"^[A-Za-z0-9 /&'\-]{3,40}:$")

# Encyclopedia infobox cells that describe the MODEL's powertrain catalogue and
# were stamped onto whichever trim shared the row. Proven wrong in production:
# the 2025 F-150 STX rung carried "Electric motor 35 kW (47 hp) BorgWarner
# HVH250 (hybrid)" — the STX has never been offered as a hybrid.
_POWERTRAIN_INFOBOX_RE = re.compile(
    r"^(?:"
    r"electric motors?\b(?:.*?\b(?:kw|hp)\b|\s+(?:permanent magnet|induction|synchronous)\b)"
    # Wikipedia's "Layout" field. Every trim of the model shares the layout, so
    # it is never something a rung adds — and it rendered as an *engine* claim
    # ("Engine Options: Front-engine, rear-wheel-drive") on 674 rungs. The
    # "<x>-engine layout" spelling is included because the 2019 Corvette CSV put
    # "Mid-engine layout with mid-mounted V-8" on C7 rungs, which were front-engine.
    # "Rear mid-engine, rear-wheel-drive" is the C8 Corvette's infobox spelling,
    # so the position qualifier in front of the hyphenated term is optional.
    r"|(?:(?:front|mid|rear)\s+)?(?:front|mid|rear)-engine(?:\s+or\s+motor)?\s*(?:[,/]|\s*layout\b)"
    # A gearbox code from the EPA transmissionOptions column ("Automatic (S8)",
    # "Automatic (variable gear ratios)", "e-CVT (hybrid)") is a field value,
    # not equipment this rung adds over the one below it.
    r"|(?:\d+-speed )?(?:e-?)?(?:automatic|manual|cvt)\s*\([A-Za-z0-9 \-/]{1,24}\)\.?$"
    r")",
    re.I,
)

# Wikipedia's infobox "Engine" field is written as the bare word followed
# straight by a displacement ("Engine 351 cu in (5.8 L) 351M V8"). Our own
# producers always emit the EPA column heading with a colon ("Engine Options:
# …"), so a colon-less "Engine <number>" head is a scrape signature — and it is
# how a 1975 Bronco V8 came to be listed on 2026 Bronco Sport rungs.
_WIKI_ENGINE_INFOBOX_RE = re.compile(r"^engines?\s+\d", re.I)

# Cubic-inch displacement went out of OEM use around 1980; the trim ladder only
# runs on model year 2010 and later, so a "cu in" figure is provably a quote
# about a different generation of the car.
_CUBIC_INCH_RE = re.compile(r"\b\d{2,4}\s*cu\.?\s*in\.?\b", re.I)

# A reference headline the scrape lifted out of the article's citation list
# ('"Toyota Adds to Prius Lineup With Smallest Hybrid".').
_CITATION_TITLE_RE = re.compile(r'^["“].{6,}["”]\s*\.?$')

# The EPA ``fuelType`` column value on its own. It names the energy source, not
# anything the trim adds.
_EPA_FUEL_FIELD_RE = re.compile(
    r"^(?:EV|FCV|PHEV|HEV|BEV)?\s*(?:\([A-Z]{2,6}\)\s*)?"
    r"(?:electricity|hydrogen|regular gasoline|premium gasoline|midgrade gasoline)\.?$",
    re.I,
)

# Wikipedia's "Assembly" infobox field ("Malaysia: Kulim, Kedah (Hyundai-Sime
# Darby Motors, hybrid only)") — a factory list, not equipment.
_PLANT_LOCATION_RE = re.compile(
    r"^[A-Z][A-Za-z .'\-]{2,24}:\s*[A-Z][A-Za-z .'\-]{2,24},\s*[A-Z]",
)

# The chassis layout, wherever it sits in the line. ``_POWERTRAIN_INFOBOX_RE``
# only sees it at the head, which missed the abbreviated infobox spelling
# "FR (Front-engine, rear-wheel drive)" on Escalade ESV rungs. Every trim of a
# model shares its layout, so this is never something one rung adds.
_CHASSIS_LAYOUT_RE = re.compile(
    r"(?:(?:front|mid|rear)\s+)?(?:front|mid|rear)-engine(?:\s+or\s+motor)?\s*(?:[,/]|\s+layout\b)",
    re.I,
)

# "(268 kW; 365 PS)" — the dual metric conversion after a US horsepower figure
# is an encyclopedia house style; no US OEM publishes PS. It marks the row as a
# lift from an engine-family table, which is how the 2007–2013 LY6 Vortec 6000
# came to be listed on 2025 Silverado 1500 rungs.
_METRIC_CONVERSION_RE = re.compile(
    r"\([^()]*\bkW\b[^()]*;[^()]*\bPS\b[^()]*\)",
)

# "RV3RV4RV5RV6RS1 (electric)" — a column of platform codes that lost its cell
# boundaries in the scrape. A run of eight or more alphanumerics with letters in
# it but no vowel is not a word and not a name a shopper can act on.
_UNREADABLE_CODE_RUN_RE = re.compile(r"[A-Za-z0-9]{8,}")


def _has_unreadable_code_run(text: str) -> bool:
    for tok in _UNREADABLE_CODE_RUN_RE.findall(str(text or "")):
        if re.search(r"[aeiouAEIOU]", tok):
            continue
        if len(re.findall(r"[A-Za-z]", tok)) >= 2:
            return True
    return False

# Spec-table column headings a producer may prepend to its own value, and which
# defeat every ``^``-anchored artifact test below. Stripping them is only used to
# re-run those tests on the body — the bullet itself is never rewritten here.
_SPEC_LABEL_PREFIX_RE = re.compile(
    r"^(?:engine options?|engines?|powertrain options?|powertrain|layout"
    r"|drivetrain options?|drivetrain|transmission options?|transmission"
    r"|fuel type|body style|audio system layout|screen size|displacement"
    r"|hybrid engine|hybrid system net power|interior materials)"
    r"\s*:?\s+(?=\S)",
    re.I,
)

# Wording that makes a phrase an engine claim. Used only to check a value
# against the label above it: an "Engine Options" cell whose body contains none
# of this is not a quote about an engine, whatever the CSV column said.
_ENGINE_CONTENT_RE = re.compile(
    r"\b\d(?:\.\d)?\s?[lL]\b|\bv-?\d{1,2}\b|\bi-?\d\b|\bh-?\d\b|\bflat-\d\b|\binline-\d\b"
    r"|\b\d{2,4}\s?(?:hp|kw|bhp|ps)\b|\blb-?ft\b|\bhybrid\b|\belectric\b|\bdiesel\b"
    r"|\bturbo\w*\b|\bsupercharged\b|\bcylinders?\b|\bkwh\b|\bcc\b|\bmotors?\b|\bcu\.?\s*in\b",
    re.I,
)
# The colon is required: it is what makes the head a COLUMN LABEL rather than
# the first word of an equipment name. Without it this rule read "Engine oil
# cooler" as an engine cell with no engine in it and dropped a real bullet.
_ENGINE_LABEL_RE = re.compile(
    r"^(?:engine options?|engines?|hybrid engine|powertrain options?|powertrain)\s*:\s*(?=\S)",
    re.I,
)


def strip_spec_label_prefix(text: str) -> str:
    """Drop a leading spec-table column heading, keeping the value it labelled.

    Only for re-running the ``^``-anchored artifact tests on the body: the
    producers stamp the column name onto the cell, which is why "Front-engine,
    rear-wheel-drive" slipped past ``_POWERTRAIN_INFOBOX_RE`` once it arrived as
    "Engine Options: Front-engine, rear-wheel-drive".
    """
    return spec_label_prefix_strips(text)[-1]


def spec_label_prefix_strips(text: str) -> list[str]:
    """The original text and every successive label-strip of it.

    Callers must test EVERY step, not just the last: "Engine Options: Engine
    5.0 L S85 …" needs one strip to expose the Wikipedia "Engine <number>"
    infobox head, and a second strip removes that head again — testing only the
    fully stripped form let a BMW M5 S85 V10 through on three model years.
    """
    s = str(text or "").strip()
    out = [s]
    for _ in range(3):
        stripped = _SPEC_LABEL_PREFIX_RE.sub("", s, count=1).strip()
        if stripped == s or not stripped:
            break
        s = stripped
        out.append(s)
    return out


def engine_label_without_engine_content(text: str) -> bool:
    """True when a cell labelled as an engine does not describe one.

    A quote is only usable as evidence of the thing its label names. This is what
    separates "Engine Options: LV3 EcoTec3 4.3 L V6" (a real quote of an engine)
    from "Engine Options: Years Engine Power Torque" and "Engine Options: Review
    standard and optional interior, exterior, mechanical comfort, …" — the same
    CSV column, but the scrape put a table header and a page banner in it.
    """
    s = str(text or "").strip()
    if not _ENGINE_LABEL_RE.match(s):
        return False
    body = _ENGINE_LABEL_RE.sub("", s, count=1).strip()
    if not body:
        return True
    return not _ENGINE_CONTENT_RE.search(body)


# A model-year window the bullet states about itself: "(2020–2021)",
# "(2003–present)", "(China, electric, 2022–2025)". Curated ladders use it to
# scope a feature to the years it was offered; encyclopedia infoboxes use it to
# date a whole generation. Either way the window is quoted, not inferred, so a
# car outside it must not be shown the bullet.
# Three spellings occur in the corpus: a full range "(2020–2021)", an
# abbreviated end "(2017-20)", and an open-ended start "(2024+)" / "(2003–present)".
_STATED_YEAR_WINDOW_RE = re.compile(
    r"\((?:[^()]*?[,\s])?((?:19|20)\d{2})\s*"
    r"(?:[–—-]\s*((?:19|20)\d{2}|\d{2}|present)|(\+))[^()]*\)",
    re.I,
)


def stated_year_window(text: str) -> tuple[int, int] | None:
    """The model-year range a bullet states about itself, or None."""
    m = _STATED_YEAR_WINDOW_RE.search(str(text or ""))
    if not m:
        return None
    try:
        lo = int(m.group(1))
    except (TypeError, ValueError):
        return None
    if m.group(3):  # "(2024+)"
        return (lo, 9999)
    tail = (m.group(2) or "").lower()
    if tail == "present":
        hi = 9999
    elif len(tail) == 2:
        # "(2017-20)" — the end year shares the decade prefix of the start.
        hi = (lo // 100) * 100 + int(tail)
    else:
        hi = int(tail)
    if hi < lo:
        return None
    return (lo, hi)


# A parenthesised market qualifier means the row describes a car sold somewhere
# else: "Everus VE-1 (China, electric)" and "Ciimo X-NV/M-NV (China, electric)"
# are Honda's Chinese HR-V rebadges and were rendering on US HR-V rungs.
_FOREIGN_MARKET_RE = re.compile(
    r"\((?:[^()]{0,40}?[,;]\s*)?(?:china|japan|europe|korea|australia|brazil|india"
    r"|mexico|malaysia|taiwan|thailand|russia|indonesia|philippines)\b[^()]{0,40}\)"
    r"|\b(?:china|japan|europe|korea|australia|brazil|india|mexico|malaysia|taiwan"
    r"|thailand)[- ](?:only|market|domestic|spec)\b",
    re.I,
)


def is_foreign_market_variant(text: str) -> bool:
    """True when the line is scoped to a market this listing is not sold in."""
    return bool(_FOREIGN_MARKET_RE.search(str(text or "")))


def bullet_year_window_excludes(text: str, year: int | None) -> bool:
    """True when the bullet's own stated year window does not cover ``year``.

    Production had "Uconnect 3 with 5-inch display (2020–2021)" on 2022–2026
    Jeep Gladiators and "456 hp twin-turbo V8 with xDrive AWD (2019–2020)" on
    2021–2026 BMW X5s: the source stated when the content applied and the
    renderer ignored it.
    """
    if year is None:
        return False
    window = stated_year_window(text)
    if not window:
        return False
    lo, hi = window
    return not (lo <= int(year) <= hi)


def is_encyclopedia_spec_artifact(text: str) -> bool:
    """True for a cell that is an encyclopedia page structure, not a spec.

    Every branch is anchored to a line that reached a shopper: an infobox field
    (layout, engine, assembly plant), a spec-table column heading, a citation
    headline, or a displacement in units no OEM has used since the 1970s.
    """
    s = str(text or "").strip()
    if not s:
        return False
    for candidate in spec_label_prefix_strips(s):
        if not candidate:
            continue
        if _WIKI_ENGINE_INFOBOX_RE.match(candidate):
            return True
        if _CUBIC_INCH_RE.search(candidate):
            return True
        if _CITATION_TITLE_RE.match(candidate):
            return True
        if _EPA_FUEL_FIELD_RE.match(candidate):
            return True
        if _PLANT_LOCATION_RE.match(candidate):
            return True
    if _CHASSIS_LAYOUT_RE.search(s):
        return True
    if is_foreign_market_variant(s):
        return True
    if _METRIC_CONVERSION_RE.search(s):
        return True
    if _has_unreadable_code_run(s):
        return True
    return engine_label_without_engine_content(s)

# ---------------------------------------------------------------------------
# Bullet well-formedness
#
# Every producer that feeds "what this trim adds" splits multi-value spec cells
# on ";". Doing that with a plain ``str.split`` cuts straight through a
# parenthesised list: the EPA-style cell "Hybrid 2.5L I4 (SIDI & PFI; Hybrid)"
# becomes "Hybrid 2.5L I4 (SIDI & PFI" and "Hybrid)", and both halves reached
# the page. The rule below is the one the earlier Wikipedia-prose incident
# forced on us: a bullet has to stand on its own, or it is not shown at all.
# ---------------------------------------------------------------------------

# 8" / 12.3" is a measurement, not an opening quote — normalise it away before
# counting quote characters.
_INCH_MARK_RE = re.compile(r'(?<=[\d.])\s*"')

# Mid-sentence openers. A bullet that begins with one of these is the tail of a
# sentence some upstream splitter cut in half ("of torque, and front-wheel drive
# as standard."). ``includes``/``plus`` are deliberately absent: curated ladders
# legitimately write "Includes SV plus ...".
_FRAGMENT_HEAD_RE = re.compile(
    r"^(?:and|or|but|with|of|to|from|for|in|on|at|by|as|that|which|when|while|"
    r"the|a|an|is|are|was|were|it|its|they|their|this|these|those|"
    r"alongside|however|although|because|though|than|then|so)\b",
    re.I,
)

# A trailing connective, a lone lowercase letter (the tail of a word the scrape
# truncated: "... front-row acoustic laminated glass, your s"), or dangling
# punctuation all mean the line was cut short. ``&`` belongs here — it ended the
# truncated table heading "Model Years of Production Engine &" — but ``+`` does
# not: "Sport S+" and "Lexus Safety System+" are how the OEM writes the name.
_FRAGMENT_TAIL_RE = re.compile(
    r"(?:\b(?:and|or|with|of|to|from|for|in|on|at|by|as|the|a|an|is|are|was|were|"
    r"that|which|includes?|including|featuring)\b|\b[a-z]|[,;:/\\\-–—&])\s*$",
    re.I,
)

# Lowercase-leading marque wording that is genuinely how the OEM writes it.
# Anything else that starts lowercase is a fragment.
_LOWERCASE_MARQUE_HEADS = frozenset(
    {
        "quattro",
        "e-tron",
        "e-hybrid",
        "iforce",
        "idrive",
        "xdrive",
        "sdrive",
        "etorque",
        "ecodiesel",
        "ecoboost",
        "bluetec",
        "bz",
        "eassist",
    }
)

# A bare unit symbol at the head of a bullet ("kWh AWD: 279 mi (449 km)") is the
# right-hand half of a number that got cut off, even though it carries an
# uppercase letter.
_UNIT_HEAD_TOKENS = frozenset(
    {
        "kwh",
        "kw",
        "hp",
        "ps",
        "mpg",
        "mpge",
        "lb-ft",
        "lbft",
        "nm",
        "mi",
        "km",
        "cc",
        "cu",
        "in",
        "ft",
        "mm",
        "rpm",
        "l",
    }
)

# Column headings from an encyclopedia spec table ("Engines Capacity Model year
# Power Torque ..."). Three or more of these words with no connective between
# them is a table header, not a sentence about this trim.
_TABLE_HEADER_TOKENS: tuple[str, ...] = (
    "model year",
    "rpo code",
    "vin code",
    "capacity",
    "power",
    "torque",
    "notes",
    "engines",
    "displacement",
    "bore",
    "stroke",
    "compression ratio",
    "curb weight",
    "wheelbase",
    # Added for "Years Engine Power Torque" and "Model Years of Production
    # Engine &", both of which rendered as engine bullets. ``looks_like_spec_table_header``
    # additionally requires no connective, and the guard below requires the run
    # to carry no figures — a heading names columns, it does not fill them.
    "years",
    "engine",
    "production",
    "trim level",
)

# A heading names columns; the moment a run carries a number it is a data row
# ("204-hp Hybrid Powertrain"), so only digit-free runs are treated as headings.
_HEADER_DIGIT_RE = re.compile(r"\d")

# ``Engine Options: Engine Options: LV3 EcoTec3 4.3 L V6`` — the extractor
# prefixes its own label onto a value that already carried it. Also matches the
# singular/plural mismatch ("Engine Option Engine Options: ...").
_DOUBLED_LABEL_RE = re.compile(
    r"^(?P<first>[A-Za-z][A-Za-z0-9 /&'\-]{1,38}?)s?\s*:?\s+"
    r"(?P<second>(?P=first)s?\s*:\s*)",
    re.I,
)

# Longest bullet we will render. Past this a line stops reading as equipment and
# starts reading as a paragraph; a clean comma enumeration is split instead.
MAX_TRIM_BULLET_CHARS = 140

_OPENERS = {"(": ")", "[": "]"}
_CLOSERS = {")": "(", "]": "["}


def split_outside_brackets(text: str, separators: str = ";") -> list[str]:
    """Split on ``separators`` that sit outside ``()`` / ``[]``.

    ``"Hybrid 2.5L I4 (SIDI & PFI; Hybrid)"`` stays whole; ``"A; B"`` splits.
    A line whose brackets never balance is returned as a single piece so the
    caller can reject it rather than shed orphan halves.
    """
    s = str(text or "")
    if not s:
        return []
    out: list[str] = []
    depth = 0
    buf: list[str] = []
    for ch in s:
        if ch in _OPENERS:
            depth += 1
        elif ch in _CLOSERS:
            depth = max(0, depth - 1)
        if ch in separators and depth == 0:
            out.append("".join(buf))
            buf = []
            continue
        buf.append(ch)
    out.append("".join(buf))
    return [p.strip() for p in out if p.strip()]


def bullet_is_balanced(text: str) -> bool:
    """True when every bracket and quote in the bullet is closed."""
    s = _INCH_MARK_RE.sub("", str(text or ""))
    stack: list[str] = []
    for ch in s:
        if ch in _OPENERS:
            stack.append(ch)
        elif ch in _CLOSERS:
            if not stack or stack[-1] != _CLOSERS[ch]:
                return False
            stack.pop()
    if stack:
        return False
    return s.count('"') % 2 == 0 and s.count("“") == s.count("”")


# Provenance markers the dictionary importers write into the stored value
# ("[Brochure] Bose audio"). They are bookkeeping, not something a shopper reads.
_SOURCE_TAG_RE = re.compile(r"\[(?:Brochure|OEM|Spec|Source|Dealer)\]\s*", re.I)


def strip_source_tag(text: str) -> str:
    """Remove an inline ``[Brochure]``-style provenance marker."""
    return _SOURCE_TAG_RE.sub("", str(text or "")).strip()


def collapse_repeated_label_prefix(text: str) -> str:
    """Drop a label prefix the producer stamped on twice, keeping one copy."""
    s = str(text or "").strip()
    for _ in range(4):
        m = _DOUBLED_LABEL_RE.match(s)
        if not m:
            break
        s = s[m.start("second") :].strip()
    return s


# "Includes Luxury plus digital rearview mirror and cool box" — the rung below
# is already the heading of the list, so the prefix is noise, and "Includes
# Overtrail plus Overtrail+ …" is circular. Keep the equipment, drop the framing.
_LOWER_RUNG_REFERENCE_RE = re.compile(
    r"^(?i:includes|include|builds on|adds to)\s+"
    r"(?P<rung>[A-Z][A-Za-z0-9+/\-]{0,14}(?:\s+[A-Z][A-Za-z0-9+/\-]{0,14})?)"
    r"\s+(?i:plus)\s+"
)
_ADDS_PREFIX_RE = re.compile(
    # "Featuring/Offering <equipment>" is the same dependent-clause framing as
    # "Includes <equipment>" — strip the participle and keep the equipment,
    # rather than dropping a bullet whose content is fine.
    r"^(?i:plus it adds|also includes|also adds|it adds|adds|includes|include|including"
    r"|featuring|offering|providing)"
    r"\s+(?=[A-Za-z0-9])"
)


def strip_lower_rung_reference(text: str) -> str:
    """Drop a leading "Includes <lower trim> plus " / "Adds " / "Includes " framing.

    The panel is already headed "What this trim adds", so the verb is noise, and
    it is the reason bullets read as half-sentences ("Includes Blind Spot
    Monitor" instead of "Blind Spot Monitor").
    """
    s = str(text or "").strip()
    # Repeat: the CSVs stack them ("Adds Includes 4.6L V8 engine").
    for _ in range(4):
        before = s
        for pattern in (_LOWER_RUNG_REFERENCE_RE, _ADDS_PREFIX_RE):
            m = pattern.match(s)
            if not m or m.end() == 0:
                continue
            rest = s[m.end() :].strip()
            if len(rest) >= 6:
                # "Featuring quilted Nappa leather seats" → "Quilted …": what is
                # left has to read as a bullet in its own right, and a lowercase
                # head is exactly what ``bullet_is_fragment`` rejects. A head
                # with an interior capital ("eTorque", "iForce") is a marque
                # name and is left alone.
                head = re.split(r"[\s,;:]+", rest, maxsplit=1)[0]
                if head[:1].islower() and not any(c.isupper() for c in head[1:]):
                    rest = rest[0].upper() + rest[1:]
                s = rest
        if s == before:
            break
    return s


def looks_like_spec_table_header(text: str) -> bool:
    """True for a run of spec-table column headings with no sentence in it."""
    s = strip_spec_label_prefix(str(text or "").strip()).lower()
    if not s or _HEADER_DIGIT_RE.search(s):
        return False
    hits = sum(1 for token in _TABLE_HEADER_TOKENS if token in s)
    if hits < 3:
        return False
    return not re.search(r"\b(?:with|and|for|includes?|adds?|standard|available)\b", s)


# A scrape that truncates at a fixed width leaves a stub of the next word
# ("… paddle shifters 7-inch Display Au"). Two-letter tails that are real are
# either an all-caps code (AC, XL, GT, ID) or a unit, or they follow a
# conjunction ("Stop and Go").
_SHORT_TAIL_RE = re.compile(r"[\s\-]([A-Za-z]{1,2})$")
_COMPLETE_SHORT_TAILS = frozenset(
    {"up", "hp", "kw", "ps", "nm", "mi", "km", "ft", "lb", "mm", "cc", "ah", "wh"}
)
_TAIL_CONNECTIVES = frozenset({"and", "or", "&", "with", "plus", "to", "the", "a"})


def _has_truncated_tail(text: str) -> bool:
    s = str(text or "").rstrip()
    m = _SHORT_TAIL_RE.search(s)
    if not m:
        return False
    tok = m.group(1)
    if tok.isupper():
        return False
    if tok.lower() in _COMPLETE_SHORT_TAILS:
        return False
    if len(tok) == 2 and tok[0].isupper() and tok[1].islower():
        prev = re.split(r"\s+", s[: m.start()].strip())[-1:] or [""]
        return prev[0].lower().strip(",;") not in _TAIL_CONNECTIVES
    return tok.islower()


def _head_token(text: str) -> str:
    return re.split(r"[\s,;:]+", str(text or "").strip(), maxsplit=1)[0]


def bullet_is_fragment(text: str) -> bool:
    """True when the bullet is the severed half of a longer line."""
    s = str(text or "").strip()
    if not s:
        return True
    # "–515 hp (242–384 kW; 330–522 PS)": the line begins mid-range, so its
    # left-hand half is somewhere else.
    if s[0] in "-–—,;:/&":
        return True
    if _FRAGMENT_HEAD_RE.match(s):
        return True
    if _FRAGMENT_TAIL_RE.search(s):
        return True
    if _has_truncated_tail(s):
        return True
    head = _head_token(s)
    if not head:
        return True
    if head[0].islower():
        bare = re.sub(r"[^a-z0-9\-]+", "", head.lower())
        if bare in _UNIT_HEAD_TOKENS:
            return True
        if bare in _LOWERCASE_MARQUE_HEADS:
            return False
        # "i-FORCE", "eAWD", "i-ACTIV": an interior capital marks a marque name,
        # not an English word the splitter cut loose.
        return not any(c.isupper() for c in head[1:])
    return False


# Equipment is a noun phrase. Anything with a finite verb, a relative clause, a
# footnote marker or a URL is encyclopedia narrative or brochure legalese that
# the scrape stamped onto whichever trim shared a row — the class that put
# "the 4Runner carried over the same engine options from the" on a car page.
_PROSE_RE = re.compile(
    r"(?:"
    r"\b(?:is|are|was|were|has|have|had|can|could|will|would|do|does|may|must)\s+"
    r"(?:also\s+|not\s+|only\s+|still\s+|now\s+|been\s+)*"
    r"(?:\w+ed|available|standard|optional|required|offered|equipped|refer|vary|have)\b"
    r"|\bwhich\s+\w+"
    # "…driver information module that utilize Android Automotive OS…". Only
    # "utilize" is listed: "a system that includes 13 speakers" is ordinary OEM
    # copy, so the broader verb set would start eating real brochure bullets as
    # the brochure corpus grows.
    r"|\bthat\s+utili[sz]es?\b"
    r"|\brefers? to\b|\bcarried over\b|\bmanufactured by\b|\bcan be had\b"
    r"|\b(?:now|later|initially|subsequently|eventually)\s+"
    r"(?:featured|received|gained|included|became|offered|replaced)\b"
    r"|\bfor this generation\b"
    # "The 2012 model year A6 features all the driver assistance systems from the
    # A8" — a third-person verb after a noun is a sentence, not a feature name.
    r"|\b\w+\s+(?:features|offers|provides|delivers|replaces|carries)\s+\w+"
    r"|\b(?:other\s+)?options include\b|\bfind sources\b"
    r"|\bis not a substitute for\b|\bsee usage precautions\b|\bcustomer agreement\b"
    r"|\bterms and conditions\b|\bsubject to change\b|\bmay vary by\b"
    r"|\bcostly to replace\b|\bdepends on many factors\b"
    r"|\bwww\.\S+|https?://"
    r")",
    re.I,
)

# Coverage a buyer gets with the car, not hardware on it. Never a trim "add".
_SERVICE_PROGRAM_RE = re.compile(
    r"\bwarrant(?:y|ies)\b|\broadside assistance\b|\bcorrosion (?:protection|coverage|perforation)\b"
    r"|\bsafety restraint coverage\b|\bmaintenance (?:plan|program)\b"
    r"|\bcomplimentary (?:scheduled )?maintenance\b|\bscheduled maintenance\b"
    r"|\bcourtesy transportation\b|\bconcierge\b|\bpickup ?& ?delivery service\b"
    r"|\b\d+-(?:year|month)/[\d,]+-mile\b|\b\d+-year/unlimited-mile\b",
    re.I,
)


def reads_as_prose(text: str) -> bool:
    """True for a sentence, a disclaimer, or a URL — not an equipment phrase."""
    return bool(_PROSE_RE.search(str(text or "")))


def is_service_program_line(text: str) -> bool:
    """True for warranty / maintenance / concierge coverage lines."""
    return bool(_SERVICE_PROGRAM_RE.search(str(text or "")))


def is_wellformed_trim_bullet(text: str) -> bool:
    """Gate every rendered bullet: self-contained, quotable, not a table row."""
    s = str(text or "").strip()
    if len(s) < 3 or len(s) > MAX_TRIM_BULLET_CHARS:
        return False
    if not bullet_is_balanced(s):
        return False
    if bullet_is_fragment(s):
        return False
    if reads_as_prose(s):
        return False
    if is_service_program_line(s):
        return False
    return not looks_like_spec_table_header(s)


def part_is_contaminated(text: str) -> bool:
    """True when a piece of a multi-value cell proves the cell is scraped prose.

    Distinguished from "not renderable": a piece can be merely too long, which
    ``expand_long_enumeration`` rescues. A piece that is a severed fragment, a
    sentence, a column heading, or an encyclopedia infobox field is evidence
    about the *whole* cell — "Engine 351 cu in (5.8 L) 351M V8; Based on a design
    proposal originally used in the development of the previous-ge" is one
    Wikipedia paragraph, and dropping only its second half published the first
    half as a 2026 Bronco Sport engine.
    """
    s = str(text or "").strip()
    if not s:
        return False
    if not bullet_is_balanced(s):
        return True
    if bullet_is_fragment(s):
        return True
    if reads_as_prose(s):
        return True
    if looks_like_spec_table_header(s):
        return True
    return is_encyclopedia_spec_artifact(s)


def expand_long_enumeration(text: str) -> list[str]:
    """Split an over-long comma enumeration into one bullet per item.

    Only fires on a line that is already too long to render and that splits
    cleanly at top-level commas — a paragraph does not, so it is dropped by the
    caller instead of being cut mid-thought.
    """
    s = str(text or "").strip()
    if len(s) <= MAX_TRIM_BULLET_CHARS or not bullet_is_balanced(s):
        return []
    # A paragraph also has commas in it. Splitting one yields clause-shaped
    # fragments that read like specs but are not, so prose is never expanded.
    if reads_as_prose(s):
        return []
    parts = split_outside_brackets(s, separators=",")
    if len(parts) < 3:
        return []
    out: list[str] = []
    for part in parts:
        piece = re.sub(r"^(?:and|or)\s+", "", part.strip(), flags=re.I).strip(" ,;")
        piece = strip_lower_rung_reference(piece)
        if len(piece) < 12 or len(piece) > MAX_TRIM_BULLET_CHARS:
            return []
        head = _head_token(piece)
        if head[:1].islower() and not any(c.isupper() for c in head[1:]):
            piece = piece[0].upper() + piece[1:]
        if not is_wellformed_trim_bullet(piece):
            return []
        out.append(piece)
    return out


def is_generic_trim_add(text: str, trim_name: str | None = None) -> bool:
    """True for tautological placeholder lines that restate the trim name."""
    s = (text or "").strip()
    if not s:
        return True
    if _LADDER_PLACEHOLDER_PROSE_RE.search(s):
        return True
    if _LINEUP_NARRATIVE_RE.search(s):
        return True
    # The three tests below are ``^``-anchored, and every producer that feeds
    # this function stamps the CSV column heading onto the front of its own
    # value. Testing only ``s`` is why "Engine Options: L Volkswagen-Audi
    # EA839TT TFSI V6 twin-turbo" and "Engine Options: Front-engine,
    # rear-wheel-drive" rendered while the bare forms were correctly rejected.
    for candidate in spec_label_prefix_strips(s):
        if not candidate:
            continue
        if _ORPHANED_DISPLACEMENT_RE.match(candidate):
            return True
        if _SECTION_HEADING_RE.match(candidate):
            return True
        if _POWERTRAIN_INFOBOX_RE.match(candidate):
            return True
    if is_encyclopedia_spec_artifact(s):
        return True
    if re.search(r"\bmarketing trim level from oem brochure\b", s, re.I):
        return True
    # "2022+ model year", "Manual or automatic by model year": a note about when
    # the lineup changed, not equipment this rung adds.
    if re.fullmatch(r"\d{4}\+?\s*(?:[–-]\s*\d{4}\s*)?model year\.?", s, re.I):
        return True
    if re.search(r"\bby model year\.?$", s, re.I):
        return True
    # Warranty / roadside coverage is sold with the whole model line, so it is
    # never something one rung "adds" over the rung below it.
    if is_service_program_line(s):
        return True
    if re.search(r"\bauthorized\b.*\bcenter\b|\bbmwusa\.com\b", s, re.I):
        return True
    if _GENERIC_TRIM_ADD_RE.search(s):
        return True
    if len(s) < 20:
        return False
    if trim_name:
        tn = trim_name.strip()
        if tn and re.fullmatch(
            rf"(?:Equipment and features|Factory equipment and features) typical of the {re.escape(tn)} trim\.?",
            s,
            re.I,
        ):
            return True
        if re.fullmatch(rf"Typical .+ {re.escape(tn)} equipment and packaging\.?", s, re.I):
            return True
    if re.search(r"\btypical listing price near \$", s, re.I):
        return True
    return False


def sanitize_trim_adds(
    adds: list[str] | None,
    trim_name: str | None = None,
    *,
    max_items: int = 6,
) -> list[str]:
    """Drop placeholder / junk add lines; keep substantive feature bullets only."""
    from backend.enrichment.trim_spec_extractor import is_junk_spec_text

    out: list[str] = []
    seen: set[str] = set()
    for raw in adds or []:
        line = str(raw or "").strip()
        if not line or is_generic_trim_add(line, trim_name) or is_junk_spec_text(line):
            continue
        key = line.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(line[:240])
    if max_items <= 0:
        return out
    return out[:max_items]


def sanitize_brochure_trim_adds(adds: list[str] | None, trim_name: str | None = None) -> list[str]:
    """Brochure-sourced bullets: keep full OEM equipment list (minimal filtering)."""
    out: list[str] = []
    seen: set[str] = set()
    for raw in adds or []:
        line = str(raw or "").strip()
        if not line or is_generic_trim_add(line, trim_name):
            continue
        if len(line) < 8:
            continue
        key = line.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(line[:280])
    return out[:80]
