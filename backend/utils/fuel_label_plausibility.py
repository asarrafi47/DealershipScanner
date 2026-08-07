"""
Plausibility of an "Electric" fuel-type claim, judged against the row's own
evidence.

Dealer feeds have stored bare "Electric" on gas and hybrid cars (Lexus GX 550
gas V6 ×25, BMW 330i/X5 40i, Honda Pilot, Lexus 350h/500h hybrids, and BMW
330e/530e plug-ins), and the guards then defended the wrong field: a
positive cylinder count next to a BEV label was treated as a CYLINDERS error
(``engine_consistency``), and ``spec_field_normalize`` forced cylinders to 0
whenever ``fuel_type == 'electric'`` — so the one field that could disprove the
bad label kept getting erased. This module is the tiebreaker: given the claim,
the ENGINE TEXT, and the nameplate, decide whether the electric label is the
thing to trust.

Deliberately conservative, mirroring ``msrp_trust``: only high-confidence
combustion evidence flags a claim — displacement-liter text ("3.4L"), layout
tokens (V6, I4) or cylinder phrases FROM ENGINE TEXT (never from the
``cylinders`` column alone, which carries feed junk on true EVs), and a short
table of nameplates that have no battery-electric variant. Anything ambiguous
stays ``plausible`` and the label is left alone.

Everything here is a pure function of the row dict — no DB, no imports from
higher layers — so the same rules run at write time (scanner upsert), at read
time (card serializer + facet cascade via ``fuel_type_normalize``) and in the
one-shot heal script (``backend/scripts/heal_ev_cylinders.py``).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

# Verdicts for an "Electric" fuel-type claim.
PLAUSIBLE = "plausible"
IMPLAUSIBLE = "implausible"  # combustion evidence contradicts the claim
PHEV_MISLABEL = "phev_mislabel"  # plug-in hybrid stored as bare "Electric"

GASOLINE_LABEL = "Gasoline"
HYBRID_LABEL = "Hybrid"
PLUG_IN_HYBRID_LABEL = "Plug-In Hybrid"
DIESEL_LABEL = "Diesel"

# Labels that claim the car is battery-only. Anything mentioning gas / hybrid /
# plug-in is a compound label, not a bare electric claim.
_BARE_ELECTRIC_LABEL_RE = re.compile(
    r"^(?:electric|electricity|ev|bev|battery[\s-]*electric(?:\s*vehicle)?)$", re.I
)

# ---------------------------------------------------------------------------
# Evidence extraction (engine text)
# ---------------------------------------------------------------------------

# "3.4L", "5.7 L", "2.0 Liter" — a displacement is a combustion engine.
_DISPLACEMENT_RE = re.compile(
    r"\b(\d{1,2}(?:\.\d)?)\s*[- ]?(?:l|liter|litre)s?\b", re.I
)
# "V6" / "V-8" / "I4" / "H6" / "W12" / "inline-6" / "flat-4" — but not "24V".
_LAYOUT_RE = re.compile(
    r"\b(?:[VIW][-\s]?(?:2|3|4|5|6|8|10|12|16)|H[-\s]?(?:4|6)"
    r"|inline[-\s]?\d|flat[-\s]?(?:4|6)|straight[-\s]?\d)\b",
    re.I,
)
# "6-cyl", "8 cylinder" — 2..16 only, so "0 cylinders" never counts.
_CYL_PHRASE_RE = re.compile(r"\b([2-9]|1[0-6])\s*[-\s]?cyl(?:inder)?s?\b", re.I)
# Unambiguous combustion family names.
_COMBUSTION_WORD_RE = re.compile(r"\bhemi\b|\bpentastar\b|\becoboost\b|twin[-\s]?turbo", re.I)
_DIESEL_RE = re.compile(r"\bdiesel\b|\bcummins\b|\bduramax\b|power\s*stroke", re.I)

# Engine text that AFFIRMS an electric drivetrain.
_ELECTRIC_TEXT_RE = re.compile(
    r"electric|\bbev\b|\bev\b|battery|\bkwh?\b|\bmotor\b|fuel\s*cell", re.I
)

# ---------------------------------------------------------------------------
# Evidence extraction (nameplate: model / trim / title)
# ---------------------------------------------------------------------------

# Plug-in hybrid cues. The 3-digit "e" suffix (330e, 530e, 550e, 745e, GLE 350e)
# and the xDrive##e trims are OEM PHEV designations; "h+" is Lexus's plug-in
# suffix (450h+/550h+) as opposed to the plain hybrid "h".
_PHEV_NAMEPLATE_RE = re.compile(
    r"plug[\s-]?in|\bphev\b|\b4xe\b|\bprime\b"
    r"|\b\d{3}e\b|\b[sx]drive\d{2}e\b|\bp\d{3}e\b|\b\d{3}h\+",
    re.I,
)
# Non-plug-in hybrid cues: the word itself, or the Lexus/Toyota "h" suffix
# (350h, 500h, 300h) NOT followed by "+".
_HYBRID_WORD_RE = re.compile(r"\bhybrid\b|\bhev\b|e:hev", re.I)
_H_SUFFIX_RE = re.compile(r"\b\d{3}h\b(?!\s*\+)", re.I)


@dataclass(frozen=True)
class _Nameplate:
    """One make-gated nameplate rule (model matched against model, then title)."""

    make_re: re.Pattern[str]
    model_re: re.Pattern[str]
    year_min: int | None = None


# Nameplates with NO battery-electric variant: an "Electric" label on these is a
# feed error by construction. Kept minimal and unambiguous — a nameplate with
# any EV variant (Kona, F-150, Blazer) must never appear here.
_KNOWN_GAS_NAMEPLATES: tuple[_Nameplate, ...] = (
    _Nameplate(re.compile(r"^lexus$", re.I), re.compile(r"^gx\b", re.I)),
    _Nameplate(re.compile(r"^bmw$", re.I), re.compile(r"^x7\b", re.I)),
    _Nameplate(re.compile(r"^bmw$", re.I), re.compile(r"^m[234]\b(?!0)", re.I)),
    _Nameplate(
        re.compile(r"^honda$", re.I),
        re.compile(r"^(?:pilot|passport|odyssey|ridgeline|accord|civic)\b", re.I),
    ),
)

# BMW's gasoline trim suffixes: 330i / 540i / xDrive28i / sDrive30i / M60i.
# Word-boundary digits-then-i never matches the i4/i5/i7/iX EV models.
_BMW_GAS_SUFFIX_RE = re.compile(r"\b(?:m\s?)?\d{2,3}i\b|\b[sx]drive\d{2}i\b", re.I)
_BMW_MAKE_RE = re.compile(r"^bmw$", re.I)

# Nameplates that ARE battery-electric. Used to (a) short-circuit the classifier
# to "plausible" before any junk engine text can be misread as combustion, and
# (b) let the heal script zero junk cylinder counts with evidence.
_KNOWN_BEV_NAMEPLATES: tuple[_Nameplate, ...] = (
    _Nameplate(re.compile(r"^(?:tesla|rivian|lucid|polestar|fisker|vinfast)$", re.I), re.compile(r".", re.I)),
    _Nameplate(re.compile(r"^bmw$", re.I), re.compile(r"^i[3-8]\b|^ix\b", re.I)),
    _Nameplate(re.compile(r"^chevrolet$", re.I), re.compile(r"\bbolt\b|\bev\b", re.I)),
    _Nameplate(re.compile(r"^gmc$", re.I), re.compile(r"\bhummer\b|\bev\b", re.I)),
    _Nameplate(
        re.compile(r"^cadillac$", re.I),
        re.compile(r"\b(?:lyriq|celestiq|optiq|vistiq|escalade\s*iq)\b", re.I),
    ),
    _Nameplate(re.compile(r"^ford$", re.I), re.compile(r"mach-?\s?e|\blightning\b", re.I)),
    _Nameplate(re.compile(r"^hyundai$", re.I), re.compile(r"\bioniq\s*\d", re.I)),
    _Nameplate(re.compile(r"^kia$", re.I), re.compile(r"\bev\s?\d\b|\bniro\s+ev\b", re.I)),
    _Nameplate(re.compile(r"^nissan$", re.I), re.compile(r"\bleaf\b|\bariya\b", re.I)),
    _Nameplate(re.compile(r"^toyota$", re.I), re.compile(r"\bbz\w*\b", re.I)),
    # The 2026+ Toyota C-HR is battery-only in the US (the gas C-HR ended 2022).
    _Nameplate(re.compile(r"^toyota$", re.I), re.compile(r"^c[\s-]?hr\b", re.I), year_min=2025),
    _Nameplate(re.compile(r"^volkswagen$", re.I), re.compile(r"\bid\.?\s?(?:\d|buzz)", re.I)),
    _Nameplate(re.compile(r"^mercedes", re.I), re.compile(r"^eq\w*\b", re.I)),
    # The electric G-Class: badged "G 580 with EQ Technology", fed to us with a
    # "G 580e" trim whose e-suffix would otherwise read as a PHEV designation.
    _Nameplate(re.compile(r"^mercedes", re.I), re.compile(r"\bg\s?580e?\b|\beqg\b", re.I)),
    _Nameplate(re.compile(r"^audi$", re.I), re.compile(r"e-?tron", re.I)),
    _Nameplate(re.compile(r"^honda$", re.I), re.compile(r"\bprologue\b", re.I)),
    _Nameplate(re.compile(r"^acura$", re.I), re.compile(r"\bzdx\b", re.I)),
    _Nameplate(re.compile(r"^subaru$", re.I), re.compile(r"\bsolterra\b", re.I)),
    _Nameplate(re.compile(r"^genesis$", re.I), re.compile(r"\bgv60\b|electrified", re.I)),
    _Nameplate(re.compile(r"^volvo$", re.I), re.compile(r"\be[xc]\d{2}\b|\bc40\b", re.I)),
    _Nameplate(re.compile(r"^porsche$", re.I), re.compile(r"\btaycan\b|macan\s+(?:electric|ev|4s?\b|turbo)", re.I)),
    _Nameplate(re.compile(r"^lexus$", re.I), re.compile(r"^rz\b", re.I)),
    _Nameplate(re.compile(r"^jaguar$", re.I), re.compile(r"\bi-?pace\b", re.I)),
    # Any make: a model that names itself "BEV" or "Electric" (Kona Electric,
    # Macan Electric, C-HR BEV).
    _Nameplate(re.compile(r".", re.I), re.compile(r"\bbev\b|\belectric\b", re.I)),
)

_ENGINE_TEXT_KEYS = (
    "engine_description",
    "engine_display",
    "engine",
    "engine_detail",
    "master_engine_string",
)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def is_bare_electric_label(fuel_type: Any) -> bool:
    """True when *fuel_type* claims the car is battery-only (bare "Electric")."""
    s = _clean(fuel_type)
    return bool(s) and bool(_BARE_ELECTRIC_LABEL_RE.match(s))


def engine_text_for_car(car: dict[str, Any], extra: str | None = None) -> str:
    """Every field that can carry engine identity, joined for one regex pass."""
    parts = [extra] + [car.get(k) for k in _ENGINE_TEXT_KEYS]
    return " ".join(_clean(p) for p in parts if _clean(p))


def _nameplate_text(car: dict[str, Any]) -> str:
    return " ".join(_clean(car.get(k)) for k in ("model", "trim", "title", "series"))


def combustion_evidence_from_text(engine_text: str | None) -> str | None:
    """
    The high-confidence combustion token found in *engine_text*, or None.

    Only engine-shaped evidence counts: a displacement in liters (0.6–9.9),
    a V/I/W/H layout token, an explicit cylinder phrase, or an unambiguous
    combustion family word. The ``cylinders`` COLUMN is deliberately not
    consulted anywhere in this module — true EVs carry feed junk there.
    """
    text = _clean(engine_text)
    if not text:
        return None
    m = _DISPLACEMENT_RE.search(text)
    if m:
        try:
            liters = float(m.group(1))
        except ValueError:
            liters = 0.0
        if 0.6 <= liters <= 9.9:
            return m.group(0)
    for rx in (_LAYOUT_RE, _CYL_PHRASE_RE, _COMBUSTION_WORD_RE, _DIESEL_RE):
        m = rx.search(text)
        if m:
            return m.group(0)
    return None


def electric_evidence_from_text(engine_text: str | None) -> bool:
    """True when the engine text itself affirms an electric drivetrain."""
    text = _clean(engine_text)
    return bool(text) and bool(_ELECTRIC_TEXT_RE.search(text))


def _year_of(car: dict[str, Any]) -> int | None:
    try:
        return int(str(car.get("year")).strip())
    except (TypeError, ValueError):
        return None


def _match_nameplates(
    table: tuple[_Nameplate, ...], car: dict[str, Any]
) -> bool:
    make = _clean(car.get("make"))
    if not make:
        return False
    model = _clean(car.get("model"))
    trim = _clean(car.get("trim"))
    title = _clean(car.get("title"))
    year = _year_of(car)
    for np in table:
        if not np.make_re.search(make):
            continue
        if np.year_min is not None and (year is None or year < np.year_min):
            continue
        # The identifying token can live in ``model`` ("Blazer EV"), in ``trim``
        # ("G-Class" / "G 580e SUV"), or — when the feed left model blank — in
        # the title. Anchored patterns simply fail on fields they don't fit.
        if (
            np.model_re.search(model)
            or np.model_re.search(trim)
            or (not model and np.model_re.search(title))
        ):
            return True
    return False


def is_known_bev_nameplate(car: dict[str, Any]) -> bool:
    """True when make/model (or title, if model is blank) is a battery-only nameplate."""
    return _match_nameplates(_KNOWN_BEV_NAMEPLATES, car)


def is_known_gas_nameplate(car: dict[str, Any]) -> bool:
    """True when the nameplate has no battery-electric variant at all."""
    if _match_nameplates(_KNOWN_GAS_NAMEPLATES, car):
        return True
    make = _clean(car.get("make"))
    return bool(
        _BMW_MAKE_RE.search(make) and _BMW_GAS_SUFFIX_RE.search(_nameplate_text(car))
    )


@dataclass(frozen=True)
class ElectricClaimAssessment:
    verdict: str  # PLAUSIBLE / IMPLAUSIBLE / PHEV_MISLABEL
    suggested_label: str | None  # evidence-backed label when verdict != PLAUSIBLE
    evidence: str  # human-readable reason (for logs / heal-script output)


def assess_electric_claim(
    car: dict[str, Any], *, engine_text: str | None = None
) -> ElectricClaimAssessment:
    """
    Classify an "Electric" fuel-type claim against the row's own evidence.

    The caller is expected to have already checked :func:`is_bare_electric_label`
    on the stored label; this function judges only the claim itself.
    """
    engine_blob = engine_text_for_car(car, engine_text)
    nameplate = _nameplate_text(car)
    combustion = combustion_evidence_from_text(engine_blob)

    # Battery-only nameplates first: junk engine text copied from a gas sibling
    # (a BMW i4 whose feed engine reads "I4") must not flip a real EV to gas.
    if is_known_bev_nameplate(car):
        return ElectricClaimAssessment(PLAUSIBLE, None, "known battery-electric nameplate")

    # Engine text that affirms electric, with nothing combustion-shaped in it,
    # settles the claim (Toyota C-HR BEV: engine says electric, cylinders=4 junk).
    if electric_evidence_from_text(engine_blob) and not combustion:
        return ElectricClaimAssessment(PLAUSIBLE, None, "electric engine text")

    # Plug-in hybrid stored as bare "Electric" (330e/530e/550e, 450h+).
    if _PHEV_NAMEPLATE_RE.search(nameplate) or _PHEV_NAMEPLATE_RE.search(engine_blob):
        return ElectricClaimAssessment(
            PHEV_MISLABEL, PLUG_IN_HYBRID_LABEL, "plug-in hybrid nameplate/engine cue"
        )

    # Non-plug-in hybrid stored as "Electric" (ES 350h, TX 500h).
    if (
        _HYBRID_WORD_RE.search(nameplate)
        or _HYBRID_WORD_RE.search(engine_blob)
        or _H_SUFFIX_RE.search(nameplate)
    ):
        return ElectricClaimAssessment(
            IMPLAUSIBLE, HYBRID_LABEL, "hybrid nameplate/engine cue"
        )

    # Combustion evidence in the engine text (Lexus GX 550 "3.4L V6").
    if combustion:
        label = DIESEL_LABEL if _DIESEL_RE.search(engine_blob) else GASOLINE_LABEL
        return ElectricClaimAssessment(
            IMPLAUSIBLE, label, f"combustion engine text ({combustion})"
        )

    # No engine text, but the nameplate has no EV variant (Honda Pilot, BMW X7,
    # BMW ###i / xDrive##i gas trims).
    if is_known_gas_nameplate(car):
        return ElectricClaimAssessment(
            IMPLAUSIBLE, GASOLINE_LABEL, "nameplate has no battery-electric variant"
        )

    return ElectricClaimAssessment(PLAUSIBLE, None, "no contradicting evidence")


def corrected_label_for_electric_claim(
    car: dict[str, Any], *, fuel_type: Any = None, engine_text: str | None = None
) -> str | None:
    """
    The evidence-backed label an implausible "Electric" claim should display
    (Gasoline / Hybrid / Plug-In Hybrid / Diesel), or None to leave it alone.
    """
    ft = fuel_type if fuel_type is not None else car.get("fuel_type")
    if not is_bare_electric_label(ft):
        return None
    a = assess_electric_claim(car, engine_text=engine_text)
    if a.verdict in (IMPLAUSIBLE, PHEV_MISLABEL):
        return a.suggested_label
    return None


# ---------------------------------------------------------------------------
# Cylinder-count hygiene (write path + heal script)
# ---------------------------------------------------------------------------

# No production engine has more than 16 cylinders; GM feeds use 99 as an
# "electric" sentinel, and it must never reach the column.
MAX_REAL_CYLINDERS = 16


def sanitize_cylinder_count(value: Any) -> int | None:
    """Coerce to an int in 0..16; feed sentinels (99) and junk become None."""
    if value is None or isinstance(value, bool):
        return None
    try:
        n = int(float(str(value).replace(",", "").strip()))
    except (TypeError, ValueError):
        return None
    return n if 0 <= n <= MAX_REAL_CYLINDERS else None


def cylinders_override_for_electric_claim(
    car: dict[str, Any], *, engine_text: str | None = None
) -> int | None:
    """
    ``0`` when a bare-electric row's positive cylinder count is feed junk and
    should be zeroed at write time; ``None`` to leave the column alone.

    Zeroes ONLY when the electric claim itself is plausible. When combustion
    evidence contradicts the label (a gas GX 550 stored "Electric"), the
    cylinders ARE the evidence — they are kept, and the label correction /
    read-time display handles the fuel type instead.

    A plausible electric row gets 0 even when the incoming count is empty (the
    feed sentinel 99 is nulled by ``clean_car_row_dict`` before this runs): 0 is
    definitionally correct for a BEV, and storing it keeps the upsert's
    ``COALESCE(excluded.cylinders, cars.cylinders)`` from resurrecting an older
    junk count already in the row.
    """
    if not is_bare_electric_label(car.get("fuel_type")):
        return None
    a = assess_electric_claim(car, engine_text=engine_text)
    if a.verdict == PLAUSIBLE:
        cyl = sanitize_cylinder_count(car.get("cylinders"))
        if cyl != 0:
            return 0
    return None
