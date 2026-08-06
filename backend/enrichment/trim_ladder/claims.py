"""Normalised forms of a rung's name, for matching claims to rungs."""
from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

from ._common import (
    _clean_trim_label,
    _norm_token,
)

# --- trim -> rung attribution: FUZZY SCORING REVOKED 2026-08-02 ------------
#
# What used to live here was ``_trim_match_score``, which graded a listing's
# trim string against a rung name on a 0-100 scale: 100 for equality, 80 for a
# prefix, 70 for a word-boundary hit, 60 for plain substring containment. The
# caller in ``_build_ladder_result`` then took the best-scoring rung with
# ``best_score > 0`` and stamped it ``is_current`` — the "This vehicle" badge.
#
# 60 was enough. A 2026 Toyota RAV4 LE, on a ladder whose rungs are Limited /
# XLE Premium / XSE / SE and which has no LE rung at all, matched "XLE Premium"
# at 60 (``"le" in "xlepremium"``) and was rendered as an XLE Premium — power
# liftgate and all. The bullets on that rung are properly cited to a document;
# they are simply not this car's. Correct text attributed to the wrong car is a
# wrong spec, so the whole tolerance is gone rather than retuned: there is no
# score below equality that means "is".
#
# ``_exact_rung_match`` replaces it and admits exactly one relation — the
# listing trim and the rung are THE SAME NAME. Nothing else can produce a
# vehicle claim. A car whose trim is not a rung gets no rung.


# Tokens a dealer appends to a trim string that name the DRIVETRAIN, the CAB or
# BODY configuration, the TRANSMISSION or the ENGINE BADGE. None of them is ever
# a trim name, so removing them cannot turn one trim into another — which is the
# only property that lets them be removed before a "this car IS that rung"
# comparison. The list is closed and enumerated on purpose: every entry is a
# claim that the word is not part of any trim name, and a word that might be
# (Porsche's "Turbo", Mercedes' "Coupe", "Hybrid", "Built to Serve") is NOT
# here. ``_build_ladder_result`` refuses the claim outright if a strip leaves
# two differently-named rungs both matching, so a wrong entry fails closed
# rather than picking one.
_CLAIM_NOISE_RES = (
    # drivetrain
    re.compile(
        r"\b(?:2wd|4wd|awd|fwd|rwd|4x4|4x2|xdrive|sdrive|edrive|quattro|4matic\+?|4motion|"
        r"all[\s-]?wheel[\s-]?drive|front[\s-]?wheel[\s-]?drive|rear[\s-]?wheel[\s-]?drive)\b",
        re.I,
    ),
    # cab / body configuration
    re.compile(
        r"\b(?:crewmax|supercrew|supercab|megacab|"
        r"(?:crew|quad|regular|reg|double|access|king|extended|ext|club|mega)\s+cab)\b",
        re.I,
    ),
    # door count. The separator has to allow a HYPHEN as well as a space: the
    # first version of this pattern used ``\s*``, so it folded "2 door" and
    # "2dr" but not "2-door", which is the spelling Stellantis feeds use. That
    # gap is why "Scat Pack 2-door" and "R/T 4-door" could only reach their
    # rungs through the alias table (31 active cars, measured 2026-08-02). No
    # new claim is being made here — a door count is already asserted above to
    # be a body configuration and never a trim name — the enumerated rule simply
    # did not fire as written.
    re.compile(r"\b(?:[2-5]\s*[-–]?\s*(?:dr|door)s?)\b", re.I),
    # transmission
    re.compile(r"\b(?:automatic|manual|cvt|\d+[\s-]*speed(?:\s+automatic)?)\b", re.I),
    # engine badge: Audi/VW "45 TFSI", and a displacement with an optional
    # "Turbo" bound to it ("3.3 Turbo"). A bare "Turbo" is left alone — it is a
    # Porsche trim.
    re.compile(r"\b\d{2}\s*(?:tfsi|tdi|tsi)\b", re.I),
    re.compile(r"\b\d\.\d\s*l?\b(?:\s*turbo\b)?", re.I),
)


def _strip_claim_noise(label: str) -> str:
    out = str(label or "")
    for rx in _CLAIM_NOISE_RES:
        out = rx.sub(" ", out)
    return re.sub(r"\s+", " ", out).strip()


def _rung_claim_forms(label: str, make: str, model: str | None = None) -> set[str]:
    """Spellings of ONE trim label that drop no trim word. Empty for an empty label.

    Three normalisations, all of which only fold case, punctuation and the
    enumerated non-trim noise in ``_CLAIM_NOISE_RES``:

      * the raw label, punctuation and case removed;
      * ``_clean_trim_label`` — strips a trailing drivetrain / transmission /
        cab-style token and a "(2024+)" year suffix;
      * ``_strip_claim_noise`` — the same idea generalised, so "Big Horn Crew
        Cab 4x4" is the "Big Horn" rung, "330i xDrive" is the "330i" rung and
        "3.3 Turbo Premium Plus AWD" is "Premium Plus".

    Note what the noise strip does NOT reach: it removes a drivetrain token
    only where the token stands alone ("330i xDrive"), never where it is fused
    into the name ("xDrive40i"), because there ``\\b`` never fires. That is
    correct — see ``_drivetrain_neutral_forms`` for the one place a fused
    drivetrain may be dropped, and the conditions on it.

    ``canonical_trim_name`` IS DELIBERATELY NOT HERE, and must not be added.
    It is a *truncating* map onto a table of known base trims — it turns "Sport
    Prestige" into "Sport", "SX-Prestige" into "SX", "Limited Platinum" into
    "Platinum" and "Big Horn Built to Serve" into "Big Horn". Those are our own
    reductions of one trim onto a DIFFERENT trim's name, and comparing on them
    is the same wrong-attribution the fuzzy scorer above was revoked for: an
    Acura Sport Prestige would be stamped as the Sport rung and read its
    bullets as its own. Verified directly, not assumed —
    ``canonical_trim_name("Sport Prestige", "Acura", "TLX") == "Sport"`` and
    ``canonical_trim_name("SX-Prestige", "Kia", "Sportage") == "SX"``. An
    interim build of this function that did include it made a Kia SX-Prestige
    match the "SX Prestige" AND the "SX" rung of the same ladder, and came out
    right only because the steps happen to be ordered luxury-first.

    The noise strip is a different thing and is allowed: it removes words that
    name the drivetrain, cab, transmission or engine badge and that no trim is
    called. It never rewrites one trim's name into another's.
    """
    raw = str(label or "").strip()
    if not raw:
        return set()
    forms = {
        _norm_token(raw),
        _norm_token(_clean_trim_label(raw)),
        _norm_token(_strip_claim_noise(raw)),
        _norm_token(_strip_claim_noise(_clean_trim_label(raw))),
    }
    return {f for f in forms if f}


def _drivetrain_neutral_forms(label: str, make: str, model: str | None = None) -> set[str]:
    """``_rung_claim_forms`` plus the drivetrain-merged key — the WEAKER comparison.

    ``drivetrain_merge_key`` collapses "xDrive40i" and "sDrive40i" to "40i". On
    a ladder whose rung is called "40i" that is right: the rung names a motor
    and takes both driven axles. On a ladder whose rung is called "xDrive40i"
    it is not — that rung's NAME commits to a drivetrain, and stamping "This
    vehicle" on it for an sDrive car would print a drivetrain the car does not
    have.

    So this set is used only on the losing side of a two-tier comparison, and
    only against a rung whose own name is already drivetrain-neutral. See
    ``_exact_rung_match``.
    """
    from backend.enrichment.trim_ladder_knowledge import drivetrain_merge_key

    raw = str(label or "").strip()
    if not raw:
        return set()
    forms = _rung_claim_forms(raw, make, model)
    merged = _norm_token(drivetrain_merge_key(raw, make, model))
    if merged:
        forms = forms | {merged}
    return forms
