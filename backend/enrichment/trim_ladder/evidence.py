"""Rungs have to be justified by observed inventory, not just their bullets."""
from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any

logger = logging.getLogger(__name__)

from ._common import (
    _norm_make,
    _norm_model,
    _norm_token,
    _normalize_listing_model,
)

# --- the rungs themselves have to be justified, not just their bullets -------
#
# The gate above decides what a rung may SAY. This decides whether the rung may
# be LISTED at all. A ladder is read as "these are the trims of this vehicle",
# which is a factual claim about the lineup, and until now nothing checked it:
# the names came from hand-written ladders, machine-written ladders, CSV scrapes
# and — for anything none of those matched — a hard-coded generic table, so the
# 2023 Ram 1500 Limited page asserted nine trims and could quote none of them.
#
# ``brochure_extract.LADDER_RUNG_STORES`` is the policy; this is the mechanism.
# Two stores are admissible and both name something outside our own synthesis:
# an active row in ``cars``, or a verified citation into a PDF we re-opened.
#
# BOTH MATCH THE RUNG NAME EXACTLY (``_rung_evidence_key``). The first version of
# this gate matched fuzzily and thereby did the opposite of its job: it let a
# near-miss listing label mint a rung name nobody had written down and then
# stamped that rung ``{"store": "active_inventory"}``, which reads on the page as
# "dealers are listing this trim". An ungated rung is merely unsupported; a
# fabricated rung wearing a provenance label is laundered. If the evidence and
# the rendered name are not the same string, the rung does not render.


@lru_cache(maxsize=1)
def _active_make_spellings() -> dict[str, tuple[str, ...]]:
    """``_norm_make`` key → the literal ``cars.make`` strings that reduce to it.

    ``cars.make`` is not case-normalised ("Chevrolet" and "CHEVROLET" are both
    live), so a ``LOWER(make) = ?`` lookup silently splits one make's inventory
    in two and under-counts the evidence for its rungs. Resolving the spellings
    once keeps the per-vehicle query an indexable equality test.
    """
    from backend.db.inventory_db import db_conn

    out: dict[str, list[str]] = {}
    try:
        with db_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT DISTINCT make FROM cars "
                "WHERE COALESCE(listing_active, 1) = 1 AND make IS NOT NULL"
            )
            rows = cur.fetchall()
    except Exception as e:  # pragma: no cover - no inventory → no justification
        logger.debug("make spelling lookup failed: %s", e)
        return {}
    for row in rows:
        raw = str((row.get("make") if isinstance(row, dict) else row[0]) or "").strip()
        if not raw:
            continue
        out.setdefault(_norm_make(raw), []).append(raw)
    return {k: tuple(v) for k, v in out.items() if k}


@lru_cache(maxsize=4096)
def _active_trims_by_make_year(make_norm: str, year: int) -> tuple[tuple[str, str, int], ...]:
    """``(model, trim, active listing count)`` for one make in one EXACT model year."""
    from backend.db.inventory_db import db_conn

    spellings = _active_make_spellings().get(make_norm) or ()
    if not spellings:
        return ()
    placeholders = ",".join("?" for _ in spellings)
    sql = (
        "SELECT model, trim, COUNT(*) AS n FROM cars "
        "WHERE COALESCE(listing_active, 1) = 1 "
        f"AND make IN ({placeholders}) "
        "AND year = ? "
        "AND model IS NOT NULL AND TRIM(model) != '' "
        "AND trim IS NOT NULL AND TRIM(trim) != '' "
        "GROUP BY model, trim"
    )
    try:
        with db_conn() as conn:
            cur = conn.cursor()
            cur.execute(sql, (*spellings, year))
            rows = cur.fetchall()
    except Exception as e:  # pragma: no cover - no inventory → no justification
        logger.debug("inventory rung evidence query failed: %s", e)
        return ()
    out: list[tuple[str, str, int]] = []
    for row in rows:
        if isinstance(row, dict):
            model_raw, trim_raw, n = row.get("model"), row.get("trim"), row.get("n")
        else:
            model_raw, trim_raw, n = row[0], row[1], row[2]
        try:
            count = int(n)
        except (TypeError, ValueError):
            continue
        out.append((str(model_raw or "").strip(), str(trim_raw or "").strip(), count))
    return tuple(out)


def _inventory_rung_evidence(make: str, model: str, year: Any) -> tuple[tuple[str, int], ...]:
    """``(trim label, active listing count)`` observed for this exact year/make/model.

    EXACT model year on purpose. The neighbour-year reach that
    ``load_brochure_trim_overlay`` uses for brochures is wrong for a factual
    claim about a lineup — trims are added and dropped every model year — and the
    same objection applies to reading last year's listings.

    Model is matched on ``_norm_model`` equality, not prefix: "1500 Classic" is a
    different vehicle from "1500" and its trims (Warlock, SLT) are not the
    other's.
    """
    try:
        y = int(year)
    except (TypeError, ValueError):
        return ()
    make_norm = _norm_make(make)
    target = _norm_model(_normalize_listing_model(make, model))
    if not make_norm or not target:
        return ()
    counts: dict[str, int] = {}
    for model_raw, trim_raw, n in _active_trims_by_make_year(make_norm, y):
        if _norm_model(_normalize_listing_model(make, model_raw)) != target:
            continue
        counts[trim_raw] = counts.get(trim_raw, 0) + n
    return tuple(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))


def _rung_evidence_key(label: str) -> str:
    """The ONE normalisation a rung-name justification is allowed to apply.

    Case folding plus removal of every non-alphanumeric character, and nothing
    else. That collapses the spelling noise a dealer feed adds to a name it did
    not change — "Tremor®"/"Tremor", "ST-Line"/"ST Line" — and collapses nothing
    else.

    In particular this deliberately does NOT go through ``canonical_trim_name``
    /``preserve_trim_label``/``_clean_trim_label``, which the rest of this module
    uses to *guess* which rung a car sits on. Those rewrite a label into a
    different label: measured on the live fleet,
    ``canonical_trim_name("Sport Utility", "Ford", "Explorer")`` returns
    ``"Sport"`` and ``canonical_trim_name("Platinum RWD", …)`` returns
    ``"Platinum"``. Guessing a rung for the car in front of the shopper is a
    presentation choice we can be wrong about cheaply. Asserting "this trim
    exists in this model year" is a factual claim, so the string has to be one a
    dealer actually typed, not one we derived from it. The cost is real and
    accepted: a lot that only ever lists "Platinum RWD" is not counted as
    evidence for a "Platinum" rung, and that rung stays off the page.

    HTML entities and mojibake are decoded FIRST, because they are exactly the
    feed-added spelling noise this key exists to collapse and the squash alone
    gets them wrong: "Platinum&reg;" squashes to "platinumreg", not "platinum",
    so the ® the dealer's CMS escaped was silently un-justifying real rungs.
    Decoding changes no name — it restores the one the dealer typed.
    """
    import html as _html

    s = _html.unescape(_html.unescape(str(label or "")))  # twice: '&amp;reg;'
    # UTF-8 read as latin-1 leaves 'Â' before ®/™/degree signs; it is never a
    # letter a trim name contains.
    s = s.replace("Â", "")
    return _norm_token(s)


def _exact_inventory_rung_hits(
    steps_def: list[dict[str, Any]],
    *,
    make: str,
    model: str,
    year: Any,
) -> dict[int, tuple[int, tuple[str, ...]]]:
    """step index → (active listing count, the literal trim spellings observed).

    A step is hit only when its DISPLAYED NAME equals an observed trim string
    under ``_rung_evidence_key``. No fuzzy score, no prefix rule, no alias
    lookup:

      * fuzzy is what broke this. A ``>= 80`` prefix score let the 3 active
        "Sport Utility" listings of the 2026 Ford Explorer justify a rung named
        "Sport", and the gate then stamped it ``{"store": "active_inventory"}``.
        Measured 2026-08-01: of the 13 distinct trim strings on active 2026
        Explorer rows, none is "Sport", and no verified brochure citation for
        that model year is filed under that heading — so nothing we hold says
        the string. Inventing the name and then labelling it observed is worse
        than not gating at all.
      * the name is matched, not the aliases, because the name is the string
        rendered to the shopper. An alias table is our own synthesis; letting
        an observed "Ltd" print a rung called "Limited" would be us renaming
        the evidence.

    The literal spellings come back with the count so ``name_provenance`` can
    name the rows the claim rests on instead of just asserting a number.
    """
    if not steps_def:
        return {}
    observed: dict[str, list[tuple[str, int]]] = {}
    for trim_label, count in _inventory_rung_evidence(make, model, year):
        key = _rung_evidence_key(trim_label)
        if not key:
            continue
        observed.setdefault(key, []).append((trim_label, count))
    if not observed:
        return {}
    hits: dict[int, tuple[int, tuple[str, ...]]] = {}
    for i, step in enumerate(steps_def):
        key = _rung_evidence_key(str(step.get("name") or ""))
        if not key:
            continue
        rows = observed.get(key)
        if not rows:
            continue
        hits[i] = (
            sum(c for _, c in rows),
            tuple(sorted({lbl for lbl, _ in rows})),
        )
    return hits


def _justified_ladder_steps(
    steps_def: list[dict[str, Any]],
    *,
    make: str,
    model: str,
    year: Any,
    brochure_overlay: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Drop every rung we cannot point at a document or a row for.

    Returns the surviving steps (order preserved) and a ``{reason: n dropped}``
    tally for reporting. With ``TRIM_RUNGS_REQUIRE_PROVENANCE=0`` it is a no-op.
    """
    from backend.enrichment.brochure_extract import (
        trim_rungs_provenance_required,
        verified_overlay_trim_names,
    )

    if not trim_rungs_provenance_required():
        for step in steps_def:
            step.setdefault("name_provenance", {"store": "gate_off"})
        return list(steps_def), {}

    inventory_hits = _exact_inventory_rung_hits(
        steps_def, make=make, model=model, year=year
    )
    # Same exact-equality rule on the citation side. ``_trim_identity_keys`` used
    # to be used here and carries the same laundering risk as it does on the
    # inventory side — it would let a brochure heading "XSE Premium" justify a
    # rung printed as "XSE".
    quoted_keys = {
        k for k in (_rung_evidence_key(nm) for nm in verified_overlay_trim_names(brochure_overlay, for_year=year)) if k
    }

    kept: list[dict[str, Any]] = []
    dropped: dict[str, int] = {}
    for i, step in enumerate(steps_def):
        n_listings, observed_trims = inventory_hits.get(i, (0, ()))
        cited = _rung_evidence_key(str(step.get("name") or "")) in quoted_keys
        if n_listings:
            out = dict(step)
            out["name_provenance"] = {
                "store": "active_inventory",
                "active_listings": n_listings,
                "observed_trims": list(observed_trims),
                "year": year,
                "make": make,
                "model": model,
            }
            kept.append(out)
        elif cited:
            out = dict(step)
            out["name_provenance"] = {
                "store": "brochure_text_quoted",
                "source": str((brochure_overlay or {}).get("source_pdf") or ""),
                "year": (brochure_overlay or {}).get("year"),
            }
            kept.append(out)
        else:
            dropped["no_active_listing_and_no_verified_citation"] = (
                dropped.get("no_active_listing_and_no_verified_citation", 0) + 1
            )
    return kept, dropped
