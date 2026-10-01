#!/usr/bin/env python3
"""
TRIM-level coverage of active inventory: how much of the lineup we can quote.

THE QUESTION THIS ANSWERS
-------------------------
"Is every make/model/trim since 2015 accounted for?" Every count produced before
this one was at MODEL level ("we hold a brochure for the 2026 RAV4"). A shopper
does not buy a model; they buy a 2026 RAV4 XLE Premium. This report counts
(year, make, model, TRIM) combos and the active cars behind them, and says for
each one whether a document exists, whether a trim walk was parsed out of it, and
whether a bullet for THAT rung reaches the page.

WHY THIS VERSION EXISTS -- THE 2026-08-01 REPORT IS REFUTED
-----------------------------------------------------------
The previous run mapped an inventory trim onto a brochure rung with
``trim_ladder._lookup_brochure_adds_key``, the ladder's fuzzy map, and disclosed
the consequence only as an "upper bound" footnote. That was not good enough: the
map could name the WRONG rung, so some share of the covered number was one trim's
bullets counted for a different trim. The underlying defect has since been fixed
in the attribution lane -- ``_trim_match_score`` (100 equality / 80 prefix /
70 word-boundary / 60 substring, best score > 0 wins) is deleted and
``_exact_rung_match`` admits one relation, "the listing trim and the rung are the
same name". This report is rebuilt on top of that, and the ``superseded`` block
in the artifact carries the old numbers beside the new ones so the change is
readable rather than asserted.

HOW "COVERED" IS DECIDED NOW -- BY RENDERING, NOT BY RE-IMPLEMENTING
--------------------------------------------------------------------
This report does not reimplement the matching rule and then claim it agrees with
production. For every combo it CALLS ``trim_ladder.resolve_trim_ladder(make,
model, year, trim)`` -- the same function the car page calls -- and looks at what
came back:

  * the step flagged ``is_current`` is the rung production says this car IS. It
    is set from ``_exact_rung_match`` and from nothing else, and stays unset when
    the trim is not one of the rungs.
  * ``adds_citations`` on that step, entries whose ``store`` is
    ``brochure_text_quoted``, are the bullets that reach a shopper.

So ``verified_cited_bullet_for_this_trim`` is a measurement of the render path's
own output, not an upper bound on it. Where the old report said "the page must
also build a ladder step for this trim", this one has already checked that it
does.

WHERE THE VERIFIED-NESS COMES FROM
----------------------------------
Not from the citation register. The register is asked, at render time, whether it
issued a citation; asking it to confirm its own issuance is circular and has
certified synthesised text twice already. A bullet only reaches ``adds_citations``
after ``brochure_extract.admissible_overlay_adds`` has kept it, and for the one
admissible store (``brochure_text_quoted``) that function requires the
``verified: true`` stamp that ``backend/scripts/verify_trim_citations.py`` wrote
into ``derived/trim_adds_by_year/*.json`` after re-opening the PDF, hashing it and
re-extracting the cited page. This report reads those stamps. ``--recheck-stamps``
goes one step further and re-runs the verifier's document check underneath the
report instead of trusting the stamps; it is slow, off by default, and the
artifact records which mode ran.

THE ONE FUZZY HOP THAT SURVIVES, MEASURED RATHER THAN EXCUSED
--------------------------------------------------------------
``_exact_rung_match`` decides which LADDER STEP the car is. A second hop then maps
that step's NAME onto a key of the brochure overlay's ``adds_by_trim``, and that
hop is still ``_lookup_brochure_adds_key``, still fuzzy. So a rung the car
genuinely is could in principle be filled with a neighbouring overlay key's
bullets. That is not this lane's code to change, so it is measured instead: for
every covered combo the rendered bullet texts are looked up in the overlay file
and the key that actually holds them is compared to the step name. The result is
``attribution.rendered_bullets_by_owning_overlay_rung``, and any mismatch is
listed in full under ``attribution.cross_rung_bindings``.

THE CEILING
-----------
A BROCHURE COVERS A MODEL, NOT A TRIM. The 2026 Camry book is one document that
speaks for LE, SE, XLE and XSE at once, and a trim walk in it may list adds for
three of those four and leave the fourth to a spec grid. Model-level coverage
therefore cannot be multiplied out into trim-level coverage, and this report never
does. ``ceiling`` states the realistic maximum two ways: the combos whose exact
model year we hold a document for (the arithmetic bound, which assumes every book
walks every rung -- an assumption, not a measurement), and that bound multiplied
by the rate ACTUALLY achieved on models that already have a verified trim walk,
which is the only empirical read we have on how much of a lineup a walked book
really covers.

Usage
-----
    python backend/scripts/trim_coverage_report.py
    python backend/scripts/trim_coverage_report.py --min-year 2015 --top 40
    python backend/scripts/trim_coverage_report.py --json workspace/trim_coverage_2015_plus.json
    python backend/scripts/trim_coverage_report.py --no-render     # corpus only, fast
    python backend/scripts/trim_coverage_report.py --recheck-stamps # re-open the PDFs
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.db.connect import connection as db_connection, inventory_dsn  # noqa: E402

from backend.enrichment.brochure_extract import (  # noqa: E402
    ADMISSIBLE_LADDER_BULLET_STORES,
    ADMISSIBLE_OVERLAY_SOURCES,
    CITATION_VERIFIED_KEY,
    admissible_overlay_adds,
    trim_adds_provenance_required,
)
from backend.enrichment.brochure_sources import gap_group_key  # noqa: E402
from backend.enrichment.dictionary_catalog import canonical_make, catalog_key  # noqa: E402
from backend.enrichment.dictionary_paths import (  # noqa: E402
    BROCHURE_TEXT_DIR,
    DERIVED_DIR,
    TRIM_ADDS_BY_YEAR_DIR,
)
from backend.enrichment.trim_ladder import (  # noqa: E402
    _exact_rung_match,
    _lookup_brochure_adds_key,
    _norm_token,
    _rung_claim_forms,
    resolve_trim_ladder,
)

CONTENT_INDEX_PATH = DERIVED_DIR / "brochure_content_index.json"
VERIFICATION_REPORT_PATH = DERIVED_DIR / "trim_citation_verification.json"
DEFAULT_ARTIFACT = _REPO / "workspace" / "trim_coverage_2015_plus.json"

#: Bumped because the definition of ``covered`` changed, not just the data. Any
#: consumer comparing this artifact to a 2026-08-01.x one is comparing two
#: different questions.
REPORT_VERSION = "2026-08-02.2"

#: .2 adds two sections and changes no existing definition:
#:   ``wrong_spec_exposure`` -- mis-bound bullets scored on EVERY rendered rung,
#:     where .1 scored only the step flagged ``is_current`` (that figure is kept
#:     beside the new one as ``if_scored_on_is_current_only``);
#:   ``ordering``            -- where each ladder's ORDER came from, per ladder
#:     and per rung, read off ``resolve_trim_ladder``'s ``order_provenance``.
#: ``coverage`` is comparable between .1 and .2.

#: Gap buckets, in the order a fix would be applied. Each needs a different fix.
BUCKET_NO_DOCUMENT = "no_document"
BUCKET_DOCUMENT_NO_TRIM_WALK = "document_but_no_trim_walk"
BUCKET_TRIM_WALK_WITHOUT_THIS_TRIM = "trim_walk_but_trim_not_covered"

#: Why a combo landed in ``trim_walk_but_trim_not_covered``. Same bucket, four
#: very different jobs: re-quote the book, re-run the verifier, extract a rung,
#: or get the ladder to list a rung the book already walks.
REASON_SOURCE_NOT_ADMISSIBLE = "trim_walk_source_not_quotable"
REASON_NO_VERIFIED_BULLETS = "quoted_walk_but_nothing_verified"
REASON_TRIM_ABSENT = "verified_walk_but_this_trim_absent"
REASON_RUNG_NOT_ON_LADDER = "verified_walk_has_this_trim_but_ladder_has_no_such_rung"


# --------------------------------------------------------------------------
# Inventory
# --------------------------------------------------------------------------


def _inventory_dsn() -> str:
    """The inventory DSN (backend.db.connect precedence), EXPORTED to the process.

    THE DSN IS EXPORTED, not just returned, and that is load-bearing. This report
    calls ``resolve_trim_ladder``, which builds a ladder out of active inventory
    (``_ladder_from_inventory``) for every model with no curated or CSV ladder,
    and that query reads ``INVENTORY_DATABASE_URL`` from the process environment.
    A first cut of this script read the DSN straight out of ``.env`` for its own
    connection and left the environment empty; ``resolve_trim_ladder`` then
    returned None for every inventory-backed model, the probe saw no ladder, and
    the run reported 48 combos with a rung where the true figure is thousands.
    Reading .env without exporting it is a silent, plausible-looking undercount,
    so it is done once, here, for the whole process (``export=True``).
    """
    return inventory_dsn(export=True)


def assert_ladders_reachable() -> None:
    """Fail loudly if the inventory-backed ladder path is dead before measuring.

    ``resolve_trim_ladder`` swallows a failed inventory query and returns None,
    which is indistinguishable from "this model legitimately has no ladder". The
    difference decides the headline number, so it is checked rather than assumed:
    the most numerous model in active inventory must produce a ladder with at
    least two rungs.
    """
    with db_connection(timeout=None, dsn=_inventory_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT year, make, model, trim, COUNT(*) AS n
                FROM cars
                WHERE listing_removed_at IS NULL AND year >= 2015
                  AND make IS NOT NULL AND btrim(make) <> ''
                  AND model IS NOT NULL AND btrim(model) <> ''
                  AND trim IS NOT NULL AND btrim(trim) <> ''
                GROUP BY 1, 2, 3, 4
                ORDER BY n DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
    if not row:
        raise SystemExit("no active inventory 2015+ -- nothing to measure")
    year, make, model, trim, _n = row
    probe = resolve_trim_ladder(make=make, model=model, year=year, trim=trim)
    steps = (probe or {}).get("steps") or []
    if len(steps) < 2:
        raise SystemExit(
            "resolve_trim_ladder returned no usable ladder for the largest model in "
            f"inventory ({year} {make} {model}). The ladder path is broken or the "
            "inventory DSN is not visible to it; refusing to publish a coverage "
            "number measured through a dead render path."
        )


def fetch_inventory(min_year: int) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """
    One row per (year, make, model, trim) spelling in ACTIVE inventory.

    Active = ``listing_removed_at IS NULL``, the predicate the rest of the
    codebase uses. Rows with no trim string at all are returned too, flagged
    blank: they cannot be matched to a rung by anyone, so they are reported
    separately rather than silently inflating or deflating the base.
    """
    with db_connection(timeout=None, dsn=_inventory_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT year, make, model, trim, COUNT(*)
                FROM cars
                WHERE listing_removed_at IS NULL
                  AND year IS NOT NULL AND year >= %s
                  AND make IS NOT NULL AND btrim(make) <> ''
                  AND model IS NOT NULL AND btrim(model) <> ''
                GROUP BY 1, 2, 3, 4
                """,
                (min_year,),
            )
            raw = cur.fetchall()

    rows: list[dict[str, Any]] = []
    stats = {"active_cars": 0, "blank_trim_cars": 0, "raw_rows": len(raw)}
    for year, make, model, trim, count in raw:
        count = int(count)
        stats["active_cars"] += count
        label = " ".join(str(trim or "").split())
        if not label:
            stats["blank_trim_cars"] += count
        rows.append(
            {
                "year": int(year),
                "make": make,
                "model": model,
                "trim": label,
                "cars": count,
                "catalog_key": catalog_key(int(year), make, model),
                "group_key": gap_group_key(int(year), make, model),
            }
        )
    return rows, stats


def fold_combos(
    rows: list[dict[str, Any]],
) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[str, int]]:
    """
    Fold inventory spellings into one entry per (catalog_key, normalized trim).

    Trims are folded case-insensitively with runs of whitespace collapsed, and
    NOTHING else: "SE" and "S E" stay different vehicles because they are. The
    most common raw spelling is kept for display, and is the string handed to
    ``resolve_trim_ladder`` -- matching a cleaned-up spelling would measure a
    string no listing actually carries.
    """
    combos: dict[tuple[str, str], dict[str, Any]] = {}
    stats = {"blank_trim_combos": 0}
    for row in rows:
        if not row["trim"]:
            continue
        key = (row["catalog_key"], row["trim"].lower())
        entry = combos.get(key)
        if entry is None:
            entry = combos[key] = {
                "catalog_key": row["catalog_key"],
                "group_key": row["group_key"],
                "year": row["year"],
                "make": canonical_make(row["make"]),
                "model": row["model"],
                "trim": row["trim"],
                "cars": 0,
                "_spellings": defaultdict(int),
            }
        entry["cars"] += row["cars"]
        entry["_spellings"][(row["make"], row["model"], row["trim"])] += row["cars"]

    blank: set[tuple[str, str]] = set()
    for row in rows:
        if not row["trim"]:
            blank.add((row["catalog_key"], ""))
    stats["blank_trim_combos"] = len(blank)

    for entry in combos.values():
        make, model, trim = max(entry.pop("_spellings").items(), key=lambda kv: kv[1])[0]
        entry["raw_make"], entry["raw_model"] = make, model
        entry["make"], entry["model"], entry["trim"] = canonical_make(make), model, trim
    return combos, stats


# --------------------------------------------------------------------------
# What we hold
# --------------------------------------------------------------------------


def load_document_index() -> dict[str, Any]:
    """
    Catalog keys we hold a document for, split by what exactly we hold.

    ``text_keys``  -- ``derived/brochure_text/<key>.json`` exists. The extraction
                      may outlive the PDF; those are the entries the verifier
                      fails as ``source_pdf_not_held``.
    ``pdf_keys``   -- the content index names a stored PDF for the key AND the
                      file is on disk right now (checked, not assumed).
    Both are keyed by EXACT model year. No +/-2 reach: a 2019 book is not
    evidence about a 2022 car, which is the same rule the citation path applies.
    """
    text_keys: set[str] = set()
    newest = 0.0
    for path in BROCHURE_TEXT_DIR.glob("*.json"):
        if path.name.startswith("_"):
            continue
        text_keys.add(path.stem.replace("__", "|"))
        try:
            newest = max(newest, path.stat().st_mtime)
        except OSError:
            pass

    pdf_keys: set[str] = set()
    documents: dict[str, Any] = {}
    if CONTENT_INDEX_PATH.exists():
        documents = (json.loads(CONTENT_INDEX_PATH.read_text(encoding="utf-8")) or {}).get(
            "documents"
        ) or {}
    held_files = 0
    for doc in documents.values():
        path = str((doc or {}).get("path") or "")
        if not path or not Path(path).exists():
            continue
        held_files += 1
        for key in (doc or {}).get("catalog_keys") or []:
            pdf_keys.add(str(key))

    return {
        "text_keys": text_keys,
        "pdf_keys": pdf_keys,
        "indexed_documents": len(documents),
        "indexed_documents_on_disk": held_files,
        # The corpus is a live directory that other work adds to and prunes. The
        # newest transcript's mtime is recorded so a reader can tell whether the
        # corpus moved around this run instead of assuming it is a fixed asset.
        "brochure_text_newest_mtime": (
            datetime.fromtimestamp(newest, timezone.utc).isoformat(timespec="seconds")
            if newest
            else None
        ),
    }


def load_overlays(wanted: set[str]) -> dict[str, dict[str, Any]]:
    """
    Load the ``trim_adds_by_year`` overlay for each wanted catalog key.

    Only the keys inventory actually asks about are opened -- the store holds
    thousands of files and inventory 2015+ touches a fraction of them.
    """
    out: dict[str, dict[str, Any]] = {}
    for key in wanted:
        path = TRIM_ADDS_BY_YEAR_DIR / f"{key.replace('|', '__')}.json"
        if not path.exists():
            continue
        try:
            overlay = json.loads(path.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            continue
        if isinstance(overlay, dict):
            overlay["_file"] = path.name
            out[key] = overlay
    return out


def overlay_verified_rungs(overlay: dict[str, Any], year: int) -> dict[str, list[str]]:
    """
    Rungs of one overlay that can put a bullet in front of a shopper, for ``year``.

    This is the production gate, called as production calls it:
    ``admissible_overlay_adds`` drops non-quotable sources whole, drops bullets
    whose provenance entry names no file or page, drops any bullet the build-time
    verifier did not stamp ``verified: true``, and drops the whole overlay when
    its model year is not the car's. Empty dict = nothing renders.
    """
    adds = admissible_overlay_adds(overlay, for_year=year)
    return {trim: bullets for trim, bullets in adds.items() if bullets}


def overlay_stamp_counts(overlay: dict[str, Any]) -> dict[str, int]:
    """Raw ``verified`` stamp tally for one overlay -- reported, not trusted for coverage."""
    counts = {"stamped_true": 0, "stamped_false": 0, "unstamped": 0}
    for entries in (overlay.get("adds_provenance") or {}).values():
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            flag = entry.get(CITATION_VERIFIED_KEY)
            if flag is True:
                counts["stamped_true"] += 1
            elif CITATION_VERIFIED_KEY in entry:
                counts["stamped_false"] += 1
            else:
                counts["unstamped"] += 1
    return counts


def parsed_trim_walk(overlay: dict[str, Any] | None) -> dict[str, list[str]]:
    """``adds_by_trim`` as parsed, before any admissibility gate. Presence, not permission."""
    if not isinstance(overlay, dict):
        return {}
    adds = overlay.get("adds_by_trim")
    if not isinstance(adds, dict):
        return {}
    return {str(k): list(v) for k, v in adds.items() if isinstance(v, list) and v}


# --------------------------------------------------------------------------
# Attribution: which rung, if any, IS this car
# --------------------------------------------------------------------------


def exact_rung(
    listing_trim: str,
    rung_names: list[str],
    *,
    make: str,
    model: str,
) -> tuple[str | None, str]:
    """
    The one rung this trim IS, out of ``rung_names``, or None -- production's rule.

    This is ``_build_ladder_result``'s choke point applied to a bare list of rung
    names instead of to ladder steps, and it reproduces all three of its parts:

      * tier 1 (labels agree as written, bar case, punctuation and the enumerated
        drivetrain / cab / transmission / engine-badge noise) is tried across
        every name first;
      * tier 2 (a FUSED drivetrain dropped from the listing, onto a rung whose own
        name is drivetrain-neutral) is consulted only if tier 1 found nothing
        anywhere -- mixing them lets the weaker reading of one rung out-argue the
        literal reading of another;
      * two DIFFERENTLY-NAMED rungs both answering means the names cannot tell us
        which one this is, so the claim is refused rather than resolved by order.

    ``aliases`` is empty here because an overlay key is a bare brochure heading
    and carries none. Ladder steps do carry aliases, so this predicate is very
    slightly STRICTER than the ladder's; the coverage number does not rest on it
    -- ``covered`` comes from the render probe -- and the difference between the
    two is reported as ``attribution.exact_overlay_rung_vs_rendered``.

    Returns the rung name and a status: ``matched``, ``none`` or ``ambiguous``.
    """
    if not str(listing_trim or "").strip() or not rung_names:
        return None, "none"

    def hits(tier: int) -> list[str]:
        return [
            name
            for name in rung_names
            if _exact_rung_match(
                str(listing_trim), str(name), [], make=make, model=model, tier=tier
            )
        ]

    matched = hits(1) or hits(2)
    if not matched:
        return None, "none"
    if len({_norm_token(name) for name in matched}) != 1:
        return None, "ambiguous"
    return matched[0], "matched"


def same_rung_name(a: str, b: str, *, make: str, model: str) -> bool:
    """True when two labels name one trim under the production claim forms."""
    forms_a = _rung_claim_forms(str(a or ""), make, model)
    forms_b = _rung_claim_forms(str(b or ""), make, model)
    return bool(forms_a and forms_b and (forms_a & forms_b))


def render_probe(entry: dict[str, Any]) -> dict[str, Any]:
    """
    Call the production ladder for this combo and report what a shopper would see.

    ``resolve_trim_ladder`` is the car page's own entry point. The RAW inventory
    trim string is passed, not a cleaned one, because that is what the page is
    given. What comes back that matters here:

      ``matched`` / ``match_kind``  -- whether ``_exact_rung_match`` found the rung
      ``is_current`` step            -- the rung production says the car IS
      ``adds_citations``             -- the bullets that reach the page, each
                                        naming the store it came from. Only
                                        ``brochure_text_quoted`` is admissible,
                                        and it is verified-per-bullet, so a
                                        citation with that store is a bullet the
                                        verifier signed off against the PDF.

    Any exception is caught and returned as ``error``: one bad ladder definition
    must not decide the report's headline number by aborting the run.
    """
    try:
        result = resolve_trim_ladder(
            make=entry.get("raw_make") or entry["make"],
            model=entry.get("raw_model") or entry["model"],
            year=entry["year"],
            trim=entry["trim"],
        )
    except Exception as exc:  # noqa: BLE001 - a broken ladder must not kill the report
        return {"error": f"{type(exc).__name__}: {exc}", "ladder": False}

    if not result:
        return {"ladder": False, "matched": False, "rung": None, "bullets": [], "stores": {}}

    steps = result.get("steps") or []
    current = next((s for s in steps if s.get("is_current")), None)
    stores: dict[str, int] = defaultdict(int)
    quoted: list[str] = []
    if current:
        for citation in current.get("adds_citations") or []:
            store = str((citation or {}).get("store") or "")
            stores[store] += 1
            if store in ADMISSIBLE_LADDER_BULLET_STORES:
                quoted.append(str((citation or {}).get("text") or ""))

    # EVERY rung, not only the shopper's own. The card stack renders the whole
    # ladder, so a bullet bound to the wrong section is on the shopper's screen
    # whether or not it sits on the rung flagged ``is_current``. A previous
    # revision of this report collected bullets from ``current`` alone and
    # therefore could not see the other rungs' bindings at all.
    per_step: list[dict[str, Any]] = []
    for step in steps:
        step_quoted: list[str] = []
        step_stores: dict[str, int] = defaultdict(int)
        for citation in step.get("adds_citations") or []:
            store = str((citation or {}).get("store") or "")
            step_stores[store] += 1
            if store in ADMISSIBLE_LADDER_BULLET_STORES:
                text = str((citation or {}).get("text") or "")
                if text:
                    step_quoted.append(text)
        per_step.append(
            {
                "name": str(step.get("name") or ""),
                "is_current": bool(step.get("is_current")),
                "bullets": step_quoted,
                "stores": dict(step_stores),
                "order_basis": str(
                    (step.get("order_basis") or {}).get("basis") or "unproven"
                ),
            }
        )

    return {
        "ladder": True,
        "ladder_source": str(result.get("source") or ""),
        "matched": bool(result.get("matched")),
        "match_kind": str(result.get("match_kind") or ""),
        "rung": str((current or {}).get("name") or "") or None,
        "rungs_on_ladder": [str(s.get("name") or "") for s in steps],
        "bullets": [b for b in quoted if b],
        "stores": dict(stores),
        "steps": per_step,
        "order_provenance": dict(result.get("order_provenance") or {}),
    }


def _norm_bullet(text: Any) -> str:
    """Normalise a bullet for ownership comparison: case, whitespace, edge punctuation."""
    collapsed = re.sub(r"\s+", " ", str(text or "")).strip().lower()
    return collapsed.strip(" .,;:-–—")


def owning_overlay_rungs(
    overlay: dict[str, Any] | None, bullets: list[str]
) -> list[str]:
    """
    Which ``adds_by_trim`` keys hold the bullets that actually rendered.

    The render path binds a ladder step to an overlay key through the still-fuzzy
    ``_lookup_brochure_adds_key``, so the step's name and the key whose text it
    printed are not guaranteed to be the same trim. Comparing the RENDERED TEXT
    back to the overlay file answers that without asking either function to grade
    itself.

    CONTAINMENT, NOT EQUALITY, AND IT IS NOT A DETAIL. The ladder does not print
    overlay bullets verbatim: ``trim_ladder`` splits a long brochure line on its
    commas and re-capitalises the pieces, so the 2026 Highlander's
    "Color-keyed heated power outside mirrors with turn signal and blind spot
    warning indicators, power folding, reverse tilt-down ..." reaches the page as
    three separate bullets, none of which is string-equal to anything in the
    overlay. An equality test therefore reports a rung's OWN bullets as
    un-owned. Measured here on 2026-08-02: equality found no owner for 100 of the
    1,708 rendered bullets, every one of them a fragment of that same rung's own
    line. Containment finds all 1,708.

    MORE THAN ONE OWNER IS NORMAL, not a finding. Brochures repeat a line on
    several rungs ("LED fog lights" under both Rock Creek and SL), so a bullet
    can legitimately be found under two keys.
    """
    if not isinstance(overlay, dict) or not bullets:
        return []
    wanted = [b for b in (_norm_bullet(x) for x in bullets) if b]
    if not wanted:
        return []
    owners: list[str] = []
    for key, values in (overlay.get("adds_by_trim") or {}).items():
        if not isinstance(values, list):
            continue
        haystack = [_norm_bullet(v) for v in values]
        if any(w in h for w in wanted for h in haystack if h):
            owners.append(str(key))
    return sorted(owners)


def unowned_bullets(
    overlay: dict[str, Any] | None, rung_name: str, bullets: list[str]
) -> list[str]:
    """
    Rendered bullets on ``rung_name`` that this year's overlay does NOT file under it.

    PER BULLET, deliberately. The set-level question "does some owner share the
    rung's name" passes a rung as soon as ONE of its bullets is its own, which
    hides a rung that prints four correct lines and one from the neighbouring
    section -- exactly the mixed case that matters most. Every bullet is checked
    against the key that bears the rung's own name.
    """
    if not isinstance(overlay, dict) or not bullets:
        return []
    own = [
        _norm_bullet(v)
        for v in ((overlay.get("adds_by_trim") or {}).get(rung_name) or [])
    ]
    stray: list[str] = []
    for bullet in bullets:
        needle = _norm_bullet(bullet)
        if not needle:
            continue
        if not any(needle in hay for hay in own if hay):
            stray.append(bullet)
    return stray


# --------------------------------------------------------------------------
# Classification
# --------------------------------------------------------------------------


def classify(
    combos: dict[tuple[str, str], dict[str, Any]],
    docs: dict[str, Any],
    overlays: dict[str, dict[str, Any]],
    *,
    render: bool,
    progress: bool = False,
) -> list[dict[str, Any]]:
    """
    Decide, per combo, the furthest of the three steps it reached.

    ``covered`` is decided by ``render_probe`` -- the production ladder printed at
    least one ``brochure_text_quoted`` bullet on the rung it says this car is. The
    overlay-level exact match is computed alongside it and kept as
    ``exact_overlay_rung`` so the two can be compared, but it does not set
    ``covered``. With ``--no-render`` there is no probe and ``covered`` falls back
    to the overlay-level exact match; the artifact records which mode ran, because
    the two are not the same number.
    """
    text_keys: set[str] = docs["text_keys"]
    pdf_keys: set[str] = docs["pdf_keys"]
    group_has_text: dict[str, bool] = defaultdict(bool)
    for entry in combos.values():
        if entry["catalog_key"] in text_keys:
            group_has_text[entry["group_key"]] = True

    out: list[dict[str, Any]] = []
    total = len(combos)
    started = time.time()
    for index, entry in enumerate(combos.values(), 1):
        if progress and index % 1000 == 0:
            rate = index / max(time.time() - started, 1e-6)
            print(
                f"  ... {index:,}/{total:,} combos ({rate:.0f}/s)", file=sys.stderr, flush=True
            )
        key = entry["catalog_key"]
        overlay = overlays.get(key)
        walk = parsed_trim_walk(overlay)
        verified_rungs = overlay_verified_rungs(overlay, entry["year"]) if overlay else {}
        source = str((overlay or {}).get("source") or "")
        make, model, trim = entry["make"], entry["model"], entry["trim"]

        exact_v, exact_v_status = exact_rung(
            trim, list(verified_rungs), make=make, model=model
        )
        exact_w, _ = exact_rung(trim, list(walk), make=make, model=model)

        # The revoked rule, computed only so the change can be stated as a
        # number. It is never allowed to set ``covered``.
        legacy_v = (
            _lookup_brochure_adds_key(trim, [], verified_rungs, make=make, model=model)
            if verified_rungs
            else None
        )
        legacy_w = (
            _lookup_brochure_adds_key(trim, [], walk, make=make, model=model) if walk else None
        )

        probe = render_probe(entry) if render else {}
        rendered_bullets = list(probe.get("bullets") or [])
        owners = owning_overlay_rungs(overlay, rendered_bullets)

        # WRONG-SPEC EXPOSURE, scored on EVERY rung the page draws.
        # For each rung that printed at least one quotable bullet, look the
        # rendered TEXT back up in the overlay file and ask which adds_by_trim
        # key actually holds it. If no owning key names the same trim as the
        # rung the bullet is printed under, the shopper is reading one trim's
        # equipment under another trim's heading.
        crossed_rungs: list[dict[str, Any]] = []
        rungs_with_bullets = 0
        quoted_bullets_all_rungs = 0
        stray_bullets_all_rungs = 0
        for step in probe.get("steps") or []:
            step_bullets = list(step.get("bullets") or [])
            if not step_bullets:
                continue
            rungs_with_bullets += 1
            quoted_bullets_all_rungs += len(step_bullets)

            stray = unowned_bullets(overlay, step["name"], step_bullets)
            if not stray:
                continue
            stray_bullets_all_rungs += len(stray)
            # The stray text exists somewhere -- say where. If another key of THIS
            # year's overlay holds it, the shopper is reading that trim's
            # equipment under this heading. If no key does, we cannot name the
            # owner and say so rather than guessing.
            elsewhere = [
                owner
                for owner in owning_overlay_rungs(overlay, stray)
                if not same_rung_name(step["name"], owner, make=make, model=model)
            ]
            crossed_rungs.append(
                {
                    "rung": step["name"],
                    "is_current": step["is_current"],
                    "bullets": len(step_bullets),
                    "stray_bullets": len(stray),
                    "stray_examples": stray[:3],
                    "owned_by": elsewhere,
                    "kind": (
                        "bound_to_a_differently_named_section"
                        if elsewhere
                        else "owner_not_found_in_this_years_overlay"
                    ),
                }
            )

        order_prov = probe.get("order_provenance") or {}

        # Kept only long enough for ``exposure_detector_control`` to re-score
        # these same rungs against a rung they are NOT, and popped before the
        # artifact is written. A zero exposure number is worth nothing unless the
        # detector that produced it demonstrably fires on a wrong binding.
        control_payload = (
            [(s["name"], list(s["bullets"])) for s in (probe.get("steps") or []) if s["bullets"]]
            if rungs_with_bullets
            else []
        )

        record = dict(entry)
        record.pop("raw_make", None)
        record.pop("raw_model", None)
        record.update(
            {
                "has_brochure_text": key in text_keys,
                "has_brochure_pdf": key in pdf_keys,
                "has_document_under_sibling_spelling": (
                    key not in text_keys and group_has_text.get(entry["group_key"], False)
                ),
                "trim_walk_parsed": bool(walk),
                "trim_walk_source": source,
                "trim_walk_rungs": len(walk),
                "trim_in_parsed_walk": exact_w is not None,
                "verified_rungs": len(verified_rungs),
                "exact_overlay_rung": exact_v,
                "exact_overlay_rung_status": exact_v_status,
                "exact_overlay_bullets": len(verified_rungs.get(exact_v, [])) if exact_v else 0,
                "legacy_fuzzy_rung": legacy_v,
                "legacy_fuzzy_walk_rung": legacy_w,
                "legacy_credited_a_different_rung": bool(
                    legacy_v and not same_rung_name(trim, legacy_v, make=make, model=model)
                ),
                "rendered": bool(render),
                "render_has_ladder": bool(probe.get("ladder")),
                "render_rungs_on_ladder": len(probe.get("rungs_on_ladder") or []),
                "render_matched": bool(probe.get("matched")),
                "render_rung": probe.get("rung"),
                "render_bullets": len(rendered_bullets),
                "render_bullet_owner_rungs": owners,
                "render_error": probe.get("error"),
                "cross_rung_binding": bool(
                    rendered_bullets
                    and probe.get("rung")
                    and owners
                    and not any(
                        same_rung_name(str(probe.get("rung")), owner, make=make, model=model)
                        for owner in owners
                    )
                ),
                # --- every-rung scoring (item 6) ---
                "render_rungs_with_quoted_bullets": rungs_with_bullets,
                "render_quoted_bullets_all_rungs": quoted_bullets_all_rungs,
                "render_stray_bullets_all_rungs": stray_bullets_all_rungs,
                "crossed_rungs": crossed_rungs,
                "crossed_rung_count": len(crossed_rungs),
                "any_rung_cross_bound": bool(crossed_rungs),
                "any_rung_bound_to_other_section": any(
                    c["kind"] == "bound_to_a_differently_named_section" for c in crossed_rungs
                ),
                "any_rung_owner_not_found": any(
                    c["kind"] == "owner_not_found_in_this_years_overlay" for c in crossed_rungs
                ),
                # --- ordering (item 7) ---
                "order_basis_verdict": str(order_prov.get("basis") or "") or None,
                "order_counts": dict(order_prov.get("counts") or {}),
                "order_ordered_rungs": order_prov.get("ordered_rungs"),
                "order_total_rungs": order_prov.get("total_rungs"),
                "_control_payload": control_payload,
            }
        )
        record["covered"] = (
            record["render_bullets"] > 0 if render else record["exact_overlay_rung"] is not None
        )

        if record["covered"]:
            record["bucket"] = None
            record["reason"] = None
        elif not (record["has_brochure_text"] or record["has_brochure_pdf"]):
            record["bucket"] = BUCKET_NO_DOCUMENT
            # A trim walk with no document under it is still a no-document gap:
            # every one of those walks came from a non-quotable source, so it can
            # never render, and the fix is to go and get the book.
            if record["has_document_under_sibling_spelling"]:
                record["reason"] = "document_held_under_sibling_spelling"
            elif walk:
                record["reason"] = "no_document_but_an_unquotable_walk_exists"
            else:
                record["reason"] = "no_document_at_all"
        elif not walk:
            record["bucket"] = BUCKET_DOCUMENT_NO_TRIM_WALK
            record["reason"] = "document_never_trim_walked"
        else:
            record["bucket"] = BUCKET_TRIM_WALK_WITHOUT_THIS_TRIM
            if source not in ADMISSIBLE_OVERLAY_SOURCES:
                record["reason"] = REASON_SOURCE_NOT_ADMISSIBLE
            elif not verified_rungs:
                record["reason"] = REASON_NO_VERIFIED_BULLETS
            elif exact_v is not None:
                # The book walks this exact trim and the verifier signed the
                # bullets off, yet nothing rendered: the ladder never offered a
                # rung by that name for the page to stamp. A different fix from
                # the three above -- it is a ladder-definition gap, not a
                # document gap.
                record["reason"] = REASON_RUNG_NOT_ON_LADDER
            else:
                record["reason"] = REASON_TRIM_ABSENT
        out.append(record)
    return out


def summarize(records: list[dict[str, Any]]) -> dict[str, Any]:
    """The five headline numbers, each in combos and in active cars."""

    def tally(predicate) -> dict[str, int]:
        hits = [r for r in records if predicate(r)]
        return {"combos": len(hits), "cars": sum(r["cars"] for r in hits)}

    total = tally(lambda r: True)
    return {
        "combos_total": total["combos"],
        "cars_total": total["cars"],
        "brochure_text_for_exact_model_year": tally(lambda r: r["has_brochure_text"]),
        "brochure_pdf_on_disk_for_exact_model_year": tally(lambda r: r["has_brochure_pdf"]),
        "trim_walk_parsed_for_model": tally(lambda r: r["trim_walk_parsed"]),
        "this_trim_present_in_parsed_walk": tally(lambda r: r["trim_in_parsed_walk"]),
        "verified_cited_bullet_for_this_trim": tally(lambda r: r["covered"]),
        "covered_combos": sorted(
            (
                {
                    "year": r["year"],
                    "make": r["make"],
                    "model": r["model"],
                    "trim": r["trim"],
                    "cars": r["cars"],
                    "rung": r["render_rung"] or r["exact_overlay_rung"],
                    "bullets": r["render_bullets"] or r["exact_overlay_bullets"],
                    "bullet_owner_rungs": r["render_bullet_owner_rungs"],
                    "cross_rung_binding": r["cross_rung_binding"],
                }
                for r in records
                if r["covered"]
            ),
            key=lambda r: -r["cars"],
        ),
    }


def attribution_audit(records: list[dict[str, Any]]) -> dict[str, Any]:
    """
    What the exact-match fix did, and what fuzziness is left, as numbers.

    Three separate things live here and they must not be read as one:

      * ``revoked_fuzzy_rung_credit`` -- combos the OLD rule credited with a rung
        whose name is not this trim's. These are the wrong-car claims the
        previous report counted as covered.
      * ``exact_overlay_rung_vs_rendered`` -- this report's own strict predicate
        against the render probe. Disagreement is expected in one direction
        (aliases and ladder-step names the overlay key does not carry) and is a
        bug in the other.
      * ``cross_rung_bindings`` -- the surviving fuzzy hop, step name to overlay
        key, caught by comparing rendered TEXT back to the overlay file.
    """
    revoked = [r for r in records if r["legacy_credited_a_different_rung"]]
    strict_only = [
        r for r in records if r["exact_overlay_rung"] and r["rendered"] and not r["covered"]
    ]
    render_only = [r for r in records if r["covered"] and not r["exact_overlay_rung"]]
    ambiguous = [r for r in records if r["exact_overlay_rung_status"] == "ambiguous"]
    crossed = [r for r in records if r["cross_rung_binding"]]
    errors = [r for r in records if r.get("render_error")]

    def block(rows: list[dict[str, Any]]) -> dict[str, int]:
        return {"combos": len(rows), "cars": sum(r["cars"] for r in rows)}

    return {
        "rule": (
            "trim_ladder._exact_rung_match, tier 1 then tier 2, with the "
            "two-differently-named-rungs ambiguity guard. _trim_match_score is deleted."
        ),
        # Sanity floor for the whole report. A run where the ladder path is dead
        # -- no DSN in the environment, a broken definition store -- looks exactly
        # like a run where coverage is genuinely poor, except that almost nothing
        # has a ladder at all. These three lines make the difference visible in
        # the artifact rather than only in ``assert_ladders_reachable``.
        "ladder_reach": {
            "combos_with_a_ladder": block([r for r in records if r.get("render_has_ladder")]),
            "combos_whose_trim_IS_one_of_its_rungs": block(
                [r for r in records if r["render_matched"]]
            ),
            "of_those_with_a_quotable_bullet": block([r for r in records if r["covered"]]),
        },
        "revoked_fuzzy_rung_credit": {
            **block(revoked),
            "note": (
                "the old _lookup_brochure_adds_key named a rung whose name is not "
                "this trim; every one of these was counted covered on 2026-08-01"
            ),
            "examples": [
                {
                    "year": r["year"],
                    "make": r["make"],
                    "model": r["model"],
                    "trim": r["trim"],
                    "cars": r["cars"],
                    "old_rule_credited_rung": r["legacy_fuzzy_rung"],
                }
                for r in sorted(revoked, key=lambda r: -r["cars"])[:25]
            ],
        },
        "exact_overlay_rung_vs_rendered": {
            "agree_covered": block([r for r in records if r["covered"] and r["exact_overlay_rung"]]),
            "exact_overlay_rung_but_nothing_rendered": {
                **block(strict_only),
                "note": (
                    "the book walks this exact trim with verified bullets but the "
                    "ladder has no rung by that name to hang them on"
                ),
            },
            "rendered_but_no_exact_overlay_rung": {
                **block(render_only),
                "note": (
                    "the ladder step matched on an ALIAS or on a step name the "
                    "overlay key does not carry; inspect these, they are the only "
                    "way the render path can be looser than the strict predicate"
                ),
                "examples": [
                    {
                        "year": r["year"],
                        "make": r["make"],
                        "model": r["model"],
                        "trim": r["trim"],
                        "cars": r["cars"],
                        "render_rung": r["render_rung"],
                        "bullet_owner_rungs": r["render_bullet_owner_rungs"],
                    }
                    for r in sorted(render_only, key=lambda r: -r["cars"])[:25]
                ],
            },
        },
        "ambiguity_guard_refusals": {
            **block(ambiguous),
            "note": "two differently-named verified rungs both claimed this trim; claim refused",
        },
        "rendered_bullets_by_owning_overlay_rung": {
            "covered_combos_whose_bullets_live_under_the_same_rung_name": len(
                [r for r in records if r["covered"] and not r["cross_rung_binding"]]
            ),
            "covered_combos_whose_bullets_live_under_a_DIFFERENT_rung_name": len(crossed),
            "cars_behind_those": sum(r["cars"] for r in crossed),
        },
        "cross_rung_bindings": [
            {
                "year": r["year"],
                "make": r["make"],
                "model": r["model"],
                "trim": r["trim"],
                "cars": r["cars"],
                "render_rung": r["render_rung"],
                "bullets_actually_owned_by": r["render_bullet_owner_rungs"],
            }
            for r in sorted(crossed, key=lambda r: -r["cars"])
        ],
        "render_errors": [
            {
                "year": r["year"],
                "make": r["make"],
                "model": r["model"],
                "trim": r["trim"],
                "cars": r["cars"],
                "error": r["render_error"],
            }
            for r in sorted(errors, key=lambda r: -r["cars"])[:25]
        ],
        "render_errors_total": block(errors),
    }


def exposure_detector_control(
    records: list[dict[str, Any]], overlays: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    """
    Prove the mis-binding detector fires, by feeding it a binding we know is wrong.

    A reported exposure of zero is indistinguishable from a detector that cannot
    fire, and this report has already been bitten once by a test that was
    vacuously true (a set-level "does any owner share the name" check passed a
    rung the moment one of its bullets was its own). So every run re-scores the
    same rendered bullets against the NEXT ``adds_by_trim`` key along -- a rung
    the bullets provably are not -- and records what fraction the detector
    rejects.

    100% is NOT the expected result and would itself be suspicious: brochures
    repeat lines across grades, so some share of any rung's bullets genuinely
    does appear under its neighbour. The number to watch is that it is high.
    """
    scored = flagged = rungs = 0
    for record in records:
        payload = record.get("_control_payload") or []
        overlay = overlays.get(record["catalog_key"])
        if not payload or not isinstance(overlay, dict):
            continue
        keys = list((overlay.get("adds_by_trim") or {}).keys())
        if len(keys) < 2:
            continue
        for rung_name, bullets in payload:
            if rung_name not in keys:
                continue
            wrong = keys[(keys.index(rung_name) + 1) % len(keys)]
            rungs += 1
            scored += len(bullets)
            flagged += len(unowned_bullets(overlay, wrong, bullets))
    return {
        "ran": bool(scored),
        "rungs_rescored_against_a_rung_they_are_not": rungs,
        "bullets_rescored": scored,
        "bullets_the_detector_rejected": flagged,
        "rejection_rate": round(flagged / scored, 4) if scored else None,
        "note": (
            "negative control. The residue that is NOT rejected is text the "
            "neighbouring grade genuinely also lists -- brochures repeat lines -- "
            "so a rate near but below 1.0 is the healthy result, and a rate near "
            "0 would mean the zero exposure above is meaningless."
        ),
    }


def wrong_spec_exposure(records: list[dict[str, Any]], top: int = 60) -> dict[str, Any]:
    """
    Cars that can read a bullet printed under a trim heading that does not own it.

    SCORED ON EVERY RUNG. The car page draws the whole ladder, so the exposure is
    not limited to the step flagged ``is_current``: a shopper comparing "what do
    I get if I step up to Limited" reads the Limited card too, and if that card
    is filled from the Platinum section the claim is wrong on the same page. An
    earlier revision of this report scored ``is_current`` only; the difference
    between the two is stated here as a multiplier rather than asserted, so the
    understatement is auditable.

    Two kinds are kept apart because they are different defects:

      ``bound_to_a_differently_named_section``
          the rendered text was found in this model year's overlay, under an
          ``adds_by_trim`` key that names a different trim. This is the surviving
          fuzzy hop (``_lookup_brochure_adds_key``) landing on a neighbour.
      ``owner_not_found_in_this_years_overlay``
          the rendered text is in no key of this year's overlay at all. Weaker
          evidence -- it can mean the bullet came from another model year's
          overlay or from a store this lookup does not reach -- so it is counted
          and listed, never folded into the first number.

    A rung whose bullets are also found under its own name is NOT flagged:
    brochures repeat lines across grades, so several owners is normal.
    """
    rendered = [r for r in records if r["rendered"]]
    with_any_bullet = [r for r in rendered if r["render_rungs_with_quoted_bullets"] > 0]
    crossed_any = [r for r in rendered if r["any_rung_cross_bound"]]
    crossed_named = [r for r in rendered if r["any_rung_bound_to_other_section"]]
    orphan = [r for r in rendered if r["any_rung_owner_not_found"]]
    current_only = [r for r in rendered if r["cross_rung_binding"]]

    def block(rows: list[dict[str, Any]]) -> dict[str, int]:
        return {"combos": len(rows), "cars": sum(r["cars"] for r in rows)}

    current_cars = sum(r["cars"] for r in current_only) or 0
    all_rung_cars = sum(r["cars"] for r in crossed_any) or 0
    return {
        "scored": "every rung the ladder renders, not only the step flagged is_current",
        "ownership_test": (
            "each rendered bullet must appear, normalised for case/whitespace, as a "
            "SUBSTRING of some bullet filed under this rung's own adds_by_trim key in "
            "this model year's overlay. Substring because the ladder splits long "
            "brochure lines on commas and re-capitalises the pieces; per bullet "
            "because a set-level test passes a rung as soon as one bullet is its own."
        ),
        "rungs_carrying_a_quotable_bullet": {
            **block(with_any_bullet),
            "rungs": sum(r["render_rungs_with_quoted_bullets"] for r in rendered),
            "bullets": sum(r["render_quoted_bullets_all_rungs"] for r in rendered),
            "stray_bullets": sum(r["render_stray_bullets_all_rungs"] for r in rendered),
            "note": (
                "the denominator for everything below: a combo can only be exposed "
                "to a mis-bound bullet if some rung of its ladder printed one"
            ),
        },
        "exposed_any_rung": {
            **block(crossed_any),
            "crossed_rungs": sum(r["crossed_rung_count"] for r in rendered),
        },
        "exposed_bound_to_a_differently_named_section": block(crossed_named),
        "exposed_owner_not_found_in_this_years_overlay": block(orphan),
        "if_scored_on_is_current_only": {
            **block(current_only),
            "note": (
                "what the previous revision of this report measured. Kept so the "
                "understatement is a number and not a claim."
            ),
        },
        "understatement_factor_cars": (
            round(all_rung_cars / current_cars, 2) if current_cars else None
        ),
        "understatement_factor_note": (
            "cars exposed on ANY rung divided by cars exposed on the is_current rung "
            "alone; null when the is_current figure is zero, in which case the "
            "is_current scoring could not see the exposure at all"
        ),
        "worst_by_cars": [
            {
                "year": r["year"],
                "make": r["make"],
                "model": r["model"],
                "trim": r["trim"],
                "cars": r["cars"],
                "shoppers_own_rung": r["render_rung"],
                "crossed": r["crossed_rungs"],
            }
            for r in sorted(crossed_any, key=lambda r: -r["cars"])[:top]
        ],
    }


def ordering_audit(records: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Where each rendered ladder's ORDER came from, in combos and in active cars.

    The verdict is ``resolve_trim_ladder``'s own ``order_provenance.basis``, and
    it is deliberately the WEAKEST rung's: a ladder is only ``adds_to_edge`` when
    every rung was placed by an "<TRIM> adds to <LOWER>" line printed in that
    model year's book. One rung placed by the hand-typed ``luxury_rank`` table
    makes the whole ladder ``unproven``, because a shopper reads position as rank
    and one wrong position corrupts the reading of the stack.

    The per-RUNG tally is reported beside the per-ladder one: they answer
    different questions and the per-ladder verdict is much the harsher of the two.
    """
    rendered = [r for r in records if r["rendered"] and r["render_has_ladder"]]
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in rendered:
        buckets[str(record["order_basis_verdict"] or "unproven")].append(record)

    rung_counts: dict[str, int] = defaultdict(int)
    rung_cars: dict[str, int] = defaultdict(int)
    for record in rendered:
        for basis, n in (record["order_counts"] or {}).items():
            rung_counts[str(basis)] += n
            rung_cars[str(basis)] += record["cars"] * n

    def block(rows: list[dict[str, Any]]) -> dict[str, int]:
        return {"combos": len(rows), "cars": sum(r["cars"] for r in rows)}

    return {
        "verdict_rule": (
            "trim_ladder.resolve_trim_ladder()['order_provenance']['basis'], which is "
            "the weakest rung's basis: adds_to_edge only if EVERY rung was placed by a "
            "quoted 'adds to' line in that model year's book"
        ),
        "combos_with_a_rendered_ladder": block(rendered),
        "by_ladder_verdict": {
            "adds_to_edge_document_stated": block(buckets.get("adds_to_edge", [])),
            "printed_sequence_document_order_our_reading": block(
                buckets.get("printed_sequence", [])
            ),
            "unproven_hand_typed_or_inventory": block(buckets.get("unproven", [])),
            "other": {
                name: block(rows)
                for name, rows in sorted(buckets.items())
                if name not in ("adds_to_edge", "printed_sequence", "unproven")
            },
        },
        "by_rung": {
            "note": (
                "rungs, not ladders, and car-weighted by the combo whose page drew the "
                "rung -- so one rung of a 500-car combo counts 500. Softer than the "
                "per-ladder verdict and must not be quoted in its place."
            ),
            "counts": dict(sorted(rung_counts.items(), key=lambda kv: -kv[1])),
            "cars_weighted": dict(sorted(rung_cars.items(), key=lambda kv: -kv[1])),
        },
    }


def ordering_document_recheck(overlays: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """
    Re-open the brochure transcripts and check the ORDERING claims against them.

    ``ordering_audit`` reports what ``resolve_trim_ladder`` says, and what it says
    is derived from the ``order_basis`` blocks in our own overlays. Reporting that
    alone would be asking the register to confirm its own issuance -- the failure
    this project has already been bitten by. So this goes back to the document:

      * ``adds_to_edge`` -- the edge carries the two lines it was read off
        (``trim_quote`` / ``below_quote``) and a page. BOTH must be found, verbatim
        bar case and whitespace, in the text of THAT page.
      * ``printed_sequence`` -- the rung is claimed to be a column of a grid on the
        cited pages. The weaker thing that can be checked is whether the rung's own
        NAME is printed on any page the basis cites. Where it is not, the citation
        does not let a reviewer find the column it names, and the entry is counted
        as ``name_only_on_another_page`` (a citation-completeness defect) or
        ``name_nowhere_in_document`` (which would be fabrication).

    The transcript, not the PDF, is what is re-read here: page text is what an edge
    was quoted from. The PDF itself is re-hashed and re-extracted by
    ``--recheck-stamps``, which covers the BULLETS.
    """
    edge_ok = edge_bad = 0
    seq_named = seq_elsewhere = seq_nowhere = 0
    failures: list[dict[str, Any]] = []
    incomplete: list[dict[str, Any]] = []
    checked_overlays = 0

    def norm(text: Any) -> str:
        return re.sub(r"\s+", " ", str(text or "")).strip().lower()

    for key, overlay in overlays.items():
        if str(overlay.get("source") or "").strip().lower() not in ADMISSIBLE_OVERLAY_SOURCES:
            continue
        basis_raw = overlay.get("order_basis")
        if not isinstance(basis_raw, dict) or not basis_raw:
            continue
        transcript = BROCHURE_TEXT_DIR / f"{key.replace('|', '__')}.json"
        if not transcript.exists():
            continue
        try:
            doc = json.loads(transcript.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            continue
        checked_overlays += 1
        pages = {
            int((p or {}).get("page") or 0): str((p or {}).get("text") or "")
            for p in (doc.get("pages") or [])
            if isinstance(p, dict)
        }
        for trim, entry in basis_raw.items():
            if not isinstance(entry, dict):
                continue
            basis = str(entry.get("basis") or "")
            if basis == "adds_to_edge":
                edge = entry.get("edge") or {}
                try:
                    page = int(edge.get("page") or 0)
                except (TypeError, ValueError):
                    page = 0
                text = norm(pages.get(page, ""))
                if (
                    text
                    and norm(edge.get("trim_quote")) in text
                    and norm(edge.get("below_quote")) in text
                ):
                    edge_ok += 1
                else:
                    edge_bad += 1
                    failures.append(
                        {"catalog_key": key, "trim": trim, "page": page, "basis": basis}
                    )
            elif basis == "printed_sequence":
                cited = [p for p in (entry.get("pages") or []) if isinstance(p, int)]
                on_cited = norm(" ".join(pages.get(p, "") for p in cited))
                if norm(trim) and norm(trim) in on_cited:
                    seq_named += 1
                elif any(norm(trim) in norm(v) for v in pages.values()):
                    seq_elsewhere += 1
                    incomplete.append(
                        {"catalog_key": key, "trim": trim, "cited_pages": cited}
                    )
                else:
                    seq_nowhere += 1
                    failures.append(
                        {"catalog_key": key, "trim": trim, "pages": cited, "basis": basis}
                    )
    return {
        "ran": True,
        "quoted_overlays_with_ordering_evidence_rechecked": checked_overlays,
        "adds_to_edge": {
            "edges_whose_BOTH_quoted_lines_were_re_found_on_the_cited_page": edge_ok,
            "edges_that_failed_a_fresh_read": edge_bad,
        },
        "printed_sequence": {
            "rungs_whose_name_is_printed_on_a_cited_page": seq_named,
            "rungs_whose_name_is_only_on_another_page_of_the_same_document": seq_elsewhere,
            "rungs_whose_name_is_nowhere_in_the_document": seq_nowhere,
            "note": (
                "the middle bucket is a citation-completeness defect, not a false "
                "claim: the grid columns are on the cited pages but the header row "
                "that names them is on an earlier page the citation does not include, "
                "so a reviewer following it lands on marks with no headings. The last "
                "bucket would be fabrication and is the one that must stay zero."
            ),
        },
        "failures": failures[:50],
        "incomplete_printed_sequence_citations": incomplete[:50],
    }


def gap_split(records: list[dict[str, Any]], top: int) -> dict[str, Any]:
    """The three-way gap, ranked by active cars. Each bucket needs a different fix."""
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        if record["bucket"]:
            buckets[record["bucket"]].append(record)

    fixes = {
        BUCKET_NO_DOCUMENT: "acquire the book for that exact model year",
        BUCKET_DOCUMENT_NO_TRIM_WALK: "run the trim-walk extractor over a document already held",
        BUCKET_TRIM_WALK_WITHOUT_THIS_TRIM: (
            "per-reason: re-quote from an admissible source, run the verifier, "
            "extract the missing rung, or add the rung to the ladder definition"
        ),
    }

    out: dict[str, Any] = {}
    for name in (
        BUCKET_NO_DOCUMENT,
        BUCKET_DOCUMENT_NO_TRIM_WALK,
        BUCKET_TRIM_WALK_WITHOUT_THIS_TRIM,
    ):
        rows = sorted(buckets.get(name, []), key=lambda r: (-r["cars"], r["catalog_key"], r["trim"]))
        reasons: dict[str, dict[str, int]] = defaultdict(lambda: {"combos": 0, "cars": 0})
        for row in rows:
            slot = reasons[str(row["reason"])]
            slot["combos"] += 1
            slot["cars"] += row["cars"]
        out[name] = {
            "combos": len(rows),
            "cars": sum(r["cars"] for r in rows),
            "fix": fixes[name],
            "by_reason": dict(reasons),
            "top_by_cars": [
                {
                    "year": r["year"],
                    "make": r["make"],
                    "model": r["model"],
                    "trim": r["trim"],
                    "cars": r["cars"],
                    "reason": r["reason"],
                }
                for r in rows[:top]
            ],
        }
    return out


def by_model_year(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Where the gap sits by model year -- the answer is dominated by the current one."""
    rollup: dict[int, dict[str, int]] = defaultdict(
        lambda: {"combos": 0, "cars": 0, "document_cars": 0, "covered_combos": 0, "covered_cars": 0}
    )
    for record in records:
        slot = rollup[record["year"]]
        slot["combos"] += 1
        slot["cars"] += record["cars"]
        if record["has_brochure_text"] or record["has_brochure_pdf"]:
            slot["document_cars"] += record["cars"]
        if record["covered"]:
            slot["covered_combos"] += 1
            slot["covered_cars"] += record["cars"]
    return {str(year): rollup[year] for year in sorted(rollup)}


def ceiling(records: list[dict[str, Any]]) -> dict[str, Any]:
    """
    The honest upper bound, and the rate actually achieved where a walk exists.

    A brochure speaks for a MODEL. Holding the 2026 Camry book does not mean the
    book trim-walks every Camry rung inventory sells, so ``arithmetic_bound``
    below is a bound the corpus can only approach, never a forecast -- it counts
    every combo whose exact model year we hold a document for and then ASSUMES
    the document walks this rung.

    ``expected_at_the_observed_hit_rate`` is the same bound multiplied by the rate
    measured on the models that already have a verified trim walk for their year:
    the only empirical read we have on how much of a lineup a walked book covers.
    It is the number to plan against. Nobody should plan against the full
    ``combos_total``.
    """
    held = [r for r in records if r["has_brochure_text"] or r["has_brochure_pdf"]]
    walked_models = [r for r in records if r["verified_rungs"] > 0]
    covered_in_walked = [r for r in walked_models if r["covered"]]
    total_cars = sum(r["cars"] for r in records) or 1
    total_combos = len(records) or 1
    held_combos, held_cars = len(held), sum(r["cars"] for r in held)
    combo_rate = (len(covered_in_walked) / len(walked_models)) if walked_models else None
    walked_cars = sum(r["cars"] for r in walked_models)
    car_rate = (sum(r["cars"] for r in covered_in_walked) / walked_cars) if walked_cars else None
    return {
        "arithmetic_bound_if_every_document_we_hold_were_parsed_perfectly": {
            "combos": held_combos,
            "cars": held_cars,
            "share_of_combos": round(held_combos / total_combos, 4),
            "share_of_active_cars": round(held_cars / total_cars, 4),
            "assumption": (
                "counts a combo as reachable when we hold that exact model year's "
                "document; ASSUMES the document trim-walks this rung, which many "
                "brochures do not -- a brochure covers a MODEL, not every trim in it"
            ),
        },
        "observed_trim_hit_rate_where_a_verified_walk_exists": {
            "combos_with_verified_walk_for_their_model_year": len(walked_models),
            "of_those_covered": len(covered_in_walked),
            "combo_rate": round(combo_rate, 4) if combo_rate is not None else None,
            "cars_with_verified_walk": walked_cars,
            "cars_covered": sum(r["cars"] for r in covered_in_walked),
            "car_rate": round(car_rate, 4) if car_rate is not None else None,
        },
        "expected_at_the_observed_hit_rate": {
            "combos": int(held_combos * combo_rate) if combo_rate is not None else None,
            "cars": int(held_cars * car_rate) if car_rate is not None else None,
            "basis": (
                "the arithmetic bound scaled by the hit rate actually observed on "
                "models that already have a verified trim walk; this is the realistic "
                "maximum, not the arithmetic bound and certainly not combos_total"
            ),
        },
        "unreachable_without_new_documents": {
            "combos": total_combos - held_combos,
            "cars": total_cars - held_cars,
            "note": "no document at all for that exact model year; no amount of parsing reaches these",
        },
    }


def overlay_audit(overlays: dict[str, dict[str, Any]], records: list[dict[str, Any]]) -> dict[str, Any]:
    """Per-source overlay tally plus the raw stamp counts behind the covered number."""
    by_source: dict[str, int] = defaultdict(int)
    stamps = {"stamped_true": 0, "stamped_false": 0, "unstamped": 0}
    quoted_files: list[str] = []
    for overlay in overlays.values():
        source = str(overlay.get("source") or "")
        by_source[source] += 1
        if source in ADMISSIBLE_OVERLAY_SOURCES:
            quoted_files.append(str(overlay.get("_file") or ""))
            for k, v in overlay_stamp_counts(overlay).items():
                stamps[k] += v
    return {
        "overlays_touched_by_active_inventory": len(overlays),
        "by_source": dict(sorted(by_source.items(), key=lambda kv: -kv[1])),
        "admissible_sources": sorted(ADMISSIBLE_OVERLAY_SOURCES),
        "admissible_ladder_bullet_stores": sorted(ADMISSIBLE_LADDER_BULLET_STORES),
        "quoted_overlays_touched": sorted(quoted_files),
        "verification_stamps_in_touched_quoted_overlays": stamps,
        "provenance_gate_on": trim_adds_provenance_required(),
        "distinct_rungs_reached": len(
            {
                (r["catalog_key"], r["render_rung"] or r["exact_overlay_rung"])
                for r in records
                if r["covered"]
            }
        ),
    }


def blank_trim_audit(rows: list[dict[str, Any]], limit: int = 400) -> dict[str, Any]:
    """
    Cars with NO trim string: check none of them is stamped onto a rung any more.

    The revoked scorer normalised a missing trim to ``""``, and ``"" in cn`` is
    true for every rung, so a car with no trim at all matched the first rung of
    its ladder. This re-runs the production ladder with the empty trim these cars
    actually carry and counts how many still come back ``matched``. Anything but
    zero is a live wrong-attribution bug.
    """
    blanks = [r for r in rows if not r["trim"]]
    by_model: dict[str, dict[str, Any]] = {}
    for row in blanks:
        slot = by_model.setdefault(
            row["catalog_key"],
            {"year": row["year"], "make": row["make"], "model": row["model"], "cars": 0},
        )
        slot["cars"] += row["cars"]
    ranked = sorted(by_model.values(), key=lambda v: -v["cars"])[:limit]
    matched: list[dict[str, Any]] = []
    checked_cars = 0
    for slot in ranked:
        checked_cars += slot["cars"]
        try:
            result = resolve_trim_ladder(
                make=slot["make"], model=slot["model"], year=slot["year"], trim=""
            )
        except Exception:  # noqa: BLE001
            continue
        if result and result.get("matched"):
            current = next(
                (s for s in (result.get("steps") or []) if s.get("is_current")), None
            )
            matched.append({**slot, "stamped_rung": str((current or {}).get("name") or "")})
    return {
        "cars_with_no_trim_string": sum(r["cars"] for r in blanks),
        "distinct_models_involved": len(by_model),
        "models_probed": len(ranked),
        "cars_behind_probed_models": checked_cars,
        "models_still_stamped_onto_a_rung": len(matched),
        "cars_still_stamped_onto_a_rung": sum(m["cars"] for m in matched),
        "examples": matched[:25],
        "expected": "zero -- an empty trim string must match no rung",
    }


def verifier_run_summary() -> dict[str, Any]:
    """What the last verifier run reported, for cross-checking the stamps."""
    if not VERIFICATION_REPORT_PATH.exists():
        return {"present": False}
    try:
        data = json.loads(VERIFICATION_REPORT_PATH.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return {"present": False}
    return {
        "present": True,
        "path": str(VERIFICATION_REPORT_PATH),
        "verifier_version": data.get("verifier_version"),
        "checked_at": data.get("checked_at"),
        "applied": data.get("applied"),
        "totals": data.get("totals"),
        "overlays_checked": len(data.get("overlays") or []),
    }


def recheck_stamps(overlays: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """
    Re-run the verifier's document check under this report (``--recheck-stamps``).

    Calls ``verify_trim_citations.verify_overlay(path, apply=False)`` on every
    quoted overlay inventory touches: it re-resolves the cited transcript,
    re-hashes the PDF and re-extracts the cited page. Nothing is written. The
    result says whether today's stamps still agree with the documents, and names
    any entry stamped ``verified: true`` that a fresh read would revoke -- the
    only failure mode that could inflate the covered number. Slow (it opens and
    hashes every cited PDF), so it is off by default and the artifact records
    which mode ran.
    """
    from backend.scripts.verify_trim_citations import verify_overlay

    fresh_ok = stamped_ok = 0
    revoked: list[dict[str, Any]] = []
    checked = 0
    for key, overlay in overlays.items():
        if str(overlay.get("source") or "").strip().lower() not in ADMISSIBLE_OVERLAY_SOURCES:
            continue
        path = TRIM_ADDS_BY_YEAR_DIR / str(overlay.get("_file") or "")
        if not path.exists():
            continue
        report = verify_overlay(path, apply=False)
        if report is None:
            continue
        checked += 1
        fresh_ok += report.verified
        stamped_ok += overlay_stamp_counts(overlay)["stamped_true"]
        stamped_true_texts = {
            (trim, str(entry.get("text") or ""))
            for trim, entries in (overlay.get("adds_provenance") or {}).items()
            if isinstance(entries, list)
            for entry in entries
            if isinstance(entry, dict) and entry.get(CITATION_VERIFIED_KEY) is True
        }
        for failure in report.failures:
            pair = (str(failure.get("trim")), str(failure.get("text") or ""))
            if pair in stamped_true_texts:
                revoked.append(
                    {
                        "catalog_key": key,
                        "trim": failure.get("trim"),
                        "text": failure.get("text"),
                        "why": failure.get("why"),
                    }
                )
    return {
        "ran": True,
        "quoted_overlays_rechecked": checked,
        "bullets_verified_on_a_fresh_read": fresh_ok,
        "bullets_currently_stamped_verified": stamped_ok,
        "stamps_a_fresh_read_would_revoke": len(revoked),
        "revoked": revoked[:50],
    }


def superseded_block(path: Path) -> dict[str, Any]:
    """
    The previous artifact's headline numbers, read before this run overwrites it.

    Carried into the new artifact so "which numbers changed and why" is answerable
    from the file itself rather than from a commit message. The two runs are over
    different inventory snapshots as well as different rules, so the inventory
    line is carried too and the delta must not be read as pure rule effect.
    """
    if not path.exists():
        return {"present": False}
    try:
        prev = json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return {"present": False}
    cov = prev.get("coverage") or {}
    return {
        "present": True,
        "report_version": prev.get("report_version"),
        "generated_at": prev.get("generated_at"),
        "why_superseded": (
            "covered was decided by trim_ladder._lookup_brochure_adds_key on the "
            "listing trim -- a fuzzy map that could name a neighbouring rung. The "
            "old artifact disclosed this only as an upper-bound footnote and split "
            "the covered number into exact/fuzzy after the fact."
        ),
        "inventory": {
            k: (prev.get("inventory") or {}).get(k)
            for k in ("active_cars", "distinct_year_make_model", "distinct_year_make_model_trim")
        },
        "coverage": {k: v for k, v in cov.items() if k != "covered_combos"},
        "gap": {
            k: {"combos": v.get("combos"), "cars": v.get("cars")}
            for k, v in (prev.get("gap") or {}).items()
        },
    }


def reconcile(previous: dict[str, Any], records: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Walk the old covered number down to the new one, line by line.

    "The number went from 79 to 48" is not an explanation. This lists the steps
    between them so each one can be argued with separately. It does NOT close to
    the last car and says so: the two runs are over different inventory snapshots
    taken a day apart, and a handful of listings sold in between.
    """
    if not previous.get("present"):
        return {"available": False}
    old = ((previous.get("coverage") or {}).get("verified_cited_bullet_for_this_trim")) or {}
    revoked = [r for r in records if r["legacy_credited_a_different_rung"]]
    stranded = [
        r for r in records if r["exact_overlay_rung"] and r["rendered"] and not r["covered"]
    ]
    new = [r for r in records if r["covered"]]
    steps = [
        {
            "step": "as published 2026-08-01, on the fuzzy trim -> rung map",
            "combos": old.get("combos"),
            "cars": old.get("cars"),
        },
        {
            "step": "less combos the fuzzy map credited to a DIFFERENTLY-NAMED rung",
            "combos": -len(revoked),
            "cars": -sum(r["cars"] for r in revoked),
            "why": "_exact_rung_match refuses every one; these were another trim's bullets",
        },
        {
            "step": (
                "less combos whose book walks this exact trim but whose LADDER has no "
                "rung by that name, so nothing renders"
            ),
            "combos": -len(stranded),
            "cars": -sum(r["cars"] for r in stranded),
            "why": (
                "the old report counted overlay-level matches; this one counts what "
                "the render path actually printed"
            ),
        },
        {
            "step": "measured now",
            "combos": len(new),
            "cars": sum(r["cars"] for r in new),
        },
    ]
    residual_combos = (old.get("combos") or 0) - len(revoked) - len(stranded) - len(new)
    residual_cars = (old.get("cars") or 0) - sum(r["cars"] for r in revoked) - sum(
        r["cars"] for r in stranded
    ) - sum(r["cars"] for r in new)
    return {
        "available": True,
        "steps": steps,
        "unexplained_residual": {
            "combos": residual_combos,
            "cars": residual_cars,
            "why": (
                "inventory drift between the two runs -- different snapshots of a live "
                "table, not a rule effect. Anything large here would be a real "
                "discrepancy and is not."
            ),
        },
    }


def build_report(
    *, min_year: int, top: int, recheck: bool, render: bool, artifact: Path, progress: bool
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    previous = superseded_block(artifact)
    if render:
        assert_ladders_reachable()
    rows, inv_stats = fetch_inventory(min_year)
    combos, _fold_stats = fold_combos(rows)
    docs = load_document_index()
    overlays = load_overlays({entry["catalog_key"] for entry in combos.values()})
    records = classify(combos, docs, overlays, render=render, progress=progress)

    # Counted over ALL rows, not just the ones with a trim: a model whose every
    # listing has a blank trim is still a model we do or do not hold a book for.
    distinct_ymm = {row["catalog_key"] for row in rows}
    report = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "min_model_year": min_year,
        "covered_definition": (
            "resolve_trim_ladder() printed at least one brochure_text_quoted bullet "
            "on the step it flagged is_current -- i.e. the render path's own output, "
            "with is_current set by _exact_rung_match and the bullets gated on the "
            "verify_trim_citations.py stamps"
            if render
            else "NO RENDER PROBE (--no-render): covered = the listing trim exactly "
            "names a verified overlay rung. Not the same number as the rendered one."
        ),
        "inventory": {
            "source": "postgres cars, listing_removed_at IS NULL",
            "active_cars": inv_stats["active_cars"],
            "cars_with_a_trim_string": inv_stats["active_cars"] - inv_stats["blank_trim_cars"],
            "cars_with_no_trim_string": inv_stats["blank_trim_cars"],
            "distinct_year_make_model": len(distinct_ymm),
            "distinct_year_make_model_trim": len(combos),
            "trim_fold": "case-insensitive, whitespace collapsed; blank trims excluded",
        },
        "corpus": {
            "brochure_text_files": len(docs["text_keys"]),
            "content_index_documents": docs["indexed_documents"],
            "content_index_documents_on_disk": docs["indexed_documents_on_disk"],
            "catalog_keys_with_a_stored_pdf": len(docs["pdf_keys"]),
            "brochure_text_newest_mtime": docs["brochure_text_newest_mtime"],
            "note": (
                "counted off disk at run time, not from a manifest. The transcript "
                "store is a live directory: other work adds and prunes it, so this "
                "line and every number derived from it are a snapshot, not a fixed asset"
            ),
        },
        "coverage": summarize(records),
        "attribution": attribution_audit(records),
        "wrong_spec_exposure": (
            {
                **wrong_spec_exposure(records),
                "detector_negative_control": exposure_detector_control(records, overlays),
            }
            if render
            else {"scored": False}
        ),
        "ordering": (
            {
                **ordering_audit(records),
                "document_recheck": ordering_document_recheck(overlays),
            }
            if render
            else {"scored": False}
        ),
        "blank_trim": blank_trim_audit(rows) if render else {"probed": False},
        "by_model_year": by_model_year(records),
        "ceiling": ceiling(records),
        "gap": gap_split(records, top),
        "overlays": overlay_audit(overlays, records),
        "verifier_run": verifier_run_summary(),
        "verified_source": (
            "verified stamps in derived/trim_adds_by_year/*.json, applied by "
            "brochure_extract.admissible_overlay_adds inside the render path. The "
            "citation register was NOT asked whether it issued a citation."
        ),
        "recheck": recheck_stamps(overlays) if recheck else {"ran": False},
        "superseded": {**previous, "reconciliation": reconcile(previous, records)},
        "caveats": [
            "A brochure covers a MODEL, not every trim in it; model coverage cannot "
            "be multiplied out to trim coverage. See ceiling.",
            "Covered is measured by CALLING the render path, not by re-implementing "
            "its rule. It is what a shopper sees today, not an upper bound on it.",
            "Exact model year only. No +/-2 year reach is credited anywhere here.",
            "One fuzzy hop survives inside production: ladder step name -> overlay "
            "adds_by_trim key, via _lookup_brochure_adds_key. It is not this "
            "report's to fix, so it is measured -- see attribution.cross_rung_bindings, "
            "derived by looking the RENDERED TEXT back up in the overlay file.",
            "Trim strings are folded case-insensitively with whitespace collapsed and "
            "nothing else, so dealer spelling variants of one real trim are counted as "
            "separate combos. That inflates combos_total and deflates every combo-based "
            "share; the car-weighted numbers are unaffected and are the ones to trust.",
        ],
    }
    # Scratch data for the negative control only; never part of the artifact.
    for record in records:
        record.pop("_control_payload", None)
    return report, records


def print_summary(report: dict[str, Any]) -> None:
    cov = report["coverage"]
    inv = report["inventory"]
    total_cars = cov["cars_total"] or 1
    total_combos = cov["combos_total"] or 1

    def line(label: str, block: dict[str, int]) -> str:
        return (
            f"  {label:<46} {block['combos']:>7,} combos ({block['combos'] / total_combos:6.1%})"
            f"   {block['cars']:>7,} cars ({block['cars'] / total_cars:6.1%})"
        )

    print(
        f"TRIM COVERAGE, active inventory {report['min_model_year']}+ "
        f"({report['generated_at']}) v{report['report_version']}"
    )
    print(
        f"  active cars {inv['active_cars']:,} | distinct year/make/model "
        f"{inv['distinct_year_make_model']:,} | distinct year/make/model/TRIM "
        f"{inv['distinct_year_make_model_trim']:,}"
    )
    print(f"  cars with no trim string at all: {inv['cars_with_no_trim_string']:,}")
    print()
    print(line("brochure text, exact model year", cov["brochure_text_for_exact_model_year"]))
    print(
        line(
            "brochure PDF on disk, exact model year",
            cov["brochure_pdf_on_disk_for_exact_model_year"],
        )
    )
    print(line("trim walk parsed for this model", cov["trim_walk_parsed_for_model"]))
    print(line("this trim present in that walk", cov["this_trim_present_in_parsed_walk"]))
    print(line("VERIFIED bullet RENDERED for this trim", cov["verified_cited_bullet_for_this_trim"]))
    print()
    attr = report["attribution"]
    reach = attr.get("ladder_reach") or {}
    if reach:
        print(
            f"LADDER REACH: {reach['combos_with_a_ladder']['combos']:,} combos have a ladder"
            f" -> {reach['combos_whose_trim_IS_one_of_its_rungs']['combos']:,} whose trim IS a rung"
            f" -> {reach['of_those_with_a_quotable_bullet']['combos']:,} with a quotable bullet"
        )
    revoked = attr["revoked_fuzzy_rung_credit"]
    print(
        f"ATTRIBUTION: old fuzzy rule credited a DIFFERENT rung on "
        f"{revoked['combos']:,} combos / {revoked['cars']:,} cars -- all refused now"
    )
    crossed = attr["rendered_bullets_by_owning_overlay_rung"]
    print(
        f"  surviving step->overlay fuzz: "
        f"{crossed['covered_combos_whose_bullets_live_under_a_DIFFERENT_rung_name']:,} covered "
        f"combos print another rung's text ({crossed['cars_behind_those']:,} cars)"
    )
    expo = report.get("wrong_spec_exposure") or {}
    if expo.get("exposed_any_rung"):
        print(
            f"WRONG-SPEC EXPOSURE (every rung): "
            f"{expo['exposed_any_rung']['combos']:,} combos / "
            f"{expo['exposed_any_rung']['cars']:,} cars"
            f"  [is_current only: {expo['if_scored_on_is_current_only']['cars']:,} cars,"
            f" factor {expo.get('understatement_factor_cars')}]"
        )
        print(
            f"    bound to a differently-named section: "
            f"{expo['exposed_bound_to_a_differently_named_section']['combos']:,} combos /"
            f" {expo['exposed_bound_to_a_differently_named_section']['cars']:,} cars"
            f"   | owner not found in this year's overlay: "
            f"{expo['exposed_owner_not_found_in_this_years_overlay']['combos']:,} combos /"
            f" {expo['exposed_owner_not_found_in_this_years_overlay']['cars']:,} cars"
        )
        ctl = expo.get("detector_negative_control") or {}
        rcb = expo["rungs_carrying_a_quotable_bullet"]
        print(
            f"    scored {rcb['rungs']:,} rungs / {rcb['bullets']:,} bullets"
            f" ({rcb['stray_bullets']:,} stray); negative control rejected"
            f" {ctl.get('rejection_rate')} of {ctl.get('bullets_rescored', 0):,}"
            f" deliberately mis-bound bullets"
        )
    order = report.get("ordering") or {}
    if order.get("by_ladder_verdict"):
        v = order["by_ladder_verdict"]
        print(
            f"ORDERING (per ladder, weakest rung decides): "
            f"adds_to_edge {v['adds_to_edge_document_stated']['combos']:,}c/"
            f"{v['adds_to_edge_document_stated']['cars']:,} cars | "
            f"printed_sequence {v['printed_sequence_document_order_our_reading']['combos']:,}c/"
            f"{v['printed_sequence_document_order_our_reading']['cars']:,} cars | "
            f"unproven {v['unproven_hand_typed_or_inventory']['combos']:,}c/"
            f"{v['unproven_hand_typed_or_inventory']['cars']:,} cars"
        )
        print(f"    by rung: {order['by_rung']['counts']}")
        rc = order.get("document_recheck") or {}
        if rc.get("ran"):
            print(
                f"    document recheck: {rc['adds_to_edge']['edges_whose_BOTH_quoted_lines_were_re_found_on_the_cited_page']}"
                f" edges re-found / {rc['adds_to_edge']['edges_that_failed_a_fresh_read']} failed"
                f"  | printed_sequence names on a cited page "
                f"{rc['printed_sequence']['rungs_whose_name_is_printed_on_a_cited_page']},"
                f" only elsewhere {rc['printed_sequence']['rungs_whose_name_is_only_on_another_page_of_the_same_document']},"
                f" nowhere {rc['printed_sequence']['rungs_whose_name_is_nowhere_in_the_document']}"
            )
    blank = report.get("blank_trim") or {}
    if blank.get("cars_with_no_trim_string") is not None:
        print(
            f"  blank-trim cars still stamped onto a rung: "
            f"{blank['cars_still_stamped_onto_a_rung']:,} "
            f"(of {blank['cars_behind_probed_models']:,} probed; expected 0)"
        )
    print()
    print("GAP, ranked by active cars:")
    for name, block in report["gap"].items():
        print(f"  {name:<32} {block['combos']:>7,} combos   {block['cars']:>7,} cars")
        for reason, tallies in sorted(block["by_reason"].items(), key=lambda kv: -kv[1]["cars"]):
            print(f"      {reason:<50} {tallies['combos']:>6,} combos {tallies['cars']:>7,} cars")
    ceil_block = report["ceiling"]["arithmetic_bound_if_every_document_we_hold_were_parsed_perfectly"]
    obs = report["ceiling"]["observed_trim_hit_rate_where_a_verified_walk_exists"]
    exp = report["ceiling"]["expected_at_the_observed_hit_rate"]
    print()
    print(
        f"CEILING (arithmetic, every document we hold parsed perfectly): "
        f"{ceil_block['combos']:,} combos / {ceil_block['cars']:,} cars "
        f"({ceil_block['share_of_active_cars']:.1%} of active)"
    )
    print(
        f"  observed rung hit rate where a verified walk exists: "
        f"{obs['of_those_covered']:,}/{obs['combos_with_verified_walk_for_their_model_year']:,}"
        f" combos = {obs['combo_rate']}, cars = {obs['car_rate']}"
    )
    print(
        f"  REALISTIC maximum at that rate: {exp['combos']:,} combos / {exp['cars']:,} cars"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").strip().split("\n")[0])
    parser.add_argument("--min-year", type=int, default=2015)
    parser.add_argument("--top", type=int, default=40, help="rows per gap bucket in the artifact")
    parser.add_argument("--json", default=str(DEFAULT_ARTIFACT), help="artifact path")
    parser.add_argument(
        "--combos-json",
        default="",
        help="optional path for the full per-combo table (large)",
    )
    parser.add_argument(
        "--no-render",
        action="store_true",
        help=(
            "skip the production render probe and fall back to the strict overlay-rung "
            "match (fast, but a DIFFERENT number -- the artifact says so)"
        ),
    )
    parser.add_argument(
        "--recheck-stamps",
        action="store_true",
        help="re-open the cited PDFs instead of trusting the verifier's stamps (slow)",
    )
    parser.add_argument("--progress", action="store_true", help="progress to stderr")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    out = Path(args.json)
    report, records = build_report(
        min_year=args.min_year,
        top=args.top,
        recheck=args.recheck_stamps,
        render=not args.no_render,
        artifact=out,
        progress=args.progress,
    )

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, sort_keys=False), encoding="utf-8")

    if args.combos_json:
        combos_path = Path(args.combos_json)
        combos_path.parent.mkdir(parents=True, exist_ok=True)
        combos_path.write_text(
            json.dumps(
                sorted(records, key=lambda r: (-r["cars"], r["catalog_key"], r["trim"])),
                indent=2,
            ),
            encoding="utf-8",
        )

    if not args.quiet:
        print_summary(report)
        print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
