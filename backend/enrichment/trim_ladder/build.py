"""Assemble the resolved ladder result from steps, adds and citations."""
from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

from ._common import (
    _clean_trim_label,
    _norm_token,
    _provenance_gate_on,
)
from .adds_filter import (
    _suppress_non_adds,
    _trim_ladder_quality,
)
from .attribution import (
    _curated_adds_for_step,
    _dictionary_adds_by_trim,
    _exact_rung_match,
    _lookup_merged_trim_specs,
    _trim_spec_rows_to_bullets,
)
from .bullets import (
    _bullet_display_parts,
    _merge_trim_display_bullets,
    _strip_inventory_price_adds,
)
from .citations import (
    _CitationRegister,
    _citation_key,
    _register_epa_citations,
    _register_overlay_citations,
)
from .document_order import (
    UNPROVEN_ORDER_BASIS,
)
from .engine_steps import (
    _brochure_engine_bullet_by_step,
)
from .evidence import (
    _justified_ladder_steps,
)
from .steps import (
    _lookup_brochure_adds_key,
)

def _build_ladder_result(
    ladder_def: dict[str, Any],
    *,
    make: str,
    model: str,
    year: Any,
    trim: str | None,
    brochure_overlay: dict[str, Any] | None = None,
) -> dict[str, Any]:
    # THE choke point for rung NAMES. Every ladder-def producer funnels through
    # here — curated, generated, EPA, Complete_Options, brochure, inventory, and
    # all four ``_generic_trim_ladder_def`` fallbacks in ``resolve_trim_ladder``
    # — so a rung nobody can account for cannot reach the page by taking a
    # different route in, and emptying one producer cannot be refilled from a
    # weaker one because the replacement faces this same test.
    steps_def, _rungs_dropped = _justified_ladder_steps(
        list(ladder_def.get("steps") or []),
        make=make,
        model=model,
        year=year,
        brochure_overlay=brochure_overlay,
    )
    from backend.enrichment.trim_ladder_knowledge import canonical_trim_name, preserve_trim_label

    listing_trim = (
        canonical_trim_name(trim or "", make, model)
        or preserve_trim_label(trim or "", make, model)
        or _clean_trim_label(trim or "")
        or "—"
    )
    # THE choke point for the vehicle claim. This is the only place in the
    # module that decides which rung, if any, this car IS, and the only input to
    # ``is_current`` / ``is_passed`` / ``is_ahead`` / ``current_index`` /
    # ``matched``. It admits an exact rung only — see ``_exact_rung_match``.
    # ``exact_index`` stays -1 when the car's trim is not one of the rungs, and
    # every downstream flag then says "we do not know", never "the nearest one".
    #
    # The RAW ``trim`` is matched, not ``listing_trim`` above. ``listing_trim``
    # has been through ``canonical_trim_name``, which truncates "Sport
    # Prestige" to "Sport" — matching on it would hand the claim to the wrong
    # rung by the back door. It stays the DISPLAY label only.
    #
    # Tier 1 (labels agree as written) is tried across the whole ladder first;
    # tier 2 (a fused drivetrain dropped, onto a drivetrain-neutral rung) is
    # only consulted if tier 1 found nothing anywhere. Mixing them lets the
    # weaker reading of one rung out-argue the literal reading of another.
    def _hits(tier: int) -> list[int]:
        return [
            i
            for i, step in enumerate(steps_def)
            if _exact_rung_match(
                str(trim or ""),
                str(step.get("name") or ""),
                list(step.get("aliases") or []),
                make=make,
                model=model,
                tier=tier,
            )
        ]

    exact_hits = _hits(1) or _hits(2)
    # Two differently-named rungs both claiming to be this car means the ladder
    # cannot tell us which one it is. Fail closed rather than take the first.
    distinct_hit_names = {
        _norm_token(str(steps_def[i].get("name") or "")) for i in exact_hits
    }
    exact_index = exact_hits[0] if len(distinct_hit_names) == 1 else -1

    steps_out: list[dict[str, Any]] = []
    dict_adds = _dictionary_adds_by_trim(make, model, year)
    brochure_adds_by_trim: dict[str, list[str]] = {}
    if brochure_overlay:
        # Second application of the provenance gate. ``load_brochure_trim_overlay``
        # already ran it, but this function is also called with overlays read
        # straight off disk (scripts, tests), and the filter is idempotent, so the
        # rule holds no matter who assembled the overlay.
        from backend.enrichment.brochure_extract import admissible_overlay_adds

        brochure_adds_by_trim = admissible_overlay_adds(brochure_overlay, for_year=year)
    from backend.enrichment.trim_ladder_knowledge import (
        fallback_trim_step_adds,
        sanitize_brochure_trim_adds,
        sanitize_trim_adds,
    )

    # NOTE: there used to be a DERIVED "this trim steps up the engine" bullet
    # here, computed from cars.engine_l + cars.cylinders. It was removed because
    # none of its premises hold in our data — see the comment block above
    # ``_ENGINE_COMPARISON_REMOVED``. What is left is quotation only: an engine
    # claim the OEM brochure itself attributes to this rung, or nothing.
    brochure_engine_bullets = _brochure_engine_bullet_by_step(steps_def, make, model, year)
    from backend.enrichment.brochure_extract import (
        ladder_bullet_store_admissible,
        ladder_bullet_store_for_source,
    )

    gate_on = _provenance_gate_on()
    ladder_store = ladder_bullet_store_for_source(ladder_def.get("source"))
    for i, step in enumerate(steps_def):
        name = str(step.get("name") or "").strip()
        aliases = [str(a).strip() for a in (step.get("aliases") or []) if str(a).strip()]
        # What this rung is able to quote, filled in as each store is consulted.
        # Producers that cannot name a document never add to it, which is what
        # takes their bullets off the page at the choke point below.
        citations = _CitationRegister()
        _register_epa_citations(
            citations,
            name=name,
            aliases=aliases,
            make=make,
            model=model,
            year=year,
        )
        step_curated_adds = sanitize_trim_adds(
            _curated_adds_for_step(step, year),
            name,
            max_items=12,
        )
        brochure_key = _lookup_brochure_adds_key(
            name,
            aliases,
            brochure_adds_by_trim,
            make=make,
            model=model,
        )
        from_brochure = brochure_key is not None
        _register_overlay_citations(
            citations,
            brochure_overlay,
            brochure_key,
            make=make,
            model=model,
            year=year,
        )
        neighbor_brochure_adds: list[str] = []
        # Neighbor-year overlay backfill only when this step has thin curated content —
        # avoids copying another trim's option-package lines onto performance trims (e.g. TRX).
        # It reads a DIFFERENT model year's overlay file straight off disk, so the
        # bullets it returns are not covered by this overlay's citations and are not
        # re-checked against the neighbour's own: the provenance gate turns it off.
        if (
            not _provenance_gate_on()
            and not from_brochure
            and brochure_overlay
            and len(step_curated_adds) < 3
        ):
            try:
                y_int = int(year)
            except (TypeError, ValueError):
                y_int = int(brochure_overlay.get("year") or 0)
            if y_int:
                from backend.enrichment.brochure_overlay_backfill import _neighbor_adds

                neighbor_brochure_adds = _neighbor_adds(y_int, make, model, name)
                if neighbor_brochure_adds:
                    from_brochure = True
        ladder_source_lower = str(ladder_def.get("source") or "").lower()
        brochure_lines: list[str] = []
        if from_brochure:
            brochure_lines = list(
                brochure_adds_by_trim.get(brochure_key or "", []) or neighbor_brochure_adds
            )
            if (
                not _provenance_gate_on()
                and brochure_overlay
                and len(sanitize_brochure_trim_adds(brochure_lines, name)) < 3
            ):
                # Same neighbour-year read as above: swapping this rung's cited
                # bullets for a longer list out of another year's file would leave
                # the page quoting a document it never opened.
                try:
                    y_int = int(year)
                except (TypeError, ValueError):
                    y_int = int(brochure_overlay.get("year") or 0)
                if y_int:
                    from backend.enrichment.brochure_overlay_backfill import _neighbor_adds

                    richer = _neighbor_adds(y_int, make, model, name)
                    if len(sanitize_brochure_trim_adds(richer, name)) >= 3:
                        brochure_lines = richer

        # An overlay covers this vehicle but says nothing this rung can cite. The
        # rung then stays as quiet as if the brochure listed no adds for it: the
        # hole the provenance gate leaves must not be refilled from the
        # Complete_Options CSVs or the trim spec sheets, which carry no citation
        # either. Measured on the live fleet: without this, blocking the overlays
        # pushed 137 new uncited lines ("Front-Wheel Drive", "Bose audio",
        # "Premium Cloth seating") onto 6,234 active cars that had never shown them.
        overlay_says_nothing_here = bool(
            _provenance_gate_on()
            and brochure_overlay
            and not from_brochure
            and ladder_source_lower != "curated"
        )

        candidates: list[str] = []
        candidates.extend(step_curated_adds)
        candidates.extend(sanitize_brochure_trim_adds(brochure_lines, name))
        if overlay_says_nothing_here:
            candidates = list(step_curated_adds)
        elif not step_curated_adds and not brochure_lines:
            for alias in aliases:
                candidates.extend(dict_adds.get(_norm_token(alias), []))
            candidates.extend(dict_adds.get(_norm_token(name), []))
        elif brochure_overlay and not from_brochure and ladder_source_lower != "curated":
            # Overlay exists for this YMM but not this trim — avoid cross-trim CSV/wiki noise.
            candidates = list(step_curated_adds)

        spec_rows = (
            []
            if overlay_says_nothing_here
            else _lookup_merged_trim_specs(
                name,
                aliases,
                make=make,
                model=model,
                year=year,
                ladder_id=str(ladder_def.get("id") or ""),
                adds=candidates,
                dict_adds=dict_adds,
            )
        )
        candidates.extend(_trim_spec_rows_to_bullets(spec_rows, trim_name=name))

        adds = _merge_trim_display_bullets(
            candidates,
            trim_name=name,
            make=make,
            model=model,
            year=year,
        )
        adds = _strip_inventory_price_adds(adds)
        # Choke point for every SOURCED bullet on this rung — curated ladder
        # adds, generated-ladder adds, brochure-overlay adds, Complete_Options
        # CSV package/option cells, and trim-spec-sheet rows all arrive in
        # ``candidates`` above. Anything that reads as narrative, is truncated, or
        # states a comparison we derived ourselves is dropped here rather than shown.
        # It does NOT cover the two paths below it: the position-describing
        # fallback lines are re-gated where they are produced, and the brochure
        # engine bullet is quoted from a trim-walk page and gated there.
        from backend.enrichment.trim_spec_extractor import is_displayable_trim_bullet

        adds = [a for a in adds if is_displayable_trim_bullet(a)]
        specs: list[Any] = []

        # Position copy ("Mid-level trim with added convenience features…") is
        # written by us, not quoted from anything, so it can never satisfy the
        # citation check below. Skip producing it while the gate is on rather
        # than build it and throw it away; the store table decides, so re-admitting
        # ``oem_knowledge_prose`` / ``inventory_prose`` there turns it back on.
        prose_store = (
            "inventory_prose" if ladder_store == "inventory_prose" else "oem_knowledge_prose"
        )
        prose_allowed = (not gate_on) or ladder_bullet_store_admissible(prose_store)
        if not adds and not specs and not brochure_overlay and prose_allowed:
            less_equipped = (
                str(steps_def[i + 1].get("name") or "").strip() if i + 1 < len(steps_def) else ""
            )
            more_equipped = str(steps_def[i - 1].get("name") or "").strip() if i > 0 else ""
            if ladder_store == "inventory_prose":
                from backend.enrichment.trim_ladder_knowledge import infer_trim_step_adds

                adds = infer_trim_step_adds(
                    name,
                    index=i,
                    total=len(steps_def),
                    make=make,
                    model=model,
                    lower_trim=less_equipped,
                    higher_trim=more_equipped,
                )
            if not adds:
                adds = fallback_trim_step_adds(
                    name,
                    index=i,
                    total=len(steps_def),
                    make=make,
                    model=model,
                    lower_trim=less_equipped,
                    higher_trim=more_equipped,
                )
            # These two producers describe the rung's position rather than
            # quoting a source, so they used to reach the page WITHOUT passing
            # the gate every sourced bullet has to pass — the gate sits above
            # them, not below. That is how "Mid-level trim with added
            # convenience features and nicer interior finishes." rendered on
            # 1,614 rungs while the identical gate rejects it as prose.
            from backend.enrichment.trim_ladder_knowledge import (
                is_wellformed_trim_bullet as _wellformed,
            )

            adds = [
                a
                for a in adds
                if is_displayable_trim_bullet(a) and _wellformed(a)
            ]
        adds = _strip_inventory_price_adds(adds)

        engine_claim = brochure_engine_bullets.get(i)
        engine_bullet = engine_claim[0] if engine_claim else None
        if engine_claim:
            citations.add_exact(engine_claim[0], engine_claim[1])
        if engine_bullet and not _bullet_display_parts(
            engine_bullet, trim_name=name, make=make, model=model, year=year
        ):
            # Quoted from a brochure trim-walk page, but it is spliced in below
            # the sourced-bullet gate, so it gets the same well-formedness and
            # contamination test here rather than an exemption.
            engine_bullet = None
        if engine_bullet:
            disp = re.match(r"(\d\.\d\s?L)", engine_bullet, re.I)
            key = disp.group(1).lower().replace(" ", "") if disp else ""
            hit = (
                next(
                    (
                        n
                        for n, a in enumerate(adds)
                        if key in str(a).lower().replace(" ", "")
                    ),
                    None,
                )
                if key
                else None
            )
            if hit is None:
                adds = [engine_bullet, *adds][:8]
            else:
                # Was: swap our composed "<engine> — added over the <LOWER>"
                # sentence in for the plain engine bullet, on the theory that the
                # attribution was quoted. It was not — the wording is ours. This
                # branch is unreachable while ``brochure_trim_walk`` is revoked
                # (``engine_claim`` is always None), and must not be revived
                # without the composition being removed first.
                adds = [*adds[:hit], engine_bullet, *adds[hit + 1 :]]

        # THE choke point for provenance. Everything that can put a line on this
        # rung has now had its turn — curated ladder adds, generated-ladder adds,
        # the brochure overlay, the Complete_Options CSV cells, the trim spec
        # sheets, the EPA rows, the position-describing knowledge prose and the
        # brochure trim-walk engine line. A line survives only if the register
        # above can name the document it came from and where in it. Stores that
        # register nothing lose every bullet here, which is also why emptying one
        # store cannot be refilled from another: the replacement faces this same
        # test. Turned off wholesale by TRIM_ADDS_REQUIRE_PROVENANCE=0.
        #
        # THIS IS NO LONGER WHERE TRUTH IS DECIDED, and it never should have
        # been. A register filled in by the same request that queries it can be
        # talked into anything: two stores in a row (``epa_csv``, then
        # ``brochure_trim_walk``) passed this check by handing it text they had
        # just composed. Whether a bullet is really printed in a document is now
        # settled at build time by ``backend/scripts/verify_trim_citations.py``,
        # which re-opens the PDF; the entries replayed into this register carry
        # that verdict. What is left here is defence in depth — it still catches
        # a producer that never registered anything at all, and it is still the
        # single place a refill from a lower-quality store is blocked.
        adds_citations: list[dict[str, Any]] = []
        if gate_on:
            kept: list[str] = []
            for bullet in adds:
                citation = citations.citation_for(bullet)
                if citation is None:
                    continue
                kept.append(bullet)
                adds_citations.append({"text": bullet, **citation})
            adds = kept

        # ``is_current`` is the "This vehicle" badge in both renderers
        # (frontend/templates/car.html and frontend/static/car_trim_ladder.js).
        # It is now true only on an exact rung, so it cannot be set on a rung
        # this car is not.
        is_current = i == exact_index
        # Steps are luxury-first (index 0 = top). Below current = more base; above = more luxury.
        # These are position claims RELATIVE TO THIS CAR, so they are as
        # unknowable as the rung itself when there is no exact match — both stay
        # False rather than being anchored to a nearest guess.
        is_passed = exact_index >= 0 and i > exact_index
        steps_out.append(
            {
                "name": name,
                "aliases": aliases,
                # Why this rung is on the page at all: {store, …} naming the
                # active rows or the verified citation that justify listing it.
                # Parallel to ``adds_citations``, which justifies its bullets.
                "name_provenance": step.get("name_provenance") or {},
                # Why this rung sits at THIS POSITION. ``name_provenance`` says
                # the trim exists; this says the hierarchy the card is drawn in
                # is the OEM's and not ours. ``basis="unproven"`` means the
                # position came from the hand-typed ``luxury_rank`` table and
                # nothing else — a renderer that will not imply rank without
                # evidence should read this, not ``index``.
                "order_basis": dict(step.get("order_basis") or UNPROVEN_ORDER_BASIS),
                "index": i,
                "is_current": is_current,
                "is_passed": is_passed,
                "is_ahead": exact_index >= 0 and i < exact_index,
                "adds": adds,
                # Parallel to ``adds`` while the gate is on: {text, store,
                # source, page|row} for each surviving bullet, so a reviewer (or
                # the template, if it ever wants to print "Ram brochure, p.14")
                # can check any line without re-deriving where it came from.
                "adds_citations": adds_citations,
                "specs": specs,
                "side": "left" if i % 2 == 0 else "right",
            }
        )

    _suppress_non_adds(steps_out, make=make, model=model, year=year)
    # ``_suppress_non_adds`` drops bullets after the fact, so re-align the
    # citation list with what each rung actually kept.
    for step_out in steps_out:
        surviving = {_citation_key(a) for a in step_out.get("adds") or []}
        step_out["adds_citations"] = [
            entry
            for entry in step_out.get("adds_citations") or []
            if _citation_key(entry.get("text")) in surviving
        ]

    source = ladder_def.get("source") or ladder_def.get("id") or ""
    # ``source`` names the ladder DEFINITION — where the ORDER and the alias
    # tables came from. It is not where the rung list came from any more, and a
    # caption that says "Curated OEM trim order" while the membership was
    # decided by our own listings would be overclaiming. This is the tally the
    # template needs to say it accurately.
    rung_provenance: dict[str, int] = {}
    for step_out in steps_out:
        store = str((step_out.get("name_provenance") or {}).get("store") or "")
        if store:
            rung_provenance[store] = rung_provenance.get(store, 0) + 1
    # The same tally for the ORDER. ``order_provenance["basis"]`` is the whole
    # ladder's verdict, and it is deliberately the WEAKEST rung's:
    #
    #   adds_to_edge      every rung was placed by an "<TRIM> adds to <LOWER>"
    #                     line printed in this model year's brochure.
    #   printed_sequence  at least one rung was placed only by where the
    #                     document printed it — the sequence is the document's,
    #                     the reading of it is ours.
    #   unproven          at least one rung was placed by the hand-typed
    #                     ``luxury_rank`` table, i.e. by nothing. A renderer must
    #                     not present this ladder as a hierarchy.
    #
    # ``ordered_rungs``/``total_rungs`` let a caller say how much of the ladder
    # is carried by the document without re-deriving it.
    order_counts: dict[str, int] = {}
    for step_out in steps_out:
        key = str((step_out.get("order_basis") or {}).get("basis") or "unproven")
        order_counts[key] = order_counts.get(key, 0) + 1
    if order_counts.get("unproven"):
        order_verdict = "unproven"
    elif order_counts.get("printed_sequence") or order_counts.get("page_sequence"):
        order_verdict = "printed_sequence"
    elif order_counts.get("adds_to_edge"):
        order_verdict = "adds_to_edge"
    else:
        order_verdict = "unproven"
    result = {
        "id": ladder_def.get("id"),
        "rung_provenance": rung_provenance,
        "order_provenance": {
            "basis": order_verdict,
            "proven": order_verdict == "adds_to_edge",
            "counts": order_counts,
            "ordered_rungs": len(steps_out) - order_counts.get("unproven", 0),
            "total_rungs": len(steps_out),
        },
        "label": ladder_def.get("label") or f"{make} {model} trim lineup",
        "make": make,
        "model": model,
        "year": year,
        "listing_trim": listing_trim,
        # ``current_index``/``matched`` can no longer hold a non-match: both are
        # derived from ``exact_index``, which is only ever set by
        # ``_exact_rung_match``. ``matched`` used to mean "the nearest rung
        # scored above zero"; it now means "this car's trim IS one of these
        # rungs", which is what every reader of it already assumed.
        "current_index": exact_index if exact_index >= 0 else None,
        "matched": exact_index >= 0,
        # Said outright, so no caller has to infer it from a None: "exact" =
        # the car is the rung at ``current_index``; "none" = the car's trim is
        # not on this ladder and the rungs are lineup context only. There is no
        # third value, and there is deliberately no "near"/"approx" tier.
        "match_kind": "exact" if exact_index >= 0 else "none",
        "source": source,
        "steps": steps_out,
    }
    result["quality"] = _trim_ladder_quality(result, make, model)
    return result
