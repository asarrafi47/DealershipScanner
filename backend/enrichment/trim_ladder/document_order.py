"""Rung order taken from a source document, never invented."""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

from .evidence import (
    _rung_evidence_key,
)
from .steps import (
    _trim_identity_keys,
)

#: What a rung whose position nothing in a document supports is labelled with.
#: ``luxury_rank`` is a hand-typed table in ``trim_ladder_knowledge``; it is
#: still what places such a rung, but the label says so rather than letting the
#: page present the result as the OEM's hierarchy.
UNPROVEN_ORDER_BASIS: dict[str, Any] = {
    "basis": "unproven",
    "store": "luxury_rank_table",
    "proven": False,
}


def _document_rung_order(
    overlay: dict[str, Any] | None,
    *,
    year: Any,
    make: str,
    model: str,
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    """The brochure's own rung order for this car, TOP FIRST, and why each rung sits there.

    ``overlay_rung_order`` returns the order the document prints, base→top;
    ladder steps run the other way (index 0 is the top of the ladder), so it is
    reversed here. Returns ``([], {})`` when the document places nothing, and
    the caller then falls back to ``luxury_rank`` with every rung labelled
    :data:`UNPROVEN_ORDER_BASIS`.

    The basis map is keyed by ``_rung_evidence_key`` — case folding and removal
    of non-alphanumerics, nothing more. That is the same key the rung-NAME gate
    uses, and for the same reason: it collapses "SRT®"/"SRT" and leaves
    everything else alone. It deliberately does not go through
    ``canonical_trim_name``, which would fold "GT PLUS" onto "GT" and hand one
    rung's ordering evidence to a different rung.
    """
    if not overlay:
        return [], {}
    from backend.enrichment.brochure_extract import overlay_rung_order

    base_to_top, basis = overlay_rung_order(overlay, for_year=year)
    if len(base_to_top) < 2:
        return [], {}
    top_first = list(reversed(base_to_top))
    by_key = {
        _rung_evidence_key(name): basis[name]
        for name in base_to_top
        if _rung_evidence_key(name)
    }
    return top_first, by_key


def _apply_document_order(
    steps: list[dict[str, Any]],
    doc_order: list[str],
    doc_basis: dict[str, dict[str, Any]],
    *,
    make: str,
    model: str,
) -> list[dict[str, Any]]:
    """Re-sort *steps* into the document's order and stamp each one's basis.

    Rungs the document places take its order, top first. Rungs it does not place
    keep the position they came in with RELATIVE TO THE RUNG ABOVE THEM: each is
    re-attached under the nearest rung that precedes it in the incoming order and
    that the document did place, or floated to the top if there is none.

    That anchoring is deliberate. Simply appending the unplaced rungs after the
    placed ones reads as a claim — it puts them at the bottom of the ladder — and
    it is a claim we cannot support: on the 2022 Charger it moved SRT Hellcat
    Redeye Widebody, the top trim, to second from last, because the brochure's
    walk pages cover the Scat Pack grades and not that one. Keeping the incoming
    neighbour means the fallback table decides only what it already decided, and
    the document decides everything it speaks to.

    Every rung the document did not place carries :data:`UNPROVEN_ORDER_BASIS`,
    which drags the whole ladder's verdict down to ``unproven`` in
    ``_build_ladder_result``: some of these cards are in the OEM's order and some
    are in ours, and a shopper cannot tell which, so the ladder as a whole is not
    a proven hierarchy.

    Steps are copied, never mutated — ``_all_ladder_defs`` is ``lru_cache``d, so
    writing ``order_basis`` onto a step dict in place would persist one car's
    overlay onto every later request for that ladder.
    """
    stamped = [
        {
            **s,
            "order_basis": dict(
                doc_basis.get(_rung_evidence_key(str(s.get("name") or "")))
                or UNPROVEN_ORDER_BASIS
            ),
        }
        for s in steps
    ]
    if not doc_order:
        return stamped
    position: dict[str, int] = {}
    for i, name in enumerate(doc_order):
        key = _rung_evidence_key(name)
        if key:
            position.setdefault(key, i)

    def _pos(step: dict[str, Any]) -> int | None:
        return position.get(_rung_evidence_key(str(step.get("name") or "")))

    placed = [s for s in stamped if _pos(s) is not None]
    if len(placed) < 2:
        # One placed rung orders nothing. Leave the incoming order alone.
        return stamped
    placed.sort(key=lambda s: _pos(s) or 0)

    # Where each unplaced rung hangs: under the last placed rung before it.
    above_all: list[dict[str, Any]] = []
    trailing: dict[int, list[dict[str, Any]]] = {}
    last_placed: dict[str, Any] | None = None
    for step in stamped:
        if _pos(step) is not None:
            last_placed = step
        elif last_placed is None:
            above_all.append(step)
        else:
            trailing.setdefault(id(last_placed), []).append(step)

    out: list[dict[str, Any]] = list(above_all)
    for step in placed:
        out.append(step)
        out.extend(trailing.get(id(step), []))
    return out


def _step_trim_keys(step: dict[str, Any], *, make: str, model: str) -> set[str]:
    keys: set[str] = set()
    name = str(step.get("name") or "").strip()
    if name:
        keys |= _trim_identity_keys(name, make, model)
    for alias in step.get("aliases") or []:
        keys |= _trim_identity_keys(str(alias), make, model)
    return keys
