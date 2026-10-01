"""The attribution entry points every scan path calls.

Two passes, one rule set:

* :func:`gate_page` — ONE payload, right after a parser turned it into rows
  (``backend.parsers.parse`` ends here). Runs on every page of every path so a
  page walk can count what is this store's.
* :func:`decide` — the ALL-ROWS pass over a whole capture. A page holding nothing
  but one sibling's cars looks like a single-store payload on its own and only
  reads as a sibling next to this store's own rooftop across the whole capture,
  so the per-page marks are cleared and the gate re-runs once over the union.
  ``phases/dealer_run.run_dealer`` and ``delta_scan.delta_scan_dealer`` both
  settle attribution here (before 2026-10-01 each carried its own copy, and the
  delta copy re-gated only the rows the per-page pass had kept).

The store-scope rule lives here too, once: rows a store-scoped recipe returned
(CarsCommerce ``facetFilters.source_id`` / the site's Location facet, verified at
synthesis) are this store's by construction. ``recipes.try_fetch_via_recipes``
marks such a payload with :func:`mark_payload_feed_scoped`; :func:`gate_page`
then skips the gate and stamps the rows, and :func:`decide` exempts stamped rows
from the union re-gate (Tutton CDJR kept 5 of 346 cars when they were re-gated,
2026-09-26).
"""
from __future__ import annotations

import logging
import sys
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable

from backend.attribution.disown import split_refusals
from backend.attribution.place import gate_place, store_place

logger = logging.getLogger("scanner")

# Row / payload marker for "this store's own feed ids, verified at synthesis".
FEED_SCOPED = "_feed_scoped"
# Row marker the gate sets on every refused row (the reason string).
REJECT_KEY = "_rooftop_reject"


def mark_payload_feed_scoped(payload: Any) -> None:
    """Mark a replayed payload as store-scoped so every later parse of it (the
    union re-parse in dealer_run's recovery step) skips the gate."""
    if isinstance(payload, dict):
        payload[FEED_SCOPED] = True


def payload_is_feed_scoped(payload: Any) -> bool:
    return isinstance(payload, dict) and payload.get(FEED_SCOPED) is True


def _gate() -> Callable[..., tuple[list[dict], list[dict]]]:
    """The gate function: ``backend.parsers.resolve_rooftop_attribution`` when
    that package is loaded (its long-standing public name, which callers and
    tests stub), else the implementation in ``backend.attribution.rooftop``."""
    from backend.attribution import rooftop

    mod = sys.modules.get("backend.parsers")
    fn = getattr(mod, "resolve_rooftop_attribution", None) if mod is not None else None
    return fn or rooftop.resolve_rooftop_attribution


@dataclass
class DealerCtx:
    """The store being scanned, as the gate needs it."""

    dealer_id: str
    dealer_name: str
    dealer_url: str
    place: dict[str, str] = field(default_factory=dict)

    @classmethod
    def for_store(cls, dealer_id: str, dealer_name: str, dealer_url: str) -> "DealerCtx":
        """Look the store's place up once (:func:`store_place`). Blocking (DB);
        async callers run it in a thread."""
        try:
            place = store_place(dealer_url, dealer_id)
        except Exception:  # noqa: BLE001 - attribution help must never break a scan
            place = {}
        return cls(dealer_id, dealer_name, dealer_url, gate_place(place))

    def gate_kwargs(self) -> dict[str, str]:
        return dict(dealer_id=self.dealer_id, dealer_name=self.dealer_name, dealer_url=self.dealer_url, **self.place)


@dataclass
class Decision:
    """Outcome of the all-rows pass. ``kept`` is store-scoped rows first, then
    the gate's kept rows; every ``refused`` row carries ``_rooftop_reject``."""

    kept: list[dict[str, Any]]
    refused: list[dict[str, Any]]
    scoped: int = 0

    @property
    def reasons(self) -> dict[str, int]:
        return dict(Counter(str(r.get(REJECT_KEY) or "unknown") for r in self.refused))

    def split_refusals(self) -> tuple[list[dict[str, Any]], int]:
        """``(rows that may be un-listed, count that may not)`` — see
        ``backend.attribution.disown.EVIDENCE_BACKED_REJECTS``."""
        return split_refusals(self.refused)


def gate_page(
    rows: list[dict],
    raw_data: Any = None,
    *,
    dealer_id: str = "",
    dealer_name: str = "",
    dealer_url: str = "",
    rejected_out: list | None = None,
    trust_feed_scope: bool = False,
    dealer_address: str = "",
    dealer_city: str = "",
    dealer_state: str = "",
    dealer_zip: str = "",
    dealer_address_source: str = "",
) -> list[dict]:
    """Attribute the rows parsed from ONE payload (the tail of ``parse``).

    With ``rejected_out`` the return is only this store's rows and the refused
    rows land in that list; without it the refused rows come back too, each
    carrying ``_rooftop_reject`` (the recipe page walk counts VINs from the
    return value; storage drops marked rows).
    """
    if not trust_feed_scope and payload_is_feed_scoped(raw_data):
        trust_feed_scope = True  # marker set by the recipe replay on a store-scoped payload
    if trust_feed_scope:
        for r in rows:
            r[FEED_SCOPED] = True  # the all-rows pass must not re-gate these
        stamps = {str((r.get("_rooftop") or {}).get("key") or "")[:60] for r in rows if isinstance(r.get("_rooftop"), dict)}
        if len(stamps) > 1:
            from backend.attribution.rooftop import _log

            _log.info("rooftop attribution [%s]: gate skipped, recipe is scoped to this store's own feed ids; %d row(s) across %d stamp(s) %s",
                      dealer_id or dealer_name, len(rows), len(stamps), sorted(stamps)[:4])
        return rows
    kept, rejected = _gate()(
        rows, dealer_id=dealer_id, dealer_name=dealer_name, dealer_url=dealer_url,
        dealer_address=dealer_address, dealer_city=dealer_city,
        dealer_state=dealer_state, dealer_zip=dealer_zip,
        dealer_address_source=dealer_address_source,
    )
    if rejected_out is None:
        return kept + rejected
    rejected_out.extend(rejected)
    return kept


def parse_page(provider: str, body: Any, ctx: DealerCtx, refused_out: list, *, trust_feed_scope: bool = False) -> list[dict]:
    """Parse one payload for *ctx*'s store: its rows, refused rows into ``refused_out``."""
    import backend.parsers as parsers  # resolved per call: callers / tests stub backend.parsers.parse

    kwargs: dict[str, Any] = dict(ctx.place)
    if trust_feed_scope:
        kwargs["trust_feed_scope"] = True
    return list(parsers.parse(
        provider, body, base_url=ctx.dealer_url, dealer_id=ctx.dealer_id,
        dealer_name=ctx.dealer_name, dealer_url=ctx.dealer_url,
        rejected_out=refused_out, **kwargs,
    ))


def decide(rows: list[dict], ctx: DealerCtx) -> Decision:
    """The ONE all-rows pass: who of *rows* is *ctx*'s store.

    *rows* is everything the capture produced for this store — the rows the
    per-page gate kept AND the ones it refused (it may have refused them for
    want of the other pages), plus anything a recovery strategy added. Per-page
    marks are cleared, store-scoped rows are kept without the gate, and the
    gate runs once over the rest.
    """
    for v in rows:
        v.pop(REJECT_KEY, None)
    scoped = [v for v in rows if v.get(FEED_SCOPED)]
    open_rows = [v for v in rows if not v.get(FEED_SCOPED)]
    refused: list[dict] = []
    if open_rows:
        open_rows, refused = _gate()(open_rows, **ctx.gate_kwargs())
    if scoped:
        logger.info("rooftop attribution [%s]: %d row(s) from store-scoped recipe(s) kept without the union gate",
                    ctx.dealer_id, len(scoped))
    return Decision(kept=scoped + list(open_rows), refused=list(refused), scoped=len(scoped))


__all__ = [
    "FEED_SCOPED",
    "REJECT_KEY",
    "DealerCtx",
    "Decision",
    "decide",
    "gate_page",
    "mark_payload_feed_scoped",
    "parse_page",
    "payload_is_feed_scoped",
]
