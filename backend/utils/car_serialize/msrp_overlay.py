"""Sidecar MSRP facts for cars where the trusted resolver produced nothing.

``resolve_display_msrp`` (backend/utils/msrp_trust.py) decides what may be shown
as *this car's MSRP*, and when it returns nothing the page correctly shows no
MSRP at all. But two derived stores beside ``cars`` still hold something honest
to say about such a car, and this module is the one place that decides how each
is worded so no surface can present either as the feed's MSRP:

* ``car_image_text.sticker_msrp`` — a sticker total read by an agent off a
  document photographed in this listing's own gallery, recorded only after the
  hard gates in ``backend/scripts/image_batch.py cmd_record`` (VIN confirmed,
  USD, total read directly, arithmetic reconciliation). Since 2026-08-18 the
  document need not be an original Monroney — a dealer build sheet's total is
  stored too, and its provenance travels with it (``msrp_document``), so this
  module words the two differently rather than refusing one. It is a document
  about THIS VIN, exposed as ``sticker_msrp`` — a distinct field, never folded
  into ``msrp``.
* ``trim_msrp_bands`` — what this trim has actually been observed to sticker
  at, across every sticker we have read. A band, never a price: the observed
  spread within one trim runs to five figures because a Monroney total includes
  options (see migrations/V010__trim_msrp_bands.sql). Exposed as
  ``estimated_msrp_band`` and always labelled an estimate.

Both are read-time overlays following the ``car_attribution`` pattern: the
caller batches the ``car_image_text`` read (``cars_repo.car_sticker_msrp_values``)
and this module only shapes the payload. A car whose resolver already produced
an MSRP gets ``{}`` — the trusted figure stands alone.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _num(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def msrp_overlay_public_fields(
    car: dict[str, Any],
    sticker_msrp: float | int | None,
    *,
    identity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Fields to merge into a serialized car, or ``{}`` when there is nothing to add.

    *car* is the SERIALIZED car (``serialize_car_for_api`` output): its ``msrp``
    is already the resolver's verdict and its ``price`` is already plausibility
    guarded. *sticker_msrp* is this car's entry from
    ``cars_repo.car_sticker_msrp_values`` — a ``{"value", "sticker_is_original",
    "msrp_document"}`` mapping (a bare number is accepted too and read as a
    Monroney: only pre-2026-08-18 callers pass one, and back then only original
    documents were stored). The caller batches that read, so this never looks
    anything up per car except the in-process band cache.

    *identity* is the raw ``cars`` row when the caller holds one. The band keys
    were built from ``cars.model`` / ``cars.trim`` verbatim, while the serialized
    dict may carry BMW display-normalized model/trim; passing the raw row keeps
    the lookup honest. Defaults to *car*.

    The sticker figure obeys the same rules the resolver applies to its own
    sources: nothing at or below the asking price (an MSRP under the price is
    not an MSRP), nothing the observed band for this trim disproves. The band
    itself is only offered when NO per-car figure survived — it describes the
    trim, not the car, and the wording says so.
    """
    if not car or car.get("msrp"):
        return {}
    from backend.utils import msrp_trust

    ident = identity if identity is not None else car
    price = _num(car.get("price"))
    if price is not None and price <= 0:
        price = None

    document = "monroney"
    if isinstance(sticker_msrp, Mapping):
        raw_doc = str(sticker_msrp.get("msrp_document") or "").strip().lower()
        if raw_doc in ("monroney", "dealer_build_sheet"):
            document = raw_doc
        elif not sticker_msrp.get("sticker_is_original", True):
            document = "dealer_build_sheet"
        sticker_msrp = sticker_msrp.get("value")

    value = _num(sticker_msrp)
    if value is not None:
        value = int(round(value))
        if (price is None or value > price) and msrp_trust.implausible_for_trim(
            ident, value
        ) is None:
            # A used car's sticker total is what it cost NEW; same labelling rule
            # as the resolver, and no "below MSRP" arithmetic from here ever —
            # the gap on a used car is depreciation, not a discount.
            # ``sticker_msrp_source``, NOT ``msrp_source``: the resolver owns that
            # key, and stamping it while ``msrp`` stays null makes any consumer
            # that renders provenance whenever the source is set describe an MSRP
            # that does not exist.
            # A dealer build sheet is worded as exactly that (2026-08-18 policy:
            # store every sticker total, but the provenance must display
            # honestly) — car 874954's "NOT ORIGINAL COPY" sheet priced the base
            # chassis of a camper conversion, and only the wording tells a
            # shopper which document the figure came from.
            if document == "dealer_build_sheet":
                source = "dealer_build_sheet_photo"
                note = (
                    "From the dealer build sheet photographed in this listing — "
                    "a dealer-generated document, not the factory Monroney label."
                )
            else:
                source = "window_sticker_photo"
                note = (
                    "Read from the window sticker photographed in this listing's gallery."
                )
            return {
                "sticker_msrp": value,
                "sticker_msrp_source": source,
                "sticker_msrp_label": (
                    "MSRP" if msrp_trust.is_new_inventory(car) else "Original MSRP"
                ),
                "sticker_msrp_note": note,
            }

    band = msrp_trust.observed_trim_msrp_band(ident)
    if band:
        lo, hi, n = band
        return {
            "estimated_msrp_band": {"low": lo, "high": hi, "observations": n},
            "estimated_msrp_band_note": (
                f"Estimated from {n} window-sticker totals read for this trim — "
                "an observed range for the trim, not this car's sticker price."
            ),
        }
    return {}
