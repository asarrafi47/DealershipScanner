"""Which MSRP a car page is allowed to print, decided at read time.

``cars.msrp`` is written verbatim from the dealer's inventory feed. On new
inventory the feeds generally put the Monroney total there. On used inventory
they put a marketing "was" price there — the number a listing shows struck
through above the asking price — and nothing in the column says which of the two
it is. Counted against the live database over all 122,663 active listings
(2026-08-05):

    listings with an msrp                       18,350
      msrp EXACTLY equal to price                9,621   (52%)
      msrp LESS than price                       2,439   (13%)
      msrp greater than price                    5,900   (32%)
    of the used/CPO half (condition != New)      5,142
      msrp equal to price                        2,342

Half the column is the asking price copied into a second field. That is not a
manufacturer's suggested retail price, and printing it as one is not a cosmetic
problem: the page then subtracts it from the asking price and prints the
remainder as a saving. VIN WBS3U9C58GP969438, a 2016 BMW M4, showed
"MSRP $38,990 / LISTED $36,788 / BELOW MSRP $2,202" — while the OEM window
sticker we hold for that VIN, ``car_window_stickers/581/WBS3U9C58GP969438``,
prints a Net Total of $86,600. The $2,202 "discount" was arithmetic over a
number the manufacturer never suggested. VIN 5UX83DP05R9U16883, a 2024 X3 M40i,
showed "MSRP $48,990 / LISTED PRICE $48,990" — the same number twice, its
sticker total being $66,880.

This is fixed HERE, at read time, and not with an UPDATE over the column. The
write path keeps whatever the feed says — that is its job, and the next scan
would overwrite any correction within the hour (a previous one-shot data fix in
this repo decayed 63% in half an hour). The display layer decides what is
credible, the same way ``battery_kwh`` / ``curb_weight_lb`` / ``tow_capacity_lb``
are suppressed in ``backend/utils/car_serialize/serialize.py``.

THE RULE. :func:`resolve_display_msrp` returns a figure only when all of it
holds:

  (a) There is a source that can be trusted for THIS car:
        * the OEM/listing window sticker we hold on disk for this VIN, parsed at
          read time — a document, and the preferred source whenever it exists; or
        * ``cars.msrp`` on a car the listing calls NEW and not CPO, where the
          feed's msrp is the sticker total and the asking price is measured
          against it. On anything pre-owned the feed value is rejected outright:
          it is a "was" price wearing an MSRP's name.
  (b) The figure is greater than the listed price. An MSRP equal to the price
      conveys nothing and reads as a fabricated anchor; an MSRP below the price
      is not an MSRP. Both are dropped rather than shown, for every source.
  (c) There is something to show it next to — either a price, or (with no price
      at all) the MSRP stands alone as the only figure the page has.

A rejected MSRP renders as NOTHING — no "MSRP —" slot, no silent fall back to
the price. The listed price alone is the correct output.

THE SAVING is narrower still, and deliberately not just "(a) and (b)". A
"BELOW MSRP" figure is a claim about a discount available today, so it is
emitted only for NEW, non-CPO inventory, where the MSRP is the current
benchmark the car is being sold against. On a used car the sticker total is a
real and useful fact — it is what the car cost new — but the gap between it and
today's price is depreciation, not a dealer discount. Printing "Below MSRP
$49,812" on that 2016 M4 would replace a $2,202 fabrication with a $49,812 one.
Used cars get the figure LABELLED "Original MSRP", from the sticker, and no
subtraction.
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

#: Bounds on any MSRP we are willing to print, whichever source produced it.
#: Matches the band ``_parse_msrp_from_sticker_text`` already applies, so the
#: sticker parser and this gate cannot disagree about what is a price at all.
_MSRP_MIN = 5_000
_MSRP_MAX = 500_000

#: Set to "0" to skip reading window stickers off disk at read time (the PDF
#: text extraction shells out to ``pdftotext``). With it off, only the feed
#: rule in (a) can admit an MSRP.
_STICKER_MSRP_ENABLED = os.environ.get("MSRP_FROM_WINDOW_STICKER", "1") != "0"


def _num(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _in_band(v: float | None) -> int | None:
    if v is None:
        return None
    n = int(round(v))
    return n if _MSRP_MIN <= n <= _MSRP_MAX else None


def is_new_inventory(car: dict[str, Any]) -> bool:
    """True only for a car the listing calls New and does not flag as CPO.

    Unknown condition counts as pre-owned. The conservative direction is the
    right one here — a missing MSRP row is always acceptable, an invented one
    never is — and it costs nothing in practice: every one of the 18,350 active
    listings carrying an msrp has a non-empty ``condition``.
    """
    if car.get("is_cpo") in (1, True, "1"):
        return False
    return str(car.get("condition") or "").strip().lower() == "new"


@lru_cache(maxsize=8192)
def _sticker_msrp_from_file(path: str, mtime_ns: int, size: int) -> int | None:
    """Parse the MSRP out of one stored sticker file.

    Memoized on (path, mtime, size) because the expensive branch shells out to
    ``pdftotext``: a sticker file does not change between renders, and the same
    VIN is re-rendered constantly. ``mtime_ns``/``size`` are in the key so a
    re-fetched sticker parses again rather than serving a stale total.
    """
    del mtime_ns, size  # cache-key only
    p = Path(path)
    suffix = p.suffix.lower()
    # Images are stored for some dealers. There is no read-time OCR here, and a
    # guess off a filename is not a document.
    if suffix not in (".txt", ".pdf"):
        return None
    try:
        content = p.read_bytes()
    except OSError:
        return None

    if suffix == ".txt":
        text = content.decode("utf-8", errors="replace")
    elif len(content) >= 500 and content[:4] == b"%PDF":
        try:
            from backend.scanner.window_sticker import _extract_pdf_text

            text = _extract_pdf_text(content)
        except Exception:
            return None
    else:
        return None
    if not text.strip():
        return None
    try:
        from backend.scanner.window_sticker import _parse_msrp_from_sticker_text

        return _in_band(_num(_parse_msrp_from_sticker_text(text)))
    except Exception:
        return None


def sticker_msrp_for_car(car: dict[str, Any]) -> int | None:
    """The MSRP printed on the window sticker we hold for this VIN, or ``None``.

    Read off the stored document every time rather than out of ``cars.msrp``.
    The scanner only ever writes a parsed sticker total into that column when it
    is EMPTY (``window_sticker_service._analyze_sticker_text_and_merge``), so on
    exactly the cars this matters for — the ones where the feed already supplied
    a "was" price — the sticker's number was computed and then discarded. It is
    still on disk; this reads it back.
    """
    if not _STICKER_MSRP_ENABLED or not car:
        return None
    vin = str(car.get("vin") or "").strip().upper()
    if len(vin) < 11:
        return None
    try:
        from backend.enrichment.window_sticker_service import window_sticker_local_path

        local = window_sticker_local_path(
            vin,
            dealer_id=car.get("dealer_id"),
            dealership_registry_id=car.get("dealership_registry_id"),
        )
    except Exception:
        return None
    if not local:
        return None
    try:
        st = local.stat()
    except OSError:
        return None
    return _sticker_msrp_from_file(str(local), st.st_mtime_ns, st.st_size)


def clear_sticker_msrp_cache() -> None:
    """Drop the memoized sticker reads (tests, and after a sticker re-fetch)."""
    _sticker_msrp_from_file.cache_clear()


def resolve_display_msrp(
    car: dict[str, Any], *, allow_sticker: bool = True
) -> dict[str, Any]:
    """Decide what MSRP (if any) this car may show. See the module docstring.

    Returns a dict that is always shaped the same::

        {"msrp": int|None,          # render nothing when None
         "label": str|None,         # "MSRP" or "Original MSRP"
         "source": str|None,        # provenance string, for the UI and the API
         "from_sticker": bool,
         "savings": int|None,       # "Below MSRP" — new, non-CPO inventory only
         "savings_basis": str|None,
         "rejected": str|None}      # why nothing is shown, for debugging

    Pass ``allow_sticker=False`` on bulk paths that serialize thousands of rows
    and never render a price block; it skips the per-VIN filesystem read.
    """
    out: dict[str, Any] = {
        "msrp": None,
        "label": None,
        "source": None,
        "from_sticker": False,
        "savings": None,
        "savings_basis": None,
        "rejected": None,
    }
    if not car:
        return out

    price = _num(car.get("price"))
    if price is not None and price <= 0:
        price = None
    is_new = is_new_inventory(car)

    # (a) pick a source. The sticker is a document about this VIN and wins over
    # the feed on every car, new or used, whenever we hold one.
    sticker = sticker_msrp_for_car(car) if allow_sticker else None
    feed = _in_band(_num(car.get("msrp")))
    if sticker is not None:
        value, source, from_sticker = sticker, "window sticker (parsed)", True
    elif feed is not None and is_new:
        value, source, from_sticker = feed, "dealer listing (cars.msrp)", False
    else:
        out["rejected"] = "no_trustworthy_source" if feed is None else "feed_msrp_on_pre_owned"
        return out

    # (b) an MSRP at or below the asking price is not an MSRP.
    if price is not None and value <= price:
        out["rejected"] = "not_above_price"
        return out

    out["msrp"] = value

    # (b2) A figure far outside what this trim has actually been observed to
    # sticker at is a data error — a decimal slip, a foreign-currency figure,
    # another car's document — not an unusual options list. Suppress it and
    # record WHY, so nothing downstream labels it or subtracts it from the
    # price as a saving.
    trim_reason = implausible_for_trim(car, value)
    if trim_reason is not None:
        out["msrp"] = None
        out["rejected"] = trim_reason
        return out

    out["from_sticker"] = from_sticker
    out["source"] = source
    # A used car's sticker total is what it cost NEW; say so, so nobody reads it
    # as today's asking benchmark.
    out["label"] = "MSRP" if is_new else "Original MSRP"

    # The saving is a claim about a discount available now, so new inventory
    # only. ``value > price`` is already guaranteed by (b), which is what keeps
    # this from ever being zero or negative.
    if is_new and price is not None:
        out["savings"] = int(round(value - price))
        out["savings_basis"] = (
            "window sticker MSRP - cars.price"
            if from_sticker
            else "cars.msrp - cars.price"
        )
    return out

# --- observed-band plausibility -------------------------------------------------------

_BAND_CACHE: dict[tuple, tuple[int, int, int]] = {}
_BAND_LOADED = False


def _load_bands() -> None:
    """Load trim_msrp_bands once. Absent table or DB simply disables the check."""
    global _BAND_LOADED
    if _BAND_LOADED:
        return
    _BAND_LOADED = True
    try:
        from backend.db.inventory_pg import is_inventory_postgres, pg_connect

        if not is_inventory_postgres():
            return
        conn = pg_connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT year, make, model, trim, msrp_min, msrp_max, observations "
                "FROM trim_msrp_bands"
            )
            for year, make, model, trim, lo, hi, n in cur.fetchall():
                key = (int(year), str(make).lower(), str(model).lower(), str(trim or "").lower())
                _BAND_CACHE[key] = (int(lo), int(hi), int(n))
        finally:
            conn.close()
    except Exception:  # noqa: BLE001 - a sanity check must never break a page render
        pass


def implausible_for_trim(car: dict[str, Any], msrp: float | int | None) -> str | None:
    """
    Reason this MSRP cannot be right for this trim, or None.

    Compares against what the trim has actually been observed to sticker at
    (``trim_msrp_bands``), with generous slack: the band is built from Monroney TOTALS, so
    its width is mostly optional equipment, and a genuinely well-optioned car can sit above
    every example seen so far. Only a value far outside the observed range is called out --
    the failures worth catching are a decimal slip, a foreign-currency figure, or another
    car's document, not a car with an unusual options list.

    Returns a short reason string so a caller can log or display WHY, rather than silently
    dropping a number.
    """
    if msrp is None:
        return None
    try:
        value = float(msrp)
    except (TypeError, ValueError):
        return None
    _load_bands()
    if not _BAND_CACHE:
        return None
    try:
        year = int(car.get("year") or 0)
    except (TypeError, ValueError):
        return None
    key = (
        year,
        str(car.get("make") or "").lower(),
        str(car.get("model") or "").lower(),
        str(car.get("trim") or "").lower(),
    )
    band = _BAND_CACHE.get(key)
    if not band:
        return None
    lo, hi, n = band
    # Slack in both directions; see docstring. 35% above the widest observed sticker still
    # catches a doubled or currency-converted figure.
    if value < lo * 0.65:
        return f"{value:,.0f} is far below the {n} observed stickers for this trim (min {lo:,})"
    if value > hi * 1.35:
        return f"{value:,.0f} is far above the {n} observed stickers for this trim (max {hi:,})"
    return None


def observed_trim_msrp_band(car: dict[str, Any]) -> tuple[int, int, int] | None:
    """``(low, high, observations)`` this trim has actually stickered at, or ``None``.

    The same in-process cache :func:`implausible_for_trim` judges against, offered
    as a positive fact: with no per-car MSRP at all, "the stickers we have read
    for this trim ran low–high" is the one thing the distribution can honestly
    say about the car. It is a range over Monroney TOTALS — base + options +
    destination — so it must be presented as an estimate for the trim, never as
    this car's sticker price (see migrations/V010__trim_msrp_bands.sql).
    """
    if not car:
        return None
    _load_bands()
    if not _BAND_CACHE:
        return None
    try:
        year = int(car.get("year") or 0)
    except (TypeError, ValueError):
        return None
    key = (
        year,
        str(car.get("make") or "").lower(),
        str(car.get("model") or "").lower(),
        str(car.get("trim") or "").lower(),
    )
    return _BAND_CACHE.get(key)
