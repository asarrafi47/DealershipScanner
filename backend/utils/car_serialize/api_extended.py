"""
``serialize_car_for_api`` extended display: the plausibility guard / curated 0-60
and factory catalog lookups (both gated by ``include_extended_display``), paint
color families and the package-name list.

Split out of ``serialize.serialize_car_for_api`` (2026-10-01) with no behaviour
change; ``backend/tests/test_serialize_car_for_api_golden.py`` pins the output.
"""
from __future__ import annotations

import json
from typing import Any


# THE ``ai_engine_specs`` OVERRIDE IS GONE (2026-08-02). It used to run before
# the plausibility guard and let the engine-matched row win OUTRIGHT on horsepower,
# torque, tow capacity and 0-60, and fill curb weight when the model-level value was
# missing. Every one of that table's 888 rows carries
# ``source_host='ai-engine-research'`` — one distinct value, counted against
# the live database — so the override could only ever replace a scraped
# number with a generated one. ``fuel_tank_gal`` had already been removed
# from it on 2026-07-31 for the same reason; the other five fields had not,
# which is why fixing that one field did not close the class.
#
# ``lookup_engine_specs`` now has no production caller.


def apply_extended_plausibility(out: dict[str, Any], c: dict[str, Any]) -> None:
    """Plausibility guard on the assembled numbers, and the curated cohort 0-60.

    The curated 0-60 wins over any stored one — it is only set where the stored
    value provably belongs to a different trim.
    """
    try:
        from backend.enrichment.knowledge_engine_specs import (
            curated_zero_to_60_sec,
            implausible_extended_spec_fields,
        )

        for _f in implausible_extended_spec_fields(c, out):
            out[_f] = None
        _curated_060 = curated_zero_to_60_sec(c)
        if _curated_060 is not None:
            out["zero_to_60_sec"] = _curated_060
    except Exception:
        pass


def apply_catalog_options(
    out: dict[str, Any], c: dict[str, Any], *, include_extended_display: bool
) -> None:
    # Factory catalog packages / standalone options (catalog_trims/_options/_packages),
    # matched on this row's own year/make/model/trim — independent of dealer window
    # sticker data. Most trims have no catalog rows; None when no match.
    _catalog: dict[str, Any] = {}
    if include_extended_display:
        try:
            from backend.enrichment.catalog_lookup import lookup_catalog_options_and_packages

            _catalog = lookup_catalog_options_and_packages(
                c.get("year"), c.get("make"), c.get("model"), c.get("trim")
            )
        except Exception:
            _catalog = {}
    out["catalog_packages"] = _catalog.get("packages") or None
    out["catalog_options"] = _catalog.get("options") or None


def apply_color_families(out: dict[str, Any], c: dict[str, Any], car: dict[str, Any]) -> None:
    """Exterior/interior paint buckets. Stored interior buckets are read from the RAW row."""
    from backend.utils.interior_color_buckets import infer_paint_color_buckets, parse_stored_buckets

    out["exterior_color_families"] = infer_paint_color_buckets(c.get("exterior_color"), c.get("make"))
    _ib = parse_stored_buckets(car.get("interior_color_buckets"))
    out["interior_color_families"] = (
        _ib if _ib else infer_paint_color_buckets(c.get("interior_color"), c.get("make"))
    )


def apply_package_names(out: dict[str, Any]) -> None:
    _pkg_names: list[str] = []
    _pkg_raw = out.get("packages")
    if _pkg_raw:
        try:
            _p = json.loads(_pkg_raw) if isinstance(_pkg_raw, str) else _pkg_raw
            for _entry in (_p.get("packages_normalized") or []):
                if isinstance(_entry, dict):
                    _n = (_entry.get("canonical_name") or _entry.get("name") or "").strip()
                    if _n:
                        _pkg_names.append(_n)
            for _n in (_p.get("possible_packages") or []):
                if isinstance(_n, str) and _n.strip():
                    _pkg_names.append(_n.strip())
        except Exception:
            pass
    out["package_names"] = _pkg_names
