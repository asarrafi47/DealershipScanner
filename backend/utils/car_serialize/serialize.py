"""
Top-level API/UI serialization entry points.

``serialize_car_for_api`` (car detail / embedded payloads) and
``serialize_car_for_listings_grid`` (compact grid cards), plus the price-history
and gallery helpers they use.

Split out of the former monolith ``backend/utils/car_serialize.py`` and
re-exported from the package facade so the public import surface is unchanged.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any

from backend.parsers.base import filter_spyne_gallery_variants
from backend.utils.field_clean import (
    clean_car_row_dict,
    coerce_drivetrain_stored,
    is_effectively_empty,
    normalize_optional_url,
)
from backend.utils.safe_listing_url import normalize_listing_image_url

from ._common import (
    DISPLAY_DASH,
    SENSITIVE_CAR_ROW_KEYS,
    _dealer_spec_wins,
    _format_mpg_city_highway,
    _transmission_line_has_gear_count,
    _transmission_phrase_prefer_detail,
    format_display_value,
)
from .bmw import apply_bmw_model_trim_display
from .condition import fill_derived_condition_for_display
from .engine import _effective_fuel_type_for_display, build_engine_display
from .location_tco import resolve_car_fuel_requirement, resolve_car_state_code


def serialize_car_for_api(
    car: dict[str, Any],
    *,
    include_verified: bool = True,
    verified_specs: dict[str, Any] | None = None,
    include_extended_display: bool = True,
) -> dict[str, Any]:
    """
    Shallow copy safe for JSON: strings cleaned + display dashes, numbers preserved.
    Adds ``engine_display``; ``model`` / ``trim`` may be BMW-normalized for display only.

    ``include_verified=False`` skips EPA/trim merge (use for bulk listing payloads).
    Pass ``verified_specs`` when the caller already merged (e.g. car detail page).

    ``include_extended_display=False`` skips the two enrichment lookups that only feed
    ancillary display fields (``ai_engine_specs`` → hp/torque/tow/0-60/tank/curb; factory
    ``catalog_*`` → ``catalog_packages`` / ``catalog_options``). Those fields are set to
    ``None``/left as-is instead. Use it when the caller reads only the core spec-sheet
    fields (e.g. listing-completeness detection), to avoid two per-car reference queries.
    """
    if not car:
        return {}
    c = clean_car_row_dict(dict(car))

    vs: dict[str, Any] = {}
    if verified_specs is not None:
        vs = verified_specs
    elif include_verified:
        try:
            from backend.enrichment.knowledge_engine import merge_verified_specs

            vs = merge_verified_specs(c)
        except Exception:
            vs = {}

    model_d, trim_d = apply_bmw_model_trim_display(c)
    engine_disp = build_engine_display(c, vs if vs else None)

    out: dict[str, Any] = {}
    for k, v in c.items():
        if k in SENSITIVE_CAR_ROW_KEYS:
            continue
        if k == "gallery":
            if isinstance(v, list):
                out[k] = filter_spyne_gallery_variants(v)
            elif isinstance(v, str):
                try:
                    import json as _json
                    parsed = _json.loads(v)
                    out[k] = filter_spyne_gallery_variants(parsed) if isinstance(parsed, list) else v
                except Exception:
                    out[k] = v
            else:
                out[k] = v
            continue
        if k in ("spin_frames", "interior_pano"):
            # Handled explicitly after the loop (contract defaults: [] / null);
            # must not fall through to format_display_value (None -> em-dash).
            continue
        if k == "history_highlights":
            out[k] = v
            continue
        if k == "packages":
            if is_effectively_empty(v) or str(v).strip() in ("{}", "[]"):
                out[k] = None
            else:
                out[k] = v
            continue
        if k in (
            "price",
            "mileage",
            "year",
            "msrp",
            "cylinders",
            "mpg_city",
            "mpg_highway",
            "id",
            "distance_miles",
            "dealership_registry_id",
        ):
            out[k] = v
            continue
        if k == "engine_l":
            out[k] = v
            continue
        if k == "data_quality_score" and isinstance(v, (int, float)):
            out[k] = v
            continue
        if isinstance(v, (dict, list)) and k not in ("gallery", "history_highlights"):
            out[k] = v
            continue
        if isinstance(v, (int, float)):
            out[k] = v
            continue
        out[k] = format_display_value(v)

    for url_key in ("image_url", "source_url", "carfax_url", "dealer_url"):
        if url_key in c:
            out[url_key] = normalize_optional_url(c.get(url_key))

    # 360 spin assets: always emitted with contract defaults ([] / null).
    _spin = c.get("spin_frames")
    if isinstance(_spin, str):
        try:
            _spin = json.loads(_spin)
        except (TypeError, ValueError):
            _spin = None
    out["spin_frames"] = (
        [u for u in _spin if isinstance(u, str) and u.strip()] if isinstance(_spin, list) else []
    )
    out["interior_pano"] = normalize_optional_url(c.get("interior_pano"))

    from backend.utils.first_seen import first_seen_fields

    out.update(first_seen_fields(c))

    from backend.parsers.vdp_urls import resolve_vehicle_source_url

    src_was_placeholder = is_effectively_empty(c.get("source_url"))
    detail_was_placeholder = is_effectively_empty(c.get("_detail_url")) and is_effectively_empty(
        c.get("detail_url")
    )
    listing_vdp = resolve_vehicle_source_url(c)
    if listing_vdp:
        out["listing_vdp_url"] = listing_vdp
        if not src_was_placeholder:
            out["source_url"] = listing_vdp
    else:
        out["listing_vdp_url"] = out.get("source_url") or out.get("dealer_url")

    out["model"] = model_d
    out["trim"] = trim_d
    out["engine_display"] = engine_disp

    # price is NOT trusted verbatim from the column. Measured 2026-08-05: of
    # 122,663 active listings, 33 exceed $300k and 212 sit under $500. Most of
    # the >$300k group is genuinely priced (a McLaren 765LT at $699,900 is a
    # real ask) but a few are parser/feed artifacts wearing a plausible-looking
    # number (a 2026 Ford Bronco Base at $449,150 is exactly 9.8x its own
    # 43-car active cohort median of $46,039). Same policy as the
    # ``battery_kwh`` / ``curb_weight_lb`` suppressions below: an implausible
    # figure is withheld, never replaced with an invented one. See
    # ``backend/utils/price_plausibility.py`` for the cohort math and its
    # documented gap (thin-cohort exotics/rare trims are left unjudged).
    if out.get("price") is not None:
        try:
            from backend.utils.price_plausibility import implausible_price

            _price_val = float(out["price"])
            if implausible_price(c, _price_val):
                out["price"] = None
        except (TypeError, ValueError):
            pass

    # Forced induction: use stored value or compute on the fly.
    _fi = c.get("forced_induction") or ""
    if not _fi.strip():
        try:
            from backend.utils.forced_induction import classify_forced_induction_from_car_row
            _fi = classify_forced_induction_from_car_row(c) or ""
        except Exception:
            _fi = ""
    out["forced_induction"] = _fi or None

    ft_override = _effective_fuel_type_for_display(c, engine_disp)
    if ft_override:
        out["fuel_type"] = format_display_value(ft_override)

    vcyl = vs.get("cylinders")
    if vcyl is not None and (c.get("cylinders") is None or str(c.get("cylinders")).strip() == ""):
        try:
            out["cylinders"] = int(vcyl)
        except (TypeError, ValueError):
            out["cylinders"] = vcyl

    # Prefer persisted transmission_type bucket when present; else dealer text / EPA / normalize.
    # Feeds sometimes label geared automatics (incl. many PHEVs) as "CVT"; trust detailed transmission
    # when it clearly normalizes to a non-CVT bucket.
    from backend.utils.transmission_normalize import normalize_transmission_standard

    stored_tt = c.get("transmission_type")
    dealer_t = c.get("transmission")
    y_int = c.get("year") if isinstance(c.get("year"), int) else None
    ignore_stored_cvt_bucket = False
    if (
        isinstance(stored_tt, str)
        and stored_tt.strip() == "CVT"
        and _dealer_spec_wins(dealer_t)
    ):
        d_norm, _d_weak = normalize_transmission_standard(
            dealer_t,
            make=c.get("make"),
            model=c.get("model"),
            trim=c.get("trim"),
            title=c.get("title"),
            year=y_int,
            vin=c.get("vin"),
            log_weak=False,
        )
        if d_norm and d_norm != "CVT":
            ignore_stored_cvt_bucket = True

    if (
        isinstance(stored_tt, str)
        and stored_tt.strip() in ("Automatic", "Manual", "CVT")
        and not ignore_stored_cvt_bucket
    ):
        bucket = stored_tt.strip()
        inferred_td = vs.get("transmission_display")
        if bucket == "Automatic" and isinstance(inferred_td, str) and _transmission_line_has_gear_count(
            inferred_td
        ):
            td = format_display_value(inferred_td.strip())
        elif bucket in ("Automatic", "Manual") and _dealer_spec_wins(dealer_t):
            raw_d = str(dealer_t).strip()
            if raw_d and re.search(r"\b\d+[-\s]?speed\b", raw_d, re.I):
                td = format_display_value(raw_d)
            else:
                td = format_display_value(bucket)
        else:
            td = format_display_value(bucket)
    else:
        inferred_t = vs.get("transmission_display")
        if _dealer_spec_wins(dealer_t):
            td_src = dealer_t
        else:
            td_src = inferred_t or dealer_t

        td_norm, _td_weak = normalize_transmission_standard(
            td_src,
            make=c.get("make"),
            model=c.get("model"),
            trim=c.get("trim"),
            title=c.get("title"),
            year=y_int,
            vin=c.get("vin"),
            log_weak=False,
        )
        pick = _transmission_phrase_prefer_detail(td_src, td_norm)
        td = format_display_value(pick if pick is not None else td_src)

    dealer_d = coerce_drivetrain_stored(c.get("drivetrain"))
    bs_raw = str(c.get("body_style") or "").lower()
    if dealer_d == "FWD" and "pickup" in bs_raw:
        dealer_d = None
    inferred_dd = vs.get("drivetrain_display")
    if _dealer_spec_wins(dealer_d):
        dd = format_display_value(dealer_d)
    else:
        dd = format_display_value(inferred_dd or dealer_d)

    # vPIC TransmissionStyle outranks the feed (DC-7, visual review 2026-09-28):
    # it is the per-VIN filing, while feed codes like "DDU" bucket an e-CVT
    # hybrid as "Automatic". Kept the feed's line only when both name the same
    # family and the feed carries a gear count the decode lacks. When the two
    # disagree on family, the feed's own words ride along as
    # ``transmission_feed`` so the page can show them muted.
    from backend.utils.vpic_specs import (
        transmission_family,
        vpic_specs_for_vin as _vpic_specs_tx,
        vpic_transmission_label,
    )

    out["transmission_source"] = None
    out["transmission_feed"] = None
    _vp_tx = vpic_transmission_label(_vpic_specs_tx(c.get("vin")))
    if _vp_tx:
        _fam_feed = transmission_family(td)
        _fam_vpic = transmission_family(_vp_tx)
        _feed_has_gears = isinstance(td, str) and _transmission_line_has_gear_count(td)
        _vpic_has_gears = _transmission_line_has_gear_count(_vp_tx)
        if not (_fam_feed == _fam_vpic and _feed_has_gears and not _vpic_has_gears):
            if _fam_feed != _fam_vpic:
                _feed_raw = str(dealer_t or "").strip() or (td if isinstance(td, str) else "")
                if _feed_raw and _feed_raw not in ("—", "-"):
                    out["transmission_feed"] = _feed_raw
            td = _vp_tx
            out["transmission_source"] = "NHTSA vPIC"

    out["transmission_display"] = td
    out["drivetrain_display"] = dd

    fe = vs.get("fuel_economy_display")
    if not fe or (isinstance(fe, str) and fe.strip() in ("", "—", "-")):
        fe = _format_mpg_city_highway(c.get("mpg_city"), c.get("mpg_highway"))
    out["fuel_economy_display"] = format_display_value(fe) if fe else DISPLAY_DASH

    # Resolved once, read by both the spec list and the TCO block below.
    from backend.intelligence.ev_range_estimates import resolve_factory_epa_range
    from backend.intelligence.tco_fuel_estimates import resolve_fuel_tank_gallons

    _tank_gal_raw = resolve_fuel_tank_gallons(c)
    _tank_gal = round(_tank_gal_raw, 1) if _tank_gal_raw is not None else None
    _factory_range = resolve_factory_epa_range(c)

    # Extended specs. ``merge_verified_specs`` now admits horsepower / torque /
    # 0-60 only when a page whose title names THIS car's model year printed that
    # exact number (``knowledge_engine_specs.sourced_extended_specs``), so these
    # three are a pass-through of an already-gated value. Note the gate is at the
    # merge, not here, because the AI chat agent reads the merge output directly
    # and never calls this serializer.
    out["horsepower"] = vs.get("horsepower")
    out["torque_lb_ft"] = vs.get("torque_lb_ft")
    out["zero_to_60_sec"] = vs.get("zero_to_60_sec")
    # Fill horsepower behind the suppression, from THIS VIN's own vPIC decode.
    #
    # The gate above blanks hp whenever a (year, make, model) family scraped to
    # one value, and `_extended_family_suspicious` says outright that nothing
    # fills in behind it. That is why cars render with no horsepower: the wrong
    # number is correctly withheld and no right one exists behind it. vPIC is the
    # manufacturer's filing for the individual VIN, so it cannot carry
    # model-level contamination by construction, and 23,089 active listings
    # already have one cached locally. Only ever used when the gated value is
    # absent — a real per-trim figure that survived the gate still wins.
    #
    # Every hp figure carries its source (``horsepower_source``): a gated value
    # came from a page naming this model year ("trim page"), the fill-behind
    # from the VIN decode ("NHTSA vPIC"). On a hybrid the vPIC ``EngineHP`` is
    # the combustion engine alone, so the number stays (it is true to the
    # filing) with ``horsepower_note`` saying what it is not: 145 hp on a CR-V
    # Hybrid whose system makes 204 must never read as the car's output.
    out["horsepower_source"] = "trim page" if out["horsepower"] is not None else None
    out["horsepower_note"] = None
    if out["horsepower"] is None:
        from backend.utils.vpic_specs import hybrid_text, vpic_is_hybrid, vpic_specs_for_vin

        _vp = vpic_specs_for_vin(c.get("vin"))
        _vp_hp = _vp.get("horsepower")
        if _vp_hp is not None:
            out["horsepower"] = _vp_hp
            out["horsepower_source"] = "NHTSA vPIC"
            _is_hybrid = (
                vpic_is_hybrid(_vp)
                or hybrid_text(vs.get("vpic_electrification"))
                or hybrid_text(out.get("fuel_type"))
                or hybrid_text(engine_disp)
            )
            if _is_hybrid:
                out["horsepower_note"] = "engine only; hybrid system output not filed"
    # NOT filled from vPIC: 0-60 (vPIC does not carry it, and estimating it from
    # power-to-weight would need the very curb-weight figures suppressed below)
    # and curb weight (present on 0.2% of decodes).
    # curb_weight_lb is suppressed here as well as at the merge, so a caller that
    # hands in its own ``verified_specs`` dict cannot reintroduce it. Measured
    # 2026-08-02 over the 49,912 epa_extended_specs rows: 23,803 carry a curb
    # weight, 2,134 of them under 2,500 lb and 2,042 under 2,100 — lighter than
    # the lightest car sold in the US (2,095 lb Mirage). Every Mazda CX-50 row
    # from 2023 to 2026 stores 2,000 lb for BOTH curb weight and tow capacity;
    # 2,000 is the tow rating and the car weighs about 3,700. The page extraction
    # repeats the same swapped figure, so quoting it proves nothing.
    out["curb_weight_lb"] = None
    # battery_kwh is suppressed, not passed through. epa_extended_specs carries it on 427
    # rows across 12 nameplates, and ALL TWELVE stamp a single value on every trim and
    # model year -- the same model-level contamination as curb weight and 0-60. BMW
    # 7 Series is 14.4 kWh on all 211 rows (the 750e plug-in pack, shown on gas cars);
    # Tesla Model S is 100.0 on all 59, so a 2016 75D reads as a 100 kWh car; Lucid Air
    # 88.0 on 41; i4 70.2 on 33. Pack size is precisely what varies BETWEEN trims, so a
    # nameplate-wide value is never right except by accident, and a shopper reads it as a
    # range/charging-cost proxy. Restore this only from a per-trim source.
    out["battery_kwh"] = None
    # tow_capacity_lb is suppressed for a stronger reason than any of the above:
    # there is no gate that could admit it. 9,864 epa_extended_specs rows carry a
    # towing figure, spread across 2,037 distinct (year, make, model) groups, and
    # the number of those groups holding more than one distinct value is ZERO
    # (counted 2026-08-02). One tow rating per nameplate-year, stamped on every
    # trim — it cannot tell a Tradesman from a Limited, so it describes neither.
    out["tow_capacity_lb"] = None

    # Tank and EV range do NOT come from ``vs``. They are the two extended-spec
    # numbers a shopper reads as a running cost — the tank is multiplied by a
    # live fuel price for the cost of a fill-up, the range anchors the battery
    # health readout — so both go through the resolvers that admit only a
    # figure traceable to a document, and return None otherwise.
    #
    # ``vs`` cannot be used for either. ``merge_verified_specs`` builds it with
    # ``lookup_epa_extended_specs``, which ends in ``_merge_ai_model_specs``, so
    # its ``fuel_tank_gal`` may have come out of the AI ``ai_model_specs`` table;
    # and even when it comes from ``epa_extended_specs`` the column is 99.9%
    # body-class and per-model defaults rather than anything the scrape read.
    # Its ``ev_range_miles`` is a total driving range mislabelled as an
    # all-electric one. Both are documented in ``backend/intelligence``.
    #
    # These two keys used to be assigned here and are the second, older shopper
    # path — ``car.fuel_tank_gal`` / ``car.ev_range_miles`` in the car.html spec
    # list, separate from the ``fuel_tank_gallons`` / ``factory_range`` keys the
    # TCO block reads. Same fact, so they are now the same number.
    out["fuel_tank_gal"] = _tank_gal
    out["ev_range_miles"] = _factory_range

    # Model generation (backend.catalog model_generations — read-time join)
    out["generation_code"] = vs.get("generation_code")
    out["generation_years"] = vs.get("generation_years")

    # EV range / battery are ONLY real for battery-electric and plug-in hybrids. The
    # model-level spec match can pull an EV trim's row onto a gas car of the same
    # nameplate (e.g. gas Kona matching Kona Electric), so gate on the car's own fuel
    # type and drop these fields for anything that can't be plugged in.
    _ft = str(out.get("fuel_type") or c.get("fuel_type") or "").strip().lower()
    _ev_capable = ("plug-in" in _ft) or ("plug in" in _ft) or _ft in ("electric", "ev") or (
        "electric" in _ft and "gas" not in _ft and "hybrid" not in _ft
    )
    if not _ev_capable:
        out["ev_range_miles"] = None
        out["battery_kwh"] = None

    # THE ``ai_engine_specs`` OVERRIDE IS GONE (2026-08-02). It used to run here
    # and let the engine-matched row win OUTRIGHT on horsepower, torque, tow
    # capacity and 0-60, and fill curb weight when the model-level value was
    # missing. Every one of that table's 888 rows carries
    # ``source_host='ai-engine-research'`` — one distinct value, counted against
    # the live database — so the override could only ever replace a scraped
    # number with a generated one. ``fuel_tank_gal`` had already been removed
    # from it on 2026-07-31 for the same reason; the other five fields had not,
    # which is why fixing that one field did not close the class.
    #
    # ``lookup_engine_specs`` now has no production caller.
    if include_extended_display:
        # Plausibility guard on the assembled numbers, and the curated cohort
        # 0-60, which wins over any stored one — it is only set where the stored
        # value provably belongs to a different trim.
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

    # Factory catalog packages / standalone options (catalog_trims/_options/_packages),
    # matched on this row's own year/make/model/trim — independent of dealer window
    # sticker data above. Most trims have no catalog rows; None when no match.
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

    bsd = vs.get("body_style_display")
    if bsd and (is_effectively_empty(c.get("body_style")) or out.get("body_style") == DISPLAY_DASH):
        out["body_style"] = format_display_value(bsd)

    if out.get("body_style") == DISPLAY_DASH or is_effectively_empty(out.get("body_style")):
        try:
            from backend.enrichment.knowledge_engine import decode_trim_logic

            _hints = decode_trim_logic(c.get("make"), c.get("model"), c.get("trim"), c.get("title"))
            _bh = _hints.get("body_style_hint")
            if _bh:
                out["body_style"] = format_display_value(_bh)
        except Exception:
            pass

    from backend.utils.field_clean import normalize_body_style_for_car

    _bs_raw = out.get("body_style")
    if _bs_raw and _bs_raw != DISPLAY_DASH:
        _bs_corrected = normalize_body_style_for_car(
            str(_bs_raw),
            make=c.get("make"),
            model=c.get("model"),
            trim=c.get("trim"),
            title=c.get("title"),
        )
        if _bs_corrected:
            out["body_style"] = format_display_value(_bs_corrected)

    fill_derived_condition_for_display(c, out)

    from backend.utils.interior_color_buckets import infer_paint_color_buckets, parse_stored_buckets

    out["exterior_color_families"] = infer_paint_color_buckets(c.get("exterior_color"), c.get("make"))
    _ib = parse_stored_buckets(car.get("interior_color_buckets"))
    out["interior_color_families"] = (
        _ib if _ib else infer_paint_color_buckets(c.get("interior_color"), c.get("make"))
    )

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

    out["created_at"] = c.get("first_seen_at") or c.get("scraped_at")
    out["price_history"] = _price_history_for_vdp(c)
    out["state"] = resolve_car_state_code(c)
    out["fuel_requirement"] = resolve_car_fuel_requirement(
        c, engine_display=engine_disp, verified_specs=vs if vs else None
    )
    from backend.intelligence.tco_fuel_estimates import (
        resolve_tco_avg_mpg,
        resolve_tco_ev_efficiency,
    )

    out["tco_avg_mpg"] = resolve_tco_avg_mpg(c)
    out["tco_ev_efficiency"] = resolve_tco_ev_efficiency(c)
    # Both resolved above, already None when no traceable figure exists (the
    # rounding is done there, so ``round(None, 1)`` cannot be reached). The spec
    # list and this block must never show different numbers for the same tank.
    out["factory_range"] = _factory_range
    out["fuel_tank_gallons"] = _tank_gal

    # msrp is NOT passed through from the column. ``cars.msrp`` is written
    # verbatim from the dealer feed, and on used inventory the feeds put a
    # marketing "was" price in it: of the 18,350 active listings carrying one,
    # 9,621 are EXACTLY the asking price and 2,439 are below it. Shown as an
    # MSRP those become a fabricated anchor, and the page then subtracts them
    # from the price and calls the remainder a saving. Same shape as the
    # ``battery_kwh`` suppression above — the write path keeps what the feed
    # says, the display layer decides what is credible. ``resolve_display_msrp``
    # admits a figure only from a window sticker we hold for this VIN or from a
    # new, non-CPO listing, and only above the asking price; see
    # ``backend/utils/msrp_trust.py`` for the counts and the rule.
    #
    # ``allow_sticker`` follows ``include_extended_display`` so the bulk callers
    # that never render a price block (listing-completeness gap detection) don't
    # pay for a per-VIN filesystem read and a pdftotext.
    from backend.utils.msrp_trust import resolve_display_msrp

    _msrp = resolve_display_msrp(c, allow_sticker=include_extended_display)
    out["msrp"] = _msrp["msrp"]
    out["msrp_label"] = _msrp["label"]
    out["msrp_source"] = _msrp["source"]
    out["msrp_from_sticker"] = _msrp["from_sticker"]
    out["below_msrp"] = _msrp["savings"]
    # See serialize_car_for_listings_grid: the hero prints the figure as an
    # advertised payment and shows no MSRP delta or finance estimate against it.
    from backend.utils.market_price import is_payment_shaped_price  # lazy: market_price imports the DB layer

    out["payment_listed"] = is_payment_shaped_price(out.get("price"), year=c.get("year"))
    if out["payment_listed"]:
        out["msrp"] = None
        out["below_msrp"] = None

    # Coarse market deal score (free consumer hook). Scored offline against the
    # in-process market_price_stats cache — no per-car DB round-trip. The detailed
    # band breakdown is gated behind FEATURE_MARKET_INTEL in the route/template.
    try:
        from backend.intelligence.deal_score_cache import public_deal_score

        out["deal_score"] = public_deal_score(c)
    except Exception:
        out["deal_score"] = None

    return out


def _price_history_for_vdp(car: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Price history for the VDP: ``[{date, price}, ...]`` oldest first.

    Reads operator ``price_provenance_json`` when it stores sweep history; never
    exposes the raw provenance blob on the public car payload.

    Returns a Python list (the template applies ``| tojson`` exactly once).
    The scanner appends a point every time a scan sees a different price than
    the stored row, so two sources that disagree (a VDP price vs an SRP
    payment-shaped price, say) leave an alternating trail with several points
    on one day.  Those are collapsed here: one point per UTC day (the last
    observation wins), then consecutive equal prices are dropped, so what is
    left is only the days on which the listed price actually changed.
    """
    events: list[dict[str, Any]] = []
    raw = car.get("price_provenance_json")
    if raw and str(raw).strip():
        try:
            parsed = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError, ValueError):
            parsed = None
        if isinstance(parsed, list):
            source = parsed
        elif isinstance(parsed, dict):
            source = parsed.get("history") or parsed.get("sweeps") or parsed.get("price_history") or []
        else:
            source = []
        if isinstance(source, list):
            for item in source:
                if not isinstance(item, dict):
                    continue
                when = item.get("date") or item.get("recorded_at") or item.get("scraped_at")
                amt = item.get("price")
                if when is None or amt is None:
                    continue
                try:
                    events.append({"date": str(when), "price": float(amt)})
                except (TypeError, ValueError):
                    continue
    return _dedupe_price_history(events)


def _dedupe_price_history(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the last point per UTC day, then drop consecutive equal prices."""
    if not events:
        return []
    by_day: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for evt in events:  # scanner appends chronologically; the last write per day wins
        day = str(evt["date"])[:10]
        if day not in by_day:
            order.append(day)
        by_day[day] = evt
    out: list[dict[str, Any]] = []
    for day in order:
        evt = by_day[day]
        if out and out[-1]["price"] == evt["price"]:
            continue
        out.append(evt)
    return out


def _price_history_json_for_vdp(car: dict[str, Any]) -> str:
    """JSON string form of :func:`_price_history_for_vdp` (kept for older callers)."""
    return json.dumps(_price_history_for_vdp(car))


_PRICE_DROP_RECENT_DAYS = max(1, int(os.environ.get("PRICE_DROP_RECENT_DAYS", "14")))


def _latest_price_drop_for_grid(car: dict[str, Any]) -> tuple[float | None, int | None]:
    """
    ``(amount, days_ago)`` for the most recent price drop within
    :data:`_PRICE_DROP_RECENT_DAYS`, else ``(None, None)``.

    Cheap grid-card signal — compares only the last two ``price_provenance_json``
    snapshots (scanner appends chronologically, so ``[-1]`` is newest). Does not
    parse/expose the full history array; see ``_price_history_json_for_vdp`` for that.
    """
    raw = car.get("price_provenance_json")
    if not raw or not str(raw).strip():
        return None, None
    try:
        history = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError, ValueError):
        return None, None
    if not isinstance(history, list) or len(history) < 2:
        return None, None
    prev, latest = history[-2], history[-1]
    if not isinstance(prev, dict) or not isinstance(latest, dict):
        return None, None
    try:
        prev_price = float(prev.get("price"))
        latest_price = float(latest.get("price"))
    except (TypeError, ValueError):
        return None, None
    if latest_price >= prev_price:
        return None, None
    when = latest.get("date")
    if not when:
        return None, None
    import re as _re
    from datetime import datetime, timezone

    try:
        when_dt = datetime.fromisoformat(_re.sub(r"Z$", "+00:00", str(when)))
        if when_dt.tzinfo is None:
            when_dt = when_dt.replace(tzinfo=timezone.utc)
        days_ago = (datetime.now(timezone.utc) - when_dt).days
    except (TypeError, ValueError):
        return None, None
    if days_ago < 0 or days_ago > _PRICE_DROP_RECENT_DAYS:
        return None, None
    return round(prev_price - latest_price, 2), days_ago


_LISTINGS_GRID_GALLERY_MAX = max(1, int(os.environ.get("LISTINGS_GRID_GALLERY_MAX", "4")))


def _package_names_from_raw(packages_raw: Any) -> list[str]:
    names: list[str] = []
    if is_effectively_empty(packages_raw) or str(packages_raw).strip() in ("{}", "[]"):
        return names
    try:
        pkg = json.loads(packages_raw) if isinstance(packages_raw, str) else packages_raw
    except Exception:
        return names
    if not isinstance(pkg, dict):
        return names
    for entry in pkg.get("packages_normalized") or []:
        if isinstance(entry, dict):
            n = (entry.get("canonical_name") or entry.get("name") or "").strip()
            if n:
                names.append(n)
    for n in pkg.get("possible_packages") or []:
        if isinstance(n, str) and n.strip():
            names.append(n.strip())
    return names


def _listings_grid_gallery(raw: Any) -> list[str]:
    if isinstance(raw, list):
        parsed = raw
    elif isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except Exception:
            return []
        if not isinstance(parsed, list):
            return []
    else:
        return []
    out = filter_spyne_gallery_variants(parsed)
    safe = [u for u in out if normalize_listing_image_url(u)]
    return safe[:_LISTINGS_GRID_GALLERY_MAX] if safe else []


def _parse_gallery_url_list(raw: Any) -> list[str]:
    if isinstance(raw, list):
        parsed = raw
    elif isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except Exception:
            return []
        if not isinstance(parsed, list):
            return []
    else:
        return []
    return [u.strip() for u in parsed if isinstance(u, str) and u.strip()]


# Photo counting is by far the most expensive part of a listings-grid rebuild:
# ~1.8M per-URL heuristic evaluations across the fleet, ~22s of the ~29s build.
# The gallery blob for a given car almost never changes between rebuilds (which
# fire on a 60s cache token), so the same inputs are re-scored over and over
# while holding the GIL — starving every concurrent request, which is what made
# a car page take 30s+.  Memoize on a digest of the exact inputs: the counting
# rule is a pure function of (gallery blob, image_url), so this cannot change a
# single displayed value.  Digest keys (not the raw JSON) keep the table small.
_PHOTO_COUNT_MEMO: dict[bytes, int] = {}
# One entry per active listing plus churn headroom; each entry is ~100 bytes.
_PHOTO_COUNT_MEMO_MAX = 200_000


def _photo_count_memo_key(raw: Any, image_url: Any) -> bytes | None:
    """16-byte digest of the counting inputs, or ``None`` when they aren't hashable text."""
    if raw is None or isinstance(raw, str):
        raw_part = raw or ""
    elif isinstance(raw, (list, tuple)):
        try:
            raw_part = json.dumps(list(raw), separators=(",", ":"))
        except (TypeError, ValueError):
            return None
    else:
        return None
    import hashlib

    h = hashlib.blake2b(digest_size=16)
    h.update(raw_part.encode("utf-8", "replace"))
    h.update(b"\x00")
    h.update(str(image_url or "").encode("utf-8", "replace"))
    return h.digest()


def _count_public_gallery_urls(urls: list[str]) -> int:
    """
    ``len(filter_public_gallery_urls(urls))`` without materialising or sorting.

    The filter keeps the first occurrence of each raw URL that survives the
    fluff heuristic, then collapses the survivors by
    :func:`prefer_full_gallery_url` (a thumbnail and its full-size twin count
    once).  Its ``kept.sort(...)`` only decides which twin is *returned* — the
    number of distinct upgraded URLs does not depend on the order — so a count
    can skip the sort and the two list builds entirely.  Measured over 20,000
    live active rows with the memo emptied: photo counting 6.30s -> 4.41s
    (fleet-scale 22.6s -> 15.8s), whole-grid serialization 7.84s -> 6.24s.
    """
    if not urls:
        return 0
    from backend.vision.url_heuristics import (
        heuristic_listing_gallery_fluff_url,
        prefer_full_gallery_url,
    )

    seen: set[str] = set()
    seen_full: set[str] = set()
    for u in urls:
        if not u or not isinstance(u, str):
            continue
        s = u.strip()
        if not s or s in seen:
            continue
        if heuristic_listing_gallery_fluff_url(s):
            continue
        seen.add(s)
        full = prefer_full_gallery_url(s)
        if full:
            seen_full.add(full)
    return len(seen_full)


def _public_gallery_photo_count(raw: Any, *, image_url: Any = None) -> int:
    """Count public listing photos (same rules as car detail gallery)."""
    key = _photo_count_memo_key(raw, image_url)
    if key is not None:
        hit = _PHOTO_COUNT_MEMO.get(key)
        if hit is not None:
            return hit

    from backend.vision.url_heuristics import heuristic_listing_gallery_fluff_url

    n = _count_public_gallery_urls(_parse_gallery_url_list(raw))
    if not n and image_url:
        iu = str(image_url).strip()
        if iu and not heuristic_listing_gallery_fluff_url(iu):
            n = 1

    if key is not None:
        # Fleet-sized bound: drop the whole table rather than track LRU order —
        # a rebuild refills it in one pass and correctness never depends on it.
        if len(_PHOTO_COUNT_MEMO) >= _PHOTO_COUNT_MEMO_MAX:
            _PHOTO_COUNT_MEMO.clear()
        _PHOTO_COUNT_MEMO[key] = n
    return n


def serialize_car_for_listings_grid(
    car: dict[str, Any],
    *,
    attribution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Compact listings grid JSON: filter/cascade fields + card display only.

    Skips full ``serialize_car_for_api`` (EPA merge, transmission heuristics, etc.)
    to keep ``GET /api/listings/cars`` fast on SQLite (~2.5k rows).

    *attribution* is this car's entry from ``cars_repo.car_attribution_states`` — the
    caller batches that read for the whole fleet, so the card never looks it up itself.
    """
    if not car:
        return {}
    c = clean_car_row_dict(dict(car))
    from backend.utils.field_clean import normalize_body_style_for_car
    from backend.utils.interior_color_buckets import infer_paint_color_buckets, parse_stored_buckets

    model_d, trim_d = apply_bmw_model_trim_display(c)
    ext_fam = infer_paint_color_buckets(c.get("exterior_color"), c.get("make"))
    _ib = parse_stored_buckets(car.get("interior_color_buckets"))
    int_fam = _ib if _ib else infer_paint_color_buckets(c.get("interior_color"), c.get("make"))

    def num(key: str) -> Any:
        return c.get(key)

    dt_stored = coerce_drivetrain_stored(c.get("drivetrain"))
    if dt_stored == "FWD" and "pickup" in str(c.get("body_style") or "").lower():
        dt_stored = None
    fuel_display = _effective_fuel_type_for_display(c, c.get("engine_description") or "") or c.get("fuel_type")

    out: dict[str, Any] = {
        "id": num("id"),
        "title": format_display_value(c.get("title")),
        "year": num("year"),
        "make": format_display_value(c.get("make")),
        "model": model_d,
        "trim": trim_d,
        "price": num("price"),
        "mileage": num("mileage"),
        "fuel_type": format_display_value(fuel_display),
        "cylinders": num("cylinders"),
        "transmission": format_display_value(c.get("transmission")),
        "drivetrain": format_display_value(dt_stored),
        "body_style": format_display_value(
            normalize_body_style_for_car(
                c.get("body_style"),
                make=c.get("make"),
                model=c.get("model"),
                trim=c.get("trim"),
                title=c.get("title"),
            )
            or c.get("body_style")
        ),
        "exterior_color": format_display_value(c.get("exterior_color")),
        "interior_color": format_display_value(c.get("interior_color")),
        "exterior_color_families": ext_fam,
        "interior_color_families": int_fam,
        "engine_l": c.get("engine_l"),
        "engine_description": c.get("engine_description"),
        "image_url": (img_url := normalize_listing_image_url(c.get("image_url"))),
        # Grid cards only need a primary image; omit duplicate gallery URLs from JSON.
        "gallery": [] if img_url else _listings_grid_gallery(c.get("gallery")),
        "photo_count": _public_gallery_photo_count(c.get("gallery"), image_url=c.get("image_url")),
        "dealer_name": format_display_value(c.get("dealer_name")),
        "dealer_url": normalize_optional_url(c.get("dealer_url")),
        "dealership_registry_id": num("dealership_registry_id"),
        "dealer_id": c.get("dealer_id"),
        "package_names": _package_names_from_raw(c.get("packages")),
        "data_quality_score": num("data_quality_score"),
    }
    out["condition"] = format_display_value(c.get("condition"))
    fill_derived_condition_for_display(c, out)
    # Raw column, matching exactly what search_cars(cpo_only=True) filters on server-side —
    # NOT derived from out["condition"], which can read "Certified" (no "Pre-Owned") for some
    # rows even when is_cpo=1, which would disagree with the SQL-side filter.
    out["is_cpo"] = c.get("is_cpo") in (1, True, "1")
    # Whether this listing has a vehicle-history (Carfax/AutoCheck) report linked.
    # We only know a report EXISTS (the URL) — not its contents (owners/accidents),
    # which aren't parsed — so a card chip should say "Carfax", not "1-owner".
    _carfax = c.get("carfax_url")
    out["has_carfax"] = bool(_carfax and str(_carfax).strip())
    price_drop_amount, price_drop_days_ago = _latest_price_drop_for_grid(c)
    out["price_drop_amount"] = price_drop_amount
    out["price_drop_days_ago"] = price_drop_days_ago
    # An advertised monthly payment scraped into the price field ($85, $122)
    # is not a sale price: the card says so, the sorter sinks it with
    # call-for-price, and the price cap filter ignores the figure
    # (visual review 2026-09-28, IH-01 / ES-2). The figure is kept so the
    # card can print "$122/mo advertised" instead of pretending it is unknown.
    from backend.utils.market_price import is_payment_shaped_price  # lazy: market_price imports the DB layer

    out["payment_listed"] = is_payment_shaped_price(out.get("price"), year=c.get("year"))
    # Same cohort-relative guard as serialize_car_for_api (see
    # backend/utils/price_plausibility.py) — in-memory cache, no per-card DB hit.
    if out.get("price") is not None and not out["payment_listed"]:
        try:
            from backend.utils.price_plausibility import implausible_price

            if implausible_price(c, float(out["price"])):
                out["price"] = None
        except (TypeError, ValueError):
            pass
    # Coarse deal score for grid cards (in-memory band cache; no per-card DB hit).
    try:
        from backend.intelligence.deal_score_cache import public_deal_score

        out["deal_score"] = public_deal_score(c)
    except Exception:
        out["deal_score"] = None
    # Softens the dealer line when this listing's own photos (or a proven group feed)
    # contradict the filed rooftop. Adds nothing to the card otherwise.
    from backend.utils.car_serialize.attribution import attribution_public_fields

    out.update(attribution_public_fields(attribution))
    return out


def _format_mileage_mi(mileage: Any) -> str:
    if mileage is None or str(mileage).strip() == "":
        return "—"
    try:
        return f"{int(mileage):,} mi"
    except (TypeError, ValueError):
        return "—"


def build_detail_display_snapshot(
    verified_specs: dict[str, Any],
    ser: dict[str, Any],
) -> dict[str, Any]:
    """
    Text the car detail template would show for key spec rows (for /dev/api/car-debug).
    *ser* must be from serialize_car_for_api(..., verified_specs=verified_specs).
    """
    # Raw cylinder count for dev/debug; user-facing copy is folded into ``engine_display``.
    cc = ser.get("cylinders")
    if cc is None or str(cc).strip() == "":
        cyl_render = "—"
    else:
        cyl_render = str(cc)
    return {
        "year": ser.get("year"),
        "make": ser.get("make"),
        "model": ser.get("model"),
        "trim": ser.get("trim"),
        "mileage_mi": _format_mileage_mi(ser.get("mileage")),
        "engine": ser.get("engine_display"),
        "transmission": ser.get("transmission_display"),
        "drivetrain": ser.get("drivetrain_display"),
        "body_style": ser.get("body_style"),
        "fuel_type": ser.get("fuel_type"),
        "condition": ser.get("condition"),
        "cylinders": cyl_render,
        "efficiency": ser.get("fuel_economy_display"),
        "exterior_color": ser.get("exterior_color"),
        "interior_color": ser.get("interior_color"),
        "vin": ser.get("vin"),
        "stock_number": ser.get("stock_number"),
    }
