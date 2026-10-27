"""P8A.2: write-time fact precedence (pure). Table-driven per rule."""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

from backend.vehicle_facts import precedence as P
from backend.vehicle_facts.precedence import decide, field_source, plan_updates, provenance_patch


def prov(field, source, value):
    return {field: {"source": source, "value": value, "fetched_at": "2026-10-01T00:00:00+00:00"}}


# (field, stored, stored_source, candidate, cand_source, ctx, expect_write)
RANK_PAIRS = [
    # drivetrain
    ("drivetrain", "FWD", "dealer_feed", "AWD", "nhtsa_vpic", {}, True),
    ("drivetrain", "AWD", "nhtsa_vpic", "FWD", "dealer_feed", {"vpic_drive": "AWD"}, False),
    ("drivetrain", "RWD", "epa_catalog_tiebreak", "2WD", "dealer_feed", {}, False),
    ("drivetrain", "FWD", "dealer_feed", "4x2", "dealer_feed", {}, False),
    ("drivetrain", "FWD", "dealer_feed", "Other", "dealer_feed", {}, False),
    ("drivetrain", None, None, "RWD", "model_specs", {}, True),
    ("drivetrain", "FWD", "dealer_feed", "RWD", "model_specs", {}, False),
    ("drivetrain", "FWD", "dealer_feed", "RWD", "llm", {}, False),
    # engine text: dealer beats vPIC synthesized, catalog never overwrites
    ("engine_description", "3.5L V6 (vPIC)", "nhtsa_vpic", "3.5L V6 24V", "dealer_feed", {}, True),
    ("engine_description", "3.5L V6 24V", "dealer_feed", "3.5L V6 (vPIC)", "nhtsa_vpic", {}, False),
    ("engine_description", "2.0L I4 Turbo", "dealer_feed", "2.5L I4", "epa_catalog", {}, False),
    ("engine_description", "2.0L I4 Turbo", "dealer_feed", "2.5L I4", "epa_catalog", {"corroborated": True}, True),
    ("engine_description", None, None, "2.5L I4", "epa_catalog", {}, True),
    # transmission: sticker > dealer > vPIC fill > catalog fill
    ("transmission", "8-Speed Automatic", "dealer_feed", "Automatic", "nhtsa_vpic", {}, False),
    ("transmission", "Automatic", "nhtsa_vpic", "8-Speed Automatic", "dealer_feed", {}, True),
    ("transmission", "8-Speed Automatic", "dealer_feed", "10-Speed Automatic", "oem_window_sticker", {}, True),
    ("transmission", "Automatic", "nhtsa_vpic", "CVT", "epa_catalog", {}, False),
    # body fill-only
    ("body_type", "SUV", "dealer_feed", "Sedan", "oem_window_sticker", {}, False),
    ("body_type", None, None, "SUV", "nhtsa_vpic", {}, True),
    # colors: catalog and vPIC never write
    ("exterior_color", "Red", "dealer_feed", "Blue", "oem_window_sticker", {}, True),
    ("exterior_color", None, None, "Blue", "epa_catalog", {}, False),
    ("exterior_color", None, None, "Blue", "nhtsa_vpic", {}, False),
    # unverified sticker fills only
    ("exterior_color", "Red", "dealer_feed", "Blue", "oem_window_sticker_unverified", {}, False),
    ("exterior_color", None, None, "Blue", "oem_window_sticker_unverified", {}, True),
    # web_search ranks below catalog
    ("fuel_type", "Gasoline", "epa_catalog", "Diesel", "web_search", {}, False),
]


@pytest.mark.parametrize("field,stored,ssrc,cand,csrc,ctx,expect", RANK_PAIRS)
def test_rank_pairs(field, stored, ssrc, cand, csrc, ctx, expect):
    pv = prov(field, ssrc, stored) if ssrc else {}
    assert decide(field, stored, pv, cand, csrc, ctx).write is expect


def test_rank_tables_cover_sources_and_never_write():
    assert P.rank_of("exterior_color", "epa_catalog") is None
    assert P.rank_of("drivetrain", "nhtsa_vpic") > P.rank_of("drivetrain", "dealer_feed")
    assert P.rank_of("drivetrain", "dealer_feed") > P.rank_of("drivetrain", "epa_catalog")
    assert P.rank_of("fuel_type", "epa_catalog") > P.rank_of("fuel_type", "web_search")
    assert P.rank_of("engine_description", "dealer_feed") > P.rank_of("engine_description", "nhtsa_vpic")
    for f in P.FIELDS:
        assert set(P.RANKS[f]) <= set(P.SOURCES)
        assert "llm" not in P.RANKS[f]


def test_legacy_labels():
    assert P.canonical_source("inventory_repair") == "unknown"
    assert P.canonical_source("listing_gap_fill") == "unknown"
    assert P.canonical_source("nhtsa_vpic_heal") == "nhtsa_vpic"
    assert P.canonical_source("ev_cylinders_heal") == "nhtsa_vpic"
    assert P.canonical_source("epa_master") == "epa_catalog"
    assert P.canonical_source("bogus") == "unknown"


# --- drivetrain --------------------------------------------------------------
def test_vpic_4x2_never_maps_to_awd_or_end():
    for stored in (None, "FWD", "RWD"):
        d = decide("drivetrain", stored, {}, "4x2", "nhtsa_vpic")
        if stored is None:
            assert d.write and d.value == "2WD"
        else:
            assert not d.write  # the stored end already satisfies the constraint
    d = decide("drivetrain", "AWD", prov("drivetrain", "dealer_feed", "AWD"), "4x2", "nhtsa_vpic")
    assert d.write and d.value == "2WD" and d.cannot_assess
    for v in ("4x2", "2WD", "2-Wheel Drive"):
        d = decide("drivetrain", None, {}, v, "nhtsa_vpic")
        assert d.value not in ("AWD", "4WD", "RWD", "FWD")


def test_vpic_4x2_vetoes_feed_all_wheel():
    d = decide("drivetrain", "FWD", {}, "AWD", "dealer_feed", {"vpic_drive": "4x2"})
    assert not d.write and "vPIC" in d.reason


def test_vpic_awd_equals_4wd_no_churn():
    assert not decide("drivetrain", "4WD", {}, "AWD", "nhtsa_vpic").write
    assert not decide("drivetrain", "AWD", {}, "4WD", "dealer_feed").write


def test_vpic_overrides_dealer_end():
    d = decide("drivetrain", "RWD", prov("drivetrain", "dealer_feed", "RWD"), "FWD", "nhtsa_vpic")
    assert d.write and d.value == "FWD"


def test_feed_contradicting_vpic_end_loses_and_two_signals_cannot_assess():
    ctx = {"vpic_drive": "FWD"}
    d = decide("drivetrain", "AWD", {}, "RWD", "dealer_feed", ctx)
    assert not d.write and d.cannot_assess  # stored AWD and feed RWD both contradict (D-PR6)
    d = decide("drivetrain", "FWD", {}, "RWD", "dealer_feed", ctx)
    assert not d.write and not d.cannot_assess and d.discrepancy["value"] == "RWD"


def test_tiebreak_rwd_survives_rescan_saying_2wd():
    pv = prov("drivetrain", "epa_catalog_tiebreak", "RWD")
    assert not decide("drivetrain", "RWD", pv, "2WD", "dealer_feed").write
    assert not decide("drivetrain", "RWD", pv, "4x2", "dealer_feed").write


def test_tiebreak_scope_d_pr4():
    # fills 2WD / blank
    assert decide("drivetrain", "2WD", {}, "RWD", "epa_catalog_tiebreak").value == "RWD"
    assert decide("drivetrain", None, {}, "FWD", "epa_catalog_tiebreak").write
    # 4MATIC / xDrive trim veto
    assert not decide("drivetrain", "2WD", {}, "RWD", "epa_catalog_tiebreak", {"trim_drive": "AWD"}).write
    # vPIC decided: tiebreak silent
    assert not decide("drivetrain", "2WD", {}, "RWD", "epa_catalog_tiebreak", {"vpic_drive": "FWD"}).write
    # stored dealer end only loses when the trim decoder agrees
    assert not decide("drivetrain", "FWD", {}, "RWD", "epa_catalog_tiebreak", {"vpic_drive": "4x2"}).write
    assert decide("drivetrain", "FWD", {}, "RWD", "epa_catalog_tiebreak",
                  {"vpic_drive": "4x2", "trim_drive": "RWD"}).write
    # vPIC 4x2 forbids a catalog AWD
    assert not decide("drivetrain", "2WD", {}, "AWD", "epa_catalog_tiebreak", {"vpic_drive": "4x2"}).write


# --- fuel --------------------------------------------------------------------
@pytest.mark.parametrize("stored,cand,expect", [
    ("Gasoline", "Plug-in Hybrid", True),
    ("Hybrid", "Plug-in Hybrid", True),
    ("Gasoline", "Electric", True),
    ("Gasoline", "Diesel", True),
    ("Gasoline", "Mild Hybrid", False),
    ("Gasoline", "Gasoline", False),
    ("Plug-in Hybrid", "Gasoline", False),   # vPIC gasoline is not decisive between ICE and HEV
])
def test_vpic_fuel_override(stored, cand, expect):
    assert decide("fuel_type", stored, {}, cand, "nhtsa_vpic").write is expect


def test_sticker_hybrid_never_downgrades_plug_in():
    assert not decide("fuel_type", "Plug-in Hybrid", prov("fuel_type", "nhtsa_vpic", "Plug-in Hybrid"),
                      "Hybrid", "oem_window_sticker").write
    assert not decide("fuel_type", "Electric", {}, "Hybrid", "oem_window_sticker").write


def test_dealer_electric_vetoed_by_gasoline_decode():
    vpic = {"fuel_type_primary": "Gasoline"}
    d = decide("fuel_type", "Gasoline", {}, "Electric", "dealer_feed", {"vpic": vpic})
    assert not d.write


# --- cylinders ---------------------------------------------------------------
def test_vpic_cylinders_only_when_not_contradicting_engine_text():
    ctx = {"engine_description": "3.5L V6", "fuel_type": "Gasoline"}
    assert not decide("cylinders", 6, {}, 8, "nhtsa_vpic", ctx).write
    assert decide("cylinders", None, {}, 6, "nhtsa_vpic", ctx).write
    assert decide("cylinders", 4, prov("cylinders", "dealer_feed", 4), 6, "nhtsa_vpic",
                  {"engine_description": "3.5L engine"}).write
    assert not decide("cylinders", None, {}, 6, "epa_catalog", {"fuel_type": "Electric"}).write


# --- stale provenance --------------------------------------------------------
def test_stale_entry_lets_feed_win():
    pv = prov("drivetrain", "nhtsa_vpic", "Other")  # F10: junk stored under vPIC
    assert field_source("drivetrain", "Other", pv) == "unknown"
    stale = prov("engine_description", "oem_window_sticker", "2.0L I4")
    assert field_source("engine_description", "2.5L I4", stale) == "unknown"
    assert decide("engine_description", "2.5L I4", stale, "2.0L I4 Turbo", "dealer_feed").write
    live = prov("engine_description", "oem_window_sticker", "2.5L I4")
    assert field_source("engine_description", "2.5L I4", live) == "oem_window_sticker"
    assert not decide("engine_description", "2.5L I4", live, "2.0L I4 Turbo", "dealer_feed").write


def test_entry_without_value_or_bare_label_is_unknown():
    assert field_source("drivetrain", "FWD", {"drivetrain": {"source": "nhtsa_vpic"}}) == "unknown"
    assert field_source("drivetrain", "FWD", {"drivetrain": "nhtsa_vpic"}) == "unknown"
    assert field_source("drivetrain", "FWD", '{"drivetrain": {"source": "dealer_feed", "value": "FWD"}}') \
        == "dealer_feed"
    assert field_source("drivetrain", "FWD", "not json") == "unknown"


def test_awd_and_4wd_provenance_compare_equal():
    assert field_source("drivetrain", "AWD", prov("drivetrain", "nhtsa_vpic", "4WD")) == "nhtsa_vpic"


# --- patches, plans, idempotence --------------------------------------------
def test_provenance_patch_carries_value_and_fetched_at():
    d = decide("drivetrain", None, {}, "AWD", "nhtsa_vpic")
    e = provenance_patch(d)["drivetrain"]
    assert e["source"] == "nhtsa_vpic" and e["value"] == "AWD" and e["fetched_at"]


def test_discrepancy_sink():
    d = decide("drivetrain", "FWD", prov("drivetrain", "nhtsa_vpic", "FWD"), "RWD", "dealer_feed")
    assert not d.write
    disc = provenance_patch(d)["drivetrain"]["discrepancy"]
    assert disc["source"] == "dealer_feed" and disc["value"] == "RWD" and disc["seen_at"]


def test_plan_updates_idempotent():
    row = {"drivetrain": "2WD", "fuel_type": "Gasoline", "cylinders": None,
           "engine_description": "3.5L V6", "spec_source_json": None}
    props = [("drivetrain", "FWD", "nhtsa_vpic"), ("fuel_type", "Plug-in Hybrid", "nhtsa_vpic"),
             ("cylinders", 6, "nhtsa_vpic"), ("cylinders", 8, "epa_catalog")]
    changes, patch, _ = plan_updates(row, props)
    assert changes == {"drivetrain": "FWD", "fuel_type": "Plug-in Hybrid", "cylinders": 6}
    row2 = {**row, **changes, "spec_source_json": patch}
    changes2, _, decs = plan_updates(row2, props)
    assert changes2 == {} and not any(d.write for d in decs)


def test_mild_hybrid_normalization_identical_through_both_paths():
    # restated test_phev_fuel_bucket_f09: mild is never decisive, in either order
    assert not decide("fuel_type", "Gasoline", {}, "Mild Hybrid", "nhtsa_vpic").write
    assert not decide("fuel_type", "Plug-in Hybrid", {}, "Mild Hybrid", "dealer_feed").write


def test_no_db_or_network_imports():
    code = ("import sys; import backend.vehicle_facts.precedence; "
            "bad=[m for m in sys.modules if m in ('requests','backend.db','psycopg','psycopg2','sqlite3')]; "
            "print(bad)")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60,
                         cwd=Path(__file__).resolve().parents[2])
    assert out.stdout.strip() == "[]", out.stdout + out.stderr
