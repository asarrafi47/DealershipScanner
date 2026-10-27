"""gap_fill writes through fact precedence (P8B.9): page values are ``dealer_vdp``,
DuckDuckGo snippet values are ``web_search`` (below the catalog), and nothing new
carries the ``listing_gap_fill`` label.
"""
from __future__ import annotations

import json

import pytest

from backend.enrichment import window_sticker_service as wss
from backend.scanner.post_scan import gap_fill
from backend.vehicle_facts import precedence as prec

VIN = "1FTFW1E50NFA00001"


def _prov(**fields):
    return json.dumps({f: {"source": s, "value": v, "fetched_at": "2026-10-01T00:00:00+00:00"}
                       for f, (s, v) in fields.items()})


def _vpic(monkeypatch, drive=None):
    ctx = {"vpic": {"DriveType": drive}, "vpic_drive": drive} if drive else {}
    monkeypatch.setattr(wss, "_uncached_vpic_context", lambda vin: dict(ctx))


def _raw(**kw):
    r = {"id": 7, "vin": VIN, "drivetrain": None, "spec_source_json": None}
    r.update(kw)
    return r


def test_ddg_awd_on_a_4x2_vin_is_rejected(monkeypatch):
    _vpic(monkeypatch, drive="4x2")
    cols, prov = gap_fill._route_through_precedence(
        _raw(), {"drivetrain": "AWD"}, {"drivetrain": prec.WEB_SEARCH}, url=None)
    assert "drivetrain" not in cols
    entry = json.loads(prov)["drivetrain"]
    assert entry["discrepancy"]["source"] == prec.WEB_SEARCH


def test_ddg_value_fills_blank_tagged_web_search(monkeypatch):
    _vpic(monkeypatch)
    cols, prov = gap_fill._route_through_precedence(
        _raw(), {"transmission": "10-Speed Automatic"}, {"transmission": prec.WEB_SEARCH}, url="https://x/vdp")
    assert cols == {"transmission": "10-Speed Automatic"}
    entry = json.loads(prov)["transmission"]
    assert entry["source"] == "web_search" and entry["value"] == "10-Speed Automatic"
    assert "listing_gap_fill" not in {e.get("source") for e in json.loads(prov).values()}


def test_ddg_never_replaces_a_catalog_value(monkeypatch):
    _vpic(monkeypatch)
    raw = _raw(drivetrain="RWD", spec_source_json=_prov(drivetrain=(prec.EPA_CATALOG, "RWD")))
    cols, _ = gap_fill._route_through_precedence(raw, {"drivetrain": "4WD"}, {"drivetrain": prec.WEB_SEARCH}, url=None)
    assert "drivetrain" not in cols


def test_vdp_value_is_accepted_over_the_catalog(monkeypatch):
    _vpic(monkeypatch)
    raw = _raw(drivetrain="RWD", spec_source_json=_prov(drivetrain=(prec.EPA_CATALOG, "RWD")))
    cols, prov = gap_fill._route_through_precedence(raw, {"drivetrain": "4WD"}, {"drivetrain": prec.DEALER_VDP}, url="https://x/vdp")
    assert cols["drivetrain"] == "4WD"
    entry = json.loads(prov)["drivetrain"]
    assert entry["source"] == "dealer_vdp" and entry["value"] == "4WD" and entry["url"] == "https://x/vdp"


def test_non_fact_columns_keep_plain_provenance_without_the_old_label(monkeypatch):
    _vpic(monkeypatch)
    cols, prov = gap_fill._route_through_precedence(
        _raw(), {"price": 31000, "condition": "Used"}, {"price": prec.DEALER_VDP}, url=None)
    assert cols == {"price": 31000, "condition": "Used"}
    p = json.loads(prov)
    assert p["price"]["source"] == "dealer_vdp" and p["condition"]["source"] == "dealer_vdp"


def test_nothing_to_record_returns_no_provenance(monkeypatch):
    _vpic(monkeypatch)
    assert gap_fill._route_through_precedence(_raw(), {}, {}, url=None) == ({}, None)
