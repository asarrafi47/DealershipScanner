"""N21.4a (D-N31): a no-trim feed (Dealer Masters / Gardena) gets its blank trim from
vPIC, provenance ``nhtsa_vpic``, only when the decode names exactly one trim."""
from __future__ import annotations

import json

import pytest

from backend.enrichment import vpic_facts as vf
from backend.scanner.pipeline import vpic as pipeline_vpic

pg = pytest.mark.pg

V1, V2, V3, V4 = "1HGCY2F63TA066331", "3TYLB5JNXRT022195", "5J6RS4H29VL003929", "2HGFE1F94RH300001"


@pytest.mark.parametrize("trim,expected", [
    ("EX-L", "EX-L"), ("  Sport ", "Sport"), ("", None), (None, None), ("Not Applicable", None),
    ("EX / EX-L", None), ("Sport, Touring", None), ("LX or EX", None), ("Base;SE", None),
])
def test_single_vpic_trim(trim, expected):
    assert vf.single_vpic_trim({"trim": trim}) == expected


def test_second_trim_field_means_not_single():
    assert vf.single_vpic_trim({"trim": "EX"}, {"Trim2": "EX-L"}) is None
    assert vf.single_vpic_trim({"trim": "EX"}, {"Trim2": ""}) == "EX"


@pytest.fixture
def conn(pg_db, monkeypatch):
    c = pg_db.get_conn()
    decodes = {V1: {"trim": "EX-L"}, V2: {"trim": "SR5"}, V3: {"trim": "EX / EX-L"}, V4: {"trim": "LX"}}
    flat = {v: {"VIN": v, "Trim": d["trim"]} for v, d in decodes.items()}
    monkeypatch.setattr("backend.enrichment.knowledge_engine.prime_vpic_cache", lambda vins: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.clear_vpic_lookup_cache", lambda: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.lookup_vpic_from_cache",
                        lambda vin: dict(decodes.get(vin, {})))
    monkeypatch.setattr(vf, "_vpic_flat_rows", lambda _c, vins: {v: flat[v] for v in vins if v in flat})
    # V1, V3 at the no-trim dealer; V2 at a dealer whose feed carries trims; V4 at the no-trim
    # dealer but already trimmed by the feed (so that dealer is not a no-trim dealer when it has one).
    rows = [(1, V1, "gardena", None), (2, V2, "trimmed", ""), (3, V3, "gardena", ""), (4, "9ABCD2F63TA000004", "trimmed", "LX")]
    for i, vin, dealer, trim in rows:
        c.execute("INSERT INTO cars (id, vin, dealer_id, title, listing_active, trim) VALUES (?, ?, ?, 't', 1, ?)",
                  (i, vin, dealer, trim))
    c.commit()
    return c


def _trim(conn, i):
    t, raw = conn.execute("SELECT trim, spec_source_json FROM cars WHERE id=?", (i,)).fetchone()
    prov = (raw if isinstance(raw, dict) else json.loads(raw)) if raw else {}
    return t, prov.get("trim")


@pg
def test_no_trim_dealer_gets_single_valued_trim_with_provenance(conn):
    stats = vf.heal_rows(conn, trim_dealers=["gardena"])
    assert stats["trim"] == 1
    t, prov = _trim(conn, 1)
    assert t == "EX-L"
    assert prov["source"] == "nhtsa_vpic" and prov["value"] == "EX-L" and prov["previous"] is None


@pg
def test_multi_valued_decode_leaves_trim_blank(conn):
    vf.heal_rows(conn, trim_dealers=["gardena"])
    assert _trim(conn, 3) == ("", None)


@pg
def test_trim_bearing_dealer_is_not_touched_without_opt_in(conn):
    vf.heal_rows(conn)  # no trim_dealers: nothing fills
    assert _trim(conn, 1)[0] is None and _trim(conn, 2)[0] == ""
    vf.heal_rows(conn, trim_dealers=["gardena"])
    assert _trim(conn, 2)[0] == ""  # dealer 'trimmed' was not named


@pg
def test_existing_trim_is_never_overwritten(conn):
    conn.execute("UPDATE cars SET trim = 'Touring' WHERE id = 1")
    conn.commit()
    vf.heal_rows(conn, trim_dealers=["gardena"])
    assert _trim(conn, 1)[0] == "Touring"


@pg
def test_dry_run_and_fields_filter(conn):
    assert vf.heal_rows(conn, trim_dealers=["gardena"], dry_run=True)["trim"] == 1
    assert _trim(conn, 1)[0] is None
    vf.heal_rows(conn, trim_dealers=["gardena"], fields=["fuel_type"])
    assert _trim(conn, 1)[0] is None


@pg
def test_pipeline_selects_only_dealers_with_no_trim_at_all(conn):
    cur = conn.cursor()
    assert pipeline_vpic.no_trim_dealers(cur, ["gardena", "trimmed"]) == ["gardena"]
    assert pipeline_vpic.no_trim_dealers(cur, []) == []
