"""P8A.4: vPIC writer through precedence. Provenance carries the written value,
the heal commits per chunk, ``fields=`` filters, the tiebreak respects D-PR4,
and ``decode_missing_vins`` counts real inserts.

The pure tests run everywhere; the heal / decode tests run on the Postgres
harness (``pg`` tier, skipped without ``TEST_PG_ADMIN_DSN``)."""
from __future__ import annotations

import json

import pytest

from backend.enrichment import vpic_facts as vf
from backend.vehicle_facts import precedence as prec

pg = pytest.mark.pg


class _CountingConn:
    """Counts commits on the connection product code gets from ``inventory_db.get_conn``."""

    def __init__(self, c):
        self._c, self.commits = c, 0

    def commit(self):
        self.commits += 1
        self._c.commit()

    def __getattr__(self, name):
        return getattr(self._c, name)


@pytest.fixture
def decode(monkeypatch):
    table: dict[str, dict] = {}
    monkeypatch.setattr("backend.enrichment.knowledge_engine.prime_vpic_cache", lambda vins: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.clear_vpic_lookup_cache", lambda: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.lookup_vpic_from_cache", lambda vin: table.get(vin, {}))
    monkeypatch.setattr(vf, "_resync_incomplete_index", lambda ids: len(ids))
    return table


@pytest.fixture
def conn(pg_db):
    c = _CountingConn(pg_db.get_conn())
    yield c
    c._c.close()


def _car(conn, i, vin, drive, fuel="Gasoline", *, engine="2.0L", epa=None, dealer="d1"):
    conn.execute(
        "INSERT INTO cars (id, vin, dealer_id, title, listing_active, drivetrain, fuel_type, engine_description,"
        " body_style, epa_master_id) VALUES (?, ?, ?, 'Test car', 1, ?, ?, ?, 'SUV', ?)",
        (i, vin, dealer, drive, fuel, engine, epa))
    conn.commit()


def _epa(conn, drive):
    cur = conn.execute("INSERT INTO epa_master (year, make, model, drive) VALUES (2024, 'Ford', 'F-150', ?) RETURNING id",
                       (drive,))
    eid = cur.fetchone()[0]
    conn.commit()
    return eid


def _prov(conn, car_id):
    raw = conn.execute("SELECT spec_source_json FROM cars WHERE id=?", (car_id,)).fetchone()[0]
    return (raw if isinstance(raw, dict) else json.loads(raw)) if raw else {}


def test_override_on_cached_phev_stores_vpic_fuel_provenance_with_value(monkeypatch):
    monkeypatch.setattr(vf, "vin_facts_enabled", lambda: True)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.prime_vpic_cache", lambda vins: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.lookup_vpic_from_cache",
                        lambda vin: {"electrification": "phev", "drivetrain": "AWD"})
    cars = [{"vin": "5UXTA6C09R9L00001", "fuel_type": "Gasoline", "drivetrain": "AWD"}]
    vf.override_vehicles(cars)
    assert cars[0]["fuel_type"] == "Plug-In Hybrid"
    prov = json.loads(cars[0]["spec_source_json"])
    assert prov["fuel_type"]["source"] == "nhtsa_vpic"
    assert prov["fuel_type"]["value"] == "Plug-In Hybrid"
    assert prov["fuel_type"]["previous"] == "Gasoline"
    assert prov["fuel_type"]["detail"] == "scan-time cached decode" and prov["fuel_type"]["fetched_at"]
    assert "drivetrain" not in prov  # AWD agreed: nothing written, nothing stamped
    # The stored value and the provenance value agree, so the entry still counts (stale-provenance rule).
    assert prec.field_source("fuel_type", cars[0]["fuel_type"], prov) == prec.NHTSA_VPIC


def test_override_merges_into_existing_provenance(monkeypatch):
    monkeypatch.setattr(vf, "vin_facts_enabled", lambda: True)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.prime_vpic_cache", lambda vins: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.lookup_vpic_from_cache", lambda vin: {"drivetrain": "4WD"})
    cars = [{"vin": "3TYLB5JNXRT022195", "drivetrain": "RWD", "spec_source_json": json.dumps({"price": {"source": "x"}})}]
    vf.override_vehicles(cars)
    prov = json.loads(cars[0]["spec_source_json"])
    assert prov["price"] == {"source": "x"} and prov["drivetrain"]["value"] == "4WD"


def test_stored_sticker_value_is_not_downgraded_but_vpic_outranks_it():
    # vPIC (100) outranks the sticker (80) on drive; a vPIC "4x2" never replaces a stored end.
    row = {"drivetrain": "RWD", "spec_source_json": json.dumps({"drivetrain": {"source": "oem_window_sticker", "value": "RWD"}})}
    assert vf.vin_overrides(row, {"drivetrain": "AWD"}) == {"drivetrain": ("RWD", "AWD")}
    assert vf.vin_overrides(row, {"drivetrain": None, "drive_type_raw": "4x2"}) == {}


def test_vpic_4x2_against_stored_awd_writes_2wd_flagged_cannot_assess():
    row = {"drivetrain": "AWD"}
    dec = vf._vin_decisions(row, {"drivetrain": None, "drive_type_raw": "4x2"})
    assert dec["drivetrain"].value == "2WD" and dec["drivetrain"].cannot_assess


def test_cylinders_decode_refused_when_engine_text_contradicts():
    row = {"cylinders": 6, "engine_description": "2.0L I4 Turbo"}
    assert vf.vin_overrides(row, {"cylinders": "8"}) == {}


@pg
def test_heal_commits_per_chunk(conn, decode, monkeypatch):
    monkeypatch.setattr(vf, "_HEAL_FLAT_CHUNK", 400)
    for i in range(1200):
        vin = f"1HGCY2F6{i:09d}"
        decode[vin] = {"drivetrain": "AWD"}
        conn.execute("INSERT INTO cars (id, vin, dealer_id, title, listing_active, drivetrain, fuel_type, engine_description,"
                     " body_style) VALUES (?, ?, 'd1', 't', 1, 'FWD', 'Gasoline', '2.0L', 'SUV')", (i + 1, vin))
    conn.commit()
    before = conn.commits
    stats = vf.heal_rows(conn)
    assert stats["rows"] == 1200 and stats["drivetrain"] == 1200
    assert conn.commits - before >= 3
    assert _prov(conn, 1)["drivetrain"]["value"] == "AWD"


@pg
def test_dry_run_makes_no_writes_and_no_commits(conn, decode):
    decode["3TYLB5JNXRT022195"] = {"drivetrain": "4WD"}
    _car(conn, 1, "3TYLB5JNXRT022195", "RWD")
    before = conn.commits
    stats = vf.heal_rows(conn, dry_run=True)
    assert stats["drivetrain"] == 1 and conn.commits == before
    assert conn.execute("SELECT drivetrain, spec_source_json FROM cars").fetchone() == ("RWD", None)


@pg
def test_fields_filter_limits_the_heal(conn, decode):
    decode["3TYLB5JNXRT022195"] = {"drivetrain": "4WD", "electrification": "hybrid"}
    _car(conn, 1, "3TYLB5JNXRT022195", "RWD")
    stats = vf.heal_rows(conn, fields=["fuel_type"])
    assert stats["fuel_type"] == 1 and stats["drivetrain"] == 0
    assert conn.execute("SELECT drivetrain, fuel_type FROM cars").fetchone() == ("RWD", "Hybrid")
    prov = _prov(conn, 1)
    assert "drivetrain" not in prov and prov["fuel_type"]["value"] == "Hybrid"
    with pytest.raises(ValueError):
        vf.heal_rows(conn, fields=["price"])


@pg
def test_tiebreak_never_fires_on_dealer_awd_or_4wd(conn, decode):
    for i, d in enumerate(("AWD", "4WD"), start=1):
        vin = f"1FTFW1E5{i:09d}"
        decode[vin] = {"drivetrain": None, "fuel_type": "Gasoline"}
        _car(conn, i, vin, d, engine="3.5L", epa=_epa(conn, "Rear-Wheel Drive"))
    stats = vf.heal_rows(conn)
    assert stats.get("drivetrain_catalog_tiebreak", 0) == 0
    assert [r[0] for r in conn.execute("SELECT drivetrain FROM cars ORDER BY id").fetchall()] == ["AWD", "4WD"]


@pg
def test_tiebreak_fills_blank_and_records_its_source(conn, decode):
    vin = "1FTFW1E5X00000001"
    decode[vin] = {"drivetrain": None, "fuel_type": "Gasoline"}
    _car(conn, 1, vin, None, engine="3.5L", epa=_epa(conn, "Rear-Wheel Drive"))
    stats = vf.heal_rows(conn)
    assert stats["drivetrain_catalog_tiebreak"] == 1
    assert conn.execute("SELECT drivetrain FROM cars").fetchone()[0] == "RWD"
    assert _prov(conn, 1)["drivetrain"]["source"] == "epa_catalog_tiebreak"


@pg
def test_tiebreak_does_not_swap_an_explicit_dealer_end(conn, decode):
    vin = "1FTFW1E5X00000002"
    decode[vin] = {"drivetrain": None, "fuel_type": "Gasoline"}
    _car(conn, 1, vin, "FWD", engine="3.5L", epa=_epa(conn, "Rear-Wheel Drive"))
    vf.heal_rows(conn)
    assert conn.execute("SELECT drivetrain FROM cars").fetchone()[0] == "FWD"


@pg
def test_decode_missing_vins_counts_real_inserts(conn, monkeypatch):
    vins = ["1HGCY2F63TA066331", "3TYLB5JNXRT022195"]

    def fake_fetch(chunk):
        # a concurrent writer lands the first VIN between the cache check and the insert
        conn.execute("INSERT INTO nhtsa_vpic_cache (vin, response_json, fetched_at) VALUES (?, '{}', 'x') "
                     "ON CONFLICT (vin) DO NOTHING", (chunk[0],))
        return {v: {"Results": [{"VIN": v}]} for v in chunk}

    monkeypatch.setattr(vf, "fetch_vpic_batch", fake_fetch)
    monkeypatch.setattr(vf, "_PAUSE_S", 0)
    stats = vf.decode_missing_vins(conn, vins)
    assert stats["requested"] == 2 and stats["stored"] == 1


# --- P8A.5: decode refresh; dry run writes nothing --------------------------

def _cache(conn, vin, code, year="2027", fetched="2026-07-18T00:00:00Z", disp="2"):
    payload = {"Results": [{"VIN": vin, "ErrorCode": code, "ModelYear": year, "DisplacementL": disp}]}
    conn.execute("INSERT INTO nhtsa_vpic_cache (vin, response_json, fetched_at) VALUES (?, ?, ?)",
                 (vin, json.dumps(payload), fetched))
    conn.commit()


def test_partial_decode_and_mixed_fetched_at_formats():
    assert vf._is_partial_decode({"ErrorCode": "5,14"})
    assert not vf._is_partial_decode({"ErrorCode": "1,400"})
    assert not vf._is_partial_decode({"ErrorCode": "0"})
    a = vf._cache_text_time("2026-07-18T00:00:00Z")
    b = vf._cache_text_time("2026-07-18 00:00:00+00")
    assert a == b and a is not None
    assert vf._cache_text_time("") is None


@pg
def test_stale_selection_by_error_code_year_and_age(conn):
    _cache(conn, "W1NKM4GB9VU154744", "5,14")                       # partial, MY2027
    _cache(conn, "W1NKM4GB9VU154745", "0")                          # full, old
    _cache(conn, "W1NKM4GB9VU154746", "0", fetched="2026-10-20T00:00:00Z")  # full, fresh
    _cache(conn, "W1NKM4GB9TU154747", "5", year="2024")             # partial but old model year
    vins = ["W1NKM4GB9VU154744", "W1NKM4GB9VU154745", "W1NKM4GB9VU154746", "W1NKM4GB9TU154747"]
    now = vf._cache_text_time("2026-10-26T00:00:00Z")
    assert vf.stale_decode_vins(conn, vins, min_year=2026, now=now) == ["W1NKM4GB9VU154744"]
    assert vf.stale_decode_vins(conn, vins, partial=False, max_age_days=30, min_year=2026, now=now) == [
        "W1NKM4GB9VU154744", "W1NKM4GB9VU154745"]


@pg
def test_dry_run_decode_and_refresh_write_nothing(conn, monkeypatch):
    calls = []
    monkeypatch.setattr(vf, "fetch_vpic_batch", lambda v: calls.append(v) or {})
    _cache(conn, "W1NKM4GB9VU154744", "5,14")
    before = conn.execute("SELECT count(*), max(fetched_at) FROM nhtsa_vpic_cache").fetchone()
    miss = vf.decode_missing_vins(conn, ["W1NKM4GB9VU154744", "W1NKM4GB9VU154799"], dry_run=True)
    assert miss["would_store"] == 1 and miss["stored"] == 0
    ref = vf.refresh_decodes(conn, ["W1NKM4GB9VU154744"], dry_run=True)
    assert ref["selected"] == 1 and ref["refreshed"] == 0
    assert calls == []
    assert conn.execute("SELECT count(*), max(fetched_at) FROM nhtsa_vpic_cache").fetchone() == before


@pg
def test_refresh_replaces_partial_but_never_a_full_decode(conn, monkeypatch):
    monkeypatch.setattr(vf, "_PAUSE_S", 0)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.clear_vpic_lookup_cache", lambda: None)
    _cache(conn, "W1NKM4GB9VU154744", "5,14", disp="")
    good = {"Results": [{"VIN": "W1NKM4GB9VU154744", "ErrorCode": "0", "ModelYear": "2027", "DisplacementL": "2"}]}
    monkeypatch.setattr(vf, "fetch_vpic_batch", lambda v: {"W1NKM4GB9VU154744": good})
    st = vf.refresh_decodes(conn, ["W1NKM4GB9VU154744"], min_year=2026)
    assert st["refreshed"] == 1
    raw = conn.execute("SELECT response_json FROM nhtsa_vpic_cache WHERE vin=?", ("W1NKM4GB9VU154744",)).fetchone()[0]
    assert json.loads(raw)["Results"][0]["ErrorCode"] == "0"
    conn.execute("UPDATE nhtsa_vpic_cache SET fetched_at = '2026-07-18T00:00:00Z' WHERE vin = 'W1NKM4GB9VU154744'")
    conn.commit()
    # the full decode now cached is old enough to select by age, but a partial answer must not replace it
    bad = {"Results": [{"VIN": "W1NKM4GB9VU154744", "ErrorCode": "5", "ModelYear": "2027"}]}
    monkeypatch.setattr(vf, "fetch_vpic_batch", lambda v: {"W1NKM4GB9VU154744": bad})
    st = vf.refresh_decodes(conn, ["W1NKM4GB9VU154744"], partial=False, max_age_days=0.0001, min_year=2026)
    assert st["kept_full"] == 1 and st["refreshed"] == 0
