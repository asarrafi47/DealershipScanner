"""N1.3 clean-room acceptance tool: metric functions on SQLite fixtures, the
read-only guard, the run-artifact parsers, the fixture contract, and the refusals
of the parameterized clean-room run scripts (scripts/cleanroom/*).

No test touches Postgres or the network; the Postgres-only parts (the recompute
subprocess, the vPIC JSON join) are covered by their SQLite equivalents and by the
acceptance run recorded in the N1.3 checkpoint.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from backend.scripts import cleanroom_acceptance as cli
from backend.scripts.cleanroom import db as crdb
from backend.scripts.cleanroom import expect, metrics, recompute, run_artifacts, signals

REPO = Path(__file__).resolve().parents[2]
FIX = REPO / "backend" / "tests" / "fixtures" / "cleanroom_acceptance"
BASELINE = REPO / "backend" / "tests" / "fixtures" / "cleanroom_baseline_expected.json"
SCRIPTS = REPO / "scripts" / "cleanroom"

CARS_DDL = """
CREATE TABLE cars (
  id INTEGER PRIMARY KEY, vin TEXT, dealer_id TEXT, make TEXT, model TEXT, trim TEXT, year INTEGER,
  price REAL, msrp REAL, mileage INTEGER, condition TEXT, is_cpo INTEGER, exterior_color TEXT,
  interior_color TEXT, image_url TEXT, gallery TEXT, engine_description TEXT, engine_l TEXT,
  drivetrain TEXT, fuel_type TEXT, transmission TEXT, transmission_type TEXT, body_style TEXT,
  cylinders INTEGER, listing_active INTEGER, listing_removed_at TEXT, scraped_at TEXT,
  first_seen_at TEXT, source_url TEXT, stock_number TEXT, description TEXT
);
CREATE TABLE vin_owner_conflicts (vin TEXT, owner_dealer_id TEXT, claimant_dealer_id TEXT, seen_at TEXT,
  PRIMARY KEY (vin, owner_dealer_id, claimant_dealer_id));
CREATE TABLE nhtsa_vpic_cache (vin TEXT PRIMARY KEY, response_json TEXT, fetched_at TEXT);
CREATE TABLE incomplete_listings (car_id INTEGER PRIMARY KEY, vin TEXT, missing_fields_json TEXT, updated_at TEXT);
"""

_COLS = ("id", "vin", "dealer_id", "make", "model", "trim", "price", "msrp", "mileage", "condition", "is_cpo",
         "exterior_color", "interior_color", "image_url", "gallery", "drivetrain", "body_style", "listing_active",
         "listing_removed_at", "scraped_at", "first_seen_at", "source_url", "stock_number", "description")


def _vin(i: int) -> str:
    return f"1HGCM82633A{i:06d}"


def _car(i: int, dealer: str, **kw):
    row = {"id": i, "vin": _vin(i), "dealer_id": dealer, "make": "Honda", "model": "Accord", "trim": "EX",
           "price": 30000.0, "msrp": None, "mileage": 10, "condition": "Used", "is_cpo": 0,
           "exterior_color": "Blue", "interior_color": "Black", "image_url": "https://img.example/1.jpg",
           "gallery": None, "drivetrain": "FWD", "body_style": "Sedan", "listing_active": 1,
           "listing_removed_at": None, "scraped_at": "2026-10-13T15:00:00Z", "first_seen_at": "2026-10-13T15:00:00Z",
           "source_url": f"https://www.{dealer.split('-')[0]}.com/vdp/{i}", "stock_number": f"S{i}",
           "description": ""}
    row.update(kw)
    return row


def _seed_cars() -> list[dict]:
    rows = [
        # mbbeverlyhills rows on mbontario's host: 3, two of them conflict rows with claimant mbontario
        _car(1, "mbbeverlyhills-com", source_url="https://www.mbontario.com/inventory/a"),
        _car(2, "mbbeverlyhills-com", source_url="https://www.mbontario.com/inventory/b"),
        _car(3, "mbbeverlyhills-com", source_url="https://www.mbontario.com/inventory/c"),
        _car(4, "mbbeverlyhills-com", source_url="https://www.mbbeverlyhills.com/inventory/d"),
        _car(5, "mbontario-com"),
        # cleveland-nissan stock suffixes; mtnviewnissan CH
        _car(6, "cleveland-nissan-com", stock_number="26990CL"),
        _car(7, "cleveland-nissan-com", stock_number="261467CH"),
        _car(8, "cleveland-nissan-com", stock_number="26130BR"),
        _car(9, "cleveland-nissan-com", stock_number="261197ACH"),
        _car(10, "mtnviewnissan-com", stock_number="1234CH"),
        # chapman lot codes in the photo path (one only in the gallery)
        _car(11, "chapmanfordaz-com", image_url="https://photos.chapmanchoice.com/vehicles/CAE/640/X-1.jpg"),
        _car(12, "chapmanfordaz-com", image_url="/static/placeholder.svg",
             gallery='["https://photos.chapmanchoice.com/vehicles/CAY/640/X-2.jpg"]'),
        _car(13, "chapmanfordaz-com", image_url="https://photos.chapmanchoice.com/vehicles/CAU/640/X-3.jpg"),
        # DealerOn photo accounts
        _car(14, "bentleycadillac-com", image_url="https://www.bentleycadillac.com/inventoryphotos/18411/x/ip/1.jpg"),
        _car(15, "bentleycadillac-com", image_url="https://www.bentleycadillac.com/inventoryphotos/27303/x/ip/1.jpg"),
        _car(16, "landersmclartychevrolet-com", image_url="https://x.com/inventoryphotos/20574/v/ip/1.jpg"),
        _car(17, "landersmclartysubaru-net", image_url="https://x.com/inventoryphotos/24529/v/ip/1.jpg"),
        # dealer.com accounts at bmwofmonrovia
        _car(18, "bmwofmonrovia-net", image_url="https://pictures.dealer.com/s/sonicbmwmonrovia/1/a.jpg"),
        _car(19, "bmwofmonrovia-net", image_url="https://pictures.dealer.com/s/sonicbeverlyhillsbmw/1/a.jpg"),
        _car(20, "bmwofmonrovia-net", image_url="/static/placeholder.svg"),
        # 5starford VDP city
        _car(21, "5starford-com", source_url="https://www.5starford.com/vehicle/X/New--2026--Ford----North_Richland_Hills--TX/"),
        _car(22, "5starford-com", source_url="https://www.5starford.com/vehicle/Y/Used--2026--Chevrolet----Carrollton--TX/"),
        # fletcherjones Henderson text
        _car(23, "fletcherjones-com", description="Visit Mercedes-Benz of Henderson today"),
        _car(24, "fletcherjones-com", description="Fletcher Jones Motorcars"),
        # parser junk, prices, VINs, photos, mileage at alpha
        _car(25, "alpha-com", trim="{'_type': 's', 'en_US': ''}", exterior_color="[[exteriorColor]]"),
        _car(26, "alpha-com", price=799.0, model="Civic"),
        _car(27, "alpha-com", vin="ABC123", price=None, msrp=None),
        _car(28, "alpha-com", vin="4T36CRAV1TU41Q602",
             image_url="https://cdn.example/unavailable_stockphoto.jpg"),
        _car(29, "alpha-com", mileage=0, condition="Used"),
        _car(30, "alpha-com", mileage=None, condition="New", drivetrain="AWD"),
        _car(31, "beta-com", drivetrain="4WD", condition="New", mileage=None),
        _car(32, "beta-com", listing_active=0, listing_removed_at="2026-10-14T13:29:30Z"),
        _car(33, "gamma-com", body_style="Cars"),
        _car(34, "gamma-com", body_style="EX"),
        _car(35, "delta-com"),
        _car(36, "delta-com"),
    ]
    return rows


@pytest.fixture
def cr_sqlite(tmp_path: Path) -> Path:
    p = tmp_path / "cr.db"
    conn = sqlite3.connect(p)
    conn.executescript(CARS_DDL)
    for r in _seed_cars():
        conn.execute(f"INSERT INTO cars ({','.join(_COLS)}) VALUES ({','.join('?' * len(_COLS))})",
                     [r.get(c) for c in _COLS])
    conn.executemany("INSERT INTO vin_owner_conflicts VALUES (?,?,?,?)", [
        (_vin(1), "mbbeverlyhills-com", "mbontario-com", "t"),
        (_vin(2), "mbbeverlyhills-com", "mbontario-com", "t"),
        (_vin(4), "mbbeverlyhills-com", "fjmercedes-com", "t"),
        (_vin(25), "gamma-com", "alpha-com", "t"),
        (_vin(26), "gamma-com", "alpha-com", "t"),
    ])
    dt = lambda v: json.dumps({"Results": [{"DriveType": v}]})  # noqa: E731
    conn.executemany("INSERT INTO nhtsa_vpic_cache VALUES (?,?,?)", [
        (_vin(30), dt("4x2"), "t"),  # AWD stored, vPIC 2WD -> counted
        (_vin(31), dt("FWD/Front-Wheel Drive"), "t"),  # 4WD stored, vPIC FWD -> counted
        (_vin(29), dt("AWD/All-Wheel Drive"), "t"),  # FWD stored -> not counted
        (_vin(32), dt("4x2"), "t"),  # inactive -> not counted
    ])
    conn.executemany("INSERT INTO incomplete_listings VALUES (?,?,?,?)", [
        (26, _vin(26), json.dumps(["body_style"]), "t"),
        (27, _vin(27), json.dumps(["price"]), "t"),
        (32, _vin(32), json.dumps(["trim"]), "t"),  # inactive car: ignored
    ])
    conn.commit()
    conn.close()
    return p


@pytest.fixture
def ref_sqlite(tmp_path: Path) -> Path:
    p = tmp_path / "ref.db"
    conn = sqlite3.connect(p)
    conn.executescript(CARS_DDL)
    ref = [
        _car(101, "alpha-com", vin=_vin(25), first_seen_at="2026-09-01T00:00:00Z"),
        _car(102, "alpha-com", vin=_vin(26), first_seen_at="2026-09-20T00:00:00Z", listing_active=0),
        _car(103, "alpha-com", vin="OLDONLY0000000001"),
        _car(104, "beta-com", vin=_vin(31), first_seen_at="2026-10-12T00:00:00Z"),
    ] + [_car(200 + i, "delta-com", vin=f"DELTAREF{i:09d}") for i in range(10)]
    for r in ref:
        conn.execute(f"INSERT INTO cars ({','.join(_COLS)}) VALUES ({','.join('?' * len(_COLS))})",
                     [r.get(c) for c in _COLS])
    conn.commit()
    conn.close()
    return p


@pytest.fixture
def run_dir(tmp_path: Path) -> Path:
    """The fixture run tree, copied with its logs renamed back (``*.log`` is gitignored)."""
    root = tmp_path / "cleanroom_acceptance"
    shutil.copytree(FIX, root)
    for p in root.rglob("*.log.txt"):
        p.rename(p.with_suffix(""))
    return root / "runs" / "fleet_test"


@pytest.fixture
def target(cr_sqlite: Path):
    t = crdb.open_target(str(cr_sqlite))
    yield t
    t.close()


# ---------------------------------------------------------------------------
# read-only guard
# ---------------------------------------------------------------------------

class _FakeCursor:
    def __init__(self, values):
        self.values = values
        self.last = None

    def execute(self, sql, params=()):
        self.last = sql

    def fetchone(self):
        key = "default" if "default_transaction_read_only" in self.last else "now"
        return (self.values[key],)

    def close(self):
        pass


class _FakePg:
    def __init__(self, default="off", now="off"):
        self.values = {"default": default, "now": now}
        self.closed = False

    def cursor(self):
        return _FakeCursor(self.values)

    def close(self):
        self.closed = True


def test_guard_refuses_pg_session_without_default_transaction_read_only():
    with pytest.raises(crdb.ReadOnlyRequired, match="default_transaction_read_only=off"):
        crdb.assert_read_only(_FakePg("off", "off"), "postgres")
    with pytest.raises(crdb.ReadOnlyRequired):
        crdb.assert_read_only(_FakePg("on", "off"), "postgres")
    crdb.assert_read_only(_FakePg("on", "on"), "postgres")


def test_guard_refuses_writable_sqlite(tmp_path: Path):
    p = tmp_path / "w.db"
    conn = sqlite3.connect(p)
    with pytest.raises(crdb.ReadOnlyRequired):
        crdb.assert_read_only(conn, "sqlite")
    conn.close()


def test_open_target_sqlite_cannot_write(cr_sqlite: Path):
    t = crdb.open_target(str(cr_sqlite))
    try:
        assert t.dialect == "sqlite"
        with pytest.raises(sqlite3.OperationalError):
            t.conn.execute("DELETE FROM cars")
    finally:
        t.close()


def test_open_target_closes_and_raises_on_writable_pg_session():
    fake = _FakePg("off", "off")
    with pytest.raises(crdb.ReadOnlyRequired):
        crdb.open_target("cars_cleanroom", connector=lambda dialect, dsn: fake)
    assert fake.closed


def test_cli_refuses_to_run_without_read_only(tmp_path: Path, capsys):
    out = tmp_path / "out.json"
    rc = cli.main(["--db", "cars_cleanroom", "--json", str(out)], connector=lambda d, dsn: _FakePg("off", "off"))
    assert rc == cli.EXIT_REFUSED
    assert not out.exists()
    assert "REFUSED" in capsys.readouterr().err


def test_pg_connector_always_requests_read_only(monkeypatch):
    seen = {}

    def fake_connect(dsn, **kw):
        seen.update(kw, dsn=dsn)
        return _FakePg("on", "on")

    import psycopg

    monkeypatch.setattr(psycopg, "connect", fake_connect)
    t = crdb.open_target("cars_cr_test")
    assert "default_transaction_read_only=on" in seen["options"]
    assert "statement_timeout=120000" in seen["options"]
    assert seen["dsn"] == "dbname=cars_cr_test host=/tmp port=5432"
    t.close()


def test_recompute_worker_refuses_without_read_only_pgoptions(tmp_path: Path):
    env = {k: v for k, v in os.environ.items() if k != "PGOPTIONS"}
    p = subprocess.run([sys.executable, str(Path(recompute.__file__)), "--worker", "--conninfo", "dbname=x",
                        "--chunk", "0", "--of", "1", "--out", str(tmp_path / "o.jsonl"), "--code-root", str(REPO)],
                       env=env, capture_output=True, text=True, timeout=60)
    assert p.returncode == 3
    assert "REFUSED" in p.stderr
    assert not (tmp_path / "o.jsonl").exists()


# ---------------------------------------------------------------------------
# DB metrics on SQLite
# ---------------------------------------------------------------------------

def test_db_counts(target):
    c = metrics.db_counts(target)
    assert (c["total_rows"], c["active"], c["inactive"], c["removed_at_set"]) == (36, 35, 1, 1)
    assert c["_inactive_by_dealer"] == {"beta-com": 1}
    assert c["_active_by_dealer"]["delta-com"] == 2


def test_conflicts_and_groups(target):
    c = metrics.conflicts(target)
    assert (c["rows"], c["vins"], c["pairs"], c["owners"], c["claimants"]) == (5, 5, 3, 2, 3)
    # ties on rows break by owner, then claimant
    assert c["top_pairs"][:2] == [{"owner": "gamma-com", "claimant": "alpha-com", "rows": 2},
                                  {"owner": "mbbeverlyhills-com", "claimant": "mbontario-com", "rows": 2}]
    g = metrics.conflict_groups(c["_pairs"], min_rows=3)
    assert g["components"] == 2 and g["groups"] == 1
    assert g["_groups"][0]["dealers"] == ["fjmercedes-com", "mbbeverlyhills-com", "mbontario-com"]
    assert metrics.conflict_groups(c["_pairs"], min_rows=2)["groups"] == 2


def test_cf7_class_b_checks(target):
    c = metrics.cf7_checks(target)
    assert c["mbbeverlyhills_mbontario_host"] == {
        "dealer_id": "mbbeverlyhills-com", "host": "www.mbontario.com", "dealer_active": 4, "total": 3,
        "conflict_rows_claimant_mbontario": 2}
    assert c["mbontario_active"] == 1
    assert c["cleveland_nissan_ch_da_br"]["rows"] == 3
    assert c["cleveland_nissan_ch_da_br"]["by_suffix"] == {"BR": 1, "CH": 2}
    assert c["mtnviewnissan_ch"]["rows"] == 1
    assert c["chapmanfordaz_cae_cay_cav"]["by_lot"] == {"CAE": 1, "CAY": 1, "CAV": 0}
    assert c["bentleycadillac_don_18411_21776_19779"]["rows"] == 1
    assert c["landers"]["rows"] == 2
    assert c["bmwofmonrovia_non_sonicbmwmonrovia"]["rows"] == 1
    assert c["bmwofmonrovia_non_sonicbmwmonrovia"]["no_account"] == 1
    assert c["5starford_non_nrh_city"]["rows"] == 1
    assert c["fletcherjones_henderson"]["rows"] == 1


def test_column_scan(target):
    s = metrics.column_scan(target)
    assert s["parser_junk"]["dict_trim"] == {"rows": 1, "dealers": 1}
    assert s["parser_junk"]["token_color"] == {"rows": 1, "dealers": 1}
    assert s["junk_prices"]["price_10_999"] == 1
    assert s["vin_quality"]["not_17_chars"] == 1
    assert s["vin_quality"]["len17_with_i_o_q"] == 1
    assert s["placeholder_images"]["stored_placeholder_svg"] == 2
    assert s["placeholder_images"]["http_extra_tokens"] == {"unavailable_stockphoto": 1}
    # car 12 has a gallery photo, car 20 has nothing: one photo-less active car
    assert s["placeholder_images"]["no_http_photo"] == 1
    assert s["mileage"] == {"used_zero": 1, "used_null": 0, "new_or_blank_condition_null": 2}
    assert s["headline"]["price_missing"] == 1
    assert s["_per_dealer"]["alpha-com"]["dict_trim"] == 1


def test_drivetrain_vs_vpic(target):
    d = metrics.drivetrain_vs_vpic(target)
    assert (d["decoded_active"], d["rows"], d["dealers"]) == (3, 2, 2)
    assert d["by_pair"] == {"AWD|4x2": 1, "4WD|FWD/Front-Wheel Drive": 1}


def test_body_style_vocabulary(target):
    b = metrics.body_style_vocabulary(target)
    assert b["category"] == 1  # "Cars"
    assert b["trim_copy"] == 1  # body "EX" equals the trim
    assert b["noncanonical_total"] == 2


def test_coverage_and_first_seen(target, ref_sqlite: Path):
    ref = crdb.open_target(str(ref_sqlite))
    try:
        first, active, mx = metrics.vin_first_seen(ref)
        assert first[_vin(25)] == "2026-09-01T00:00:00Z"
        assert active[_vin(26)] == 0
        rc = metrics.db_counts(ref)
        db = metrics.db_counts(target)
        cov = metrics.coverage_vs_ref(db["_active_by_dealer"], rc["_active_by_dealer"], metrics.active_vins(target),
                                      active, roster={"alpha-com", "beta-com", "delta-com", "gamma-com"})
    finally:
        ref.close()
    assert cov["dealers_without_ref_rows"] == 1  # gamma
    assert cov["db_active_vins_in_ref_any_status"] == 3
    assert cov["db_active_vins_ref_retired"] == 1
    # delta: 10 ref -> 2 db (-80%); alpha 2 -> 6 (+200%); beta 1 -> 1
    assert cov["change_buckets"]["-90_to_-30pct"]["dealers"] == 1
    assert cov["change_buckets"]["ge_+30pct"]["dealers"] == 1
    assert cov["change_buckets"]["-10_to_+10pct"]["dealers"] == 1


@pytest.mark.parametrize("ch,bucket", [(-0.95, "le_-90pct"), (-0.9, "le_-90pct"), (-0.5, "-90_to_-30pct"),
                                       (-0.2, "-30_to_-10pct"), (-0.1, "-10_to_+10pct"), (0.0, "-10_to_+10pct"),
                                       (0.1, "+10_to_+30pct"), (0.3, "ge_+30pct")])
def test_change_bucket_edges(ch, bucket):
    assert metrics.change_bucket(ch) == bucket


def test_index_rows_only_active(target):
    rows = dict(metrics.index_rows(target))
    assert set(rows) == {26, 27}


# ---------------------------------------------------------------------------
# recompute aggregation (summarize.py / split.py / idxcmp.py / arrivals.py ports)
# ---------------------------------------------------------------------------

def _rec(i, codes, used=True, mileage=10, fs="2026-10-13T00:00:00Z", sa="2026-10-13T12:00:00Z", vin=None,
         dealer="alpha-com"):
    return {"id": i, "vin": vin or f"V{i}", "dealer_id": dealer, "codes": codes, "err": None, "used": used,
            "mileage": mileage, "first_seen_at": fs, "scraped_at": sa}


RECS = [
    _rec(1, []),
    _rec(2, ["mileage"], used=False, mileage=None),  # new-NULL only gap
    _rec(3, ["mileage", "price"], used=False, mileage=None),
    _rec(4, ["mileage"], used=True, mileage=0),  # used at 0 mi
    _rec(5, ["mileage"], used=True, mileage=None),  # used NULL
    _rec(6, ["interior_color", "images", "on_lot"]),  # non-actionable only
    _rec(7, ["trim"]),
]


def test_summarize_matches_split_rules():
    s = recompute.summarize(RECS)
    assert s["active"] == 7 and s["incomplete"] == 5
    assert s["incomplete_excl_new_null_mileage"] == 4  # 3 (price), 4, 5, 7
    assert s["mileage_split"] == {"mileage_code_cars": 4, "new_or_blank_condition_null": 2, "used_zero": 1,
                                  "used_null": 1, "mileage_only_gap": 3, "new_null_only_gap": 1,
                                  "new_or_blank_condition_null_active": 2}
    assert s["by_code"]["mileage"] == {"count": 4, "only_gap": 3}
    assert s["by_code"]["images"]["only_gap"] == 0


def test_stale_index_flags():
    st = recompute.stale_index_flags(RECS, [(1, '["body_style"]'), (7, '["trim", "fuel_type"]'), (6, "[]"),
                                            (99, "not json")])
    assert st["index_active_rows"] == 4
    assert st["stale_cars"] == 3  # 1, 6 and 99 are complete now
    assert st["incomplete_not_in_index"] == 4  # 2, 3, 4, 5
    assert st["stale_codes"] == {"body_style": 1, "fuel_type": 1}
    assert st["unparsable_rows"] == 1


def test_window_split_uses_reference_first_seen():
    recs = [_rec(1, ["price"], vin="A", fs="2026-10-13T00:00:00Z", sa="2026-10-14T00:00:00Z"),
            _rec(2, [], vin="B", fs="2026-10-13T00:00:00Z", sa="2026-10-14T00:00:00Z"),
            _rec(3, ["trim"], vin="C", fs="2026-10-13T00:00:00Z", sa="2026-10-14T00:00:00Z"),
            _rec(4, [], vin="D", fs=None, sa=None)]
    w = recompute.window_split(recs, {"B": "2026-10-08T12:00:00Z", "C": "2026-09-01T00:00:00Z"},
                               ref_vins={"B", "C"}, ref_max_first_seen="2026-10-12T00:00:00Z")
    b = w["buckets"]
    assert b["le_3d"]["n"] == 1 and b["le_3d"]["price"] == 1
    assert b["le_7d"]["n"] == 1
    assert b["older"]["n"] == 1 and b["older"]["trim"] == 1
    assert b["no_timestamp"]["n"] == 1
    assert w["not_in_reference"] == 2 and w["split_reliable"] is True
    stale = recompute.window_split(recs, {}, ref_vins=set(), ref_max_first_seen="2026-09-28T00:00:00Z")
    assert stale["split_reliable"] is False


def test_transit_and_paired():
    t = recompute.transit_split(RECS, ref_vins={"V6"})
    assert t["in_transit/in_ref"]["n"] == 1
    p = recompute.paired_vs_reference([_rec(1, ["price"], vin="A"), _rec(2, [], vin="B")],
                                      [_rec(9, [], vin="A"), _rec(8, ["price"], vin="B"), _rec(7, [], vin="Z")])
    assert p["active_in_both"] == 2 and p["ref_active_not_in_db"] == 1
    assert p["fields"]["price"] == {"missing_in_ref": 1, "filled_now": 1, "still_missing": 0, "newly_missing": 1}


def test_run_inprocess_records_errors_per_car():
    def codes(car):
        if car["id"] == 2:
            raise ValueError("boom")
        return ["trim"]

    out = recompute.run_inprocess([{"id": 1, "vin": "A", "condition": "New"}, {"id": 2, "vin": "B"}], codes)
    assert out[0]["codes"] == ["trim"] and out[0]["used"] is False
    assert out[1]["codes"] is None and "boom" in out[1]["err"]


def test_real_codes_inprocess_on_sqlite_inventory(sqlite_inventory):
    """The production path (merge_verified_specs + listing_missing_field_codes) runs per car."""
    sqlite_inventory.add_cars([
        {"vin": "1HGCM82633A004352", "year": 2024, "make": "Honda", "model": "Accord", "trim": "EX",
         "price": 30000, "mileage": None, "condition": "New", "listing_active": 1, "dealer_id": "alpha-com"},
    ])
    conn = sqlite3.connect(str(sqlite_inventory.path))
    conn.row_factory = sqlite3.Row
    cars = [dict(r) for r in conn.execute("SELECT * FROM cars")]
    conn.close()
    out = recompute.run_inprocess(cars, recompute.real_codes)
    assert out[0]["err"] is None
    # IL-01 / D-N12 (N2.1): a NULL odometer on a new car is no longer flagged; the
    # record still carries the F12 shape so the with/without split stays computable.
    assert "mileage" not in out[0]["codes"]
    assert recompute.is_new_null_mileage(out[0])


def test_run_chunks_reuses_matching_chunks_without_spawning(tmp_path: Path):
    fp = {"db_state": "x", "chunks": 2}
    for k in range(2):
        out, meta = recompute.chunk_paths(tmp_path, k, 2)
        out.write_text(json.dumps(_rec(k, ["trim"])) + "\n")
        meta.write_text(json.dumps({"chunk": k, "ok": True, "fingerprint": fp, "rows": 1, "seconds": 1.0}))
    res = recompute.run_chunks(conninfo="dbname=none", work=tmp_path, fingerprint=fp, chunks=2, jobs=1,
                               code_root=str(REPO), python="/nonexistent/python")
    assert res["complete"] and not res["failed"] and res["rows"] == 2
    assert recompute.cached_complete(tmp_path, 2, fp)
    assert not recompute.cached_complete(tmp_path, 2, {"db_state": "other", "chunks": 2})
    assert len(recompute.load_records(tmp_path, 2)) == 2


def test_conninfo_to_url():
    assert recompute.conninfo_to_url("dbname=cars_cr_x host=/tmp port=5432") == \
        "postgresql:///cars_cr_x?host=/tmp&port=5432"


# ---------------------------------------------------------------------------
# run-dir parsers
# ---------------------------------------------------------------------------

def _maps(run: Path):
    triage = run_artifacts.load_triage(run)
    names, hosts = run_artifacts.label_maps(run_artifacts.find_manifests(run), {d["dealer_id"] for d in triage})
    return triage, names, hosts


def test_regexes_match_fleet_scan():
    from backend.scripts import fleet_scan

    for mine, theirs in ((run_artifacts.STATUS_RE, fleet_scan._STATUS_RE), (run_artifacts.URL_HOST_RE,
                         fleet_scan._URL_HOST_RE), (run_artifacts.FOR_HOST_RE, fleet_scan._FOR_HOST_RE),
                         (run_artifacts.LABEL_RE, fleet_scan._LABEL_RE),
                         (run_artifacts.CHALLENGE_RE, fleet_scan._CHALLENGE_RE)):
        assert (mine.pattern, mine.flags) == (theirs.pattern, theirs.flags)


def test_max_replay_pages_reads_recipes_constant():
    from backend.scanner import recipes

    assert run_artifacts.max_replay_pages(REPO) == recipes._MAX_REPLAY_PAGES


def test_identity_and_verdicts(run_dir: Path):
    triage, _, _ = _maps(run_dir)
    ident = run_artifacts.identity(triage, db_active=14, db_inactive=1,
                                   db_active_by_dealer={"alpha-com": 4, "beta-com": 3, "gamma-com": 5, "delta-com": 2})
    assert (ident["captured"], ident["refused"], ident["written"], ident["active"], ident["inactive"]) == (14, 2, 12, 14, 1)
    assert ident["rows_null_dealers_active"] == 2
    assert ident["captured_minus_refused_eq_written"] and ident["written_plus_rows_null_active_eq_active"]
    assert ident["dealers_rows_minus_active_eq_refused"] == 3
    assert run_artifacts.verdicts(triage) == {"error": 1, "inaccurate": 1, "ok": 1, "thin": 1}


def test_vdp_blocked_floor_and_accuracy(run_dir: Path):
    triage, _, _ = _maps(run_dir)
    v = run_artifacts.vdp(triage)
    assert (v["wanted"], v["fetched"], v["dealers_ran"]) == (152, 45, 3)
    # beta (12 candidates, all 404) is under the 20-candidate floor
    assert (v["blocked_dealers"], v["blocked_dealers_no_floor"], v["blocked_wanted"], v["blocked_fetched"]) == (1, 2, 100, 5)
    assert v["blocked_statuses_empty"] == 1
    a = run_artifacts.accuracy(triage)
    assert a["vpic_fuel"] == {"rows": 1, "dealers": 1, "dealers_all_examples_phev": 1, "inaccurate_reason_mentions": 1}


def test_page_cap_http_and_retirements(run_dir: Path):
    triage, names, hosts = _maps(run_dir)
    pc = run_artifacts.page_cap_walks(run_dir, 40, names, hosts)
    assert pc["walks"] == 2 and pc["dealers"] == ["alpha-com", "gamma-com"]
    h = run_artifacts.http_403_429(run_dir, names, hosts)
    assert h["by_status"] == {"403": 3, "429": 1}  # same as fleet_summary.json
    assert h["_by_dealer"] == {"alpha-com": {"403": 1}, "beta-com": {"429": 1}, "gamma-com": {"403": 1}}
    assert h["lines_unattributed"] == 1
    r = run_artifacts.retirements(triage, run_dir, {"beta-com": 1, "zeta-com": 2})
    assert r["by_writer"] == {"scanner.inventory_reconcile": 1, "pipeline.reconcile_dealer": 0, "unattributed": 2}
    assert r["by_dealer"]["beta-com"] == {"inactive_rows": 1, "scanner.inventory_reconcile": 1, "unattributed": 0}


# ---------------------------------------------------------------------------
# signals
# ---------------------------------------------------------------------------

def test_signals_flag_rule_and_cap(run_dir: Path):
    triage, names, hosts = _maps(run_dir)
    common = dict(
        triage=triage, db_active={"alpha-com": 4, "beta-com": 3, "gamma-com": 5, "delta-com": 2},
        db_inactive={"beta-com": 1}, ref_active={"alpha-com": 10, "beta-com": 3, "gamma-com": 5, "delta-com": 2},
        pairs=[("gamma-com", "alpha-com", 2)], groups=[],
        page_cap_dealers={"alpha-com": 1, "gamma-com": 1},
        http_by_dealer={"alpha-com": {"403": 1}, "beta-com": {"429": 1}},
        column_per_dealer={}, awd_by_dealer={}, incomplete_by_dealer=None,
    )
    res = signals.build_signals(**common, analyzed={"gamma-com"}, cap=46)
    by = {r["dealer_id"]: r for r in res["dealers"]}
    assert by["alpha-com"]["signals"] == ["one_condition", "drop_over_30pct", "claimant", "page_cap", "http_403_429"]
    assert by["beta-com"]["signals"] == ["unsampled_platform", "http_403_429"]
    assert by["gamma-com"]["flag_n11_10"] is False  # analysed already
    assert by["delta-com"]["pause_error"] and not by["delta-com"]["flag_n11_10"]
    assert res["summary"]["flagged"] == 2 and res["summary"]["overflow"] == []
    capped = signals.build_signals(**common, analyzed=set(), cap=1)
    assert capped["summary"]["flagged"] == 1
    assert capped["summary"]["overflow"] == ["beta-com", "gamma-com"]  # alpha has the most signals
    grouped = signals.build_signals(**{**common, "groups": [{"dealers": ["alpha-com", "gamma-com"], "rows": 20}]},
                                    analyzed=set(), cap=46)
    assert [r["dealer_id"] for r in grouped["dealers"] if r["flag_n11_10"]] == ["beta-com"]


# ---------------------------------------------------------------------------
# expect / fixture contract / CLI
# ---------------------------------------------------------------------------

def test_expect_compare():
    out = {"a": {"b": 1, "c": [1, 2]}, "d": 3}
    assert expect.compare(out, {"a": {"b": 1}, "_note": "x"}) == []
    bad = expect.compare(out, {"a": {"b": 2, "z": 1}})
    assert {m["path"] for m in bad} == {"a.b", "a.z"}
    assert [m["actual"] for m in bad if m["path"] == "a.z"] == ["<missing>"]


def test_baseline_fixture_holds_the_spec_numbers():
    """Guards the hand-built fixture against drift: these are the N1.3 acceptance numbers."""
    e = expect.load_expected(BASELINE, "cars_cleanroom")
    assert (e["conflicts"]["rows"], e["conflicts"]["vins"], e["conflicts"]["pairs"]) == (9638, 7393, 92)
    mb = e["cf7_class_b"]["mbbeverlyhills_mbontario_host"]
    assert (mb["host"], mb["total"], mb["conflict_rows_claimant_mbontario"]) == ("www.mbontario.com", 1284, 1278)
    i = e["identity"]
    assert (i["captured"], i["refused"], i["written"], i["active"], i["inactive"]) == (208233, 9610, 198623, 199457, 21)
    assert e["parser_junk"] == {"dict_trim": {"rows": 1228, "dealers": 52}, "token_color": {"rows": 882, "dealers": 60}}
    assert (e["vpic_fuel"]["rows"], e["vpic_fuel"]["dealers"]) == (3585, 262)
    assert (e["drivetrain_vs_vpic"]["rows"], e["drivetrain_vs_vpic"]["dealers"]) == (5306, 142)
    assert e["page_cap"]["walks"] == 13
    assert (e["vdp"]["fetched"], e["vdp"]["wanted"]) == (92282, 188597)
    assert (e["incomplete"]["incomplete"], e["incomplete"]["incomplete_excl_new_null_mileage"]) == (34023, 14345)
    assert e["stale_index_flags"]["stale_cars"] == 4581
    assert expect.load_expected(BASELINE, "cars")["incomplete"]["incomplete"] == 11719


def test_cli_end_to_end_on_sqlite(cr_sqlite: Path, ref_sqlite: Path, run_dir: Path, tmp_path: Path):
    out, sig = tmp_path / "out.json", tmp_path / "signals.json"
    fixture = tmp_path / "expected.json"
    fixture.write_text(json.dumps({"runs": {"small": {"expected": {
        "conflicts": {"rows": 5, "pairs": 3},
        "identity": {"captured": 14, "refused": 2, "written": 12},
        "parser_junk": {"dict_trim": {"rows": 1}},
        "page_cap": {"walks": 2},
        "http_403_429": {"by_status": {"403": 3, "429": 1}},
    }}}}))
    rc = cli.main(["--db", str(cr_sqlite), "--run-dir", str(run_dir), "--ref-db", str(ref_sqlite), "--json", str(out),
                   "--signals", str(sig), "--expect", str(fixture), "--expect-key", "small"])
    data = json.loads(out.read_text())
    assert rc == cli.EXIT_OK, data.get("expect")
    assert data["read_only"] is True and data["dialect"] == "sqlite"
    assert data["expect"]["match"] is True
    assert data["recompute"]["db"]["skipped"].startswith("SQLite")
    assert data["retirements"]["by_writer"]["scanner.inventory_reconcile"] == 1
    assert not any(k.startswith("_") for k in data["conflicts"])  # private keys stripped
    s = json.loads(sig.read_text())
    assert s["summary"]["dealers"] == 4 and s["summary"]["analyzed"] == 1
    # a mismatching expectation exits 1
    fixture.write_text(json.dumps({"runs": {"small": {"expected": {"conflicts": {"rows": 6}}}}}))
    assert cli.main(["--db", str(cr_sqlite), "--json", str(out), "--expect", str(fixture), "--expect-key", "small",
                     "--sections", "conflicts"]) == cli.EXIT_MISMATCH


def test_cli_rejects_unknown_section(cr_sqlite: Path, tmp_path: Path):
    assert cli.main(["--db", str(cr_sqlite), "--json", str(tmp_path / "o.json"), "--sections", "nope"]) == cli.EXIT_REFUSED


# ---------------------------------------------------------------------------
# scripts/cleanroom refusals
# ---------------------------------------------------------------------------

BASH = shutil.which("bash")
pytestmark_scripts = pytest.mark.skipif(BASH is None, reason="bash not available")

_STRIP = ("INVENTORY_DATABASE_URL", "DATABASE_URL", "PGHOST", "PGPORT", "PGDATABASE", "PGOPTIONS", "PGSERVICE",
          "RECIPES_CACHE_DIR", "DEALER_LOGS_ROOT", "SCAN_OUT_ROOT", "SCANNER_LOCK_PATH", "DEALERS_MANIFEST_PATH",
          "SCAN_PIPELINE_ARGS", "SCAN_DEALERS", "CR_ALLOW_ART_OUTSIDE_RUNS")
# Test run roots live under tmp_path, outside every workspace/: the scripts only accept
# that with this explicit override (check_run_root.sh), which preflight reports.
ALLOW_TMP_ART = {"CR_ALLOW_ART_OUTSIDE_RUNS": "1"}


def _env(**kw):
    env = {k: v for k, v in os.environ.items() if k not in _STRIP}
    env.update(kw)
    return env


def _run(args, env=None, cwd=None):
    return subprocess.run([BASH, *args], capture_output=True, text=True, timeout=60, env=env or _env(), cwd=cwd)


def _good_env(art: Path, db: str = "cars_cr_test", **over):
    env = _env(
        **ALLOW_TMP_ART,
        INVENTORY_DATABASE_URL=f"postgresql:///{db}?host=/tmp&port=5432",
        DATABASE_URL=f"postgresql:///{db}?host=/tmp&port=5432",
        RECIPES_CACHE_DIR=str(art / "recipes_cache"), DEALER_LOGS_ROOT=str(art / "dealer_logs"),
        SCAN_OUT_ROOT=str(art / "runs"), SCANNER_LOCK_PATH=str(art / "runs" / "scanner.lock"),
        DEALERS_MANIFEST_PATH=str(art / "manifest" / "scanner_dealers.json"),
        SCAN_PIPELINE_ARGS=f"--no-discover --manifest {art}/manifest/pipeline_dealers.json",
    )
    env.update(over)
    return env


def _head() -> str:
    return subprocess.run(["git", "-C", str(REPO), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()


@pytestmark_scripts
@pytest.mark.parametrize("db", ["cars_cleanroom", "cars", "prod_cars"])
def test_preflight_refuses_frozen_and_non_scratch_targets(db, tmp_path: Path):
    roster = tmp_path / "roster.txt"
    roster.write_text("alpha-com\n")
    p = _run([str(SCRIPTS / "preflight.sh"), "--db", db, "--art", str(tmp_path / "art"), "--roster", str(roster),
              "--sha", _head()], env=_good_env(tmp_path / "art", db=db))
    assert p.returncode == 1
    assert "FAIL" in p.stdout and "PREFLIGHT FAIL" in p.stdout
    assert "PREFLIGHT OK" not in p.stdout


@pytestmark_scripts
@pytest.mark.parametrize("override,needle", [
    ({"INVENTORY_DATABASE_URL": "postgresql:///cars_cr_test?host=/tmp&port=15432"}, "15432"),
    ({"INVENTORY_DATABASE_URL": "postgresql://db.example.com:5432/cars_cr_test"}, "is not /tmp"),
    ({"DATABASE_URL": "postgresql:///cars_cleanroom?host=/tmp&port=5432"}, "refused"),
    ({"RECIPES_CACHE_DIR": "/tmp/elsewhere/recipes_cache"}, "RECIPES_CACHE_DIR"),
    ({"SCANNER_LOCK_PATH": "/tmp/scanner.lock"}, "SCANNER_LOCK_PATH"),
    ({"PGPORT": "15432"}, "PGPORT is set"),
])
def test_preflight_refuses_bad_env(override, needle, tmp_path: Path):
    art = tmp_path / "art"
    roster = tmp_path / "roster.txt"
    roster.write_text("alpha-com\n")
    p = _run([str(SCRIPTS / "preflight.sh"), "--db", "cars_cr_test", "--art", str(art), "--roster", str(roster),
              "--sha", _head()], env=_good_env(art, **override))
    assert p.returncode == 1, p.stdout
    assert "PREFLIGHT FAIL" in p.stdout
    assert needle in p.stdout


@pytestmark_scripts
def test_preflight_refuses_wrong_sha(tmp_path: Path):
    art = tmp_path / "art"
    roster = tmp_path / "roster.txt"
    roster.write_text("alpha-com\n")
    p = _run([str(SCRIPTS / "preflight.sh"), "--db", "cars_cr_test", "--art", str(art), "--roster", str(roster),
              "--sha", "0000000deadbeef"], env=_good_env(art))
    assert p.returncode == 1 and "does not match --sha" in p.stdout


@pytestmark_scripts
def test_run_env_template_refuses_and_exports_nothing(tmp_path: Path):
    roster = tmp_path / "roster.txt"
    roster.write_text("alpha-com\nbeta-com\n")
    tpl = SCRIPTS / "run.env.template"
    for db in ("cars_cleanroom", "cars", "other_db"):
        p = _run(["-c", f'set -a; . "{tpl}" --db {db} --art "{tmp_path}/art" --roster "{roster}"; '
                        'echo "rc=$? url=${INVENTORY_DATABASE_URL:-unset}"'], env=_env(**ALLOW_TMP_ART))
        assert "rc=2 url=unset" in p.stdout, (db, p.stdout, p.stderr)
        assert "REFUSED" in p.stderr


@pytestmark_scripts
def test_run_env_template_puts_every_writer_under_the_run_root(tmp_path: Path):
    roster = tmp_path / "roster.txt"
    roster.write_text("alpha-com\n# comment\nbeta-com\n")
    art = tmp_path / "art"
    p = _run(["-c", f'set -a; . "{SCRIPTS / "run.env.template"}" --db cars_cr_test --art "{art}" --roster "{roster}"; '
                    'env'], env=_env(**ALLOW_TMP_ART))
    env = dict(line.split("=", 1) for line in p.stdout.splitlines() if "=" in line)
    assert env["INVENTORY_DATABASE_URL"] == "postgresql:///cars_cr_test?host=/tmp&port=5432"
    for k in ("RECIPES_CACHE_DIR", "DEALER_LOGS_ROOT", "SCAN_OUT_ROOT", "SCANNER_LOCK_PATH", "DEALERS_MANIFEST_PATH"):
        assert env[k].startswith(str(art) + "/"), k
    assert env["SCAN_DEALERS"] == "alpha-com,beta-com"
    assert env["ANTHROPIC_API_KEY"] == "" and env["SCANNER_AUTO_HEAL"] == "0"
    assert "_cr_db" not in env and "PGOPTIONS" not in env


@pytestmark_scripts
@pytest.mark.parametrize("args,needle", [
    (["--db", "cars_cleanroom"], "frozen baseline"),
    (["--db", "cars"], "frozen baseline"),
    (["--db", "mydb"], "not a scratch name"),
    (["--db", "cars_cr_x", "--from-template", "cars_cr_x"], "same DB"),
])
def test_build_db_refusals(args, needle):
    p = _run([str(SCRIPTS / "build_db.sh"), *args])
    assert p.returncode == 2 and needle in p.stderr


@pytestmark_scripts
def test_isolation_snapshot_refuses_protected_target(tmp_path: Path):
    p = _run([str(SCRIPTS / "isolation_snapshot.sh"), "before", "--db", "cars_cleanroom", "--art", str(tmp_path)])
    assert p.returncode == 2 and "protected" in p.stderr


@pytestmark_scripts
def test_make_manifest_filters_and_refuses(tmp_path: Path):
    roster = tmp_path / "roster.txt"
    roster.write_text("beta-com\nalpha-com\n")
    art = tmp_path / "art"
    p = _run([str(SCRIPTS / "make_manifest.sh"), "--art", str(art), "--roster", str(roster),
              "--from", str(FIX / "manifest")], env=_env(**ALLOW_TMP_ART))
    assert p.returncode == 0, p.stderr
    rows = json.loads((art / "manifest" / "pipeline_dealers.json").read_text())
    assert [r["dealer_id"] for r in rows] == ["beta-com", "alpha-com"]
    assert (art / "roster.txt").read_text().split() == ["beta-com", "alpha-com"]
    roster.write_text("alpha-com\nnew-rooftop-com\n")
    p = _run([str(SCRIPTS / "make_manifest.sh"), "--art", str(art), "--roster", str(roster),
              "--from", str(FIX / "manifest")], env=_env(**ALLOW_TMP_ART))
    assert p.returncode == 2 and "new-rooftop-com" in p.stderr


# ---------------------------------------------------------------------------
# run-root isolation: workspace/cleanroom_runs/<db> only (scripts/cleanroom/check_run_root.sh)
# ---------------------------------------------------------------------------


def _main_checkout() -> Path:
    out = subprocess.run(["git", "-C", str(REPO), "rev-parse", "--path-format=absolute", "--git-common-dir"],
                         capture_output=True, text=True).stdout.strip()
    return Path(out).parent if out else REPO


MAIN = _main_checkout()
# Run roots that must always be refused, whatever CR_ALLOW_ART_OUTSIDE_RUNS says: the
# real workspace (the review case: DEALER_LOGS_ROOT would be the real dealer logs), the
# frozen 2026-10 run (relative and absolute, this checkout and the main one), the real
# dealer logs and the checkout root itself.
_FORBIDDEN_ARTS = list(dict.fromkeys([
    str(REPO / "workspace"), "workspace", "workspace/cleanroom_2026_10",
    str(REPO / "workspace" / "cleanroom_2026_10"), str(MAIN / "workspace" / "cleanroom_2026_10"),
    str(MAIN / "workspace"), str(REPO / "workspace" / "dealer_logs"), str(REPO / "workspace" / "recipes"),
    str(REPO / "workspace" / "cleanroom_runs"), str(REPO),
]))


@pytestmark_scripts
@pytest.mark.parametrize("art", _FORBIDDEN_ARTS)
@pytest.mark.parametrize("allow", [False, True], ids=["strict", "override"])
def test_check_run_root_refuses_workspace_and_frozen_dirs(art, allow):
    env = _env(**(ALLOW_TMP_ART if allow else {}))
    p = _run([str(SCRIPTS / "check_run_root.sh"), "--db", "cars_cr_test", "--art", art], env=env)
    assert p.returncode == 1 and p.stdout.startswith("REFUSED"), p.stdout + p.stderr


@pytestmark_scripts
def test_check_run_root_accepts_only_this_dbs_run_root(tmp_path: Path):
    def run(art, db="cars_cr_test", **env):
        return _run([str(SCRIPTS / "check_run_root.sh"), "--art", str(art), *(["--db", db] if db else [])],
                    env=_env(**env))
    for root in {REPO, MAIN}:
        p = run(root / "workspace" / "cleanroom_runs" / "cars_cr_test")
        assert p.returncode == 0 and p.stdout.startswith("ok   run root"), p.stdout
    # no --db: the basename of --art, which must be a scratch name
    assert run(REPO / "workspace" / "cleanroom_runs" / "cars_cr_abc", db=None).returncode == 0
    assert run(REPO / "workspace" / "cleanroom_runs" / "foo", db=None).returncode == 1
    # another DB's run root, a non-scratch --db
    p = run(REPO / "workspace" / "cleanroom_runs" / "cars_cr_other")
    assert p.returncode == 1 and "cleanroom_runs/cars_cr_test" in p.stdout
    assert run(REPO / "workspace" / "cleanroom_runs" / "cars", db="cars").returncode == 1
    # outside every workspace/: only with the explicit override, which is reported
    p = run(tmp_path / "art")
    assert p.returncode == 1 and "CR_ALLOW_ART_OUTSIDE_RUNS" in p.stdout
    p = run(tmp_path / "art", **ALLOW_TMP_ART)
    assert p.returncode == 0 and p.stdout.startswith("note") and "CR_ALLOW_ART_OUTSIDE_RUNS=1" in p.stdout
    assert not (tmp_path / "art").exists()


@pytestmark_scripts
@pytest.mark.parametrize("art", _FORBIDDEN_ARTS)
def test_preflight_refuses_real_workspace_and_frozen_run_root(art, tmp_path: Path):
    """The review case: every env var under --art <repo>/workspace used to print 'ok'."""
    roster = tmp_path / "roster.txt"
    roster.write_text("alpha-com\n")
    art_abs = Path(art) if Path(art).is_absolute() else REPO / art
    p = _run([str(SCRIPTS / "preflight.sh"), "--db", "cars_cr_test", "--art", art, "--roster", str(roster),
              "--sha", _head()], env=_good_env(art_abs))  # override set: it must not relax these
    assert p.returncode == 1 and "PREFLIGHT FAIL" in p.stdout, p.stdout
    fails = [ln for ln in p.stdout.splitlines() if ln.startswith("  FAIL")]
    assert len(fails) == 1 and "run root" in fails[0], p.stdout
    assert "== power" not in p.stdout  # stopped at the refusals


@pytestmark_scripts
def test_preflight_run_root_strict_unless_override(tmp_path: Path):
    roster = tmp_path / "roster.txt"
    roster.write_text("alpha-com\n")
    art = tmp_path / "art"
    args = [str(SCRIPTS / "preflight.sh"), "--db", "cars_cr_test", "--art", str(art), "--roster", str(roster),
            "--sha", "0000000deadbeef"]  # wrong sha: always stops at the refusals
    strict = _good_env(art)
    strict.pop("CR_ALLOW_ART_OUTSIDE_RUNS")
    p = _run(args, env=strict)
    assert "  FAIL run root" in p.stdout and "CR_ALLOW_ART_OUTSIDE_RUNS" in p.stdout, p.stdout
    p = _run(args, env=_good_env(art))
    assert "  note run root" in p.stdout and "CR_ALLOW_ART_OUTSIDE_RUNS=1" in p.stdout, p.stdout
    assert "FAIL run root" not in p.stdout
    good = REPO / "workspace" / "cleanroom_runs" / "cars_cr_test"
    env = _good_env(good)
    env.pop("CR_ALLOW_ART_OUTSIDE_RUNS")
    p = _run([*args[:3], "--art", str(good), *args[5:]], env=env)
    assert f"  ok   run root {good.resolve()} is workspace/cleanroom_runs/cars_cr_test" in p.stdout, p.stdout


@pytestmark_scripts
@pytest.mark.parametrize("art", [str(REPO / "workspace"), "workspace/cleanroom_2026_10",
                                 str(REPO / "workspace" / "dealer_logs")])
@pytest.mark.parametrize("allow", [False, True], ids=["strict", "override"])
def test_run_env_template_refuses_real_workspace_run_root(art, allow, tmp_path: Path):
    roster = tmp_path / "roster.txt"
    roster.write_text("alpha-com\n")
    p = _run(["-c", f'set -a; . "{SCRIPTS / "run.env.template"}" --db cars_cr_test --art "{art}" --roster "{roster}"; '
                    'echo "rc=$? logs=${DEALER_LOGS_ROOT:-unset} url=${INVENTORY_DATABASE_URL:-unset}"'],
             env=_env(**(ALLOW_TMP_ART if allow else {})))
    assert "rc=2 logs=unset url=unset" in p.stdout, (p.stdout, p.stderr)
    assert "REFUSED" in p.stderr and "run root" in p.stderr


@pytestmark_scripts
def test_run_env_template_refuses_tmp_run_root_without_override(tmp_path: Path):
    roster = tmp_path / "roster.txt"
    roster.write_text("alpha-com\n")
    p = _run(["-c", f'set -a; . "{SCRIPTS / "run.env.template"}" --db cars_cr_test --art "{tmp_path}/art" '
                    f'--roster "{roster}"; echo "rc=$? url=${{INVENTORY_DATABASE_URL:-unset}}"'])
    assert "rc=2 url=unset" in p.stdout and "CR_ALLOW_ART_OUTSIDE_RUNS" in p.stderr, (p.stdout, p.stderr)


@pytest.fixture
def sandbox(tmp_path: Path):
    """scripts/cleanroom copied into a throwaway repo whose workspace/ holds the files the
    scripts used to overwrite (the frozen run's build/, recipes_cache/, snapshots/, START;
    the real dealer logs and recipe cache), with psql/createdb/pg_dump/dropdb on PATH
    replaced by stubs that log every call. A refusal must leave all of it untouched and
    make no DB call. Relative --art resolves against the sandbox repo."""
    repo = tmp_path / "repo"
    shutil.copytree(SCRIPTS, repo / "scripts" / "cleanroom")
    ws = repo / "workspace"
    frozen = ws / "cleanroom_2026_10"
    for f in (frozen / "build" / "cars_stat_before.txt", frozen / "build" / "cars_stat_after.txt",
              frozen / "build" / "schema_pre.sql", frozen / "build" / "schema_post.sql",
              frozen / "recipes_cache" / "_aliases.json", frozen / "snapshots" / "before.txt", frozen / "START",
              frozen / "manifest" / "pipeline_dealers.json",
              ws / "dealer_logs" / "alpha-com" / "discovery.md", ws / "recipes" / "_aliases.json"):
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(f"sentinel {f.name}\n")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    calls = tmp_path / "db_calls.log"
    for tool in ("psql", "createdb", "pg_dump", "dropdb"):
        exe = bindir / tool
        exe.write_text(f'#!/bin/sh\necho "{tool} $*" >> "{calls}"\necho 0\n')
        exe.chmod(0o755)
    roster = tmp_path / "roster.txt"
    roster.write_text("alpha-com\n")
    env = _env(PATH=f"{bindir}{os.pathsep}{os.environ.get('PATH', '')}", GIT_CEILING_DIRECTORIES=str(tmp_path))
    return {"repo": repo, "ws": ws, "calls": calls, "env": env, "roster": roster,
            "script": lambda name: str(repo / "scripts" / "cleanroom" / name)}


def _tree(root: Path) -> dict:
    return {str(p.relative_to(root)): ((p.stat().st_mtime_ns, p.read_bytes()) if p.is_file() else None)
            for p in root.rglob("*")}


_SANDBOX_FORBIDDEN = ["workspace", "{repo}/workspace", "workspace/cleanroom_2026_10", "{repo}/workspace/cleanroom_2026_10",
                      "workspace/cleanroom_2026_10/build", "workspace/dealer_logs", "{repo}"]


@pytestmark_scripts
@pytest.mark.parametrize("art", _SANDBOX_FORBIDDEN)
@pytest.mark.parametrize("allow", [False, True], ids=["strict", "override"])
def test_build_db_refuses_real_workspace_and_frozen_run_root(sandbox, art, allow):
    art = art.format(repo=sandbox["repo"])
    before = _tree(sandbox["repo"])
    env = dict(sandbox["env"], **(ALLOW_TMP_ART if allow else {}))
    p = _run([sandbox["script"]("build_db.sh"), "--db", "cars_cr_x", "--art", art], env=env)
    assert p.returncode == 2 and "REFUSED" in p.stderr and "run root" in p.stderr, p.stdout + p.stderr
    assert _tree(sandbox["repo"]) == before  # no mkdir, no stat/schema/_aliases overwrite
    assert not sandbox["calls"].exists()  # refused before any DB call


@pytestmark_scripts
@pytest.mark.parametrize("mode", ["before", "after"])
@pytest.mark.parametrize("art", _SANDBOX_FORBIDDEN)
def test_isolation_snapshot_refuses_real_workspace_and_frozen_run_root(sandbox, mode, art):
    art = art.format(repo=sandbox["repo"])
    before = _tree(sandbox["repo"])
    env = dict(sandbox["env"], **ALLOW_TMP_ART)  # the override does not relax these
    p = _run([sandbox["script"]("isolation_snapshot.sh"), mode, "--db", "cars_cr_x", "--art", art], env=env)
    assert p.returncode == 2 and "REFUSED" in p.stderr and "run root" in p.stderr, p.stdout + p.stderr
    assert _tree(sandbox["repo"]) == before  # snapshots/before.txt and START untouched
    assert not sandbox["calls"].exists()


@pytestmark_scripts
def test_make_manifest_refuses_real_workspace_run_root(sandbox):
    before = _tree(sandbox["repo"])
    for art in ("workspace", "workspace/cleanroom_2026_10", str(sandbox["repo"])):
        p = _run([sandbox["script"]("make_manifest.sh"), "--art", art, "--roster", str(sandbox["roster"]),
                  "--from", str(FIX / "manifest")], env=dict(sandbox["env"], **ALLOW_TMP_ART))
        assert p.returncode == 2 and "REFUSED" in p.stderr, (art, p.stderr)
    assert _tree(sandbox["repo"]) == before


@pytestmark_scripts
def test_build_db_and_snapshot_accept_the_dbs_run_root(sandbox, tmp_path: Path):
    repo, ws = sandbox["repo"], sandbox["ws"]
    before = _tree(repo)
    # strict, outside every workspace/: refused before any DB call
    p = _run([sandbox["script"]("build_db.sh"), "--db", "cars_cr_x", "--art", str(tmp_path / "art")], env=sandbox["env"])
    assert p.returncode == 2 and "CR_ALLOW_ART_OUTSIDE_RUNS" in p.stderr and not sandbox["calls"].exists()
    # the DB's own run root (explicit and default) passes the check and reaches the DB
    # stubs (which report that the source DB does not exist), still before any mkdir
    for extra in (["--art", "workspace/cleanroom_runs/cars_cr_x"], []):
        p = _run([sandbox["script"]("build_db.sh"), "--db", "cars_cr_x", *extra], env=sandbox["env"])
        assert "ok   run root" in p.stdout and "source cars does not exist" in p.stderr, p.stdout + p.stderr
    assert sandbox["calls"].exists()
    assert _tree(repo) == before
    # isolation_snapshot before: writes only <art>/snapshots/before.txt and <art>/START
    p = _run([sandbox["script"]("isolation_snapshot.sh"), "before", "--db", "cars_cr_x",
              "--art", "workspace/cleanroom_runs/cars_cr_x"], env=sandbox["env"])
    assert p.returncode == 0, p.stdout + p.stderr
    after = _tree(repo)
    changed = {k for k in set(before) | set(after) if k not in before or k not in after or before[k] != after[k]}
    run_root = "workspace/cleanroom_runs/cars_cr_x"
    assert changed == {"workspace/cleanroom_runs", run_root, f"{run_root}/snapshots",
                       f"{run_root}/snapshots/before.txt", f"{run_root}/START"}, changed
    assert (ws / "cleanroom_2026_10" / "snapshots" / "before.txt").read_text() == "sentinel before.txt\n"
