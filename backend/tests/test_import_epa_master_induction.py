"""P10A.8: ``import_epa_master --refresh-induction`` and ``--restore``.

The planner and the label rules run on plain dicts. The ``pg`` tests seed a throwaway
Postgres built from the migration chain (the refresh is set-based Postgres SQL).
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from backend.scripts import import_epa_master as imp

CSV_HEADER = ["id", "year", "make", "model", "cylinders", "displ", "atvType", "tCharger", "sCharger", "eng_dscr"]
CSV_ROWS = [
    # EPA truth: Integra NA, RDX turbo, A6 3.0 supercharged, a plain Camry, a Bolt EV
    ["1001", "2000", "Acura", "Integra", "4", "1.8", "", "", "", ""],
    ["1002", "2019", "Acura", "RDX", "4", "2.0", "", "T", "", "(FFS,TRBO)"],
    ["1003", "2014", "Audi", "A6", "6", "3.0", "", "", "S", "(FFS,SC)"],
    ["1004", "2020", "Toyota", "Camry", "4", "2.5", "", "", "", ""],
    ["1005", "2020", "Chevrolet", "Bolt EV", "", "", "EV", "", "", ""],
]


def _write_csv(path: Path) -> Path:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(CSV_HEADER)
        w.writerows(CSV_ROWS)
    return path


def _row(id, vid, year, make, model, cyl, displ, fi, disp, t=None, s=None, atv=None):
    return {"id": id, "epa_vehicle_id": vid, "year": year, "make": make, "model": model, "trim": None,
            "cylinders": cyl, "displacement": displ, "fuel_type": "Regular Gasoline", "atv_type": atv,
            "forced_induction": fi, "engine_display": disp, "t_charger": t, "s_charger": s}


def _seeded():
    return [
        _row(1, 1001, 2000, "Acura", "Integra", 4, 1.8, "Turbocharged", "1.8L I4 Turbo"),   # wrongly turbo
        _row(2, 1002, 2019, "Acura", "RDX", 4, 2.0, "Turbocharged", "2.0L I4 Turbo"),        # right
        _row(3, 1003, 2014, "Audi", "A6", 6, 3.0, "Turbocharged", "3.0L V6 Turbo"),           # wrongly turbo
        _row(4, 1004, 2020, "Toyota", "Camry", 4, 2.5, None, "2.5L I4"),                      # never refreshed
        _row(5, 1004, 2020, "Toyota", "Camry", 4, 2.5, None, "2.5L I4"),                      # duplicate vid
        _row(6, 1004, 2020, "Toyota", "Camry", 4, 2.5, None, None),                           # id-only: no display
        _row(7, None, 2001, "Honda", "Legacy Row", 4, 1.6, "Turbocharged", "1.6L I4 Turbo"),   # no epa id
        _row(8, 9999, 2001, "Honda", "Not In Csv", 4, 1.6, None, "1.6L I4"),                   # id not in csv
    ]


def test_rewrite_display_suffix_only_touches_the_suffix():
    assert imp.rewrite_display_suffix("1.8L I4 Turbo", "Naturally Aspirated") == "1.8L I4"
    assert imp.rewrite_display_suffix("3.0L V6 Turbo", "Supercharged") == "3.0L V6 Supercharged"
    assert imp.rewrite_display_suffix("3.0L V6 Twin Turbo", "Twin Turbocharged") == "3.0L V6 Twin Turbo"
    assert imp.rewrite_display_suffix("2.5L I4", "") == "2.5L I4"
    # a NULL / empty display is never created
    assert imp.rewrite_display_suffix(None, "Turbocharged") is None
    assert imp.rewrite_display_suffix("", "Turbocharged") == ""


def test_plan_ends_at_epa_truth_updates_duplicates_and_reports_idless(tmp_path):
    flags = imp.load_csv_flags(_write_csv(tmp_path / "v.csv"))
    computed, stats = imp.plan_refresh(_seeded(), flags)
    new = {rid: n for rid, _, n in computed}
    assert new[1] == ("Naturally Aspirated", "1.8L I4", "", "")
    assert new[2] == ("Turbocharged", "2.0L I4 Turbo", "T", "")
    assert new[3] == ("Supercharged", "3.0L V6 Supercharged", "", "S")
    # both duplicates of vid 1004 are refreshed; the NULL display stays NULL
    assert new[4] == new[5] == ("Naturally Aspirated", "2.5L I4", "", "")
    assert new[6] == ("Naturally Aspirated", None, "", "")
    assert 7 not in new and 8 not in new
    assert stats["no_epa_id"] == 1 and stats["not_in_csv"] == 1 and stats["matched"] == 6
    assert stats["changed"] == 6 and stats["unchanged"] == 0       # RDX changes only its flag columns
    assert stats["transitions"] == {
        "NULL -> Naturally Aspirated": 3,
        "Turbocharged -> Naturally Aspirated": 1,
        "Turbocharged -> Supercharged": 1,
        "Turbocharged -> Turbocharged": 1,
    }


def test_plan_never_touches_columns_it_does_not_own():
    flags = {1005: {"t": "", "s": "", "dscr": ""}}
    rows = [_row(1, 1005, 2020, "Chevrolet", "Bolt EV", None, None, None, None, atv="EV")]
    (rid, old, new), = imp.plan_refresh(rows, flags)[0]
    assert new[0] == "" and new[1] is None          # EV: blank label, still no display
    assert len(new) == 4                              # only the four induction columns are ever written


def test_a_twin_label_survives_where_the_classifier_is_silent():
    flags = {2001: {"t": "T", "s": "", "dscr": ""}}
    twin = _row(1, 2001, 2012, "Nissan", "ZZ Unlisted", 6, 3.8, "Twin Turbocharged", "3.8L V6 Twin Turbo")
    (rid, old, new), = imp.plan_refresh([twin], flags)[0]
    assert new[0] == "Twin Turbocharged" and new[1] == "3.8L V6 Twin Turbo"


def test_backup_roundtrips_null_vs_blank(tmp_path):
    computed = [(1, (None, None, None, None), ("A", "B", "", "")), (2, ("x", "", "", "T"), ("y", "", "", ""))]
    path = tmp_path / "b.csv"
    imp._write_backup(path, computed)
    with open(path, newline="") as f:
        recs = list(csv.reader(f))
    assert recs[1] == ["1", "\\N", "\\N", "\\N", "\\N"] and recs[2] == ["2", "x", "", "", "T"]
    with pytest.raises(imp.RefreshRefused, match="already exists"):
        imp._write_backup(path, computed)


def test_cli_refuses_without_csv_or_database(monkeypatch, tmp_path):
    from backend.db import inventory_pg

    monkeypatch.setattr(inventory_pg, "inventory_postgres_dsn", lambda: "")
    assert imp.main(["--refresh-induction"]) == imp.EXIT_USAGE
    assert imp.main(["--refresh-induction", "--csv", str(tmp_path / "v.csv")]) == imp.EXIT_USAGE
    assert imp.main(["--restore", str(tmp_path / "b.csv")]) == imp.EXIT_USAGE


# --- Postgres --------------------------------------------------------------------------------

_COLS = ("id", "epa_vehicle_id", "year", "make", "model", "cylinders", "displacement", "fuel_type",
         "atv_type", "forced_induction", "engine_display", "t_charger", "s_charger")


def _seed(conn):
    for r in _seeded():
        vals = [r[c] for c in _COLS]
        conn.execute(f"INSERT INTO public.epa_master ({','.join(_COLS)}) VALUES ({','.join(['%s'] * len(_COLS))})", vals)
    conn.commit()


def _snapshot(conn):
    return conn.execute(
        "SELECT id, epa_vehicle_id, atv_type, city_e, forced_induction, engine_display, t_charger, s_charger "
        "FROM public.epa_master ORDER BY id").fetchall()


@pytest.mark.pg
def test_dry_run_changes_nothing_and_apply_matches_the_plan(pg_db, tmp_path):
    v = _write_csv(tmp_path / "v.csv")
    with pg_db.connect() as conn:
        _seed(conn)
        before = _snapshot(conn)
        dry = imp.refresh_induction(conn, v)
        assert dry["dry_run"] and _snapshot(conn) == before
        with pytest.raises(imp.RefreshRefused, match="--backup"):
            imp.refresh_induction(conn, v, apply=True)
        done = imp.refresh_induction(conn, v, apply=True, backup_path=tmp_path / "b.csv")
        assert done["applied"] == dry["changed"] == sum(dry["transitions"].values())
        rows = {r[0]: r for r in _snapshot(conn)}
        assert rows[1][4:] == ("Naturally Aspirated", "1.8L I4", "", "")
        assert rows[3][4:] == ("Supercharged", "3.0L V6 Supercharged", "", "S")
        assert rows[4][4:] == rows[5][4:] == ("Naturally Aspirated", "2.5L I4", "", "")
        assert rows[6][5] is None                      # NULL display never created
        assert rows[7][4:] == before[6][4:] and rows[8][4:] == before[7][4:]
        assert [r[1:4] for r in _snapshot(conn)] == [r[1:4] for r in before]
        # a second run is a no-op
        assert imp.refresh_induction(conn, v)["changed"] == 0


@pytest.mark.pg
def test_restore_round_trip_is_checksum_equal(pg_db, tmp_path):
    v = _write_csv(tmp_path / "v.csv")
    with pg_db.connect() as conn:
        _seed(conn)
        before = _snapshot(conn)
        imp.refresh_induction(conn, v, apply=True, backup_path=tmp_path / "b.csv")
        assert _snapshot(conn) != before
        dry = imp.restore_induction(conn, tmp_path / "b.csv")
        
        out = imp.restore_induction(conn, tmp_path / "b.csv", apply=True)
        assert out["applied"] == 6
        assert _snapshot(conn) == before                # NULLs and '' both restored exactly


@pytest.mark.pg
def test_import_csv_stores_flags_label_source_and_key_on_new_rows(pg_db, tmp_path):
    v = _write_csv(tmp_path / "v.csv")
    conn = pg_db.get_conn()  # InventoryConnection is not a context manager
    try:
        assert imp.import_csv(str(v), conn, years={2019}, append=True) == 1
        row = conn.execute(
            "SELECT forced_induction, t_charger, s_charger, source, natural_key FROM epa_master "
            "WHERE epa_vehicle_id = 1002").fetchone()
        assert tuple(row) == ("Turbocharged", "T", "", "epa_vehicles_csv", "vid:1002")
    finally:
        conn.close()
