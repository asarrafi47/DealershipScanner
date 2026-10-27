"""P10B.1: the epa_master builder's loader and diff report (dry run only).

The loader and diff run on plain dicts. The ``pg`` tests seed a throwaway Postgres built from
the migration chain and check that the dry run reports and writes nothing.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from backend.scripts import build_epa_master_pg as bld

HEADER = ("Year,Make,Model,Trim,engineOptions,engineDisplay,forcedInduction,transmissionOptions,"
          "drivetrainOptions,fuelType,bodyStyle,cylinders,displacement,mpg_city,mpg_highway,"
          "mpg_combined,epaId,atvType,cityE,highwayE\n")


def _csv_line(model="Integra", trim="LS", vid="", city="21", hwy="29", disp="1.8", display="1.8L I4",
              fi="", atv="", city_e="", body="Subcompact Cars", eng="1.8L I4"):
    return (f"2000,Acura,{model},{trim},{eng},{display},{fi},Automatic 4-spd,Front-Wheel Drive,"
            f"Regular Gasoline,{body},4,{disp},{city},{hwy},24,{vid},{atv},{city_e},\n")


def _write(dirpath: Path, name: str, *lines: str) -> Path:
    p = dirpath / name
    p.write_text(HEADER + "".join(lines), encoding="utf-8")
    return p


def _db(**kw):
    base = {"id": 1, "epa_vehicle_id": None, "year": 2000, "make": "Acura", "model": "Integra",
            "trim": "LS", "engine_description": "1.8L I4", "trany": "Automatic 4-spd",
            "drive": "Front-Wheel Drive", "fuel_type": "Regular Gasoline",
            "body_style": "Subcompact Cars", "cylinders": 4, "displacement": 1.8, "city08": 21.0,
            "highway08": 29.0, "engine_display": "1.8L I4", "forced_induction": None,
            "city_e": None, "highway_e": None, "atv_type": None}
    base.update(kw)
    return base


def test_loader_maps_every_column(tmp_path):
    f = _write(tmp_path, "2000_Acura_Integra_EPA.csv",
               _csv_line(vid="4001", atv="Hybrid", city_e="55.5", fi="Turbocharged"),
               "2000,,Integra,LS,,,,,,,,,,,,,,,,\n")
    rows, bad = bld.load_catalog_rows([f])
    assert bad == 1 and len(rows) == 1
    r = rows[0]
    assert (r["epa_vehicle_id"], r["atv_type"], r["city_e"], r["forced_induction"]) == (4001, "Hybrid", 55.5, "Turbocharged")
    assert (r["city08"], r["highway08"], r["displacement"], r["cylinders"]) == (21.0, 29.0, 1.8, 4)
    assert r["engine_display"] == "1.8L I4" and r["body_style"] == "Subcompact Cars"


def test_diff_classifies_insert_update_unchanged_missing(tmp_path):
    f = _write(tmp_path, "a_EPA.csv",
               _csv_line(trim="LS"),                              # unchanged
               _csv_line(trim="GS", city="23"),                   # MPG changed
               _csv_line(trim="Type R"))                          # new
    csv_rows, _ = bld.load_catalog_rows([f])
    db_rows = [_db(id=1, trim="LS"), _db(id=2, trim="GS"), _db(id=3, trim="Gone")]
    d = bld.diff_catalog(csv_rows, db_rows)
    rep = bld.build_report(csv_rows, db_rows, d)
    assert (rep["insert"], rep["update"], rep["unchanged"], rep["missing"]) == (1, 1, 1, 1)
    assert rep["update_by_column"] == {"city08": 1}
    assert rep["sample_update"][0]["id"] == 2 and rep["sample_update"][0]["changes"] == {"city08": [21.0, 23.0]}
    assert rep["sample_missing"] == [{"id": 3, "row": "2000 Acura Integra Gone"}]


def test_blank_csv_never_erases_a_stored_value_but_a_label_change_counts(tmp_path):
    f = _write(tmp_path, "a_EPA.csv", _csv_line(display="", atv="", city_e="", fi="Turbocharged"))
    csv_rows, _ = bld.load_catalog_rows([f])
    db_rows = [_db(engine_display="1.8L I4", atv_type="Hybrid", city_e=40.0, forced_induction=None)]
    d = bld.diff_catalog(csv_rows, db_rows)
    assert [set(ch) for _, _, ch in d["updates"]] == [{"forced_induction"}]


def test_vid_pairs_before_values_so_an_mpg_correction_keeps_its_row(tmp_path):
    f = _write(tmp_path, "a_EPA.csv", _csv_line(vid="4001", city="25"), _csv_line(vid="4002", city="10"))
    csv_rows, _ = bld.load_catalog_rows([f])
    # same base key, ids cross value order: the id decides, not the closest MPG
    db_rows = [_db(id=7, epa_vehicle_id=4002, city08=25.0), _db(id=8, epa_vehicle_id=4001, city08=10.0)]
    d = bld.diff_catalog(csv_rows, db_rows)
    got = {db_rows[j]["id"]: ch for _, j, ch in d["updates"]}
    assert got == {7: {"city08": (25.0, 10.0)}, 8: {"city08": (10.0, 25.0)}}


def test_duplicate_base_key_pairs_by_value_not_position(tmp_path):
    f = _write(tmp_path, "a_EPA.csv", _csv_line(city="30"), _csv_line(city="18"))
    csv_rows, _ = bld.load_catalog_rows([f])
    db_rows = [_db(id=1, city08=18.0), _db(id=2, city08=30.0)]
    d = bld.diff_catalog(csv_rows, db_rows)
    assert d["unchanged"] == 2 and not d["updates"] and not d["insert"] and not d["missing"]


def test_mode_resolution_and_refusals():
    assert bld.resolve_mode() == "dry_run"
    assert bld.resolve_mode(dry_run=True) == "dry_run"
    assert bld.resolve_mode(apply=True, archive_missing=True) == "apply"
    for kw in ({"rebuild": True}, {"rebuild": True, "apply": True}, {"apply": True, "dry_run": True},
               {"archive_missing": True}, {"allow_large_update": True}):
        with pytest.raises(bld.BuildRefused):
            bld.resolve_mode(**kw)
    assert bld.main(["--rebuild"]) == bld.EXIT_REFUSED


def test_guard_violations():
    assert bld.guard_violations(100, 100, 5) == []
    assert "under 90%" in bld.guard_violations(89, 100, 0)[0]
    assert "exceed 5%" in bld.guard_violations(100, 100, 6)[0]
    assert bld.guard_violations(100, 100, 6, allow_large_update=True) == []


def test_update_assignments_never_null_and_atv_set_only_when_null():
    ch = {"atv_type": ("Hybrid", "FFV"), "city_e": (55.0, None), "engine_display": ("2.0L", ""), "city08": (20.0, 22.0)}
    assert bld._update_assignments(ch, {}) == {"city08": 22.0}
    assert bld._update_assignments({"atv_type": (None, "FFV")}, {}) == {"atv_type": "FFV"}


# --- Postgres --------------------------------------------------------------------------------

def _seed(conn, rows):
    for r in rows:
        cols = ",".join(f'"{c}"' for c in r)
        conn.execute(f"INSERT INTO public.epa_master ({cols}) VALUES ({','.join(['%s'] * len(r))})",
                     tuple(r.values()))
    conn.commit()


@pytest.mark.pg
def test_dry_run_reports_the_diff_and_writes_nothing(pg_db, tmp_path):
    _write(tmp_path, "a_EPA.csv", _csv_line(trim="LS"), _csv_line(trim="GS", city="23"), _csv_line(trim="Type R"))
    rows = [{k: v for k, v in _db(id=i, trim=t).items() if k not in ("id",)} | {"id": i, "source": "dictionary_epa",
            "natural_key": f"k{i}"} for i, t in ((1, "LS"), (2, "GS"), (3, "Gone"))]
    with pg_db.connect() as conn:
        _seed(conn, rows)
        before = conn.execute("SELECT id, city08, source FROM public.epa_master ORDER BY id").fetchall()
    stats = bld.build_epa_master(dry_run=True, source_dir=tmp_path, min_files=1)
    assert (stats["insert"], stats["update"], stats["unchanged"], stats["missing"]) == (1, 1, 1, 1)
    assert stats["would_insert"] == 1 and stats["skipped_existing"] == 2
    with pg_db.connect() as conn:
        assert conn.execute("SELECT id, city08, source FROM public.epa_master ORDER BY id").fetchall() == before


@pytest.mark.pg
def test_dry_run_without_provenance_is_refused(pg_db, tmp_path):
    _write(tmp_path, "a_EPA.csv", _csv_line())
    with pg_db.connect() as conn:
        _seed(conn, [{k: v for k, v in _db().items() if k != "id"} | {"id": 1}])
    with pytest.raises(bld.BuildRefused, match="provenance backfill"):
        bld.build_epa_master(dry_run=True, source_dir=tmp_path, min_files=1)


# --- P10B.2 apply (Postgres) -------------------------------------------------------------------

def _prov_row(i, trim, **kw):
    r = {k: v for k, v in _db(id=i, trim=trim, **kw).items()}
    r.update(source="dictionary_epa", natural_key=f"2000|acura|integra|{trim.lower()}#1")
    return r


def _snapshot(conn):
    return conn.execute("SELECT id, city08, natural_key, source, atv_type FROM public.epa_master ORDER BY id").fetchall()


def _apply(tmp_path, **kw):
    return bld.build_epa_master(apply=True, source_dir=tmp_path, min_files=1, **kw)


@pytest.mark.pg
def test_apply_matches_dry_run_and_second_run_is_noop(pg_db, tmp_path):
    _write(tmp_path, "a_EPA.csv", _csv_line(trim="LS"), _csv_line(trim="GS", city="23"),
           _csv_line(trim="Type R", fi="Turbocharged"), *[_csv_line(trim=f"X{i}") for i in range(30)])
    seed = [_prov_row(1, "LS"), _prov_row(2, "GS")] + [_prov_row(10 + i, f"X{i}") for i in range(30)]
    with pg_db.connect() as conn:
        _seed(conn, seed)
    dry = bld.build_epa_master(dry_run=True, source_dir=tmp_path, min_files=1)
    assert (dry["insert"], dry["update"], dry["missing"]) == (1, 1, 0)
    res = _apply(tmp_path)
    assert res["applied"]["inserted"] == 1 and res["applied"]["updated"] == 1
    with pg_db.connect() as conn:
        rows = {r[0]: r for r in _snapshot(conn)}
        assert rows[2][1] == 23.0 and rows[1][1] == 21.0              # same id updated
        new = conn.execute("SELECT id, natural_key, source, forced_induction, loaded_at IS NOT NULL "
                           "FROM public.epa_master WHERE trim = 'Type R'").fetchone()
        assert new[1] == "2000|acura|integra|type r#1" or new[1].endswith("#1")
        assert new[2:] == ("dictionary_epa", "Turbocharged", True)
        before = _snapshot(conn)
    again = _apply(tmp_path)
    assert (again["insert"], again["update"], again["missing"]) == (0, 0, 0)
    with pg_db.connect() as conn:
        assert _snapshot(conn) == before


@pytest.mark.pg
def test_apply_label_fallback_and_display_loaded(pg_db, tmp_path):
    _write(tmp_path, "a_EPA.csv", _csv_line(trim="LS"), _csv_line(trim="Turbo", eng="2.0L I4 Turbo", display="2.0L I4 Turbo"),
           *[_csv_line(trim=f"X{i}") for i in range(30)])
    with pg_db.connect() as conn:
        _seed(conn, [_prov_row(1, "LS")] + [_prov_row(10 + i, f"X{i}") for i in range(30)])
    _apply(tmp_path)
    with pg_db.connect() as conn:
        fi, disp = conn.execute("SELECT forced_induction, engine_display FROM public.epa_master "
                                "WHERE trim = 'Turbo'").fetchone()
    assert fi == "Turbocharged" and disp == "2.0L I4 Turbo"


def _link_car(conn, master_id):
    conn.execute("INSERT INTO public.cars (vin, make, model, year, epa_master_id) VALUES (%s, 'Acura', 'Integra', 2000, %s)",
                 (f"VIN{master_id:014d}", master_id))
    conn.commit()


@pytest.mark.pg
def test_missing_row_with_a_car_is_kept_and_reported_archive_touches_only_unreferenced(pg_db, tmp_path):
    keep = [_prov_row(10 + i, f"X{i}") for i in range(30)]
    _write(tmp_path, "a_EPA.csv", *[_csv_line(trim=f"X{i}") for i in range(30)])
    with pg_db.connect() as conn:
        _seed(conn, keep + [_prov_row(1, "Linked"), _prov_row(2, "Specced"), _prov_row(3, "Free")])
        _link_car(conn, 1)
        conn.execute("INSERT INTO public.epa_extended_specs (epa_master_id, year, make, model) "
                     "VALUES (2, 2000, 'Acura', 'Integra')")
        conn.commit()
    # without --archive-missing nothing is removed
    res = _apply(tmp_path)
    assert res["applied"]["archived"] == 0 and len(res["applied"]["kept_referenced"]) == 2
    res = _apply(tmp_path, archive_missing=True)
    assert res["applied"]["archived"] == 1
    assert {k["id"] for k in res["applied"]["kept_referenced"]} == {1, 2}
    with pg_db.connect() as conn:
        ids = {r[0] for r in conn.execute("SELECT id FROM public.epa_master")}
        assert {1, 2} <= ids and 3 not in ids
        arch = conn.execute("SELECT id, archive_reason, natural_key FROM public.epa_master_archive").fetchall()
        assert [a[0] for a in arch] == [3] and arch[0][1] and arch[0][2]
        assert conn.execute("SELECT count(*) FROM public.epa_extended_specs").fetchone()[0] == 1
        assert conn.execute("SELECT epa_master_id FROM public.cars").fetchone()[0] == 1


@pytest.mark.pg
def test_atv_type_never_downgraded_and_set_only_when_null(pg_db, tmp_path):
    _write(tmp_path, "a_EPA.csv", _csv_line(trim="LS", atv="FFV", city="22"), _csv_line(trim="GS", atv="", city="22"),
           _csv_line(trim="SE", atv="FFV", city="22"), *[_csv_line(trim=f"X{i}") for i in range(30)])
    with pg_db.connect() as conn:
        _seed(conn, [_prov_row(1, "LS", atv_type="Hybrid"), _prov_row(2, "GS", atv_type="Hybrid"),
                     _prov_row(3, "SE", atv_type=None)] + [_prov_row(10 + i, f"X{i}") for i in range(30)])
    _apply(tmp_path, allow_large_update=True)
    with pg_db.connect() as conn:
        got = dict(conn.execute("SELECT id, atv_type FROM public.epa_master WHERE id <= 3").fetchall())
        assert got == {1: "Hybrid", 2: "Hybrid", 3: "FFV"}
        assert conn.execute("SELECT city08 FROM public.epa_master WHERE id = 1").fetchone()[0] == 22.0


@pytest.mark.pg
def test_apply_guards_refuse_and_write_nothing(pg_db, tmp_path):
    _write(tmp_path, "a_EPA.csv", *[_csv_line(trim=f"X{i}", city="30") for i in range(10)])
    with pg_db.connect() as conn:
        _seed(conn, [_prov_row(10 + i, f"X{i}") for i in range(10)] + [_prov_row(50 + i, f"Y{i}") for i in range(10)])
        before = _snapshot(conn)
    with pytest.raises(bld.BuildRefused, match="under 90%"):         # 10 CSV rows vs 20 stored
        _apply(tmp_path)
    _write(tmp_path, "b_EPA.csv", *[_csv_line(trim=f"Y{i}") for i in range(10)])
    with pytest.raises(bld.BuildRefused, match="--allow-large-update"):   # 10 of 20 change
        _apply(tmp_path)
    with pg_db.connect() as conn:
        assert _snapshot(conn) == before


@pytest.mark.pg
def test_apply_refuses_without_v036_columns_and_rolls_back_on_broken_invariant(pg_db, tmp_path, monkeypatch):
    _write(tmp_path, "a_EPA.csv", _csv_line(trim="LS", city="19"), *[_csv_line(trim=f"X{i}") for i in range(30)])
    with pg_db.connect() as conn:
        _seed(conn, [_prov_row(1, "LS")] + [_prov_row(10 + i, f"X{i}") for i in range(30)])
        before = _snapshot(conn)
    real = bld.apply_catalog

    def _corrupt(conn, *a, **kw):
        real(conn, *a, **kw)
        conn.cursor().execute("UPDATE public.epa_master SET natural_key = 'dup' WHERE id IN (1, 10)")
        return _dup(conn)

    def _dup(conn):
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM (SELECT 1 FROM public.epa_master GROUP BY source, natural_key "
                    "HAVING count(*) > 1) t")
        if cur.fetchone()[0]:
            raise bld.BuildRefused("apply rolled back: duplicate (source, natural_key)")
        return {}

    monkeypatch.setattr(bld, "apply_catalog", _corrupt)
    with pytest.raises(bld.BuildRefused, match="rolled back"):
        _apply(tmp_path)
    with pg_db.connect() as conn:
        assert _snapshot(conn) == before                              # the UPDATE of id 1 rolled back too
    monkeypatch.setattr(bld, "apply_catalog", real)
    with pg_db.connect() as conn:
        conn.execute("ALTER TABLE public.epa_master DROP COLUMN s_charger")
        conn.commit()
    with pytest.raises(bld.BuildRefused, match="V036"):
        _apply(tmp_path)
