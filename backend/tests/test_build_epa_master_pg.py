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


def test_writes_are_still_refused():
    with pytest.raises(bld.BuildRefused):
        bld.refuse_writes(dry_run=False, rebuild=False)
    with pytest.raises(bld.BuildRefused):
        bld.refuse_writes(dry_run=True, rebuild=True)
    assert bld.main([]) == bld.EXIT_REFUSED


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
