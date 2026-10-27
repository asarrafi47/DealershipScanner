"""Legacy catalog re-resolve (P10B.6, ``pg`` tier): ``reresolve_legacy_catalog_links.py``.

Legacy rows are excluded from every resolution, each moved car lands in the snapshot table,
unreferenced legacy rows are archived under their original ids, referenced ones are kept, the
dry run writes nothing, and ``--undo`` restores links and rows exactly.
"""
from __future__ import annotations

import pytest

from backend.scripts import link_cars_to_catalog as linker_script
from backend.scripts import reresolve_legacy_catalog_links as script

pytestmark = pytest.mark.pg

SRC = "dictionary_legacy_root"
L_TUNDRA, L_GHOST, L_FREE, L_SPEC, GOOD = 500, 501, 502, 503, 1001
_ROW = dict(year=2026, make="Toyota", model="Tundra", trim="4WD", cylinders=6, displacement=3.4,
            drive="Four-Wheel Drive", fuel_type="Regular Gasoline")


@pytest.fixture()
def env(pg_db, monkeypatch):
    monkeypatch.setattr("backend.enrichment.knowledge_engine.prime_vpic_cache", lambda vins: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.lookup_vpic_from_cache", lambda vin: {})
    cols = "id, year, make, model, trim, cylinders, displacement, drive, fuel_type, atv_type, source"
    with pg_db.connect() as conn:
        def put(id_, source, atv=None, **over):
            r = dict(_ROW, **over)
            conn.execute(
                f"INSERT INTO epa_master ({cols}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (id_, r["year"], r["make"], r["model"], r["trim"], r["cylinders"], r["displacement"],
                 r["drive"], r["fuel_type"], atv, source))
        put(L_TUNDRA, SRC, atv="Hybrid")               # has a content twin (GOOD)
        put(GOOD, "dictionary_epa")
        put(L_GHOST, SRC, make="Lexus", model="Nonesuch")  # nothing else resolves for it
        put(L_FREE, SRC, trim="Unreferenced")          # no car, no twin -> archived
        put(L_SPEC, SRC, trim="Specced")               # extended specs hang off it -> kept
        conn.execute("INSERT INTO epa_extended_specs (epa_master_id, year, make, model) "
                     "VALUES (%s, 2026, 'Toyota', 'Tundra')", (L_SPEC,))
        conn.commit()
    tundra = dict(year=2026, make="Toyota", model="Tundra", trim="SR5", cylinders=6, engine_l=3.4,
                  engine_description="3.4L V6", drivetrain="4WD", fuel_type="Gasoline",
                  title="2026 Toyota Tundra SR5")
    ghost = dict(tundra, make="Lexus", model="Nonesuch", title="2026 Lexus Nonesuch")
    pg_db.add_cars([
        dict(tundra, vin="VIN-A", epa_master_id=L_TUNDRA, epa_match_confidence=0.8, epa_match_method="m1"),
        dict(tundra, vin="VIN-B-INACTIVE", epa_master_id=L_TUNDRA, epa_match_confidence=0.7,
             epa_match_method="m2", listing_active=0),
        dict(ghost, vin="VIN-GHOST", epa_master_id=L_GHOST, epa_match_confidence=0.9, epa_match_method="m3"),
        dict(tundra, vin="VIN-ELSEWHERE", epa_master_id=GOOD, epa_match_confidence=0.6, epa_match_method="m4"),
    ])
    return pg_db


def _state(env):
    with env.connect() as conn:
        links = conn.execute("SELECT vin, epa_master_id, epa_match_confidence, epa_match_method "
                             "FROM cars ORDER BY vin").fetchall()
        master = conn.execute("SELECT * FROM epa_master ORDER BY id").fetchall()
        archive = conn.execute("SELECT id, archive_reason, superseded_by FROM epa_master_archive ORDER BY id").fetchall()
    return links, master, archive


def _links(env) -> dict:
    return {r[0]: r[1:] for r in _state(env)[0]}


def _run(env, **kw):
    return script.run(apply=kw.pop("apply", False), source=None, run_id=kw.pop("run_id", "t1"), **kw)


def test_dry_run_writes_nothing_and_reports(env) -> None:
    before = _state(env)
    rep = _run(env)
    assert _state(env) == before
    assert rep["source"] == SRC
    t = rep["totals"]
    assert t["cars_examined"] == 3 and t["relinked"] == 2 and t["cleared"] == 1
    assert t["landed_on_twin"] == 2          # both Tundra cars land on GOOD, L_TUNDRA's content twin
    assert t["to_archive"] == 3              # L_TUNDRA and L_GHOST (cars move away), L_FREE
    assert rep["atv_transitions"] == {"Hybrid -> -": 2}
    assert {(k["id"], k["reason"]) for k in rep["kept"]} == {(L_SPEC, "extended_specs")}
    with env.connect() as conn:
        assert conn.execute("SELECT count(*) FROM epa_relink_snapshot").fetchone()[0] == 0


def test_apply_excludes_legacy_snapshots_and_archives(env) -> None:
    rep = _run(env, apply=True)
    links = _links(env)
    assert links["VIN-A"][0] == GOOD and links["VIN-B-INACTIVE"][0] == GOOD
    assert links["VIN-GHOST"] == (None, None, None)
    assert links["VIN-ELSEWHERE"] == (GOOD, 0.6, "m4")  # not a legacy-linked car: untouched
    assert rep["totals"]["applied_moved"] == 3
    with env.connect() as conn:
        snap = conn.execute("SELECT car_id, old_id, new_id, old_confidence, old_method FROM epa_relink_snapshot "
                            "WHERE run_id='t1' ORDER BY old_id, car_id").fetchall()
        assert [(s[1], s[2]) for s in snap] == [(L_TUNDRA, GOOD), (L_TUNDRA, GOOD), (L_GHOST, None)]
        assert {s[3] for s in snap} == {0.8, 0.7, 0.9}
        live = {r[0] for r in conn.execute("SELECT id FROM epa_master").fetchall()}
        arch = conn.execute("SELECT id, archive_reason, superseded_by FROM epa_master_archive ORDER BY id").fetchall()
    # a referenced row (extended specs) is never deleted; unreferenced legacy rows are archived with their ids
    assert L_SPEC in live and GOOD in live
    assert not live & {L_TUNDRA, L_GHOST, L_FREE}
    assert arch == [(L_TUNDRA, "legacy_root", GOOD), (L_GHOST, "legacy_root", None), (L_FREE, "legacy_root", None)]
    # nothing dangles
    with env.connect() as conn:
        assert conn.execute("SELECT count(*) FROM cars c WHERE epa_master_id IS NOT NULL AND NOT EXISTS "
                            "(SELECT 1 FROM epa_master m WHERE m.id=c.epa_master_id)").fetchone()[0] == 0


def test_rerun_with_same_run_id_resumes_with_no_new_work(env) -> None:
    _run(env, apply=True)
    after = _state(env)
    rep = _run(env, apply=True)
    assert _state(env) == after
    assert rep["totals"].get("applied_moved", 0) == 0 and rep["totals"].get("applied_archived", 0) == 0


def test_undo_round_trip_is_exact(env) -> None:
    before = _state(env)
    _run(env, apply=True)
    assert _state(env) != before
    dry = script.undo("t1", apply=False)
    assert dry["rows_restored"] == 3 and dry["cars_restored"] == 3
    assert _links(env)["VIN-A"][0] == GOOD  # a dry undo changes nothing
    out = script.undo("t1", apply=True)
    assert out["rows_restored"] == 3 and out["cars_restored"] == 3 and out["cars_moved_since"] == 0
    assert _state(env) == before
    with env.connect() as conn:  # stamped: a second undo has nothing to do
        assert conn.execute("SELECT count(*) FROM epa_relink_snapshot WHERE undone_at IS NULL").fetchone()[0] == 0
    with pytest.raises(script.Refused):
        script.undo("t1", apply=True)


def test_undo_leaves_a_car_a_later_scan_moved(env) -> None:
    _run(env, apply=True)
    with env.connect() as conn:
        conn.execute("UPDATE cars SET epa_master_id=NULL WHERE vin='VIN-A'")
        conn.commit()
    out = script.undo("t1", apply=True)
    assert out["cars_moved_since"] == 1 and out["cars_restored"] == 2
    assert _links(env)["VIN-A"][0] is None
    assert _links(env)["VIN-B-INACTIVE"][0] == L_TUNDRA


def test_make_filter_limits_the_run(env) -> None:
    _run(env, apply=True, makes=["lexus"])
    links = _links(env)
    assert links["VIN-GHOST"][0] is None and links["VIN-A"][0] == L_TUNDRA


def test_refuses_when_every_row_is_legacy(env) -> None:
    with env.connect() as conn:
        conn.execute("UPDATE epa_master SET source='dictionary_legacy_root' WHERE id=%s", (GOOD,))
        conn.commit()
    before = _state(env)
    with pytest.raises(script.Refused):
        _run(env, apply=True)
    assert _state(env) == before
    assert script.main(["--run-id", "x"]) == script.EXIT_REFUSED


def test_label_detection_accepts_the_backfill_spelling(env) -> None:
    with env.connect() as conn:
        conn.execute("UPDATE epa_master SET source='legacy' WHERE source='dictionary_legacy_root'")
        conn.commit()
    assert _run(env)["source"] == "legacy"
    _run(env, apply=True)
    with env.connect() as conn:
        assert conn.execute("SELECT DISTINCT archive_reason FROM epa_master_archive").fetchall() == [("legacy_root",)]


def test_lookup_exclude_sources_and_unlinked_report(env) -> None:
    from backend.enrichment import knowledge_engine as ke

    with env.connect() as conn:
        conn.execute("INSERT INTO epa_master (id, year, make, model, trim, cylinders, source) "
                     "VALUES (700, 2026, 'Zorg', 'Zed', 'Base', 4, %s)", (SRC,))
        conn.execute("INSERT INTO epa_master (id, year, make, model, trim, cylinders, source) "
                     "VALUES (701, 2026, 'Zorg', 'Zed', 'Base', 6, 'dictionary_epa')")
        conn.commit()
    env.add_cars([dict(year=2026, make="Zorg", model="Zed", trim="Base", vin="VIN-ZORG")])
    ke.clear_epa_trim_lookup_cache()
    assert ke.lookup_epa_by_trim(2026, "Zorg", "Zed", "Base", exclude_sources=(SRC,))["cylinders"] == 6
    assert ke.lookup_epa_by_trim(2026, "Zorg", "Zed", "Base", exclude_sources=("dictionary_epa",))["cylinders"] == 4
    conn = env.get_conn()
    try:
        rep = script.unlinked_lookup_report(conn.cursor(), SRC, "zorg")
    finally:
        conn.close()
    assert rep["unlinked_cars_in_groups"] == 1
    assert rep["cars_changed"] in (0, 1)  # LIMIT 1 with no ORDER BY: either may be first before
    # the dry run reports it per make as well
    assert "unlinked_lookup" in _run(env)["makes"]["zorg"]


def test_relink_library_is_what_the_run_calls(env, monkeypatch) -> None:
    seen = {}
    real = linker_script.relink_cars

    def spy(ids, exclude_sources=(), **kw):
        seen.setdefault("calls", []).append((tuple(exclude_sources), kw.get("dry_run")))
        return real(ids, exclude_sources, **kw)

    monkeypatch.setattr(script, "relink_cars", spy)
    _run(env)
    assert seen["calls"] and all(c == ((SRC,), True) for c in seen["calls"])


# --- P10B.5: --source epa_vehicles_csv ---------------------------------------------------------

CSV_SRC = "epa_vehicles_csv"
CSV_ROW, CSV_FREE, DICT_TWIN = 600, 601, 1002


@pytest.fixture()
def csv_env(env):
    with env.connect() as conn:
        conn.execute("DELETE FROM epa_master WHERE source=%s", (SRC,))
        conn.execute("UPDATE cars SET epa_master_id=NULL, epa_match_confidence=NULL, epa_match_method=NULL")
        cols = "id, year, make, model, trim, cylinders, displacement, drive, fuel_type, source"
        for id_, src, trim in ((CSV_ROW, CSV_SRC, "4WD"), (CSV_FREE, CSV_SRC, "Unreferenced"),
                               (DICT_TWIN, "dictionary_epa", "4WD")):
            conn.execute(f"INSERT INTO epa_master ({cols}) VALUES (%s,2026,'Toyota','Tundra',%s,6,3.4,"
                         "'Four-Wheel Drive','Regular Gasoline',%s)", (id_, trim, src))
        conn.execute("UPDATE cars SET epa_master_id=%s, epa_match_confidence=0.5, epa_match_method='m5' "
                     "WHERE vin IN ('VIN-A','VIN-B-INACTIVE')", (CSV_ROW,))
        conn.commit()
    return env


def _csv_run(env, **kw):
    return script.run(apply=kw.pop("apply", False), source=CSV_SRC, run_id=kw.pop("run_id", "c1"), **kw)


def test_csv_source_relinks_to_dictionary_twin_and_archives(csv_env) -> None:
    rep = _csv_run(csv_env)
    assert rep["totals"]["relinked"] == 2 and rep["totals"]["to_archive"] == 2
    _csv_run(csv_env, apply=True)
    links = _links(csv_env)
    assert links["VIN-A"][0] in (GOOD, DICT_TWIN) and links["VIN-B-INACTIVE"][0] in (GOOD, DICT_TWIN)
    with csv_env.connect() as conn:
        assert conn.execute("SELECT id, archive_reason FROM epa_master_archive ORDER BY id").fetchall() == [
            (CSV_ROW, CSV_SRC), (CSV_FREE, CSV_SRC)]


def test_csv_source_refuses_without_dictionary_year_coverage(csv_env) -> None:
    with csv_env.connect() as conn:
        conn.execute("UPDATE epa_master SET year=2027 WHERE source=%s", (CSV_SRC,))
        conn.commit()
    before = _state(csv_env)
    with pytest.raises(script.Refused, match="2027"):
        _csv_run(csv_env, apply=True)
    assert _state(csv_env) == before


def test_csv_source_undo_round_trips(csv_env) -> None:
    before = _state(csv_env)
    _csv_run(csv_env, apply=True)
    assert _state(csv_env) != before
    out = script.undo("c1", apply=True)
    assert out["rows_restored"] == 2 and out["cars_restored"] == 2
    assert _state(csv_env) == before
