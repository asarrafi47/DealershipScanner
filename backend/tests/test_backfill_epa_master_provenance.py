"""P10A.6: the epa_master provenance backfill.

The classification, key and collision rules run on plain dicts (no database). The ``pg``
tests seed a throwaway Postgres built from the migration chain.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from backend.catalog.epa_key import base_key
from backend.scripts import backfill_epa_master_provenance as bf

LEGACY_ROW = {"year": 2000, "make": "Acura", "model": "Integra", "trim": "Integra",
              "engine_description": "1.8L I4", "trany": "Automatic 4-spd", "drive": "Front-Wheel Drive",
              "displacement": 1.8, "cylinders": 4, "fuel_type": "Regular Gasoline"}


def test_classify_three_provenances_and_legacy_needs_a_positive_match():
    keys = {base_key(LEGACY_ROW)}
    rows = [
        {"id": 30000, "epa_vehicle_id": 111, **LEGACY_ROW},       # in the map wins over its vid
        {"id": 30001, "epa_vehicle_id": 222, **LEGACY_ROW},       # vid, not in the map
        {"id": 10, "epa_vehicle_id": None, **LEGACY_ROW},          # low id AND in DICTIONARY
    ]
    assert bf.classify(rows, {30000}, keys) == {
        30000: "dictionary_epa", 30001: "epa_vehicles_csv", 10: "legacy"}


def test_classify_refuses_a_row_matching_no_class():
    keys = {base_key(LEGACY_ROW)}
    rows = [
        {"id": 10, "epa_vehicle_id": None, **{**LEGACY_ROW, "model": "Not In Dictionary"}},
        {"id": 20007, "epa_vehicle_id": None, **LEGACY_ROW},       # in DICTIONARY but an interim id
    ]
    with pytest.raises(bf.Refused, match=r"2 row\(s\) match no provenance class.*\[10, 20007\]"):
        bf.classify(rows, set(), keys)


def test_dictionary_base_keys_reads_old_schema_headers(tmp_path: Path):
    (tmp_path / "2000_Acura_Integra_EPA.csv").write_text(
        "Year,Make,Model,Trim,engineOptions,transmissionOptions,drivetrainOptions,fuelType,bodyStyle,"
        "cylinders,displacement,mpg_city\n"
        "2000,Acura,Integra,Integra,1.8L I4,Automatic 4-spd,Front-Wheel Drive,Regular Gasoline,"
        "Subcompact Cars,4,1.8,21\n", encoding="utf-8")
    assert bf.dictionary_base_keys(tmp_path, min_files=1) == {base_key(LEGACY_ROW)}
    with pytest.raises(bf.Refused, match="fewer than --min-files"):
        bf.dictionary_base_keys(tmp_path, min_files=2)
    with pytest.raises(bf.Refused, match="not found"):
        bf.dictionary_base_keys(tmp_path / "gone", min_files=1)


def test_cli_without_a_database_url_is_refused(monkeypatch, tmp_path):
    monkeypatch.setattr("backend.db.inventory_pg.inventory_postgres_dsn", lambda: "")
    monkeypatch.setattr("backend.utils.project_env.load_project_dotenv", lambda *a, **k: None)
    assert bf.main(["--dictionary-dir", str(tmp_path), "--min-files", "1"]) == bf.EXIT_REFUSED


# --- Postgres --------------------------------------------------------------------------------

def _seed(conn, rows, live_ids=()):
    for r in rows:
        cols = ",".join(f'"{c}"' for c in r)
        conn.execute(f"INSERT INTO public.epa_master ({cols}) VALUES ({','.join(['%s'] * len(r))})",
                     tuple(r.values()))
    for lid in live_ids:
        conn.execute("INSERT INTO public.epa_master_dump_id_map (dump_id, live_id) VALUES (%s, %s)", (lid, lid))
    conn.commit()


def _rows(conn):
    return {r[0]: r[1:] for r in conn.execute(
        "SELECT id, source, natural_key FROM public.epa_master ORDER BY id").fetchall()}


def _fixture_rows():
    return [
        {"id": 5, **LEGACY_ROW},                                            # legacy
        {"id": 6, **LEGACY_ROW},                                            # legacy twin -> ordinal 2
        {"id": 30000, "epa_vehicle_id": 4001, "atv_type": "EV", "city_e": 100.0, **LEGACY_ROW,
         "model": "Bolt"},                                                  # dump, with vid
        {"id": 30001, **{**LEGACY_ROW, "model": "Dump NoVid"}},             # dump, no vid
        {"id": 30002, "epa_vehicle_id": 4002, **{**LEGACY_ROW, "model": "Csv"}},   # csv row
    ]


@pytest.mark.pg
def test_dry_run_classifies_everything_and_writes_nothing(pg_db):
    keys = {base_key(LEGACY_ROW)}
    with pg_db.connect() as conn:
        _seed(conn, _fixture_rows(), live_ids=(30000, 30001))
        stats = bf.backfill(conn, keys, check_activity=False)
        assert stats["dry_run"] and stats["classified"] == stats["targets"] == 5
        assert stats["per_source"] == {"legacy": 2, "dictionary_epa": 2, "epa_vehicles_csv": 1}
        assert set(_rows(conn).values()) == {(None, None)}


@pytest.mark.pg
def test_apply_stamps_three_provenances_and_a_rerun_is_a_noop(pg_db):
    keys = {base_key(LEGACY_ROW)}
    with pg_db.connect() as conn:
        _seed(conn, _fixture_rows(), live_ids=(30000, 30001))
        stats = bf.backfill(conn, keys, apply=True, check_activity=False)
        assert stats["applied"] == 5
        rows = _rows(conn)
        assert rows[5] == ("legacy", base_key(LEGACY_ROW) + "#1")
        assert rows[6] == ("legacy", base_key(LEGACY_ROW) + "#2")
        assert rows[30000] == ("dictionary_epa", "vid:4001")
        assert rows[30001][0] == "dictionary_epa" and rows[30001][1].endswith("#1")
        assert rows[30002] == ("epa_vehicles_csv", "vid:4002")
        again = bf.backfill(conn, keys, apply=True, check_activity=False)
        assert again["targets"] == 0 and again["applied"] == 0
        assert _rows(conn) == rows
        # the dump-carried columns survive
        assert conn.execute("SELECT atv_type, city_e FROM public.epa_master WHERE id = 30000").fetchone() == ("EV", 100.0)


@pytest.mark.pg
def test_unmatched_row_is_refused_with_no_writes(pg_db):
    keys = {base_key(LEGACY_ROW)}
    with pg_db.connect() as conn:
        _seed(conn, _fixture_rows() + [{"id": 30500, **{**LEGACY_ROW, "model": "Interim Insert"}}],
              live_ids=(30000, 30001))
        with pytest.raises(bf.Refused, match="match no provenance class"):
            bf.backfill(conn, keys, apply=True, check_activity=False)
        assert set(_rows(conn).values()) == {(None, None)}


@pytest.mark.pg
def test_collision_is_refused_with_no_writes(pg_db):
    keys = {base_key(LEGACY_ROW)}
    rows = [{"id": 30000, "epa_vehicle_id": 4001, **LEGACY_ROW},
            {"id": 30001, "epa_vehicle_id": 4001, **LEGACY_ROW}]       # same vid twice
    with pg_db.connect() as conn:
        _seed(conn, rows, live_ids=(30000, 30001))
        with pytest.raises(bf.Refused, match=r"1 \(source, natural_key\) collision.*vid:4001"):
            bf.backfill(conn, keys, apply=True, check_activity=False)
        assert set(_rows(conn).values()) == {(None, None)}


@pytest.mark.pg
def test_quiescence_guard_refuses_while_another_session_is_in_a_transaction(pg_db):
    keys = {base_key(LEGACY_ROW)}
    with pg_db.connect() as conn:
        _seed(conn, _fixture_rows(), live_ids=(30000, 30001))
        other = pg_db.connect()
        try:
            other.execute("SELECT 1")                     # leaves it idle in transaction
            with pytest.raises(bf.Refused, match="active or idle in transaction"):
                bf.backfill(conn, keys, apply=True)
        finally:
            other.close()
        assert set(_rows(conn).values()) == {(None, None)}
        for _ in range(50):                               # the server reaps the closed session asynchronously
            try:
                bf.check_quiescent(conn)
                break
            except bf.Refused:
                conn.rollback()
                time.sleep(0.1)
        stats = bf.backfill(conn, keys, apply=True)       # nobody else now
        assert stats["applied"] == 5


@pytest.mark.pg
def test_recompute_keeps_the_natural_key_and_only_restamps_source(pg_db):
    keys = {base_key(LEGACY_ROW)}
    with pg_db.connect() as conn:
        _seed(conn, _fixture_rows(), live_ids=(30000, 30001))
        bf.backfill(conn, keys, apply=True, check_activity=False)
        before = _rows(conn)
        conn.execute("UPDATE public.epa_master SET source = 'wrong' WHERE id = 5")
        conn.commit()
        assert bf.backfill(conn, keys, apply=True, check_activity=False)["targets"] == 0
        stats = bf.backfill(conn, keys, apply=True, recompute=True, check_activity=False)
        assert stats["applied"] == 5
        assert _rows(conn) == before
