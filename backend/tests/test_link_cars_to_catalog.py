"""Relink modes of ``link_cars_to_catalog.py`` (P10A.7, ``pg`` tier).

Each mode against a real migration-built Postgres: ``--dangling``, ``--include-inactive``,
``--clear-unresolved``, ``--vins-file``, ``--exclude-source``, the printed counts, and the
``relink_cars`` library entry. The default mode must behave as before.
"""
from __future__ import annotations

import pytest

from backend.scripts import link_cars_to_catalog as script

pytestmark = pytest.mark.pg

GONE = 999999  # an epa_master id with no row
LEGACY, GOOD = 500, 1001  # twins; the legacy row has the LOWER id, so it wins every tie


@pytest.fixture()
def env(pg_db, monkeypatch):
    monkeypatch.setattr("backend.enrichment.knowledge_engine.prime_vpic_cache", lambda vins: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.lookup_vpic_from_cache", lambda vin: {})
    with pg_db.connect() as conn:
        for id_, source in ((LEGACY, "dictionary_legacy_root"), (GOOD, "epa")):
            conn.execute(
                "INSERT INTO epa_master (id, year, make, model, trim, cylinders, displacement, drive, "
                "fuel_type, source) VALUES (%s, 2026, 'Toyota', 'Tundra', '4WD', 6, 3.4, "
                "'Four-Wheel Drive', 'Regular Gasoline', %s)",
                (id_, source),
            )
        conn.commit()
    tundra = dict(year=2026, make="Toyota", model="Tundra", trim="SR5", cylinders=6, engine_l=3.4,
                  engine_description="3.4L V6", drivetrain="4WD", fuel_type="Gasoline",
                  title="2026 Toyota Tundra SR5")
    ghost = dict(tundra, make="Lexus", model="Nonesuch", title="2026 Lexus Nonesuch")
    pg_db.add_cars([
        dict(tundra, vin="VIN-ACTIVE-DANGLING", epa_master_id=GONE),
        dict(tundra, vin="VIN-INACTIVE-DANGLING", epa_master_id=GONE, listing_active=0),
        dict(ghost, vin="VIN-ACTIVE-DANGLING-GHOST", epa_master_id=GONE),
        dict(ghost, vin="VIN-ACTIVE-LINKED-GHOST", epa_master_id=GOOD, epa_match_confidence=0.9,
             epa_match_method="old"),
        dict(tundra, vin="VIN-INACTIVE-STALE", epa_master_id=GOOD, epa_match_confidence=0.1,
             epa_match_method="old", listing_active=0),
        dict(tundra, vin="VIN-UNLINKED"),
    ])
    monkeypatch.setattr(script, "ensure_link_columns", lambda: None)
    return pg_db


def _links(env) -> dict[str, tuple]:
    with env.connect() as conn:
        return {
            r[0]: (r[1], r[2])
            for r in conn.execute(
                "SELECT vin, epa_master_id, epa_match_method FROM cars ORDER BY vin"
            ).fetchall()
        }


def _linked(env, vin: str):
    return _links(env)[vin][0]


def test_default_mode_backward_compatible(env) -> None:
    out = script.link_fleet(dry_run=False, only_missing=False)
    s = out["stats"]
    assert s["examined"] == 4  # active cars only
    assert s["linked"] == 2 and s["unlinked"] == 2
    assert s["cleared"] == 0 and s["skipped_catalog_error"] == 0
    links = _links(env)
    # Tie between twins: lowest id wins, so the legacy row is chosen without --exclude-source.
    assert links["VIN-ACTIVE-DANGLING"][0] == LEGACY
    assert links["VIN-UNLINKED"][0] == LEGACY
    # Unresolvable cars keep whatever they had; inactive cars are not touched.
    assert links["VIN-ACTIVE-DANGLING-GHOST"][0] == GONE
    assert links["VIN-ACTIVE-LINKED-GHOST"][0] == GOOD
    assert links["VIN-INACTIVE-DANGLING"][0] == GONE
    assert links["VIN-INACTIVE-STALE"][0] == GOOD


def test_dry_run_writes_nothing_and_reports(env) -> None:
    before = _links(env)
    out = script.link_fleet(dry_run=True, only_missing=False, dangling=True)
    assert _links(env) == before
    assert out["stats"]["changed"] == 2 and out["stats"]["cleared"] == 1


def test_dangling_repairs_inactive_and_active_and_clears_unresolvable(env) -> None:
    out = script.link_fleet(dry_run=False, only_missing=False, dangling=True,
                            exclude_sources=("dictionary_legacy_root",))
    links = _links(env)
    assert links["VIN-ACTIVE-DANGLING"][0] == GOOD
    assert links["VIN-INACTIVE-DANGLING"][0] == GOOD
    assert links["VIN-ACTIVE-DANGLING-GHOST"][0] is None
    # Not dangling: untouched even though inactive / unresolvable.
    assert links["VIN-INACTIVE-STALE"] == (GOOD, "old")
    assert links["VIN-ACTIVE-LINKED-GHOST"][0] == GOOD
    assert links["VIN-UNLINKED"][0] is None
    assert out["stats"]["examined"] == 3
    assert (out["stats"]["changed"], out["stats"]["cleared"]) == (2, 1)


def test_dangling_with_only_missing_adds_dangling_to_missing(env) -> None:
    out = script.link_fleet(dry_run=False, only_missing=True, dangling=True,
                            exclude_sources=("dictionary_legacy_root",))
    links = _links(env)
    assert links["VIN-UNLINKED"][0] == GOOD  # the missing one
    assert links["VIN-INACTIVE-DANGLING"][0] == GOOD  # and the inactive dangling one
    assert links["VIN-ACTIVE-DANGLING-GHOST"][0] is None
    assert links["VIN-INACTIVE-STALE"] == (GOOD, "old")
    assert out["stats"]["examined"] == 4


def test_include_inactive(env) -> None:
    script.link_fleet(dry_run=False, only_missing=False, include_inactive=True,
                      exclude_sources=("dictionary_legacy_root",))
    links = _links(env)
    assert links["VIN-INACTIVE-STALE"][0] == GOOD and links["VIN-INACTIVE-STALE"][1] != "old"
    assert links["VIN-INACTIVE-DANGLING"][0] == GOOD
    # Without --clear-unresolved the unresolvable cars still keep their links.
    assert links["VIN-ACTIVE-DANGLING-GHOST"][0] == GONE


def test_clear_unresolved(env) -> None:
    out = script.link_fleet(dry_run=False, only_missing=False, clear_unresolved=True)
    links = _links(env)
    assert links["VIN-ACTIVE-DANGLING-GHOST"][0] is None
    assert links["VIN-ACTIVE-LINKED-GHOST"][0] is None
    assert out["stats"]["cleared"] == 2
    # Inactive cars are outside this run.
    assert links["VIN-INACTIVE-DANGLING"][0] == GONE


def test_exclude_source_never_returns_legacy_id(env) -> None:
    out = script.link_fleet(dry_run=False, only_missing=False, include_inactive=True,
                            clear_unresolved=True, exclude_sources=("dictionary_legacy_root",))
    assert all(v[0] != LEGACY for v in _links(env).values())
    assert all(c["new"][0] != LEGACY for c in out["changes"])


def test_vins_file(env, tmp_path) -> None:
    f = tmp_path / "vins.txt"
    f.write_text("# a comment\nvin-inactive-dangling\n\nVIN-UNLINKED  # trailing\nVIN-NOT-THERE\n")
    vins = script.read_vins_file(str(f))
    assert vins == ("VIN-INACTIVE-DANGLING", "VIN-UNLINKED", "VIN-NOT-THERE")
    out = script.link_fleet(dry_run=False, only_missing=False, include_inactive=True, vins=vins,
                            exclude_sources=("dictionary_legacy_root",))
    links = _links(env)
    assert links["VIN-INACTIVE-DANGLING"][0] == GOOD and links["VIN-UNLINKED"][0] == GOOD
    assert links["VIN-ACTIVE-DANGLING"][0] == GONE  # not in the file
    assert out["stats"]["examined"] == 2


def test_unchanged_rows_are_not_rewritten(env) -> None:
    kw = dict(dry_run=False, only_missing=False, include_inactive=True,
              exclude_sources=("dictionary_legacy_root",))
    script.link_fleet(**kw)
    again = script.link_fleet(**kw)
    assert again["stats"]["changed"] == 0 and again["changes"] == []
    assert again["stats"]["unchanged"] == 4


def test_relink_cars_library(env) -> None:
    with env.connect() as conn:
        ids = {r[0]: r[1] for r in conn.execute("SELECT vin, id FROM cars").fetchall()}
    out = script.relink_cars(
        [ids["VIN-INACTIVE-DANGLING"], ids["VIN-ACTIVE-LINKED-GHOST"]],
        exclude_sources=("dictionary_legacy_root",),
    )
    links = _links(env)
    assert links["VIN-INACTIVE-DANGLING"][0] == GOOD
    assert links["VIN-ACTIVE-LINKED-GHOST"][0] is None
    assert links["VIN-ACTIVE-DANGLING"][0] == GONE  # not requested
    old = {c["id"]: c["old"][0] for c in out["changes"]}
    assert old == {ids["VIN-INACTIVE-DANGLING"]: GONE, ids["VIN-ACTIVE-LINKED-GHOST"]: GOOD}
    assert script.relink_cars([])["changes"] == []
