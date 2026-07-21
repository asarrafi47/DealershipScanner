"""The migration chain is only trustworthy if order and immutability hold."""

from pathlib import Path

from backend.scripts import migrate


def _write(directory: Path, filename: str, sql: str = "SELECT 1;\n") -> Path:
    path = directory / filename
    path.write_text(sql, encoding="utf-8")
    return path


def test_migrations_apply_in_numeric_not_lexical_order(tmp_path):
    """Sorted as text, V010 lands before V9 and the chain runs in an order no
    database has ever seen."""
    for name in ("V009__nine.sql", "V010__ten.sql", "V002__two.sql", "V001__one.sql"):
        _write(tmp_path, name)
    versions = [m.version for m in migrate.discover_migrations(tmp_path)]
    assert versions == [1, 2, 9, 10]


def test_non_migration_files_are_ignored(tmp_path):
    _write(tmp_path, "V001__one.sql")
    _write(tmp_path, "rollback_notes.sql")
    _write(tmp_path, "V2_missing_double_underscore.sql")
    found = migrate.discover_migrations(tmp_path)
    assert [m.path.name for m in found] == ["V001__one.sql"]


def test_duplicate_version_is_refused(tmp_path):
    import pytest

    _write(tmp_path, "V003__add_index.sql")
    _write(tmp_path, "V3__add_index_again.sql")
    with pytest.raises(SystemExit) as exc:
        migrate.discover_migrations(tmp_path)
    assert "duplicate migration version 3" in str(exc.value)


def test_only_unapplied_versions_are_pending(tmp_path):
    _write(tmp_path, "V001__one.sql")
    _write(tmp_path, "V002__two.sql")
    _write(tmp_path, "V003__three.sql")
    available = migrate.discover_migrations(tmp_path)
    applied = {m.version: m.checksum for m in available[:2]}

    plan = migrate.plan_migrations(available, applied)
    assert [m.version for m in plan.pending] == [3]
    assert not plan.blocked


def test_editing_an_applied_migration_blocks_the_run(tmp_path):
    """
    Every database that already ran the old text has the old schema while
    reporting the same version, so this must stop the run, not be re-applied.
    """
    path = _write(tmp_path, "V001__one.sql", "CREATE TABLE foo (id INT);\n")
    applied = {1: migrate.discover_migrations(tmp_path)[0].checksum}

    path.write_text("CREATE TABLE foo (id BIGINT);\n", encoding="utf-8")
    plan = migrate.plan_migrations(migrate.discover_migrations(tmp_path), applied)

    assert plan.blocked
    assert not plan.pending, "a drifted migration must never be queued for re-run"
    drifted, recorded = plan.drifted[0]
    assert drifted.version == 1
    assert recorded != drifted.checksum


def test_checksum_ignores_line_ending_style(tmp_path):
    """A CRLF checkout is not someone editing an applied migration."""
    lf = _write(tmp_path, "V001__one.sql", "CREATE TABLE foo (id INT);\n")
    unix_sum = migrate.discover_migrations(tmp_path)[0].checksum

    lf.write_bytes(b"CREATE TABLE foo (id INT);\r\n")
    assert migrate.discover_migrations(tmp_path)[0].checksum == unix_sum


def test_new_file_below_the_high_water_mark_blocks_the_run(tmp_path):
    """
    V002 appearing after V005 applied means a rebuilt database runs the chain in
    a different order than production did.
    """
    _write(tmp_path, "V001__one.sql")
    _write(tmp_path, "V005__five.sql")
    available = migrate.discover_migrations(tmp_path)
    applied = {m.version: m.checksum for m in available}

    _write(tmp_path, "V002__late_arrival.sql")
    plan = migrate.plan_migrations(migrate.discover_migrations(tmp_path), applied)

    assert plan.blocked
    assert [m.version for m in plan.out_of_order] == [2]


def test_applied_version_with_no_file_is_reported_but_not_blocking(tmp_path):
    """The database is still the sum of what ran; it just can't be rebuilt."""
    _write(tmp_path, "V001__one.sql")
    available = migrate.discover_migrations(tmp_path)
    applied = {1: available[0].checksum, 2: "sha256:deleted"}

    plan = migrate.plan_migrations(available, applied)
    assert plan.missing == [2]
    assert not plan.blocked


def test_parse_filename_shape():
    assert migrate.parse_filename("V007__add_cars_epa_master_id.sql") == (
        7, "add_cars_epa_master_id",
    )
    assert migrate.parse_filename("V007_add_cars.sql") is None
    assert migrate.parse_filename("baseline.sql") is None


def test_baseline_strips_psql_only_meta_commands():
    """
    pg_dump >= 17.6 wraps plain output in \\restrict/\\unrestrict, and a 17.x
    client emits SET transaction_timeout even against an older server. The chain
    runs through psycopg, not psql, so both are fatal in a baseline.
    """
    from backend.scripts.dump_baseline_schema import _strip_psql_only

    dump = (
        "-- PostgreSQL database dump\n"
        "\\restrict LZyhUHAabc123\n"
        "SET statement_timeout = 0;\n"
        "SET transaction_timeout = 0;\n"
        "CREATE TABLE public.cars (vin text);\n"
        "\\unrestrict LZyhUHAabc123\n"
    )
    body, dropped = _strip_psql_only(dump)

    assert "\\restrict" not in body
    assert "\\unrestrict" not in body
    assert "transaction_timeout" not in body
    assert "SET statement_timeout = 0;" in body, "must not strip valid SETs"
    assert "CREATE TABLE public.cars (vin text);" in body
    assert len(dropped) == 3
