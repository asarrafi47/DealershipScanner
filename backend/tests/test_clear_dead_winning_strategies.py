"""P17B.8a: cached winning-strategy hygiene script (dry run default, verified single transaction)."""
from __future__ import annotations

import pytest

from backend.scripts import clear_dead_winning_strategies as cdws


def test_dead_names_are_everything_outside_the_live_chain():
    counts = [("dealer_venom_typesense", 10), ("dealer_on_cosmos", 6), ("dealer_inspire_algolia", 5),
              ("html_next_data", 10), ("dealer_eprocess_json", 7), ("shopperexpress_api", 2), (None, 4), ("  ", 1)]
    assert cdws.dead_names(counts) == {"dealer_venom_typesense": 10, "dealer_on_cosmos": 6,
                                       "dealer_inspire_algolia": 5, "html_next_data": 10}
    assert cdws.dead_names([("a", 1), ("a", 2)], live=["b"]) == {"a": 3}


@pytest.fixture()
def profile_db(tmp_path, monkeypatch):
    from backend.db import inventory_db as inv

    monkeypatch.setattr(inv, "DB_PATH", str(tmp_path / "inventory.db"))
    monkeypatch.delenv("INVENTORY_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with inv.db_conn() as conn:
        cur = conn.cursor()
        from backend.scanner.dealer.profile import ensure_dealer_scan_profile_table

        ensure_dealer_scan_profile_table(cur)
        rows = [("a", "html_next_data"), ("b", "dealer_on_cosmos"), ("c", "dealer_eprocess_json"), ("d", None),
                ("e", "pixel_motion_html")]
        for did, strat in rows:
            cur.execute("INSERT INTO dealer_scan_profile (dealer_id, last_winning_strategy, platform_hints_json, updated_at) "
                        "VALUES (?, ?, '[]', '2026-10-01T00:00:00')", (did, strat))
        conn.commit()
    return inv


def _stored(inv):
    with inv.db_conn() as conn:
        return dict(conn.execute("SELECT dealer_id, last_winning_strategy FROM dealer_scan_profile").fetchall())


def test_dry_run_counts_and_writes_nothing(profile_db):
    out: list[str] = []
    assert cdws.main([], log=out.append) == cdws.EXIT_OK
    assert any("dead_rows: 3" in line for line in out)
    assert _stored(profile_db)["a"] == "html_next_data"


def test_apply_needs_a_backup_file(profile_db):
    out: list[str] = []
    assert cdws.main(["--apply"], log=out.append) == cdws.EXIT_REFUSED
    assert _stored(profile_db)["a"] == "html_next_data"


def test_apply_backs_up_clears_dead_names_and_keeps_rows(profile_db, tmp_path):
    backup = tmp_path / "profile.csv"
    out: list[str] = []
    assert cdws.main(["--apply", "--backup-file", str(backup)], log=out.append) == cdws.EXIT_OK
    assert _stored(profile_db) == {"a": None, "b": None, "c": "dealer_eprocess_json", "d": None, "e": None}
    lines = backup.read_text().splitlines()
    assert lines[0].startswith("dealer_id,last_winning_strategy") and len(lines) == 6
    with profile_db.db_conn() as conn:
        assert conn.execute("SELECT COUNT(*) FROM dealer_scan_profile WHERE updated_at = '2026-10-01T00:00:00'").fetchone()[0] == 5
    # a second run finds nothing and refuses to overwrite nothing: dead set is empty
    out2: list[str] = []
    assert cdws.main(["--apply", "--backup-file", str(tmp_path / "second.csv")], log=out2.append) == cdws.EXIT_OK
    assert any("dead_rows: 0" in line for line in out2)


def test_existing_backup_file_is_never_overwritten(profile_db, tmp_path):
    backup = tmp_path / "profile.csv"
    backup.write_text("keep")
    assert cdws.main(["--apply", "--backup-file", str(backup)], log=lambda s: None) == cdws.EXIT_REFUSED
    assert backup.read_text() == "keep" and _stored(profile_db)["a"] == "html_next_data"
