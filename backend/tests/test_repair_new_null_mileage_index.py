"""N2.2 (IL-02): targeted repair of the incomplete-listings index for the F12 class.

SQLite fixtures (the suite's backend): an inventory DB with ``cars`` and the
sidecar index DB with ``incomplete_listings``, both under ``tmp_path``. The
Postgres path is the same code through the inventory adapter; it is proven on a
``pytest_*`` copy of ``cars_cleanroom`` (unit acceptance), not here.
"""

from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

import pytest

from backend.db import incomplete_listings_db as ild
from backend.db import inventory_db as inv_db
from backend.scripts import repair_new_null_mileage_index as rep
from backend.utils import listing_completeness

OLD_TS = "2026-10-13T14:05:01+00:00"

# (id, condition, is_cpo, mileage, listing_active, index codes or None, expected verdict)
CARS = [
    (1, "New", 0, None, 1, ["mileage"], rep.REPAIR),  # delete: mileage was the only code
    (2, "New", 0, None, 1, ["trim", "mileage", "exterior_color"], rep.REPAIR),  # update
    (3, None, None, None, 1, ["mileage", "condition"], rep.REPAIR),  # blank condition
    (4, "ctp", 0, None, 1, ["mileage"], rep.REPAIR),  # non-used, not new: still the class
    (5, "New", 1, None, 1, ["mileage"], rep.KEPT_USED_OR_CPO),  # CPO flag wins
    (6, "Used", 0, None, 1, ["mileage"], rep.KEPT_USED_OR_CPO),
    (7, "Certified Pre-Owned", None, None, 1, ["mileage", "trim"], rep.KEPT_USED_OR_CPO),
    (8, "Used", 0, 0, 1, ["mileage"], rep.SKIP_HAS_ODOMETER),  # F12 used at 0 mi
    (9, "New", 0, None, 1, ["trim"], None),  # no mileage code: never read
    (10, "New", 0, None, 0, ["mileage"], rep.SKIP_CAR_INACTIVE),
    (12, "New", 0, "", 1, ["mileage", "price"], rep.REPAIR),  # empty-string odometer
    (13, "New", 0, None, 1, '["mileage"', rep.SKIP_BAD_JSON),  # truncated JSON
    (14, "new ", 0, None, None, ["mileage"], rep.REPAIR),  # listing_active NULL = active
    (15, "Used", 0, None, 1, None, None),  # not indexed
    (16, "New", 0, None, 1, None, None),  # class car, not indexed (already visible)
    (17, "Demo", 0, 1200, 1, ["mileage"], rep.SKIP_HAS_ODOMETER),
]
MISSING_CAR_ID = 999  # index row whose car is gone

REPAIR_IDS = {1, 2, 3, 4, 12, 14}
DELETE_IDS = {1, 4, 14}
EXPECTED_AFTER = {2: ["trim", "exterior_color"], 3: ["condition"], 12: ["price"]}


def _codes_text(codes) -> str:
    return codes if isinstance(codes, str) else json.dumps(codes)


@pytest.fixture
def dbs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Path, Path]:
    inv_path = tmp_path / "inventory.db"
    inc_path = tmp_path / "incomplete_listings.db"
    monkeypatch.setattr(inv_db, "DB_PATH", str(inv_path))
    monkeypatch.setattr(ild, "DB_PATH", str(inc_path))

    inv = sqlite3.connect(inv_path)
    inv.execute(
        "CREATE TABLE cars (id INTEGER PRIMARY KEY, vin TEXT, condition TEXT, is_cpo INTEGER, "
        "mileage INTEGER, listing_active INTEGER)"
    )
    inc = sqlite3.connect(inc_path)
    inc.execute(
        "CREATE TABLE incomplete_listings (car_id INTEGER PRIMARY KEY NOT NULL, vin TEXT NOT NULL, "
        "missing_fields_json TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )
    for cid, cond, cpo, mileage, active, codes, _verdict in CARS:
        vin = f"VIN{cid:014d}"
        inv.execute("INSERT INTO cars VALUES (?,?,?,?,?,?)", (cid, vin, cond, cpo, mileage, active))
        if codes is not None:
            inc.execute("INSERT INTO incomplete_listings VALUES (?,?,?,?)", (cid, vin, _codes_text(codes), OLD_TS))
    inc.execute(
        "INSERT INTO incomplete_listings VALUES (?,?,?,?)",
        (MISSING_CAR_ID, "VINGONE", '["mileage"]', OLD_TS),
    )
    inv.commit()
    inc.commit()
    inv.close()
    inc.close()
    return inv_path, inc_path


def _index(inc_path: Path) -> dict[int, tuple[str, str, str]]:
    conn = sqlite3.connect(inc_path)
    try:
        rows = conn.execute("SELECT car_id, vin, missing_fields_json, updated_at FROM incomplete_listings").fetchall()
    finally:
        conn.close()
    return {int(r[0]): (r[1], r[2], r[3]) for r in rows}


def _quiet(_line: str) -> None:
    return None


# ---------------------------------------------------------------------------
# the rule is the N2.1 rule, imported
# ---------------------------------------------------------------------------


def test_uses_the_n2_1_rule_object_not_a_copy() -> None:
    assert rep._mileage_blank is listing_completeness._mileage_blank


def test_selection_goes_through_the_imported_rule(dbs, monkeypatch: pytest.MonkeyPatch) -> None:
    # If the rule says "still missing" for every car, nothing is selected.
    monkeypatch.setattr(rep, "_mileage_blank", lambda car: True)
    out = rep.run(log=_quiet)
    assert out["repair_rows"] == 0
    assert out[rep.KEPT_USED_OR_CPO] == len(REPAIR_IDS) + 3


@pytest.mark.parametrize(
    "car, codes, verdict",
    [
        ({"condition": "New", "is_cpo": 0, "mileage": None, "listing_active": 1}, '["mileage"]', rep.REPAIR),
        ({"condition": "", "is_cpo": None, "mileage": None, "listing_active": 1}, '["mileage"]', rep.REPAIR),
        ({"condition": "New", "is_cpo": 1, "mileage": None, "listing_active": 1}, '["mileage"]', rep.KEPT_USED_OR_CPO),
        ({"condition": "Used", "is_cpo": 0, "mileage": None, "listing_active": 1}, '["mileage"]', rep.KEPT_USED_OR_CPO),
        ({"condition": "Used", "is_cpo": 0, "mileage": 0, "listing_active": 1}, '["mileage"]', rep.SKIP_HAS_ODOMETER),
        ({"condition": "New", "is_cpo": 0, "mileage": "  ", "listing_active": 1}, '["mileage"]', rep.REPAIR),
        ({"condition": "New", "is_cpo": 0, "mileage": None, "listing_active": 0}, '["mileage"]', rep.SKIP_CAR_INACTIVE),
        ({"condition": "New", "is_cpo": 0, "mileage": None, "listing_active": 1}, '{"mileage": 1}', rep.SKIP_BAD_JSON),
        (
            {"condition": "New", "is_cpo": 0, "mileage": None, "listing_active": 1},
            '["mileage_x"]',
            rep.SKIP_NO_MILEAGE_CODE,
        ),
        (None, '["mileage"]', rep.SKIP_CAR_MISSING),
    ],
)
def test_classify(car, codes, verdict) -> None:
    got, new_codes = rep.classify(codes, car)
    assert got == verdict
    if verdict == rep.REPAIR:
        assert new_codes == []
    else:
        assert new_codes is None


def test_classify_keeps_other_codes_in_order() -> None:
    car = {"condition": "New", "is_cpo": 0, "mileage": None, "listing_active": 1}
    verdict, new_codes = rep.classify('["trim", "mileage", "price", "exterior_color"]', car)
    assert verdict == rep.REPAIR
    assert new_codes == ["trim", "price", "exterior_color"]


# ---------------------------------------------------------------------------
# selection and counts (dry run)
# ---------------------------------------------------------------------------


def test_dry_run_counts_and_writes_nothing(dbs) -> None:
    _inv_path, inc_path = dbs
    before = _index(inc_path)
    out = rep.run(log=_quiet)
    assert _index(inc_path) == before
    assert out["mode"] == "dry_run"
    assert out["index_rows_total"] == len(before)
    assert out["repair_rows"] == len(REPAIR_IDS)
    assert out["repair_delete_index_row"] == len(DELETE_IDS)
    assert out["repair_update_keep_other_codes"] == len(REPAIR_IDS - DELETE_IDS)
    assert out[rep.KEPT_USED_OR_CPO] == 3
    assert out[rep.SKIP_HAS_ODOMETER] == 2
    assert out[rep.SKIP_CAR_INACTIVE] == 1
    assert out[rep.SKIP_CAR_MISSING] == 1
    assert out[rep.SKIP_BAD_JSON] == 1
    # every index row whose JSON text carries "mileage" is read; car 9 (["trim"]) is not
    assert out["scanned_index_rows_coded_mileage"] == len(before) - 1
    assert out["repair_by_raw_condition"] == {"new": 4, "(blank)": 1, "ctp": 1}
    # N1.6 C2 shape: NULL odometer and raw condition new/blank, is_cpo ignored:
    # cars 1, 2, 3, 14 and the CPO car 5; not ctp (4) and not the empty string (12).
    assert out["census_il02_shape"] == 5
    # class (active, not used/CPO, NULL/empty odometer), indexed or not: 1 2 3 4 9 12 13 14 16
    assert out["class_cars_active"] == 9


def test_dry_run_session_is_read_only(dbs) -> None:
    conn = ild.get_conn()
    try:
        rep._session_setup(conn, is_postgres=False, read_only=True)
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("DELETE FROM incomplete_listings")
    finally:
        conn.close()


def test_batches_of_n_cover_every_row_once(dbs) -> None:
    lines: list[str] = []
    small = rep.run(batch_size=2, log=lines.append)
    whole = rep.run(log=_quiet)
    scanned = whole["scanned_index_rows_coded_mileage"]
    assert small["batches"] == -(-scanned // 2)
    assert whole["batches"] == 1 and whole["batch_size"] == rep.BATCH_SIZE == 1000
    for key in ("repair_rows", "repair_delete_index_row", "census_il02_shape", rep.KEPT_USED_OR_CPO):
        assert small[key] == whole[key]
    assert sum(1 for line in lines if line.startswith("batch ")) == small["batches"]
    assert small["batch_seconds_max"] < rep.BATCH_SECONDS_BUDGET
    assert small["batches_over_budget"] == 0


def test_batch_size_must_be_positive(dbs) -> None:
    lines: list[str] = []
    assert rep.main(["--batch-size", "0"], log=lines.append) == rep.EXIT_REFUSED
    assert lines[-1].startswith("REFUSED")


def test_main_prints_count_and_json_summary(dbs) -> None:
    lines: list[str] = []
    assert rep.main([], log=lines.append) == rep.EXIT_OK
    assert lines[0].startswith("target: backend=sqlite")
    assert any(line.startswith(f"repair_rows (IL-02 under the N2.1 rule): {len(REPAIR_IDS)}") for line in lines)
    assert lines[-1].startswith("summary: ")
    summary = json.loads(lines[-1][len("summary: ") :])
    assert summary["repair_rows"] == len(REPAIR_IDS)
    assert summary["mode"] == "dry_run"


# ---------------------------------------------------------------------------
# the backup requirement
# ---------------------------------------------------------------------------


def test_apply_without_backup_file_is_refused(dbs) -> None:
    _inv_path, inc_path = dbs
    before = _index(inc_path)
    lines: list[str] = []
    assert rep.main(["--apply"], log=lines.append) == rep.EXIT_REFUSED
    assert "--backup-file" in lines[-1]
    assert _index(inc_path) == before


def test_apply_refuses_an_existing_backup_file(dbs, tmp_path: Path) -> None:
    _inv_path, inc_path = dbs
    before = _index(inc_path)
    existing = tmp_path / "already_there.csv"
    existing.write_text("keep me\n")
    lines: list[str] = []
    assert rep.main(["--apply", "--backup-file", str(existing)], log=lines.append) == rep.EXIT_REFUSED
    assert "already exists" in lines[-1]
    assert existing.read_text() == "keep me\n"
    assert _index(inc_path) == before


def test_apply_refuses_a_missing_backup_directory(dbs, tmp_path: Path) -> None:
    _inv_path, inc_path = dbs
    before = _index(inc_path)
    target = tmp_path / "no_such_dir" / "b.csv"
    assert rep.main(["--apply", "--backup-file", str(target)], log=_quiet) == rep.EXIT_REFUSED
    assert not target.exists()
    assert _index(inc_path) == before


def test_dry_run_and_apply_are_exclusive() -> None:
    with pytest.raises(SystemExit):
        rep.build_parser().parse_args(["--dry-run", "--apply"])


@pytest.mark.parametrize(
    "is_pg, current, expect, apply, refused",
    [
        (True, "railway", None, True, True),  # Postgres apply needs --expect-db
        (True, "cars", "railway", True, True),  # wrong database
        (True, "cars", "railway", False, True),  # a named target is checked on a dry run too
        (True, "railway", "railway", True, False),
        (True, "railway", None, False, False),  # dry run: optional
        (False, None, None, True, False),  # SQLite (tests only)
    ],
)
def test_check_target(is_pg, current, expect, apply, refused) -> None:
    if refused:
        with pytest.raises(rep.Refused):
            rep.check_target(is_postgres=is_pg, current_db=current, expect_db=expect, apply=apply)
    else:
        rep.check_target(is_postgres=is_pg, current_db=current, expect_db=expect, apply=apply)


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


def test_apply_repairs_only_the_class_then_dry_run_is_zero(dbs, tmp_path: Path) -> None:
    _inv_path, inc_path = dbs
    before = _index(inc_path)
    backup = tmp_path / "incomplete_newnull_test.csv"
    lines: list[str] = []
    assert rep.main(["--apply", "--backup-file", str(backup), "--batch-size", "2"], log=lines.append) == rep.EXIT_OK
    summary = json.loads(lines[-1][len("summary: ") :])
    assert summary["applied_deleted"] == len(DELETE_IDS)
    assert summary["applied_updated"] == len(REPAIR_IDS - DELETE_IDS)
    assert summary["changed_concurrently"] == 0
    assert summary["remaining_after_apply"] == 0
    assert summary["index_rows_total_after"] == len(before) - len(DELETE_IDS)

    after = _index(inc_path)
    for cid in DELETE_IDS:
        assert cid not in after
    for cid, codes in EXPECTED_AFTER.items():
        vin, mf, ts = after[cid]
        assert json.loads(mf) == codes
        assert vin == before[cid][0]
        assert ts > OLD_TS
    for cid, row in before.items():
        if cid not in REPAIR_IDS:
            assert after[cid] == row, cid  # used/CPO, odometer present, inactive, missing car, bad JSON

    with backup.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.reader(fh))
    assert tuple(rows[0]) == rep.BACKUP_COLUMNS
    assert {int(r[0]): (r[1], r[2], r[3]) for r in rows[1:]} == {cid: before[cid] for cid in REPAIR_IDS}

    again = rep.run(log=_quiet)
    assert again["repair_rows"] == 0
    assert again[rep.KEPT_USED_OR_CPO] == 3


def test_backup_restores_the_index_exactly(dbs, tmp_path: Path) -> None:
    _inv_path, inc_path = dbs
    before = _index(inc_path)
    backup = tmp_path / "b.csv"
    rep.run(apply=True, backup_file=str(backup), log=_quiet)
    assert _index(inc_path) != before
    with backup.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    conn = sqlite3.connect(inc_path)
    conn.executemany(
        "INSERT INTO incomplete_listings (car_id, vin, missing_fields_json, updated_at) VALUES (?,?,?,?) "
        "ON CONFLICT(car_id) DO UPDATE SET vin = excluded.vin, "
        "missing_fields_json = excluded.missing_fields_json, updated_at = excluded.updated_at",
        [(int(r["car_id"]), r["vin"], r["missing_fields_json"], r["updated_at"]) for r in rows],
    )
    conn.commit()
    conn.close()
    assert _index(inc_path) == before


def test_apply_leaves_a_row_changed_concurrently(dbs, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _inv_path, inc_path = dbs
    real_fetch = rep._fetch_cars

    def fetch_then_concurrent_write(inv, ids):
        out = real_fetch(inv, ids)
        if 1 in ids:  # another writer re-flags car 1 between our read and our write
            other = sqlite3.connect(inc_path)
            other.execute(
                "UPDATE incomplete_listings SET missing_fields_json = ? WHERE car_id = 1",
                ('["mileage", "price"]',),
            )
            other.commit()
            other.close()
        return out

    monkeypatch.setattr(rep, "_fetch_cars", fetch_then_concurrent_write)
    lines: list[str] = []
    rc = rep.main(["--apply", "--backup-file", str(tmp_path / "b.csv")], log=lines.append)
    summary = json.loads(lines[-1][len("summary: ") :])
    assert summary["changed_concurrently"] == 1
    assert summary["applied_deleted"] == len(DELETE_IDS) - 1
    assert _index(inc_path)[1][1] == '["mileage", "price"]'  # the other writer's value stands
    assert summary["remaining_after_apply"] == 1
    assert rc == rep.EXIT_ERROR


def test_missing_index_table_is_refused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    inv_path = tmp_path / "inventory.db"
    monkeypatch.setattr(inv_db, "DB_PATH", str(inv_path))
    monkeypatch.setattr(ild, "DB_PATH", str(tmp_path / "incomplete_listings.db"))
    conn = sqlite3.connect(inv_path)
    conn.execute(
        "CREATE TABLE cars (id INTEGER PRIMARY KEY, vin TEXT, condition TEXT, is_cpo INTEGER, "
        "mileage INTEGER, listing_active INTEGER)"
    )
    conn.commit()
    conn.close()
    lines: list[str] = []
    assert rep.main([], log=lines.append) == rep.EXIT_REFUSED
    assert "incomplete_listings is not readable" in lines[-1]
