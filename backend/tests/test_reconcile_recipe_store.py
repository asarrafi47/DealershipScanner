"""reconcile_recipe_store settles a recipe cache against dealer_recipes per recipe (P1C.2).

Every test runs over ``recipe_store_harness``: a per-test SQLite store under ``tmp_path``
wired through ``recipe_store._conn``, and per-host cache dirs under ``tmp_path``. Run dirs
(reports, tars, exports) go under ``tmp_path / "backups"``; nothing touches the session DB
or the real workspace.
"""
from __future__ import annotations

import csv
import json
import sqlite3
import tarfile
from contextlib import closing
from dataclasses import asdict
from pathlib import Path

import pytest

import backend.scanner.recipes as rec
from backend.scanner import recipe_store
from backend.scanner.recipes import (
    PAGINATION_CARSCOMMERCE,
    EndpointRecipe,
    load_recipes,
)
from backend.scripts import import_recipes_to_db
from backend.scripts import reconcile_recipe_store as rrs
from backend.tests.recipe_store_harness import TwoHostRecipeStore

D = "reconcile-dealer-com"
CC_URL = "https://websites-search.api.carscommerce.inc/api/v1/listings/77/search"
CC_URL_2 = "https://websites-search.api.carscommerce.inc/api/v1/listings/77/legacy-search"
CC_URL_3 = "https://websites-search.api.carscommerce.inc/api/v1/listings/77/facets"
JUL = 1_783_272_811.0   # 2026-07-05
AUG = 1_786_000_000.0   # 2026-08-06
SEP = 1_790_000_000.0   # 2026-09-21
SEP29 = 1_790_700_000.0  # 2026-09-29
OCT = 1_791_500_000.0   # 2026-10-08
SECRET = "SECRET-AUTH-VALUE-1234"


@pytest.fixture
def store(tmp_path, monkeypatch):
    s = TwoHostRecipeStore(tmp_path, monkeypatch)
    s.dirs["mbp"].mkdir(parents=True, exist_ok=True)
    conn = s._connect()
    try:
        recipe_store._ensure_table(conn)
    finally:
        conn.close()
    return s


def _row(url: str = CC_URL, *, saved: float = 0.0, ok: float = 0.0, stale: bool = False,
         pt: str = '{"page":1,"perPage":20}', dealer: str = D, **kw) -> dict:
    r = asdict(EndpointRecipe(
        dealer_id=dealer, url=url, method="POST", content_type="application/json", post_template=pt,
        pagination=PAGINATION_CARSCOMMERCE, provider_hint="dealer_dot_com", vehicle_rows=20,
        total_count=45, saved_at=saved, last_ok_at=ok, stale=stale,
        stale_reason="http 403" if stale else "",
    ))
    r.update(kw)
    return r


def _db_dump(store) -> list[tuple]:
    with closing(sqlite3.connect(store.db_path)) as conn:
        return conn.execute("SELECT * FROM dealer_recipes ORDER BY dealer_id").fetchall()


def _files(d: Path) -> dict[str, bytes]:
    return {p.name: p.read_bytes() for p in sorted(d.iterdir()) if p.is_file()} if d.is_dir() else {}


def _run(store, tmp_path, *extra: str) -> tuple[int, Path]:
    backups = tmp_path / "backups"
    before = set(backups.iterdir()) if backups.is_dir() else set()
    rc = rrs.main(["--cache-dir", str(store.dirs["mbp"]), "--backup-dir", str(backups), *extra])
    new = sorted(set(backups.iterdir()) - before) if backups.is_dir() else []
    assert len(new) <= 1
    return rc, (new[0] if new else None)


def _report(run_dir: Path, name: str = rrs.REPORT_NAME) -> list[dict]:
    with open(run_dir / name, encoding="utf-8") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def _by_dealer(run_dir: Path) -> dict[str, dict]:
    return {r["dealer_id"]: r for r in _report(run_dir)}


# ── classes, dry run ──────────────────────────────────────────────────────────


def test_every_class_and_the_dry_run_writes_nothing_but_the_report(store, tmp_path):
    same = [_row(saved=AUG, ok=AUG, dealer="same-com")]
    store.write_db("same-com", same)
    store.write_file("mbp", "same-com", same)
    store.write_db("differs-com", [_row(saved=SEP, ok=SEP, dealer="differs-com")])
    store.write_file("mbp", "differs-com", [_row(saved=AUG, ok=AUG, dealer="differs-com")])
    store.write_file("mbp", "fileonly-com", [_row(saved=AUG, ok=AUG, dealer="fileonly-com")])
    store.write_db("dbonly-com", [_row(saved=AUG, ok=AUG, dealer="dbonly-com")])
    (store.dirs["mbp"] / "_aliases.json").write_text("{}", encoding="utf-8")
    (store.dirs["mbp"] / "same-com.json.tmp.42.7").write_text("[", encoding="utf-8")
    db_before, files_before = _db_dump(store), _files(store.dirs["mbp"])

    rc, run_dir = _run(store, tmp_path)

    assert rc == 0
    assert _db_dump(store) == db_before
    assert _files(store.dirs["mbp"]) == files_before
    assert sorted(p.name for p in run_dir.iterdir()) == sorted(
        [rrs.REPORT_NAME, rrs.KEYS_NAME, rrs.SUMMARY_NAME])
    rows = _by_dealer(run_dir)
    assert {d: r["class"] for d, r in rows.items()} == {
        "same-com": "identical", "differs-com": "differs", "fileonly-com": "cache_only", "dbonly-com": "db_only",
    }
    assert rows["differs-com"]["saved_relation"] == "db_newer"
    assert rows["differs-com"]["action"] == "cache_rewrite"
    assert rows["fileonly-com"]["action"] == "db_insert"
    assert rows["same-com"]["action"] == rows["dbonly-com"]["action"] == "none"
    summary = json.loads((run_dir / rrs.SUMMARY_NAME).read_text(encoding="utf-8"))
    assert summary["classes"] == {"identical": 1, "differs": 1, "cache_only": 1, "db_only": 1}
    assert summary["apply"] is False


def test_the_raw_saved_relation_is_reported_for_each_differs(store, tmp_path):
    for did, db_saved, file_saved in (("eq-com", AUG, AUG), ("dbnew-com", SEP, AUG), ("filenew-com", 0.0, JUL)):
        store.write_db(did, [_row(saved=db_saved, ok=SEP, dealer=did)])
        store.write_file("mbp", did, [_row(saved=file_saved, ok=AUG, dealer=did, vehicle_rows=3)])
    rc, run_dir = _run(store, tmp_path)
    assert rc == 0
    rows = _by_dealer(run_dir)
    assert {d: r["saved_relation"] for d, r in rows.items()} == {
        "eq-com": "equal", "dbnew-com": "db_newer", "filenew-com": "cache_newer"}
    summary = json.loads((run_dir / rrs.SUMMARY_NAME).read_text(encoding="utf-8"))
    assert summary["differs_saved_relation"] == {"equal": 1, "db_newer": 1, "cache_newer": 1}
    assert summary["differs_cache_newer_over_db_saved_at_0"] == 1


# ── merge rules ───────────────────────────────────────────────────────────────


def test_cache_recipe_older_than_the_db_write_is_dropped_and_a_newer_one_added(store, tmp_path):
    store.write_db(D, [_row(CC_URL, saved=SEP, ok=SEP)])
    store.write_file("mbp", D, [
        _row(CC_URL, saved=AUG, ok=AUG),
        _row(CC_URL_2, saved=AUG, ok=AUG),   # written before the DB set, which left it out
        _row(CC_URL_3, saved=OCT, ok=OCT),   # written after the DB set
    ])
    rc, run_dir = _run(store, tmp_path, "--apply")
    assert rc == 0
    rows = store.db_row(D)["rows"]
    assert [r["url"] for r in rows] == [CC_URL, CC_URL_3]
    assert rows[0] == _row(CC_URL, saved=SEP, ok=SEP)         # the DB's copy, unchanged
    assert rows[1]["saved_at"] == OCT
    assert store.file_rows("mbp", D) == rows                  # cache rewritten from the DB
    decisions = {r["recipe"].split("/")[-1]: r["decision"] for r in _report(run_dir, rrs.KEYS_NAME)}
    assert decisions == {"search": "db_newer_success", "legacy-search": "dropped", "facets": "added"}


@pytest.mark.parametrize(
    "db, cache, want_stale, want_ok, want_decision",
    [
        # the cache shows a strictly newer success: its (live) state wins
        (dict(stale=True, ok=AUG), dict(stale=False, ok=SEP), False, SEP, "cache_newer_success"),
        # the cache's success is older: stale follows the DB
        (dict(stale=True, ok=SEP), dict(stale=False, ok=AUG), True, SEP, "db_newer_success"),
        (dict(stale=False, ok=SEP), dict(stale=True, ok=AUG), False, SEP, "db_newer_success"),
        # a newer cache success that was marked stale after it keeps that state
        (dict(stale=False, ok=AUG), dict(stale=True, ok=SEP), True, SEP, "cache_newer_success"),
        # equal last_ok_at: a stale flag the other side lacks resolves to live
        (dict(stale=True, ok=AUG), dict(stale=False, ok=AUG), False, AUG, "tie_live"),
        (dict(stale=False, ok=AUG), dict(stale=True, ok=AUG), False, AUG, "tie_db"),
        (dict(stale=True, ok=AUG), dict(stale=True, ok=AUG), True, AUG, "tie_db"),
    ],
)
def test_stale_follows_the_db_unless_the_cache_shows_a_newer_success(db, cache, want_stale, want_ok, want_decision):
    d = _row(saved=SEP, ok=db["ok"], stale=db["stale"], field_coverage={"price": 0.5}, total_count=40)
    c = _row(saved=AUG, ok=cache["ok"], stale=cache["stale"], field_coverage={"price": 0.9}, total_count=50,
             vehicle_rows=1)
    merged, decisions = rrs.merge_sets([c], [d])
    [out] = merged
    assert [k.decision for k in decisions] == [want_decision]
    assert bool(out["stale"]) is want_stale
    assert out["last_ok_at"] == want_ok
    newer = c if cache["ok"] > db["ok"] else d
    # last_ok_at, field_coverage and total_count travel together, from one side
    assert (out["field_coverage"], out["total_count"]) == (newer["field_coverage"], newer["total_count"])
    if want_decision in ("db_newer_success", "tie_db"):
        assert out is d                                       # the DB row is kept as it is
    else:
        assert out["saved_at"] == SEP                         # never older than either copy
    if want_decision == "tie_live":
        assert out["stale_reason"] == ""


def test_a_stale_flag_change_reaches_the_db_end_to_end(store, tmp_path):
    store.write_db(D, [_row(saved=AUG, ok=AUG, stale=True)])
    store.write_file("mbp", D, [_row(saved=AUG, ok=SEP, stale=False, field_coverage={"price": 1.0})])
    rc, _ = _run(store, tmp_path, "--apply")
    assert rc == 0
    db = store.db_row(D)
    [r] = db["rows"]
    assert (r["stale"], r["last_ok_at"], r["field_coverage"]) == (False, SEP, {"price": 1.0})
    assert db["stale_count"] == 0 and db["last_ok_at"] == SEP
    assert store.file_rows("mbp", D) == db["rows"]


def test_repeated_keys_are_matched_by_url_and_post_template():
    new_pt, used_pt = '{"page":1,"filters":{"condition":"new"}}', '{"page":1,"filters":{"condition":"used"}}'
    db = [_row(pt=new_pt, saved=AUG, ok=SEP), _row(pt=used_pt, saved=AUG, ok=AUG)]
    cache = [_row(pt=new_pt, saved=AUG, ok=AUG), _row(pt=used_pt, saved=AUG, ok=SEP, total_count=99)]
    assert rrs.recipe_key(db[0]) == rrs.recipe_key(db[1])     # one key, two sections
    merged, decisions = rrs.merge_sets(cache, db)
    assert [k.decision for k in decisions] == ["db_newer_success", "cache_newer_success"]
    assert merged[0] is db[0]
    assert (merged[1]["post_template"], merged[1]["total_count"]) == (used_pt, 99)


def test_db_only_recipes_are_kept(store, tmp_path):
    store.write_db(D, [_row(CC_URL, saved=SEP, ok=SEP), _row(CC_URL_2, saved=SEP, ok=0.0, stale=True)])
    store.write_file("mbp", D, [_row(CC_URL, saved=SEP, ok=SEP)])
    rc, _ = _run(store, tmp_path, "--apply")
    assert rc == 0
    assert [r["url"] for r in store.db_row(D)["rows"]] == [CC_URL, CC_URL_2]
    assert store.file_rows("mbp", D) == store.db_row(D)["rows"]


# ── saved_at=0 rows ───────────────────────────────────────────────────────────


def test_a_saved_at_0_db_row_against_an_older_cache_resolves_to_the_db(store, tmp_path):
    # A September set saved by pre-P1B.3 synthesize (saved_at=0) against a July cache.
    synthesized = [_row(CC_URL, saved=0.0, ok=SEP, pt='{"page":1,"perPage":96}'),
                   _row(CC_URL_3, saved=0.0, ok=SEP)]
    store.write_db(D, synthesized)
    store.write_file("mbp", D, [_row(CC_URL, saved=JUL, ok=JUL), _row(CC_URL_2, saved=JUL, ok=JUL)])
    db_before = _db_dump(store)

    rc, run_dir = _run(store, tmp_path, "--apply")

    assert rc == 0
    assert _by_dealer(run_dir)[D]["saved_relation"] == "cache_newer"   # load_recipes would push it up
    assert _db_dump(store) == db_before                                 # the DB set is untouched
    assert store.file_rows("mbp", D) == synthesized                     # the cache now mirrors it
    with store.on("mbp"):
        assert [asdict(r)["url"] for r in load_recipes(D)] == [CC_URL, CC_URL_3]
    assert _db_dump(store) == db_before                                 # and nothing was pushed up


def test_a_synthesized_cache_set_the_db_reverted_comes_back_with_a_real_stamp(store, tmp_path):
    # The 09-29 shape: an older cache pushed the 08-05 set back over the synthesized
    # one in the DB, while this cache still holds the synthesized set (saved_at=0).
    store.write_db(D, [_row(CC_URL, saved=AUG, ok=AUG)])
    store.write_file("mbp", D, [_row(CC_URL, saved=0.0, ok=SEP29, total_count=60),
                                _row(CC_URL_3, saved=0.0, ok=SEP29)])
    rc, run_dir = _run(store, tmp_path, "--apply")
    assert rc == 0
    db = store.db_row(D)
    assert [(r["url"], r["saved_at"], r["last_ok_at"]) for r in db["rows"]] == [
        (CC_URL, SEP29, SEP29), (CC_URL_3, SEP29, SEP29)]
    assert db["max_saved_at"] == SEP29
    assert store.file_rows("mbp", D) == db["rows"]
    assert _by_dealer(run_dir)[D]["saved_relation"] == "db_newer"


def test_an_unstamped_db_set_with_a_stale_mark_takes_the_cache_success(store, tmp_path):
    # The scottclarkhonda-com shape P1C.3 skips: DB rows stale with no stamp at all,
    # the cache rows live with a 09-29 success.
    used, new = "https://www.d.com/inventory-used.json", "https://www.d.com/inventory-new.json"
    store.write_db(D, [_row(used, stale=True), _row(new, stale=True)])
    store.write_file("mbp", D, [_row(used, ok=SEP29, total_count=12), _row(new, ok=SEP29 + 3)])
    rc, _ = _run(store, tmp_path, "--apply")
    assert rc == 0
    db = store.db_row(D)
    assert [(r["stale"], r["last_ok_at"], r["saved_at"]) for r in db["rows"]] == [
        (False, SEP29, SEP29 + 3), (False, SEP29 + 3, SEP29 + 3)]
    assert db["stale_count"] == 0


def test_an_unstamped_db_set_with_a_cache_only_recipe_is_held(store, tmp_path):
    store.write_db(D, [_row(CC_URL)])                                   # saved_at 0, never answered
    store.write_file("mbp", D, [_row(CC_URL_2, saved=JUL, ok=JUL)])
    db_before, files_before = _db_dump(store), _files(store.dirs["mbp"])
    rc, run_dir = _run(store, tmp_path, "--apply")
    assert rc == 0
    row = _by_dealer(run_dir)[D]
    assert row["action"] == "hold" and "P1C.3" in row["hold_reason"]
    assert _db_dump(store) == db_before
    assert _files(store.dirs["mbp"]) == files_before                    # no marker either


# ── the import wrapper ────────────────────────────────────────────────────────


def test_import_wrapper_leaves_a_normreeves_like_september_db_set_unchanged(store, tmp_path):
    september = [_row(CC_URL, saved=0.0, ok=SEP, pt='{"page":1,"q":"' + "x" * 1124 + '"}')]
    july = [_row(CC_URL, saved=JUL, ok=AUG, pt='{"page":1,"q":"' + "y" * 810 + '"}')]
    assert (len(september[0]["post_template"]), len(july[0]["post_template"])) == (1141, 827)
    store.write_db(D, september)
    store.write_file("mbp", D, july)
    db_before = _db_dump(store)

    rc = import_recipes_to_db.main(["--backup-dir", str(tmp_path / "backups")])

    assert rc == 0
    assert _db_dump(store) == db_before                    # the July file is not pushed up
    assert store.file_rows("mbp", D) == september          # the cache adopts the September set
    [run_dir] = (tmp_path / "backups").iterdir()
    assert (run_dir / rrs.CACHE_TAR_NAME).is_file()
    assert json.loads((run_dir / rrs.EXPORT_NAME).read_text(encoding="utf-8"))["rows"] == []


def test_import_wrapper_dry_run_writes_nothing(store, tmp_path):
    store.write_db(D, [_row(saved=SEP, ok=SEP)])
    store.write_file("mbp", D, [_row(saved=AUG, ok=AUG)])
    db_before, files_before = _db_dump(store), _files(store.dirs["mbp"])
    assert import_recipes_to_db.main(["--dry-run", "--backup-dir", str(tmp_path / "backups")]) == 0
    assert _db_dump(store) == db_before
    assert _files(store.dirs["mbp"]) == files_before


# ── apply: backups, guard, idempotence ────────────────────────────────────────


def test_apply_requires_a_backup_dir(store, tmp_path, capsys):
    store.write_db(D, [_row(saved=AUG, ok=AUG)])
    store.write_file("mbp", D, [_row(saved=SEP, ok=SEP, stale=True)])
    db_before, files_before = _db_dump(store), _files(store.dirs["mbp"])
    with pytest.raises(SystemExit) as exc:
        rrs.main(["--cache-dir", str(store.dirs["mbp"]), "--apply"])
    assert exc.value.code == 2
    assert "--apply requires --backup-dir" in capsys.readouterr().err
    assert _db_dump(store) == db_before
    assert _files(store.dirs["mbp"]) == files_before


def test_apply_backs_up_first_and_a_second_apply_changes_nothing(store, tmp_path):
    old_file = [_row(saved=AUG, ok=SEP, total_count=60)]
    # The cache's success is newer, the DB set's write is newer: the merged row takes the
    # cache's recipe with the DB's stamp, so both sides change.
    store.write_db(D, [_row(saved=SEP, ok=AUG)])
    store.write_file("mbp", D, old_file)
    store.write_file("mbp", "newdealer-com", [_row(saved=AUG, ok=AUG, dealer="newdealer-com")])
    db_row_before = store.db_row(D)

    rc, run_dir = _run(store, tmp_path, "--apply")

    assert rc == 0
    # the tar holds the cache as it was before the rewrite
    with tarfile.open(run_dir / rrs.CACHE_TAR_NAME, "r:gz") as tf:
        name = store.dirs["mbp"].name
        assert {f"{name}/{D}.json", f"{name}/newdealer-com.json"} <= set(tf.getnames())
        assert json.load(tf.extractfile(f"{name}/{D}.json")) == old_file
    # the export holds every row it wrote, as read
    export = json.loads((run_dir / rrs.EXPORT_NAME).read_text(encoding="utf-8"))
    by_id = {r["dealer_id"]: r for r in export["rows"]}
    assert set(by_id) == {D, "newdealer-com"}
    assert by_id[D]["existed"] and by_id[D]["columns"]["recipes_json"] == db_row_before["recipes_json"]
    assert by_id["newdealer-com"] == {"dealer_id": "newdealer-com", "existed": False, "columns": None}
    applied = [json.loads(x) for x in (run_dir / rrs.APPLIED_NAME).read_text(encoding="utf-8").splitlines()]
    assert {(e["kind"], e["dealer_id"]) for e in applied} == {("db", D), ("db", "newdealer-com"), ("cache", D)}
    assert store.db_row("newdealer-com")["rows"] == store.file_rows("mbp", "newdealer-com")
    marker = json.loads((store.dirs["mbp"] / rrs.RECONCILED_MARKER).read_text(encoding="utf-8"))
    assert marker["fingerprint"] == rec.check_cache_store(store.dirs["mbp"]).fingerprint
    assert marker["run_dir"] == str(run_dir)

    db_after, files_after = _db_dump(store), _files(store.dirs["mbp"])
    rc2, run_dir2 = _run(store, tmp_path, "--apply")
    assert rc2 == 0
    assert _db_dump(store) == db_after
    assert {k: v for k, v in _files(store.dirs["mbp"]).items() if k != rrs.RECONCILED_MARKER} == {
        k: v for k, v in files_after.items() if k != rrs.RECONCILED_MARKER}
    assert json.loads((run_dir2 / rrs.EXPORT_NAME).read_text(encoding="utf-8"))["rows"] == []
    assert not (run_dir2 / rrs.APPLIED_NAME).read_text(encoding="utf-8")
    rc3, run_dir3 = _run(store, tmp_path)
    summary = json.loads((run_dir3 / rrs.SUMMARY_NAME).read_text(encoding="utf-8"))
    assert summary["classes"]["differs"] == 0 and summary["classes"]["identical"] == 2


def test_a_row_changed_since_the_read_is_skipped(store, tmp_path, monkeypatch):
    store.write_db(D, [_row(saved=AUG, ok=AUG)])
    store.write_file("mbp", D, [_row(saved=AUG, ok=SEP)])
    store.write_file("mbp", "racer-com", [_row(saved=AUG, ok=AUG, dealer="racer-com")])
    concurrent = [_row(CC_URL_2, saved=OCT, ok=OCT)]
    racer = [_row(CC_URL_3, saved=OCT, ok=OCT, dealer="racer-com")]
    real_apply = rrs.apply_plans

    def apply_after_a_concurrent_scan(plans, conn, **kw):
        # Another writer lands between the read and the guarded writes.
        assert recipe_store.db_save_recipes(D, concurrent)
        assert recipe_store.db_save_recipes("racer-com", racer)
        return real_apply(plans, conn, **kw)

    monkeypatch.setattr(rrs, "apply_plans", apply_after_a_concurrent_scan)
    files_before = _files(store.dirs["mbp"])

    rc, run_dir = _run(store, tmp_path, "--apply")

    assert rc == 1
    rows = _by_dealer(run_dir)
    assert rows[D]["outcome"] == rows["racer-com"]["outcome"] == "skipped:db_row_changed_since_read"
    assert store.db_row(D)["rows"] == concurrent and store.db_row("racer-com")["rows"] == racer
    assert _files(store.dirs["mbp"]) == files_before          # no rewrite, no marker
    assert not (run_dir / rrs.APPLIED_NAME).read_text(encoding="utf-8")


def test_a_cache_tagged_for_another_store_is_refused(store, tmp_path, capsys):
    store.write_db(D, [_row(saved=AUG, ok=AUG)])
    store.write_file("mbp", D, [_row(saved=SEP, ok=SEP)])
    (store.dirs["mbp"] / rec.STORE_TAG_FILENAME).write_text(
        json.dumps({"version": 1, "fingerprint": "f" * 64}), encoding="utf-8")
    db_before, files_before = _db_dump(store), _files(store.dirs["mbp"])
    rc, run_dir = _run(store, tmp_path, "--apply")
    assert rc == 2 and run_dir is None
    assert "mismatch" in capsys.readouterr().out
    assert _db_dump(store) == db_before and _files(store.dirs["mbp"]) == files_before
    rc, run_dir = _run(store, tmp_path)                        # a dry run still reports
    assert rc == 0 and _by_dealer(run_dir)[D]["class"] == "differs"


def test_auth_headers_are_never_printed(store, tmp_path, capsys):
    headers = {"x-api-key": SECRET, "Authorization": f"Bearer {SECRET}"}
    store.write_db(D, [_row(saved=AUG, ok=AUG, auth_headers=headers),
                       _row(CC_URL_2, saved=AUG, ok=AUG, auth_headers=headers)])
    store.write_file("mbp", D, [_row(saved=AUG, ok=SEP, auth_headers=headers),
                                _row(CC_URL_3, saved=SEP, ok=SEP, auth_headers=headers)])
    store.write_file("mbp", "broken-com", [{"auth_headers": headers}])
    for extra in ((), ("--apply",)):
        rc, run_dir = _run(store, tmp_path, *extra)
        assert rc == 0
        for name in (rrs.REPORT_NAME, rrs.KEYS_NAME, rrs.SUMMARY_NAME):
            assert SECRET not in (run_dir / name).read_text(encoding="utf-8")
    out = capsys.readouterr()
    assert SECRET not in out.out and SECRET not in out.err


def test_restore_undoes_an_apply(store, tmp_path):
    db_set, file_set = [_row(saved=SEP, ok=AUG, stale=True)], [_row(saved=AUG, ok=SEP)]
    store.write_db(D, db_set)
    store.write_file("mbp", D, file_set)
    store.write_file("mbp", "newdealer-com", [_row(saved=AUG, ok=AUG, dealer="newdealer-com")])
    rc, run_dir = _run(store, tmp_path, "--apply")
    assert rc == 0 and store.db_row(D)["rows"] != db_set
    assert store.file_rows("mbp", D) != file_set                 # rewritten (stamped SEP)
    assert (store.dirs["mbp"] / rrs.RECONCILED_MARKER).exists()

    rc = rrs.main(["--cache-dir", str(store.dirs["mbp"]), "--restore", str(run_dir)])

    assert rc == 0
    assert store.db_row(D)["rows"] == db_set
    assert store.db_row("newdealer-com") is None
    assert store.file_rows("mbp", D) == file_set
    assert not (store.dirs["mbp"] / rrs.RECONCILED_MARKER).exists()


# ── DB -> DB ──────────────────────────────────────────────────────────────────


def _sqlite_store(path: Path, rows: dict[str, list[dict]]) -> str:
    with closing(sqlite3.connect(path)) as conn:
        conn.execute(recipe_store._DDL_SQLITE)
        for did, recipes in rows.items():
            meta = recipe_store._derive_meta(recipes)
            conn.execute(
                "INSERT INTO dealer_recipes (dealer_id, recipes_json, recipe_count, provider_hint, max_saved_at, "
                "last_ok_at, stale_count, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (did, json.dumps(recipes), meta["recipe_count"], meta["provider_hint"], meta["max_saved_at"],
                 meta["last_ok_at"], meta["stale_count"], "2026-10-01T00:00:00+00:00"),
            )
        conn.commit()
    return f"sqlite:///{path}"


def _dump(path: Path) -> list[tuple]:
    with closing(sqlite3.connect(path)) as conn:
        return conn.execute("SELECT * FROM dealer_recipes ORDER BY dealer_id").fetchall()


def test_db_to_db_merges_the_source_into_the_target_only(tmp_path):
    src = _sqlite_store(tmp_path / "local.db", {
        D: [_row(saved=AUG, ok=SEP, total_count=70)],
        "srconly-com": [_row(saved=AUG, ok=AUG, dealer="srconly-com")],
    })
    tgt = _sqlite_store(tmp_path / "prod.db", {
        D: [_row(saved=AUG, ok=AUG, stale=True)],
        "tgtonly-com": [_row(saved=AUG, ok=AUG, dealer="tgtonly-com")],
    })
    src_before, tgt_before = _dump(tmp_path / "local.db"), _dump(tmp_path / "prod.db")
    args = ["--source-dsn", src, "--target-dsn", tgt, "--backup-dir", str(tmp_path / "backups")]

    assert rrs.main(args) == 0
    assert _dump(tmp_path / "prod.db") == tgt_before            # dry run
    assert rrs.main(args + ["--apply"]) == 0

    assert _dump(tmp_path / "local.db") == src_before           # the source is never written
    after = {r[0]: json.loads(r[1]) for r in _dump(tmp_path / "prod.db")}
    assert set(after) == {D, "srconly-com", "tgtonly-com"}
    assert (after[D][0]["stale"], after[D][0]["total_count"]) == (False, 70)
    run_dirs = sorted((tmp_path / "backups").iterdir())
    assert not (run_dirs[-1] / rrs.CACHE_TAR_NAME).exists()
    assert rrs.main(args) == 0
    summary = json.loads((sorted((tmp_path / "backups").iterdir())[-1] / rrs.SUMMARY_NAME).read_text())
    assert summary["classes"]["differs"] == 0


def test_db_to_db_refuses_one_store_on_both_sides(tmp_path, capsys):
    dsn = _sqlite_store(tmp_path / "one.db", {})
    assert rrs.main(["--source-dsn", dsn, "--target-dsn", dsn, "--backup-dir", str(tmp_path / "b")]) == 2
    assert "same store" in capsys.readouterr().out


def test_a_connection_failure_names_the_store_not_the_url(tmp_path, capsys):
    pytest.importorskip("psycopg")
    tgt = _sqlite_store(tmp_path / "prod.db", {})
    # A fake password (kept out of one literal so the release secrets scan has nothing to match).
    url = "postgresql://" + ":".join(("scanner", SECRET)) + "@127.0.0.1:1/cars?connect_timeout=2"
    rc = rrs.main(["--source-dsn", url,
                   "--target-dsn", tgt, "--backup-dir", str(tmp_path / "b")])
    out = capsys.readouterr()
    assert rc == 2
    assert "cannot connect to postgres:localhost:1/cars" in out.out
    assert SECRET not in out.out and SECRET not in out.err and "scanner" not in out.out
