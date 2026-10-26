"""apply_recipe_repairs: dry run by default, backup, idempotent apply, verify (N8.9a).

Runs over ``recipe_store_harness`` (a private SQLite store under ``tmp_path``); the
``dealerships`` and ``dealer_geopoints`` tables are created in that same file. Nothing
touches the session DB or the real ``workspace/recipes/repairs_2026_10``.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import closing

import pytest

from backend.scanner import recipe_store
from backend.scripts import apply_recipe_repairs as arr
from backend.tests.recipe_store_harness import TwoHostRecipeStore

D = "audihampton-com"
SECRET = "SECRET-AUTH-VALUE-1234"
_DDL = """
CREATE TABLE dealerships (
    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, website_url TEXT NOT NULL,
    city TEXT NOT NULL, state TEXT NOT NULL, latitude REAL, longitude REAL,
    is_active INTEGER NOT NULL DEFAULT 1, street_address TEXT, zip_code TEXT, dealer_website_url TEXT,
    oem_brand TEXT, phone TEXT, platform TEXT, strategy TEXT,
    street_address_source TEXT, street_address_verified_at TEXT);
CREATE TABLE dealer_geopoints (dealer_url TEXT NOT NULL, dealer_name TEXT, lat REAL, lon REAL,
    zip_code TEXT, city TEXT, state TEXT, geocode_source TEXT, geocoded_at TEXT);
"""


@pytest.fixture
def store(tmp_path, monkeypatch):
    s = TwoHostRecipeStore(tmp_path, monkeypatch)
    with closing(s._connect()) as conn:
        recipe_store._ensure_table(conn)
        conn.executescript(_DDL)
        conn.commit()
    return s


@pytest.fixture
def repairs(tmp_path):
    d = tmp_path / "repairs"
    d.mkdir()
    return d


def _recipe(dealer=D, url="https://omnigraph.audi.com/graphql", hint="oneaudi"):
    return {"dealer_id": dealer, "url": url, "method": "POST", "content_type": "application/json",
            "post_template": "{}", "auth_headers": {"X-Key": SECRET}, "pagination": "graphql_offset",
            "vehicle_rows": 48, "total_count": 89, "provider_hint": hint, "saved_at": 0.0,
            "last_ok_at": 0.0, "stale": False, "stale_reason": "", "field_coverage": {}}


def _row(**kw):
    row = {"name": "Audi Hampton", "website_url": "https://www.audihampton.com",
           "dealer_website_url": "https://www.audihampton.com", "street_address": "2712 Neil Armstrong Pkwy Ste A",
           "city": "Hampton", "state": "VA", "zip_code": "23666", "latitude": 37.062, "longitude": -76.4,
           "phone": "+17578658000", "platform": "oneaudi", "oem_brand": "Audi", "is_active": 1}
    row.update(kw)
    return row


def _roster(repairs, dealer=D, action="add", row=None, **extra):
    doc = {"schema": arr.SCHEMA_ROSTER_ADD, "dealer_id": dealer, "action": action, "dealerships_row": row or _row()}
    doc.update(extra)
    (repairs / f"roster_add_{dealer}.json").write_text(json.dumps(doc))


def _reg(store, sql="SELECT id, name, street_address, platform FROM dealerships ORDER BY id"):
    with closing(sqlite3.connect(store.db_path)) as c:
        return c.execute(sql).fetchall()


def _run(repairs, tmp_path, *, apply=False, **kw):
    lines: list[str] = []
    rc = arr.run(repairs, apply=apply, backup_dir=(tmp_path / "bk") if apply else None, out=lines.append, **kw)
    return rc, "\n".join(lines)


def test_dry_run_is_the_default_and_writes_nothing(store, repairs, tmp_path):
    (repairs / f"recipe_{D}.json").write_text(json.dumps([_recipe()]))
    _roster(repairs)
    rc, out = _run(repairs, tmp_path)
    assert rc == 0 and "[change] recipe_set" in out and "[change] roster_add" in out and "(dry run)" in out
    assert store.db_row(D) is None and _reg(store) == []
    assert not (tmp_path / "bk").exists()
    assert SECRET not in out


def test_apply_without_backup_dir_is_refused(store, repairs):
    assert arr.run(repairs, apply=True, backup_dir=None, out=lambda s: None) == 2


def test_apply_writes_backs_up_verifies_and_is_idempotent(store, repairs, tmp_path):
    old = _recipe(url="https://old.example.com/feed", hint="dealer_dot_com")
    store.write_db(D, [old])
    (repairs / f"recipe_{D}.json").write_text(json.dumps([_recipe()]))
    _roster(repairs)
    rc, out = _run(repairs, tmp_path, apply=True)
    assert rc == 0 and "verify: 2/2" in out
    assert store.db_row(D)["rows"][0]["url"] == "https://omnigraph.audi.com/graphql"
    assert _reg(store) == [(1, "Audi Hampton", "2712 Neil Armstrong Pkwy Ste A", "oneaudi")]
    assert _reg(store, "SELECT dealer_url, lat FROM dealer_geopoints") == [("https://www.audihampton.com", 37.062)]
    run_dir = next((tmp_path / "bk").iterdir())
    backup = json.loads((run_dir / "backup.json").read_text())
    assert backup["dealer_recipes"][0]["dealer_id"] == D and old["url"] in backup["dealer_recipes"][0]["recipes_json"]
    assert backup["inserts"] == [{"table": "dealerships", "website_url": "https://www.audihampton.com",
                                  "street_address": "2712 Neil Armstrong Pkwy Ste A"}]
    assert len((run_dir / "applied.jsonl").read_text().splitlines()) == 2
    # second apply: nothing to do, nothing written
    before = (_reg(store), store.db_row(D))
    rc, out = _run(repairs, tmp_path, apply=True)
    assert rc == 0 and "noop=2" in out and "change" not in out.replace("noop", "")
    assert (_reg(store), store.db_row(D)) == before and len(list((tmp_path / "bk").iterdir())) == 1


def test_add_matches_registry_by_host_and_street_not_name(store, repairs, tmp_path):
    with closing(sqlite3.connect(store.db_path)) as c:
        c.execute("INSERT INTO dealerships (name, website_url, city, state, street_address) VALUES "
                  "('Hampton Audi (old name)', 'https://audihampton.com/', 'Hampton', 'VA', '2712 Neil Armstrong Parkway Suite A')")
        c.commit()
    _roster(repairs)
    rc, out = _run(repairs, tmp_path)
    assert "[noop] roster_add" in out and "id 1" in out
    # same host, other street: a conflict, never a second row
    _roster(repairs, row=_row(street_address="9 Other Rd"))
    rc, out = _run(repairs, tmp_path, apply=True)
    assert rc == 1 and "[conflict]" in out and len(_reg(store)) == 1


def test_repair_updates_only_differing_columns_of_the_named_row(store, repairs, tmp_path):
    with closing(sqlite3.connect(store.db_path)) as c:
        c.execute("INSERT INTO dealerships (name, website_url, city, state, street_address, platform) VALUES "
                  "('Audi Hampton', 'https://www.audihampton.com', 'Hampton', 'VA', '2712 Neil Armstrong Pkwy Ste A', 'unknown')")
        c.commit()
    _roster(repairs, action="repair", existing_dealerships_id=1)
    rc, out = _run(repairs, tmp_path, apply=True)
    assert rc == 0 and "platform: 'unknown' -> 'oneaudi'" in out and "street_address" not in out.split("platform")[0]
    assert _reg(store)[0][3] == "oneaudi" and len(_reg(store)) == 1
    # an id on another host is refused
    _roster(repairs, action="repair", existing_dealerships_id=99)
    assert "[conflict]" in _run(repairs, tmp_path)[1]


def test_gated_roster_add_and_its_recipe_are_skipped_until_allowed(store, repairs, tmp_path):
    (repairs / f"recipe_{D}.json").write_text(json.dumps([_recipe()]))
    _roster(repairs, shared_host=True, apply_gate="shared host: wait for N8.14")
    rc, out = _run(repairs, tmp_path, apply=True)
    assert rc == 0 and out.count("[gated]") == 2 and store.db_row(D) is None and _reg(store) == []
    rc, out = _run(repairs, tmp_path, apply=True, allow_gated={D})
    assert "[gated]" not in out and store.db_row(D) and len(_reg(store)) == 1


def test_registry_fix_matches_exactly_one_row_and_honours_expect(store, repairs, tmp_path):
    with closing(sqlite3.connect(store.db_path)) as c:
        c.execute("INSERT INTO dealerships (name, website_url, city, state, street_address) VALUES "
                  "('Audi Hunstville Service', 'https://www.audihuntsville.com', 'Huntsville', 'AL', '1 Main St')")
        c.execute("INSERT INTO dealerships (name, website_url, city, state, street_address) VALUES "
                  "('Other', 'https://www.audihuntsville.com', 'Huntsville', 'AL', '2 Side St')")
        c.commit()
    fix = {"schema": arr.SCHEMA_REGISTRY_FIX, "dealer_id": "audihuntsville-com",
           "match": {"host": "audihuntsville.com", "street_address": "1 Main Street"},
           "set": {"name": "Audi Huntsville"}, "expect": {"name": "Audi Hunstville Service"}}
    (repairs / "registry_fix_audihuntsville.json").write_text(json.dumps(fix))
    assert _run(repairs, tmp_path, apply=True)[0] == 0
    assert [r[1] for r in _reg(store)] == ["Audi Huntsville", "Other"]
    assert "[noop]" in _run(repairs, tmp_path)[1]
    # host alone matches two rows
    fix["match"] = {"host": "audihuntsville.com"}
    (repairs / "registry_fix_audihuntsville.json").write_text(json.dumps(fix))
    rc, out = _run(repairs, tmp_path)
    assert rc == 1 and "selects 2" in out
    # the row moved since the repair was cut
    fix["match"] = {"host": "audihuntsville.com", "id": 2}
    fix["expect"] = {"name": "Something else"}
    (repairs / "registry_fix_audihuntsville.json").write_text(json.dumps(fix))
    assert "expected" in _run(repairs, tmp_path)[1]


def test_recipe_set_dict_form_archives_without_writing_the_archive(store, repairs, tmp_path):
    keep, drop = _recipe(), _recipe(url="https://old.example.com/a")
    (repairs / "recipe_set_x.json").write_text(json.dumps(
        {"schema": arr.SCHEMA_RECIPE_SET, "dealer_id": D, "recipes": [keep], "archive": [drop]}))
    rc, out = _run(repairs, tmp_path, apply=True)
    assert rc == 0 and [r["url"] for r in store.db_row(D)["rows"]] == [keep["url"]] and "archived" in out


def test_invalid_files_are_reported_not_applied(store, repairs, tmp_path):
    (repairs / "bad.json").write_text("{not json")
    (repairs / "mixed.json").write_text(json.dumps([_recipe(), _recipe(dealer="other-com")]))
    (repairs / "unknown.json").write_text(json.dumps({"schema": "x"}))
    rc, out = _run(repairs, tmp_path, apply=True)
    assert rc == 1 and out.count("[invalid]") == 3 and store.db_row(D) is None


def test_real_staged_repair_files_all_classify(monkeypatch):
    """The staged N1.11 files (when present in this checkout) load as valid repairs."""
    d = arr.DEFAULT_DIR
    if not d.is_dir():
        pytest.skip("workspace repair set not in this checkout")
    reps = arr.load_repairs(d)
    assert reps and not [r for r in reps if r.kind == "invalid"]
