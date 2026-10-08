"""The recipe cache is tagged with the store it mirrors; push-ups are refused on a mismatch (P1B.6).

``load_recipes`` pushes a cache file up into ``dealer_recipes`` when the file is newer
than the store's copy. That is right for the store the cache was filled from and wrong
for any other: the MBP cache mirrors the local Postgres, and a home-IP run against prod
with that cache (the P6B.2 repair) would push every newer local set into prod.
``<cache dir>/_store.json`` now records the fingerprint of the store the cache mirrors
(a sha256 of host, port and database name, never credentials):

- a matching fingerprint behaves as before (push-up, DB adoption);
- a different one (or an unreadable tag) warns once per process and refuses the
  push-up, and the store's own copy is served instead of the newer file, so a caller
  that saves the set (a replay's last_ok update, an un-stale) cannot write the other
  store's file back through either;
- a missing tag is created on the first load that reached the store;
- another cache dir (``RECIPES_CACHE_DIR``) or a re-seed
  (``python -m backend.scanner.recipes --reseed``) gives the process a cache of its own.

Every test runs over the per-test SQLite store of ``recipe_store_harness`` (wired through
``recipe_store._conn``), so the Postgres URLs below only name stores, and nothing
connects to them. Nothing touches the session DB or the real workspace.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path

import pytest

import backend.scanner.recipes as rec
from backend.scanner.recipes import EndpointRecipe, load_recipes
from backend.tests.recipe_store_harness import TwoHostRecipeStore

REPO = Path(__file__).resolve().parents[2]
D = "identity-dealer-com"
D2 = "second-dealer-com"
LOCAL_URL = "https://api.identity-dealer.example/local-set"
PROD_URL = "https://api.identity-dealer.example/prod-set"
OLD = 1_700_000_000.0
NEW = 1_800_000_000.0

# Two stores, by URL only. The passwords must never reach the tag or the logs.
LOCAL_DSN = "postgresql://scanner:local-secret-pw@localhost:5432/cars"
PROD_DSN = "postgresql://railway_admin:prod-secret-pw@prod-db.example.net:41234/railway"
SECRETS = ("local-secret-pw", "prod-secret-pw", "railway_admin", "scanner:", "scanner@")


@pytest.fixture
def store(tmp_path, monkeypatch):
    s = TwoHostRecipeStore(tmp_path, monkeypatch)
    monkeypatch.setenv("DATABASE_URL", "")
    monkeypatch.setenv("INVENTORY_DATABASE_URL", LOCAL_DSN)
    return s


def _on_store(monkeypatch, dsn: str) -> None:
    """This process now writes through to the store ``dsn`` names."""
    monkeypatch.setenv("INVENTORY_DATABASE_URL", dsn)


def _row(url: str, saved_at: float, **kw) -> dict:
    return asdict(EndpointRecipe(dealer_id=D, url=url, method="GET", content_type="application/json",
                                 post_template=None, saved_at=saved_at, **kw))


def _tag(cache: Path) -> dict:
    return json.loads((cache / rec.STORE_TAG_FILENAME).read_text(encoding="utf-8"))


def _mismatch_warnings(caplog) -> list[logging.LogRecord]:
    return [r for r in caplog.records
            if r.levelno == logging.WARNING and "does not mirror this store" in r.getMessage()]


def _push_up_warnings(caplog) -> list[logging.LogRecord]:
    return [r for r in caplog.records
            if r.levelno == logging.WARNING and "newer than dealer_recipes" in r.getMessage()]


def _tag_for(cache: Path, dsn: str, monkeypatch) -> None:
    """Tag ``cache`` as the mirror of ``dsn``'s store (as a load on that store would)."""
    monkeypatch.setenv("INVENTORY_DATABASE_URL", dsn)
    rec.write_store_tag(rec.store_identity(), source="test", cache_dir=cache)


# ── the fingerprint ────────────────────────────────────────────────────────────

def _fp(monkeypatch, dsn: str) -> str | None:
    monkeypatch.setenv("INVENTORY_DATABASE_URL", dsn)
    return rec.store_fingerprint()


def test_fingerprint_is_host_port_and_dbname_never_credentials(store, monkeypatch):
    assert rec._pg_store_identity(LOCAL_DSN) == "postgres:localhost:5432/cars"
    assert rec._pg_store_identity(PROD_DSN) == "postgres:prod-db.example.net:41234/railway"
    local = _fp(monkeypatch, LOCAL_DSN)
    assert local == _fp(monkeypatch, "postgresql://other:other-pw@localhost:5432/cars?sslmode=require")
    assert local == _fp(monkeypatch, "postgres://127.0.0.1/cars"), "loopback names and the default port"
    assert local == _fp(monkeypatch, "postgresql:///cars?host=/var/run/postgresql"), "a Unix socket is local"
    assert local == _fp(monkeypatch, "postgresql://scanner@LOCALHOST:5432/cars")
    # The mini: its tunnel to the MBP's store and its own Postgres differ only by port.
    assert _fp(monkeypatch, "postgresql://localhost:15432/cars") != local
    assert _fp(monkeypatch, "postgresql://localhost:5432/cars_test") != local
    assert _fp(monkeypatch, PROD_DSN) != local
    for secret in SECRETS:
        assert secret not in rec._pg_store_identity(PROD_DSN)


def test_no_store_means_no_fingerprint_and_sqlite_is_named_by_its_file(store, monkeypatch):
    monkeypatch.setenv("RECIPES_DB_DISABLED", "1")
    assert rec.store_identity() is None and rec.store_fingerprint() is None
    monkeypatch.setenv("RECIPES_DB_DISABLED", "")
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "")
    ident = rec.store_identity()  # the suite runs SQLite (INVENTORY_SQLITE_TESTS)
    assert ident is not None and ident.startswith("sqlite:")


# ── a matching fingerprint behaves as before ──────────────────────────────────

def test_matching_fingerprint_pushes_up_and_adopts_as_before(store, monkeypatch, caplog):
    cache = store.dirs["mbp"]
    _tag_for(cache, LOCAL_DSN, monkeypatch)
    store.write_db(D, [_row(PROD_URL, OLD)])
    store.write_file("mbp", D, [_row(LOCAL_URL, NEW)])
    store.write_db(D2, [_row(PROD_URL, NEW)])
    store.write_file("mbp", D2, [_row(LOCAL_URL, OLD)])

    with caplog.at_level(logging.WARNING, logger="scanner"):
        (r,) = load_recipes(D)
        (r2,) = load_recipes(D2)

    assert r.url == LOCAL_URL
    assert [x["url"] for x in store.db_row(D)["rows"]] == [LOCAL_URL], "the newer file was pushed up"
    assert len(_push_up_warnings(caplog)) == 1
    assert r2.url == PROD_URL and store.file_rows("mbp", D2)[0]["url"] == PROD_URL, "the newer DB copy wins"
    assert _mismatch_warnings(caplog) == []
    assert rec.check_cache_store().verdict == "match"


# ── a mismatch: no push-up, one warning ───────────────────────────────────────

def test_mismatch_refuses_the_push_up_serves_the_store_copy_and_warns_once(store, monkeypatch, caplog):
    cache = store.dirs["mbp"]
    _tag_for(cache, LOCAL_DSN, monkeypatch)          # the MBP cache mirrors the local store
    _on_store(monkeypatch, PROD_DSN)                 # ... and this process writes to prod
    store.write_db(D, [_row(PROD_URL, OLD)])
    local_file = store.write_file("mbp", D, [_row(LOCAL_URL, NEW)])
    store.write_db(D2, [_row(PROD_URL, OLD)])
    store.write_file("mbp", D2, [_row(LOCAL_URL, NEW)])
    file_before = local_file.read_bytes()
    db_before = store.db_row(D)

    with caplog.at_level(logging.DEBUG, logger="scanner"):
        first = load_recipes(D)
        second = load_recipes(D)
        other = load_recipes(D2)

    assert [r.url for r in first] == [PROD_URL], "the store's own copy, not the newer foreign file"
    assert [r.url for r in second] == [PROD_URL] and [r.url for r in other] == [PROD_URL]
    assert store.db_row(D) == db_before, "nothing was pushed up"
    assert [x["url"] for x in store.db_row(D2)["rows"]] == [PROD_URL]
    assert local_file.read_bytes() == file_before, "the cache file is left alone"
    assert _push_up_warnings(caplog) == []
    (warning,) = _mismatch_warnings(caplog)          # once per process, not per load or dealer
    msg = warning.getMessage()
    assert "--reseed" in msg and "RECIPES_CACHE_DIR" in msg and str(cache) in msg
    for secret in SECRETS:
        assert secret not in msg
    assert _tag(cache)["fingerprint"] == rec.store_fingerprint(rec._pg_store_identity(LOCAL_DSN)), \
        "a mismatch never re-tags the cache"
    assert rec.check_cache_store().verdict == "mismatch"


def test_mismatch_with_no_store_copy_serves_the_file_and_still_never_pushes(store, monkeypatch, caplog):
    _tag_for(store.dirs["mbp"], LOCAL_DSN, monkeypatch)
    _on_store(monkeypatch, PROD_DSN)
    store.write_file("mbp", D, [_row(LOCAL_URL, NEW)])

    with caplog.at_level(logging.WARNING, logger="scanner"):
        (r,) = load_recipes(D)

    assert r.url == LOCAL_URL
    assert store.db_row(D) is None, "no DB row was created from the foreign cache"
    assert _push_up_warnings(caplog) == [] and len(_mismatch_warnings(caplog)) == 1


def test_mismatch_replay_success_saves_the_store_set_not_the_cache_set(store, monkeypatch):
    # The P6B.2 un-stale: a replay that answers writes last_ok / un-stale back through
    # load_recipes + save_recipes. On a foreign cache that must be the store's set.
    _tag_for(store.dirs["mbp"], LOCAL_DSN, monkeypatch)
    _on_store(monkeypatch, PROD_DSN)
    store.write_db(D, [_row(PROD_URL, OLD, stale=True, stale_reason="403 from railway")])
    store.write_file("mbp", D, [_row(LOCAL_URL, NEW)])
    (stored,) = load_recipes(D)
    stored.last_ok_at = NEW + 5

    rec._persist_replay_success(D, stored, retrying_stale=True)

    (row,) = store.db_row(D)["rows"]
    assert row["url"] == PROD_URL and row["stale"] is False and row["last_ok_at"] == NEW + 5


def test_an_unreadable_tag_counts_as_a_mismatch(store, monkeypatch, caplog):
    cache = store.dirs["mbp"]
    cache.mkdir(parents=True, exist_ok=True)
    (cache / rec.STORE_TAG_FILENAME).write_text("{not json", encoding="utf-8")
    store.write_db(D, [_row(PROD_URL, OLD)])
    store.write_file("mbp", D, [_row(LOCAL_URL, NEW)])

    with caplog.at_level(logging.WARNING, logger="scanner"):
        (r,) = load_recipes(D)

    assert r.url == PROD_URL and [x["url"] for x in store.db_row(D)["rows"]] == [PROD_URL]
    (warning,) = _mismatch_warnings(caplog)
    assert "unreadable store tag" in warning.getMessage()
    assert (cache / rec.STORE_TAG_FILENAME).read_text(encoding="utf-8") == "{not json", "never overwritten"


# ── a missing tag ──────────────────────────────────────────────────────────────

def test_a_missing_tag_is_created_on_the_first_successful_load(store, monkeypatch, caplog):
    cache = store.dirs["mbp"]
    assert rec.check_cache_store().verdict == "untagged"
    assert load_recipes("nothing-com") == []
    assert not (cache / rec.STORE_TAG_FILENAME).exists(), "a load that never reached the store tags nothing"

    store.write_db(D, [_row(PROD_URL, NEW)])
    with caplog.at_level(logging.INFO, logger="scanner"):
        (r,) = load_recipes(D)

    assert r.url == PROD_URL
    tag = _tag(cache)
    assert tag["fingerprint"] == rec.store_fingerprint() and tag["kind"] == "postgres"
    assert tag["tagged_by"] == "load_recipes" and tag["version"] == rec.STORE_TAG_VERSION
    raw = (cache / rec.STORE_TAG_FILENAME).read_text(encoding="utf-8")
    for leak in (*SECRETS, "localhost", "cars"):
        assert leak not in raw, f"{leak!r} in the tag file"
    assert any("tagged as the mirror of store" in m.getMessage() for m in caplog.records)
    assert rec.check_cache_store().verdict == "match"
    assert sorted(p.name for p in cache.glob("*.json") if not p.name.startswith("_")) == [f"{D}.json"], \
        "the tag is not a dealer file"


def test_an_untagged_cache_pushes_up_on_its_first_load_and_is_tagged_for_that_store(store, caplog):
    store.write_file("mbp", D, [_row(LOCAL_URL, NEW)])
    with caplog.at_level(logging.WARNING, logger="scanner"):
        (r,) = load_recipes(D)
    assert r.url == LOCAL_URL and [x["url"] for x in store.db_row(D)["rows"]] == [LOCAL_URL]
    assert len(_push_up_warnings(caplog)) == 1
    assert _tag(store.dirs["mbp"])["fingerprint"] == rec.store_fingerprint()


def test_no_store_writes_no_tag(store, monkeypatch):
    monkeypatch.setenv("RECIPES_DB_DISABLED", "1")
    store.write_file("mbp", D, [_row(LOCAL_URL, NEW)])
    (r,) = load_recipes(D)
    assert r.url == LOCAL_URL
    assert not (store.dirs["mbp"] / rec.STORE_TAG_FILENAME).exists()
    assert rec.check_cache_store().verdict == "no store"


# ── a cache of its own: another cache dir, or a re-seed ───────────────────────

def test_another_cache_dir_mirrors_the_new_store(store, monkeypatch, caplog):
    # The MBP cache stays tagged local; a prod run with RECIPES_CACHE_DIR elsewhere
    # ("mini" here: any other dir) gets a cache of its own, tagged for prod.
    _tag_for(store.dirs["mbp"], LOCAL_DSN, monkeypatch)
    _on_store(monkeypatch, PROD_DSN)
    store.use("mini")
    store.write_db(D, [_row(PROD_URL, OLD)])
    store.write_file("mini", D, [_row(LOCAL_URL, NEW)])
    with caplog.at_level(logging.WARNING, logger="scanner"):
        (r,) = load_recipes(D)
    assert r.url == LOCAL_URL and [x["url"] for x in store.db_row(D)["rows"]] == [LOCAL_URL]
    assert _mismatch_warnings(caplog) == []
    assert _tag(store.dirs["mini"])["fingerprint"] == rec.store_fingerprint()
    assert _tag(store.dirs["mbp"])["fingerprint"] != rec.store_fingerprint()


def test_status_cli_reads_the_cache_dir_named_by_recipes_cache_dir(tmp_path, monkeypatch):
    cache = tmp_path / "prod-lane-cache"
    monkeypatch.setenv("INVENTORY_DATABASE_URL", LOCAL_DSN)
    monkeypatch.setenv("RECIPES_DB_DISABLED", "")
    rec.write_store_tag(rec.store_identity(), source="test", cache_dir=cache)
    before = sorted(p.name for p in cache.iterdir())
    env = {**os.environ, "RECIPES_CACHE_DIR": str(cache), "INVENTORY_DATABASE_URL": PROD_DSN,
           "DATABASE_URL": "", "RECIPES_DB_DISABLED": "", "PROJECT_DOTENV_DISABLE": "1"}

    out = subprocess.run([sys.executable, "-m", "backend.scanner.recipes", "--status"], cwd=REPO, env=env,
                         capture_output=True, text=True, timeout=90)

    assert out.returncode == 1, out.stdout + out.stderr
    assert f"recipe cache: {cache}" in out.stdout
    assert "verdict:      mismatch" in out.stdout
    assert "postgres:prod-db.example.net:41234/railway" in out.stdout
    for secret in SECRETS:
        assert secret not in out.stdout + out.stderr
    assert sorted(p.name for p in cache.iterdir()) == before, "--status writes nothing"


def test_reseed_moves_the_dealer_files_aside_and_tags_the_cache_for_this_store(store, monkeypatch, capsys,
                                                                                caplog):
    cache = store.dirs["mbp"]
    _tag_for(cache, LOCAL_DSN, monkeypatch)
    store.write_file("mbp", D, [_row(LOCAL_URL, NEW)])
    store.write_file("mbp", D2, [_row(LOCAL_URL, NEW)])
    (cache / "_aliases.json").write_text(json.dumps({"old-name-com": D}), encoding="utf-8")
    (cache / "vdp").mkdir()
    (cache / "vdp" / f"{D}.json").write_text("[]", encoding="utf-8")
    _on_store(monkeypatch, PROD_DSN)
    store.write_db(D, [_row(PROD_URL, OLD)])

    # Dry run: reports, changes nothing.
    before = sorted(str(p.relative_to(cache)) for p in cache.rglob("*"))
    assert rec.main(["--reseed", "--dry-run"]) == 0
    assert "would move 2 dealer recipe file(s)" in capsys.readouterr().out
    assert sorted(str(p.relative_to(cache)) for p in cache.rglob("*")) == before

    assert rec.main(["--reseed"]) == 0
    out = capsys.readouterr().out
    assert "moved 2 dealer recipe file(s)" in out and "tagged for store postgres:prod-db.example.net" in out
    (backup,) = (cache / rec.RESEED_BACKUP_DIRNAME).iterdir()
    assert sorted(p.name for p in backup.iterdir()) == [f"{D}.json", f"{D2}.json"]
    assert sorted(p.name for p in cache.glob("*.json")) == ["_aliases.json", rec.STORE_TAG_FILENAME]
    assert (cache / "vdp" / f"{D}.json").exists(), "VDP recipes have no store mirror and stay"
    tag = _tag(cache)
    assert tag["fingerprint"] == rec.store_fingerprint() and tag["tagged_by"] == "reseed"

    # The cache refills from the store, and push-ups are this store's again.
    with caplog.at_level(logging.WARNING, logger="scanner"):
        (r,) = load_recipes(D)
        assert r.url == PROD_URL and store.file_rows("mbp", D)[0]["url"] == PROD_URL
        assert load_recipes(D2) == [] and store.db_row(D2) is None
        store.write_file("mbp", D2, [_row(LOCAL_URL, NEW)])
        load_recipes(D2)
    assert [x["url"] for x in store.db_row(D2)["rows"]] == [LOCAL_URL]
    assert _mismatch_warnings(caplog) == []
    assert rec.main(["--status"]) == 0 and "verdict:      match" in capsys.readouterr().out


def test_reseed_refuses_without_a_store(store, monkeypatch, capsys):
    monkeypatch.setenv("RECIPES_DB_DISABLED", "1")
    store.write_file("mbp", D, [_row(LOCAL_URL, NEW)])
    assert rec.main(["--reseed"]) == 2
    assert "reseed refused" in capsys.readouterr().out
    assert store.file_rows("mbp", D) is not None
    assert not (store.dirs["mbp"] / rec.STORE_TAG_FILENAME).exists()


def test_two_reseeds_never_share_a_backup_dir(store, monkeypatch):
    cache = store.dirs["mbp"]
    store.write_file("mbp", D, [_row(LOCAL_URL, NEW)])
    first = rec.reseed_cache()
    store.write_file("mbp", D, [_row(PROD_URL, NEW)])
    second = rec.reseed_cache()
    assert first.backup_dir != second.backup_dir
    assert len(list((cache / rec.RESEED_BACKUP_DIRNAME).iterdir())) == 2
    assert json.loads((first.backup_dir / f"{D}.json").read_text(encoding="utf-8"))[0]["url"] == LOCAL_URL
    assert json.loads((second.backup_dir / f"{D}.json").read_text(encoding="utf-8"))[0]["url"] == PROD_URL
