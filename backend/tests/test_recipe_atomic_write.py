"""Recipe cache writes are atomic (P1B.2).

``save_recipes``, the DB-adoption write in ``load_recipes`` and
``save_vdp_recipes`` all go through ``recipes._atomic_write_json``: the bytes go
to ``<name>.json.tmp.<pid>.<thread>``, are fsynced, then ``os.replace``d. A
failure at any point leaves the previous file byte-identical and no temp file
behind, and the ``*.json`` globs the recipe scripts run over the cache dir never
pick up a temp file. The ``dealer_recipes`` write-through failure logs at
WARNING once per error class per process.

Every write here lands under ``tmp_path``; the real ``workspace/`` is never
touched (the script checks run in subprocesses against a tmp cache dir).
"""
from __future__ import annotations

import ast
import json
import logging
import os
import sqlite3
import subprocess
import sys
import threading
from pathlib import Path

import pytest

import backend.scanner.recipe_store as rstore
import backend.scanner.recipes as rec
from backend.scanner.recipes import EndpointRecipe, load_recipes, save_recipes
from backend.scanner.vdp import vdp_recipes as vr

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _tmp_dirs(tmp_path, monkeypatch):
    monkeypatch.setattr(rec, "RECIPES_DIR", tmp_path / "recipes")
    monkeypatch.setattr(vr, "VDP_RECIPES_DIR", tmp_path / "recipes" / "vdp")
    # File-side contract only; the DB mirror is exercised explicitly below.
    monkeypatch.setenv("RECIPES_DB_DISABLED", "1")


def _recipe(dealer_id: str, url: str) -> EndpointRecipe:
    return EndpointRecipe(dealer_id=dealer_id, url=url, method="GET",
                          content_type="application/json", post_template=None)


def _vdp_recipe(dealer_id: str, url: str) -> vr.VdpRecipe:
    return vr.VdpRecipe(dealer_id=dealer_id, url_template=url)


def _leftovers(d: Path) -> list[str]:
    return sorted(p.name for p in d.iterdir() if ".tmp." in p.name)


class _Boom(OSError):
    pass


def _fail_fsync(monkeypatch):
    def boom(fd):
        raise _Boom("disk full during fsync")
    monkeypatch.setattr(os, "fsync", boom)


def _fail_replace(monkeypatch):
    def boom(src, dst):
        raise _Boom("rename refused")
    monkeypatch.setattr(os, "replace", boom)


class _TornFile:
    """A file object that writes the first half of a payload, then fails."""

    def __init__(self, real):
        self._real = real

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._real.close()
        return False

    def write(self, data):
        self._real.write(data[: max(1, len(data) // 2)])
        self._real.flush()
        raise _Boom("device went away mid-write")

    def flush(self):
        self._real.flush()

    def fileno(self):
        return self._real.fileno()


def _fail_mid_write(monkeypatch):
    real_open = open

    def torn_open(path, mode="r", *a, **kw):
        fh = real_open(path, mode, *a, **kw)
        return _TornFile(fh) if "w" in mode else fh

    # _atomic_write_json resolves ``open`` from the recipes module globals first.
    monkeypatch.setattr(rec, "open", torn_open, raising=False)


FAILURES = {
    "mid_write": _fail_mid_write,
    "fsync": _fail_fsync,
    "replace": _fail_replace,
}


# ── save_recipes ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize("failure", sorted(FAILURES))
def test_failed_save_leaves_previous_file_byte_identical(failure, monkeypatch):
    save_recipes("d1", [_recipe("d1", "https://a.example/old")])
    path = rec._recipe_path("d1")
    before = path.read_bytes()

    FAILURES[failure](monkeypatch)
    with pytest.raises(_Boom):
        save_recipes("d1", [_recipe("d1", "https://a.example/new"),
                            _recipe("d1", "https://a.example/new2")])

    assert path.read_bytes() == before
    assert _leftovers(path.parent) == []


def test_failed_first_save_leaves_nothing_behind(monkeypatch):
    _fail_fsync(monkeypatch)
    with pytest.raises(_Boom):
        save_recipes("fresh", [_recipe("fresh", "https://a.example/x")])
    assert sorted(p.name for p in rec.RECIPES_DIR.iterdir()) == []


def test_unserializable_rows_never_create_a_temp_file():
    save_recipes("d2", [_recipe("d2", "https://a.example/old")])
    path = rec._recipe_path("d2")
    before = path.read_bytes()
    with pytest.raises(TypeError):
        rec._atomic_write_json(path, [{"bad": object()}])
    assert path.read_bytes() == before
    assert _leftovers(path.parent) == []


def test_successful_save_bytes_match_the_previous_format():
    """Same bytes as the old ``write_text(json.dumps(rows, indent=1))``, apart from
    ``saved_at``, which save_recipes stamps with the save's own time (P1B.3)."""
    r = _recipe("d3", "https://a.example/feed")
    save_recipes("d3", [r])
    from dataclasses import asdict

    text = rec._recipe_path("d3").read_text(encoding="utf-8")
    stamp = json.loads(text)[0]["saved_at"]
    assert stamp > 0
    assert text == json.dumps([dict(asdict(r), saved_at=stamp)], indent=1)
    assert _leftovers(rec.RECIPES_DIR) == []


def test_temp_file_is_a_sibling_named_after_pid_and_thread(monkeypatch):
    seen: list[tuple[str, str]] = []
    real_replace = os.replace

    def spy(src, dst):
        seen.append((str(src), str(dst)))
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", spy)
    save_recipes("d4", [_recipe("d4", "https://a.example/x")])
    (src, dst) = seen[-1]
    assert Path(dst) == rec._recipe_path("d4")
    assert Path(src).parent == Path(dst).parent
    assert Path(src).name == f"d4.json.tmp.{os.getpid()}.{threading.get_ident()}"


def test_concurrent_saves_never_expose_a_torn_file():
    """Readers racing writers always parse a complete recipe list."""
    save_recipes("race", [_recipe("race", "https://a.example/seed")])
    path = rec._recipe_path("race")
    stop = threading.Event()
    errors: list[str] = []

    def writer(n: int) -> None:
        for i in range(40):
            save_recipes("race", [_recipe("race", f"https://a.example/{n}/{i}/{j}")
                                  for j in range(1 + (i % 7) * 20)])

    def reader() -> None:
        while not stop.is_set():
            try:
                rows = json.loads(path.read_text(encoding="utf-8"))
            except ValueError as exc:
                errors.append(str(exc))
                return
            if not isinstance(rows, list) or not rows:
                errors.append(f"unexpected payload {rows!r:.80}")
                return

    readers = [threading.Thread(target=reader) for _ in range(2)]
    writers = [threading.Thread(target=writer, args=(n,)) for n in range(3)]
    for t in readers + writers:
        t.start()
    for t in writers:
        t.join()
    stop.set()
    for t in readers:
        t.join()
    assert errors == []
    assert _leftovers(path.parent) == []


# ── load_recipes: DB adoption write ───────────────────────────────────────────


def _db_newer(monkeypatch, rows: list[dict], saved: float) -> None:
    monkeypatch.setattr(rstore, "db_load_recipes", lambda dealer_id: (rows, saved))


def test_db_adoption_write_is_atomic(monkeypatch):
    save_recipes("d5", [_recipe("d5", "https://a.example/file")])
    db_rows = [{"dealer_id": "d5", "url": "https://a.example/db", "method": "GET",
                "content_type": "application/json", "post_template": None,
                "saved_at": 9e9}]
    _db_newer(monkeypatch, db_rows, 9e9)

    calls: list[Path] = []
    real = rec._atomic_write_json

    def spy(path, obj):
        calls.append(Path(path))
        return real(path, obj)

    monkeypatch.setattr(rec, "_atomic_write_json", spy)
    (loaded,) = load_recipes("d5")
    assert loaded.url == "https://a.example/db"
    assert calls == [rec._recipe_path("d5")]
    assert json.loads(rec._recipe_path("d5").read_text(encoding="utf-8")) == db_rows
    assert _leftovers(rec.RECIPES_DIR) == []


@pytest.mark.parametrize("failure", sorted(FAILURES))
def test_failed_db_adoption_keeps_the_old_file_and_still_returns_db_rows(failure, monkeypatch):
    save_recipes("d6", [_recipe("d6", "https://a.example/file")])
    path = rec._recipe_path("d6")
    before = path.read_bytes()
    db_rows = [{"dealer_id": "d6", "url": "https://a.example/db", "method": "GET",
                "content_type": "application/json", "post_template": None,
                "saved_at": 9e9}]
    _db_newer(monkeypatch, db_rows, 9e9)

    FAILURES[failure](monkeypatch)
    (loaded,) = load_recipes("d6")  # adoption write is best-effort, as before
    assert loaded.url == "https://a.example/db"
    assert path.read_bytes() == before
    assert _leftovers(path.parent) == []


# ── save_vdp_recipes ──────────────────────────────────────────────────────────


@pytest.mark.parametrize("failure", sorted(FAILURES))
def test_vdp_save_is_atomic(failure, monkeypatch):
    vr.save_vdp_recipes("v1", [_vdp_recipe("v1", "https://d.example/api/{vin}")])
    path = vr._path("v1")
    before = path.read_bytes()
    assert [r.url_template for r in vr.load_vdp_recipes("v1")] == ["https://d.example/api/{vin}"]

    FAILURES[failure](monkeypatch)
    with pytest.raises(_Boom):
        vr.save_vdp_recipes("v1", [_vdp_recipe("v1", "https://d.example/api2/{vin}")])

    assert path.read_bytes() == before
    assert _leftovers(path.parent) == []


def test_vdp_save_goes_through_the_atomic_writer(monkeypatch):
    calls: list[Path] = []
    real = rec._atomic_write_json

    def spy(path, obj):
        calls.append(Path(path))
        return real(path, obj)

    monkeypatch.setattr(vr, "_atomic_write_json", spy)
    vr.save_vdp_recipes("v2", [_vdp_recipe("v2", "https://d.example/api/{vin}")])
    assert calls == [vr._path("v2")]


def test_no_write_text_left_in_the_save_paths():
    """Accept check: the recipe and VDP save paths never call ``write_text``."""
    targets = {
        REPO_ROOT / "backend/scanner/recipes.py": {"load_recipes", "save_recipes", "_atomic_write_json"},
        REPO_ROOT / "backend/scanner/vdp/vdp_recipes.py": {"save_vdp_recipes"},
    }
    for path, funcs in targets.items():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        found = {n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
        assert funcs <= set(found), f"{path.name}: missing {funcs - set(found)}"
        for name in funcs:
            calls = [c for c in ast.walk(found[name])
                     if isinstance(c, ast.Call) and isinstance(c.func, ast.Attribute)
                     and c.func.attr in ("write_text", "write_bytes")]
            assert calls == [], f"{path.name}:{name} still writes in place"


# ── *.json globs never match temp files ───────────────────────────────────────


def _cache_with_leftover_temp(root: Path) -> Path:
    """A cache dir holding two recipes, the alias map, and a crash leftover
    temp file named exactly as ``_atomic_write_json`` names them."""
    d = root / "workspace" / "recipes"
    d.mkdir(parents=True)
    rows = [{"dealer_id": "a", "url": "https://a.example/x", "saved_at": 1.0}]
    for slug in ("alpha", "beta"):
        (d / f"{slug}.json").write_text(json.dumps(rows), encoding="utf-8")
    (d / "_aliases.json").write_text("{}", encoding="utf-8")
    (d / "gamma.json.tmp.4242.123145300000000").write_text(json.dumps(rows), encoding="utf-8")
    (d / "alpha.json.tmp.4242.123145300000001").write_text('[{"half', encoding="utf-8")
    return d


def _run_script_probe(code: str, cache: Path, tmp_path: Path) -> str:
    env = dict(os.environ)
    env.update({
        "PYTHONPATH": str(REPO_ROOT),
        "PROJECT_DOTENV_DISABLE": "1",
        "RECIPES_DB_DISABLED": "1",
        "RECIPES_CACHE_DIR": str(cache),
        "INVENTORY_DATABASE_URL": "",
        "DATABASE_URL": "",
    })
    proc = subprocess.run([sys.executable, "-c", code], cwd=str(tmp_path), env=env,
                          capture_output=True, text=True, timeout=90)
    assert proc.returncode == 0, proc.stderr[-2000:]
    return proc.stdout.strip().splitlines()[-1]


def test_audit_recipe_coverage_glob_skips_temp_files(tmp_path):
    cache = _cache_with_leftover_temp(tmp_path)
    out = _run_script_probe(
        "import json, sys\n"
        "from pathlib import Path\n"
        "import backend.scripts.audit_recipe_coverage as m\n"
        f"m.RECIPES_DIR = Path({str(cache)!r})\n"
        "print(json.dumps(m._all_dealer_ids()))\n",
        cache, tmp_path,
    )
    assert json.loads(out) == ["alpha", "beta"]


def test_import_recipes_to_db_glob_skips_temp_files(tmp_path):
    """import_recipes_to_db is a ``reconcile_recipe_store --cache-dir`` wrapper (P1C.2):
    its dry run over the cache dir (RECIPES_CACHE_DIR) reports exactly the two dealer
    files, against a scratch SQLite store, and writes nothing to the store."""
    cache = _cache_with_leftover_temp(tmp_path)
    store_db = tmp_path / "store.db"
    with sqlite3.connect(store_db) as conn:
        conn.execute(rstore._DDL_SQLITE)
    out_dir = tmp_path / "backups"
    out = _run_script_probe(
        "import csv, json, os, sqlite3, sys\n"
        "from pathlib import Path\n"
        "os.environ['RECIPES_DB_DISABLED'] = ''\n"
        "os.environ['INVENTORY_SQLITE_TESTS'] = '1'\n"
        f"os.environ['INVENTORY_DB_PATH'] = {str(store_db)!r}\n"
        "import backend.scripts.import_recipes_to_db as m\n"
        f"rc = m.main(['--dry-run', '--backup-dir', {str(out_dir)!r}])\n"
        f"[run] = Path({str(out_dir)!r}).iterdir()\n"
        "with open(run / 'report.tsv', encoding='utf-8') as fh:\n"
        "    ids = sorted(r['dealer_id'] for r in csv.DictReader(fh, delimiter='\\t'))\n"
        f"rows = sqlite3.connect({str(store_db)!r}).execute('SELECT count(*) FROM dealer_recipes').fetchone()[0]\n"
        "print(json.dumps([rc, ids, rows]))\n",
        cache, tmp_path,
    )
    assert json.loads(out) == [0, ["alpha", "beta"], 0]


def test_heal_from_recipes_glob_skips_temp_files(tmp_path):
    cache = _cache_with_leftover_temp(tmp_path)
    out = _run_script_probe(
        "import json, sys\n"
        "from pathlib import Path\n"
        "import backend.scripts.heal_from_recipes as m\n"
        f"m._REPO_ROOT = Path({str(tmp_path)!r})\n"
        "seen = []\n"
        "def fake_patch(did, dry_run):\n"
        "    seen.append(did)\n"
        "    return {'vins_from_recipes': 0, 'rows_patched': 0, 'fields': {}}\n"
        "m.patch_dealer = fake_patch\n"
        "sys.argv = ['heal_from_recipes', '--dry-run']\n"
        "m.main()\n"
        "print(json.dumps(seen))\n",
        cache, tmp_path,
    )
    assert json.loads(out) == ["alpha", "beta"]


def test_migrate_recipe_aliases_never_addresses_temp_files(tmp_path):
    """migrate_recipe_aliases does not glob: it builds ``<slug>.json`` from the
    alias map, and ``_slug`` folds dots, so no alias entry can name a temp
    file. Pin both facts (a future glob must be covered here)."""
    src = (REPO_ROOT / "backend/scripts/migrate_recipe_aliases.py").read_text(encoding="utf-8")
    globs = [n for n in ast.walk(ast.parse(src))
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
             and n.func.attr in ("glob", "rglob", "iterdir")]
    assert globs == []
    cache = _cache_with_leftover_temp(tmp_path)
    out = _run_script_probe(
        "import json\n"
        "import backend.scripts.migrate_recipe_aliases as m\n"
        "names = ['gamma.json.tmp.4242.123145300000000', 'alpha.json.tmp.4242.1']\n"
        "print(json.dumps([m._slug(n) + '.json' for n in names]))\n",
        cache, tmp_path,
    )
    built = json.loads(out)
    assert all(".tmp." not in name for name in built)
    assert not any((cache / name).exists() for name in built)


# ── dealer_recipes write-through failures log at WARNING, rate-limited ────────


def test_write_through_failure_warns_once_per_error_class(monkeypatch, caplog):
    monkeypatch.setenv("RECIPES_DB_DISABLED", "")
    monkeypatch.setattr(rstore, "_save_failure_warned", set())
    errors = iter([RuntimeError("db down"), RuntimeError("db still down"),
                   ConnectionError("refused"), RuntimeError("db down again")])

    def broken_conn():
        raise next(errors)

    monkeypatch.setattr(rstore, "_conn", broken_conn)
    with caplog.at_level(logging.DEBUG, logger="scanner"):
        for i in range(4):
            save_recipes(f"wt{i}", [_recipe(f"wt{i}", "https://a.example/x")])

    mine = [r for r in caplog.records if "dealer_recipes" in r.getMessage()]
    warnings = [r for r in mine if r.levelno == logging.WARNING]
    debugs = [r for r in mine if r.levelno == logging.DEBUG]
    assert len(warnings) == 2
    assert "wt0" in warnings[0].getMessage() and "RuntimeError" in warnings[0].getMessage()
    assert "wt2" in warnings[1].getMessage() and "ConnectionError" in warnings[1].getMessage()
    assert len(debugs) == 2
    # The file is the contract: every save still landed on disk.
    for i in range(4):
        assert rec._recipe_path(f"wt{i}").exists()
