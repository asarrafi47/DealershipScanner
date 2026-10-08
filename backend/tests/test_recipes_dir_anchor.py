"""The recipe cache dir is anchored at the repo root, not the cwd (P1B.2).

``backend.scanner.recipes.RECIPES_DIR`` used to be ``Path("workspace") /
"recipes"``, so a scan started from another directory wrote a second cache
there. It is now ``<repo>/workspace/recipes`` (``/app/workspace/recipes`` in
the Railway image), or ``$RECIPES_CACHE_DIR`` when set; the VDP recipes follow
as ``<dir>/vdp`` and ``recipe_synth.RECIPES_DIR`` is the same object.

Import-time resolution is checked in subprocesses (cwd outside the repo, env
controlled), so this process's modules and the real ``workspace/`` are never
touched; nothing here writes a recipe.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

import backend.scanner.recipe_synth as rs
import backend.scanner.recipes as rec
from backend.scanner.vdp import vdp_recipes as vr

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DIR = REPO_ROOT / "workspace" / "recipes"

_PROBE = """
import json
import backend.scanner.recipes as rec
import backend.scanner.recipe_synth as rs
from backend.scanner.vdp import vdp_recipes as vr
print(json.dumps({
    "recipes": str(rec.RECIPES_DIR),
    "synth": str(rs.RECIPES_DIR),
    "vdp": str(vr.VDP_RECIPES_DIR),
    "path_x": str(rec._recipe_path("x")),
}))
"""


def _probe(cwd: Path | str, override: str | None) -> dict:
    env = dict(os.environ)
    env.update({
        "PYTHONPATH": str(REPO_ROOT),
        "PROJECT_DOTENV_DISABLE": "1",
        "RECIPES_DB_DISABLED": "1",
        "INVENTORY_DATABASE_URL": "",
        "DATABASE_URL": "",
    })
    env.pop("RECIPES_CACHE_DIR", None)
    if override is not None:
        env["RECIPES_CACHE_DIR"] = override
    proc = subprocess.run([sys.executable, "-c", _PROBE], cwd=str(cwd), env=env,
                          capture_output=True, text=True, timeout=90)
    assert proc.returncode == 0, proc.stderr[-2000:]
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_default_dir_is_repo_anchored_from_tmp_cwd():
    cwd = Path("/tmp") if Path("/tmp").is_dir() else Path(tempfile.gettempdir())
    assert REPO_ROOT not in (cwd, *cwd.resolve().parents)
    got = _probe(cwd, None)
    assert got["recipes"] == str(DEFAULT_DIR)
    assert got["synth"] == str(DEFAULT_DIR)
    assert got["vdp"] == str(DEFAULT_DIR / "vdp")
    assert got["path_x"] == str(DEFAULT_DIR / "x.json")


def test_default_dir_is_the_same_from_the_repo_root():
    """Running from the repo root (the normal case) is unchanged."""
    got = _probe(REPO_ROOT, None)
    assert got["recipes"] == str(DEFAULT_DIR)
    assert got["vdp"] == str(DEFAULT_DIR / "vdp")


@pytest.mark.parametrize("blank", ["", "   "])
def test_blank_override_means_default(blank, tmp_path):
    got = _probe(tmp_path, blank)
    assert got["recipes"] == str(DEFAULT_DIR)


def test_override_is_honoured_for_list_vdp_and_synth(tmp_path):
    cache = tmp_path / "prod-lane-cache"
    got = _probe(tmp_path, str(cache))
    assert got["recipes"] == str(cache)
    assert got["synth"] == str(cache)
    assert got["vdp"] == str(cache / "vdp")
    assert got["path_x"] == str(cache / "x.json")
    assert not cache.exists()  # resolving the dir creates nothing


def test_relative_override_is_pinned_against_the_launch_cwd(tmp_path):
    got = _probe(tmp_path, "rel/cache")
    # The child's cwd is the real path (os.getcwd resolves /var -> /private/var).
    assert got["recipes"] == str(tmp_path.resolve() / "rel" / "cache")
    assert got["vdp"] == str(tmp_path.resolve() / "rel" / "cache" / "vdp")


def test_railway_layout_resolves_to_app_workspace():
    """scripts/railway_scan_fleet.sh runs from /app with /app/workspace a
    symlink onto the volume; the cache must stay /app/workspace/recipes."""
    fake = "/app/backend/scanner/recipes.py"
    assert rec.resolve_recipes_dir(module_file=fake, environ={}) == Path("/app/workspace/recipes")
    assert rec.resolve_recipes_dir(
        module_file=fake, environ={"RECIPES_CACHE_DIR": "/data/lane"},
    ) == Path("/data/lane")


def test_module_executed_with_fake_app_file_resolves_to_app_workspace(tmp_path):
    """Execute the real recipes.py source as a module whose ``__file__`` is
    ``/app/backend/scanner/recipes.py`` (the Railway image path)."""
    code = f"""
import json, sys, types
src = open({str(REPO_ROOT / "backend/scanner/recipes.py")!r}, encoding="utf-8").read()
mod = types.ModuleType("fake_app_recipes")
mod.__file__ = "/app/backend/scanner/recipes.py"
sys.modules[mod.__name__] = mod
exec(compile(src, mod.__file__, "exec"), mod.__dict__)
print(json.dumps({{"dir": str(mod.RECIPES_DIR), "x": str(mod._recipe_path("x"))}}))
"""
    env = dict(os.environ)
    env.update({"PYTHONPATH": str(REPO_ROOT), "PROJECT_DOTENV_DISABLE": "1"})
    env.pop("RECIPES_CACHE_DIR", None)
    proc = subprocess.run([sys.executable, "-c", code], cwd=str(tmp_path), env=env,
                          capture_output=True, text=True, timeout=90)
    assert proc.returncode == 0, proc.stderr[-2000:]
    got = json.loads(proc.stdout.strip().splitlines()[-1])
    assert got == {"dir": "/app/workspace/recipes", "x": "/app/workspace/recipes/x.json"}


def test_recipe_path_is_unchanged_after_chdir(tmp_path, monkeypatch):
    before = rec._recipe_path("x")
    before_vdp = vr._path("x")
    assert before.is_absolute() and before_vdp.is_absolute()
    monkeypatch.chdir(tmp_path)
    assert rec._recipe_path("x") == before
    assert vr._path("x") == before_vdp


def test_in_process_dirs_are_absolute_and_consistent():
    # RECIPES_DIR may be monkeypatched by a fixture; the resolver is not.
    assert rec.resolve_recipes_dir().is_absolute()
    if not os.environ.get("RECIPES_CACHE_DIR"):
        assert rec.resolve_recipes_dir() == DEFAULT_DIR


def test_recipe_synth_dir_follows_recipes_live(tmp_path, monkeypatch):
    assert rs.RECIPES_DIR is rec.RECIPES_DIR
    monkeypatch.setattr(rec, "RECIPES_DIR", tmp_path / "elsewhere")
    assert rs.RECIPES_DIR == tmp_path / "elsewhere"
    with pytest.raises(AttributeError):
        rs.no_such_name  # noqa: B018 — the forwarder must not swallow typos
