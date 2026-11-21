"""Card L23: the data-repository template, ``persist.sh`` and ``ops_hydrate.sh`` (docs/scan_loop/BUNDLE.md section 10)."""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path

import pytest

from backend.scanner.loop.config import load_loop_config

ROOT = Path(__file__).resolve().parents[2]
TEMPLATE = ROOT / "docs" / "scan_loop" / "data_repo_template"
PERSIST = ROOT / "scripts" / "cloud" / "persist.sh"
HYDRATE = ROOT / "scripts" / "cloud" / "ops_hydrate.sh"

_GIT_ENV = {
    "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.invalid",
    "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.invalid",
    "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_SYSTEM": os.devnull, "GIT_TERMINAL_PROMPT": "0",
}


def _git(cwd: Path, *args: str) -> str:
    r = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, env={**os.environ, **_GIT_ENV})
    assert r.returncode == 0, r.stderr
    return r.stdout


def _sh(script: Path, *args: str, **env: str) -> subprocess.CompletedProcess:
    return subprocess.run(["bash", str(script), *args], capture_output=True, text=True,
                          env={**os.environ, **_GIT_ENV, "PERSIST_SLEEP": "0", **env})


def _seed_remote(tmp_path: Path) -> Path:
    """A bare remote holding the template on main, like a freshly created data repository."""
    seed = tmp_path / "seed"
    seed.mkdir()
    _git(seed, "init", "-q", "-b", "main")
    for src in TEMPLATE.rglob("*"):
        if src.is_file():
            dest = seed / src.relative_to(TEMPLATE)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(src.read_bytes())
    _git(seed, "add", "-A")
    _git(seed, "commit", "-q", "-m", "init")
    bare = tmp_path / "data.git"
    _git(tmp_path, "clone", "-q", "--bare", str(seed), str(bare))
    return bare


def _clone(tmp_path: Path, bare: Path, name: str) -> Path:
    _git(tmp_path, "clone", "-q", str(bare), name)
    return tmp_path / name


def test_template_files_exist_and_the_config_is_a_working_enabled_config():
    for rel in (".gitattributes", ".gitignore", "CLAUDE.md", "README.md", "config/loop.json", ".claude/settings.json"):
        assert (TEMPLATE / rel).is_file(), rel
    cfg = load_loop_config((TEMPLATE / "config" / "loop.json").read_text(encoding="utf-8"))
    assert cfg.enabled and cfg.dealers_per_cycle == 6 and cfg.cycles_per_day == 4
    settings = json.loads((TEMPLATE / ".claude" / "settings.json").read_text(encoding="utf-8"))
    assert "WebFetch" in settings["permissions"]["deny"] and "WebSearch" in settings["permissions"]["deny"]


def test_template_attributes_and_ignores_are_the_bundle_md_ones():
    attrs = (TEMPLATE / ".gitattributes").read_text(encoding="utf-8")
    for path in ("workspace/dealer_logs/*/scan_runs.md", "workspace/dealer_logs/*/discovery.md",
                 "workspace/dealer_logs/_learning/errors_index.md"):
        assert re.search(re.escape(path) + r"\s+merge=union", attrs), path
    ignores = (TEMPLATE / ".gitignore").read_text(encoding="utf-8").split()
    assert ignores == ["workspace/*", "!workspace/dealer_logs/", "proposals/_scratch/"]


@pytest.mark.parametrize("path", [p for p in TEMPLATE.rglob("*") if p.is_file()] + [PERSIST, HYDRATE], ids=lambda p: p.name)
def test_no_token_host_or_id_shapes_in_the_template_or_scripts(path):
    text = path.read_text(encoding="utf-8")
    shapes = [r"gh[pousr]_[0-9A-Za-z]{36}", r"github_pat_[0-9A-Za-z_]{20,}", r"sk-ant-[0-9A-Za-z_-]{20,}",
              r"-----BEGIN [A-Z ]*PRIVATE KEY-----", r"postgres(ql)?://[^:/@ ]+:[^@/ ]+@", r"railway\.app",
              r"https://[^/\s]+:[^@/\s]+@"]
    for shape in shapes:
        assert not re.search(shape, text), f"{path.name}: {shape}"


def test_two_clones_appending_to_one_scan_runs_md_end_with_both_blocks(tmp_path):
    bare = _seed_remote(tmp_path)
    a, b = _clone(tmp_path, bare, "a"), _clone(tmp_path, bare, "b")
    for clone, who in ((a, "railway"), (b, "session")):
        log = clone / "workspace" / "dealer_logs" / "abc-com" / "scan_runs.md"
        log.parent.mkdir(parents=True, exist_ok=True)
        log.write_text(f"## block from {who}\n", encoding="utf-8")
    r1 = _sh(PERSIST, "-m", "first", LOOP_DATA_DIR=str(a))
    assert r1.returncode == 0, r1.stderr
    r2 = _sh(PERSIST, "-m", "second", LOOP_DATA_DIR=str(b))
    assert r2.returncode == 0, r2.stderr
    merged = _git(bare, "show", "main:workspace/dealer_logs/abc-com/scan_runs.md")
    assert "block from railway" in merged and "block from session" in merged


def test_persist_stages_only_session_paths_and_leaves_railway_files_alone(tmp_path):
    bare = _seed_remote(tmp_path)
    clone = _clone(tmp_path, bare, "c")
    (clone / "findings").mkdir()
    (clone / "findings" / "f1.md").write_text("finding\n", encoding="utf-8")
    (clone / "mirror").mkdir()
    (clone / "mirror" / "index.tsv").write_text("x\n", encoding="utf-8")
    (clone / "requests").mkdir()
    (clone / "requests" / "rq-20261121T1000Z-01.status.json").write_text("{}", encoding="utf-8")
    assert _sh(PERSIST, LOOP_DATA_DIR=str(clone)).returncode == 0
    files = _git(bare, "ls-tree", "-r", "--name-only", "main").split()
    assert "findings/f1.md" in files
    assert "mirror/index.tsv" not in files and not any(f.endswith(".status.json") for f in files)


def test_persist_with_nothing_new_is_a_no_op(tmp_path):
    bare = _seed_remote(tmp_path)
    clone = _clone(tmp_path, bare, "c")
    r = _sh(PERSIST, LOOP_DATA_DIR=str(clone))
    assert r.returncode == 0 and "nothing to persist" in r.stdout


def test_persist_refuses_new_requests_when_the_validator_is_missing(tmp_path):
    if (ROOT / "backend" / "scripts" / "ops_requests.py").exists():
        pytest.skip("validator present: covered by the validation test")
    bare = _seed_remote(tmp_path)
    clone = _clone(tmp_path, bare, "c")
    (clone / "requests").mkdir()
    (clone / "requests" / "rq-20261121T1000Z-01.json").write_text("{}", encoding="utf-8")
    r = _sh(PERSIST, LOOP_DATA_DIR=str(clone))
    assert r.returncode == 1 and "refusing" in r.stderr


def test_persist_keeps_the_commit_and_exits_1_when_the_remote_rejects(tmp_path):
    bare = _seed_remote(tmp_path)
    clone = _clone(tmp_path, bare, "c")
    hook = bare / "hooks" / "pre-receive"
    hook.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
    hook.chmod(0o755)
    (clone / "findings").mkdir()
    (clone / "findings" / "f.md").write_text("x\n", encoding="utf-8")
    r = _sh(PERSIST, LOOP_DATA_DIR=str(clone))
    assert r.returncode == 1 and "committed" in r.stderr
    assert "findings/f.md" in _git(clone, "show", "--name-only", "--format=", "HEAD")


def test_hydrate_links_workspace_without_dirtying_the_code_checkout_and_exports(tmp_path):
    bare = _seed_remote(tmp_path)
    data = _clone(tmp_path, bare, "data")
    # a code repository with a dev branch; workspace/ is ignored there like in the real one
    code_src = tmp_path / "code_src"
    code_src.mkdir()
    _git(code_src, "init", "-q", "-b", "dev")
    (code_src / ".gitignore").write_text("workspace/\n", encoding="utf-8")
    (code_src / "VERSION").write_text("0\n", encoding="utf-8")
    _git(code_src, "add", "-A")
    _git(code_src, "commit", "-q", "-m", "c")
    env_file = tmp_path / "envfile"
    code = tmp_path / "code"
    r = _sh(HYDRATE, str(data), str(code), LOOP_CODE_URL=str(code_src), CLAUDE_ENV_FILE=str(env_file))
    assert r.returncode == 0, r.stderr
    assert (code / "workspace").is_symlink() and (code / "workspace").resolve() == (data / "workspace").resolve()
    assert _git(code, "status", "--porcelain") == ""
    text = env_file.read_text(encoding="utf-8")
    assert f'export DEALER_LOGS_ROOT="{data}/workspace/dealer_logs"' in text
    assert "export RECIPES_DB_DISABLED=1" in text and "RECIPES_CACHE_DIR" in text
    again = _sh(HYDRATE, str(data), str(code), LOOP_CODE_URL=str(code_src), CLAUDE_ENV_FILE=str(env_file))
    assert again.returncode == 0 and env_file.read_text(encoding="utf-8") == text  # idempotent, no repeated lines
