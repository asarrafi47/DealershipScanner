"""Railway publisher (scan-loop card L18, docs/scan_loop/BUNDLE.md sections 3, 10, 11 and 13).

After a fleet run the scanner publishes to the private data repository: the dealer-log blocks written during the
run go to ``main`` (commit ``logs: <run_id>``), the run bundle (digest, per-dealer table, triage, gate report,
queue) goes to the orphan branch ``runs``. Everything is best effort: a failed publish is logged and recorded in
``publisher.rc`` next to ``fleet_summary.json`` and never changes the fleet's exit code; the next run publishes
whatever is missing (catch-up by ``run_id``).

Environment: ``LOOP_DATA_REPO`` (https URL, no credentials in it), ``LOOP_DATA_TOKEN`` (repository-scoped token),
``LOOP_DATA_DIR`` (clone of ``main``; default ``$SCAN_DATA_DIR/loop-data`` else ``/data/loop-data``),
``LOOP_TARGET`` (``prod``|``dev``), ``SCANNER_EGRESS_TAG``. The token reaches git only through ``GIT_CONFIG_*``
environment variables (an ``Authorization`` header), never argv, ``.git/config`` or a log line.
"""

from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path

from backend.scanner.loop import bundle, scrub
from backend.scanner.loop.config import ENV_DATA_DIR, ENV_DATA_REPO, ENV_DATA_TOKEN, ENV_TARGET

ROOT = Path(__file__).resolve().parents[2]
RUN_DIR_RE = re.compile(r"^fleet_[0-9]{8}T[0-9]{6}Z$")
RETAIN_FLEET_RUNS = 30
GIT_TIMEOUT = 120
PUSH_ATTEMPTS = 4
LOG_SUFFIXES = (".md", ".json", ".txt", ".tsv")
BOT = ("claude-scan-loop", "scan-loop@users.noreply.github.com")


class PublishError(Exception):
    """A publish step failed; the message never carries the token."""


def data_dir(env) -> Path:
    given = env.get(ENV_DATA_DIR)
    if given:
        return Path(given)
    return Path(env.get("SCAN_DATA_DIR") or "/data") / "loop-data"


def runs_dir(env) -> Path:
    base = data_dir(env)
    return base.with_name(base.name + "-runs")


def configured(env) -> bool:
    return bool(env.get(ENV_DATA_REPO)) and bool(env.get(ENV_DATA_TOKEN))


def _git_env(env) -> dict:
    out = dict(env)
    out.update({
        "GIT_TERMINAL_PROMPT": "0", "GIT_AUTHOR_NAME": BOT[0], "GIT_AUTHOR_EMAIL": BOT[1],
        "GIT_COMMITTER_NAME": BOT[0], "GIT_COMMITTER_EMAIL": BOT[1],
    })
    token = env.get(ENV_DATA_TOKEN)
    if token:
        header = "Authorization: Basic " + base64.b64encode(("x-access-token:" + token).encode()).decode()
        out.update({"GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "http.extraHeader", "GIT_CONFIG_VALUE_0": header})
    return out


def _git(cwd, env, *args, check=True, timeout=GIT_TIMEOUT) -> str:
    try:
        proc = subprocess.run(["git", *args], cwd=str(cwd), env=_git_env(env), capture_output=True, text=True,
                              timeout=timeout)
    except subprocess.TimeoutExpired:
        raise PublishError("git " + args[0] + " timed out") from None
    except OSError as exc:
        raise PublishError("git is not available: " + type(exc).__name__) from None
    if check and proc.returncode != 0:
        # git's own message can echo the remote URL; the URL has no credentials, but scrub anyway.
        detail = scrub.clean_line(scrub.scrub_text(proc.stderr.strip(), env)[0], 200)
        raise PublishError("git " + args[0] + " failed: " + detail)
    return proc.stdout


def _remote_has(cwd, env, ref) -> bool:
    return bool(_git(cwd, env, "ls-remote", "--heads", "origin", ref).strip())


def sync_main(env) -> Path:
    """Clone or update the ``main`` clone that holds the dealer logs; returns its path."""
    target = data_dir(env)
    url = env[ENV_DATA_REPO]
    if not (target / ".git").is_dir():
        target.parent.mkdir(parents=True, exist_ok=True)
        _git(target.parent, env, "clone", "-q", "--depth", "50", "--branch", "main", url, str(target))
    else:
        _git(target, env, "fetch", "-q", "origin", "main")
        _git(target, env, "pull", "-q", "--rebase", "--autostash", "origin", "main")
    (target / "workspace" / "dealer_logs").mkdir(parents=True, exist_ok=True)
    return target


def _push_with_retry(cwd, env, *push_args) -> None:
    last = ""
    for attempt in range(PUSH_ATTEMPTS):
        try:
            _git(cwd, env, "push", "-q", *push_args)
            return
        except PublishError as exc:
            last = str(exc)
        # a race with a session's push: rebase on the fresh remote (not for a forced orphan push)
        if "--force" not in push_args:
            try:
                _git(cwd, env, "pull", "-q", "--rebase", "origin", push_args[-1].split(":")[-1])
            except PublishError as exc:
                last = str(exc)
                _git(cwd, env, "rebase", "--abort", check=False)
        time.sleep(min(2 ** attempt, 8) if os.environ.get("PUBLISH_SLEEP", "1") != "0" else 0)
    raise PublishError("push rejected after " + str(PUSH_ATTEMPTS) + " attempts: " + last)


def _changed_log_files(clone: Path, env) -> list:
    out = _git(clone, env, "status", "--porcelain", "--untracked-files=all", "--", "workspace/dealer_logs")
    files = []
    for line in out.splitlines():
        path = line[3:].strip().strip('"')
        if path.endswith(LOG_SUFFIXES):
            files.append(path)
    return files


def publish_logs(clone: Path, run_id: str, env) -> int:
    """Scrub and commit the dealer-log changes of the run to ``main``; returns the number of files committed."""
    files = _changed_log_files(clone, env)
    if not files:
        return 0
    for rel in files:
        path = clone / rel
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            raise PublishError("unreadable log file " + rel) from None
        clean, _counts = scrub.scrub_text(text, env)
        if clean != text:
            path.write_text(clean, encoding="utf-8")
        scrub.assert_clean({rel: clean}, env)
    _git(clone, env, "add", "-A", "--", "workspace/dealer_logs")
    _git(clone, env, "commit", "-q", "-m", "logs: " + run_id)
    _push_with_retry(clone, env, "origin", "HEAD:main")
    return len(files)


def _shard_dirs(run_dir: Path) -> list:
    return sorted(p for p in run_dir.glob("s*") if p.is_dir() and ((p / "triage.json").is_file() or (p / "triage.json.gz").is_file()))


def _ensure_runs(env) -> Path:
    """The ``runs`` branch checked out in its own clone; an empty orphan when the remote has none yet."""
    clone = runs_dir(env)
    url = env[ENV_DATA_REPO]
    if not (clone / ".git").is_dir():
        clone.mkdir(parents=True, exist_ok=True)
        _git(clone, env, "init", "-q", "-b", "runs")
        _git(clone, env, "remote", "add", "origin", url)
    if _remote_has(clone, env, "runs"):
        _git(clone, env, "fetch", "-q", "--depth", "1", "origin", "+runs:refs/remotes/origin/runs")
        _git(clone, env, "checkout", "-q", "-f", "-B", "runs", "origin/runs")
        _git(clone, env, "clean", "-q", "-fd")
    return clone


def _previous_run(clone: Path, run_id: str):
    runs = clone / "runs"
    older = sorted(p.name for p in runs.glob("fleet_*") if p.is_dir() and p.name < run_id and (p / "MANIFEST.json").is_file()) \
        if runs.is_dir() else []
    return runs / older[-1] if older else None


def build_files(run_dir: Path, run_id: str, prev_dir, env, now=None) -> dict:
    """``{path: bytes}`` of ``runs/<run_id>/...`` plus ``runs/LATEST.json`` for one run directory."""
    from backend.scripts import ops_gates  # local import: the gate CLI pulls the whole gates module

    shard_dirs = _shard_dirs(run_dir)
    if not shard_dirs:
        raise PublishError("no shard with a triage file in " + run_dir.name)
    shards = [bundle.load_shard(d) for d in shard_dirs]
    summary_path = run_dir / "fleet_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.is_file() else None
    registry = ROOT / "deploy" / "railway" / "accepted_gaps.json"
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        ops_gates.build(shard_dirs, prev_dir, None, registry if registry.is_file() else None, None, out, now)
        gates = {name: (out / name).read_bytes() for name in ("gate_report.json", "queue.json")}
    prev = None
    if prev_dir is not None and (prev_dir / "dealers.tsv").is_file():
        prev = {"manifest": json.loads((prev_dir / "MANIFEST.json").read_text(encoding="utf-8")),
                "dealers_tsv": (prev_dir / "dealers.tsv").read_bytes()}
    build = (summary or {}).get("build") if isinstance(summary, dict) else None
    context = {
        "run_id": run_id, "target": env.get(ENV_TARGET) or "prod", "kind": "fleet", "request_ids": [],
        "egress_tag": env.get("SCANNER_EGRESS_TAG") or "", "publisher_rc": 0, "env": dict(env),
        "code": {"version": (build or {}).get("version", "unknown"), "commit": (build or {}).get("commit", "unknown"),
                 "image_tag": (build or {}).get("tag", "unknown"),
                 "db_schema": str((summary or {}).get("schema_version", "unknown"))},
    }
    files = bundle.build_bundle(shards, summary, context, prev, gates, now)
    plan = {"runs/" + run_id + "/" + p: d for p, d in files.items()}
    plan["runs/LATEST.json"] = bundle.latest_json(files["MANIFEST.json"], now)
    return plan


def publish_bundle(run_dir: Path, env, now=None) -> str:
    """Publish one run directory to the ``runs`` branch; returns ``published``, ``unchanged``."""
    run_id = run_dir.name
    if not RUN_DIR_RE.match(run_id):
        raise PublishError("not a fleet run directory: " + run_id)
    clone = _ensure_runs(env)
    existing = clone / "runs" / run_id / "MANIFEST.json"
    plan = build_files(run_dir, run_id, _previous_run(clone, run_id), env, now)
    same = existing.is_file() and all(
        (clone / p).is_file() and (clone / p).read_bytes() == d for p, d in plan.items() if p != "runs/LATEST.json")
    if same:
        return "unchanged"
    for path, data in plan.items():
        target = clone / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    fleet_dirs = sorted(p.name for p in (clone / "runs").glob("fleet_*") if p.is_dir())
    newest = max(fleet_dirs + [run_id])
    if newest != run_id:  # catch-up of an older run: keep LATEST pointing at the newest one
        latest = clone / "runs" / newest / "MANIFEST.json"
        (clone / "runs" / "LATEST.json").write_bytes(bundle.latest_json(latest.read_bytes(), now))
    excess = fleet_dirs[:-RETAIN_FLEET_RUNS] if len(fleet_dirs) > RETAIN_FLEET_RUNS else []
    if excess:
        import shutil
        for name in excess:
            shutil.rmtree(clone / "runs" / name, ignore_errors=True)
    _git(clone, env, "add", "-A")
    if excess:
        # bounded size: rebuild the branch as an orphan holding only the retained runs, then force it
        _git(clone, env, "checkout", "-q", "--orphan", "runs-new")
        _git(clone, env, "add", "-A")
        _git(clone, env, "commit", "-q", "-m", "runs: " + run_id + " (retention)")
        _git(clone, env, "branch", "-q", "-M", "runs")
        _push_with_retry(clone, env, "--force", "origin", "runs")
    else:
        _git(clone, env, "commit", "-q", "-m", "run " + run_id)
        _push_with_retry(clone, env, "origin", "HEAD:runs")
    return "published"


def publish(run_dir: Path, env, now=None, catch_up=True) -> dict:
    """Publish ``run_dir``; with ``catch_up`` also any sibling fleet directory the ``runs`` branch lacks."""
    result = {"run_id": run_dir.name, "logs": 0, "bundles": []}
    if not configured(env):
        raise PublishError("LOOP_DATA_REPO and LOOP_DATA_TOKEN are not both set")
    clone = sync_main(env)
    result["logs"] = publish_logs(clone, run_dir.name, env)
    targets = [run_dir]
    if catch_up:
        siblings = sorted((p for p in run_dir.parent.glob("fleet_*") if p.is_dir() and p != run_dir and RUN_DIR_RE.match(p.name)),
                          key=lambda p: p.name)
        targets = [p for p in siblings if _shard_dirs(p)][-RETAIN_FLEET_RUNS:] + targets
    for directory in targets:
        try:
            state = publish_bundle(directory, env, now)
        except (PublishError, ValueError, OSError) as exc:
            if directory == run_dir:
                raise
            state = "skipped: " + scrub.clean_line(str(exc), 120)
        result["bundles"].append({"run_id": directory.name, "state": state})
    return result


def record_rc(run_dir: Path, rc: int, detail: str) -> None:
    try:
        (run_dir / "publisher.rc").write_text(json.dumps({"rc": rc, "detail": scrub.clean_line(detail, 300)}) + "\n",
                                              encoding="utf-8")
    except OSError:
        pass
