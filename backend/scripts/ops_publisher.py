"""Railway publisher (scan-loop card L18, docs/scan_loop/BUNDLE.md sections 3, 10, 11 and 13).

After a fleet run the scanner publishes into the scan loop's file store, a table in the Railway database
(``backend/scanner/loop/store.py``, migration V032): the dealer-log files of the run (``workspace/dealer_logs/...``),
the run bundle (``runs/<run_id>/...``: digest, per-dealer table, triage, gate report, queue) and the recipe mirror
(``mirror/...``). Nothing goes to git. Everything is best effort: a failed publish is logged and recorded in
``publisher.rc`` next to ``fleet_summary.json`` and never changes the fleet's exit code; the next run publishes
whatever is missing (catch-up by ``run_id``).

Environment: ``LOOP_STORE=db`` turns publishing on (the inventory database URL is the scanner's own),
``LOOP_TARGET`` (``prod``|``dev``), ``SCANNER_EGRESS_TAG``, ``DEALER_LOGS_ROOT`` (the live log tree, default
``workspace/dealer_logs``). Retention: the newest 30 fleet runs; per-file and total size caps live in the store.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path

from backend.scanner.loop import bundle, scrub, store as loop_store
from backend.scanner.loop.config import ENV_STORE, ENV_TARGET

ROOT = Path(__file__).resolve().parents[2]
RUN_DIR_RE = re.compile(r"^fleet_[0-9]{8}T[0-9]{6}Z$")
RETAIN_FLEET_RUNS = loop_store.RETAIN_FLEET_RUNS
LOG_SUFFIXES = (".md", ".json", ".txt", ".tsv")
LOG_PREFIX = "workspace/dealer_logs/"


class PublishError(Exception):
    """A publish step failed; the message never carries a secret."""


def configured(env) -> bool:
    return (env.get(ENV_STORE) or "").strip().lower() == "db"


def open_store(env):
    return loop_store.PgStore()


def logs_root(env) -> Path:
    given = env.get("DEALER_LOGS_ROOT")
    return Path(given) if given else ROOT / "workspace" / "dealer_logs"


def publish_logs(root: Path, store, env) -> int:
    """Scrub and store every dealer-log file under ``root``; returns the number of files that changed.

    ``summary.md`` and ``location.md`` belong to cloud sessions: Railway only fills them when the store has none.
    """
    if not root.is_dir():
        return 0
    have = {e.path: e.sha256 for e in store.list(LOG_PREFIX)}
    changed = 0
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.is_symlink() or not path.name.endswith(LOG_SUFFIXES):
            continue
        rel = LOG_PREFIX + path.relative_to(root).as_posix()
        if not loop_store.path_ok(rel):
            continue
        if path.name in loop_store.SESSION_LOG_FILES and rel in have:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            raise PublishError("unreadable log file " + rel) from None
        clean, _counts = scrub.scrub_text(text, env)
        scrub.assert_clean({rel: clean}, env)
        data = clean.encode("utf-8")
        if len(data) > loop_store.file_limit(rel):
            data = data[-loop_store.file_limit(rel):]  # keep the newest part of an oversize append-only log
        if have.get(rel) == loop_store.sha(data):
            continue
        if store.put(rel, data, "railway"):
            changed += 1
    return changed


def publish_mirror(store, run_id: str, env, source=None) -> str:
    """Refresh ``mirror/`` in the store from ``dealer_recipes``; best effort.

    ``source`` returns ``(sets, hints)`` (tests inject it; the default is a read-only read of the inventory database).
    Returns ``unchanged``, ``published`` or ``skipped: <reason>``; a failure never stops the logs and bundle.
    """
    try:
        if source is None:
            from backend.db.inventory_db import get_conn

            conn = get_conn()
            try:
                sets, hints = read_store(conn)
            finally:
                conn.rollback()
                conn.close()
        else:
            sets, hints = source()
        with tempfile.TemporaryDirectory() as tmp:
            write_mirror(Path(tmp), sets, hints)
            wanted = {}
            for file in sorted((Path(tmp) / "mirror").rglob("*")):
                if file.is_file():
                    wanted[file.relative_to(tmp).as_posix()] = file.read_bytes()
        changed = 0
        for path, data in wanted.items():
            if store.put(path, data, "railway"):
                changed += 1
        for entry in store.list("mirror/"):
            if entry.path not in wanted:
                store.delete_prefix(entry.path)
                changed += 1
        return "published" if changed else "unchanged"
    except Exception as exc:  # noqa: BLE001 - the mirror must never stop the logs and the bundle
        return "skipped: " + scrub.clean_line(type(exc).__name__ + " " + str(exc), 160)


def _shard_dirs(run_dir: Path) -> list:
    return sorted(p for p in run_dir.glob("s*") if p.is_dir() and ((p / "triage.json").is_file() or (p / "triage.json.gz").is_file()))


def _previous_run(store, run_id: str, into: Path):
    """The newest older fleet run in the store, written out under ``into`` (MANIFEST.json and dealers.tsv)."""
    names = sorted({e.path.split("/")[1] for e in store.list("runs/fleet_")
                    if e.path.count("/") >= 2 and e.path.split("/")[1] < run_id})
    for name in reversed(names):
        manifest = store.get("runs/" + name + "/MANIFEST.json")
        table = store.get("runs/" + name + "/dealers.tsv")
        if manifest is not None:
            into.mkdir(parents=True, exist_ok=True)
            (into / "MANIFEST.json").write_bytes(manifest)
            if table is not None:
                (into / "dealers.tsv").write_bytes(table)
            return into
    return None


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


def publish_bundle(run_dir: Path, store, env, now=None) -> str:
    """Publish one run directory into ``runs/``; returns ``published`` or ``unchanged``."""
    run_id = run_dir.name
    if not RUN_DIR_RE.match(run_id):
        raise PublishError("not a fleet run directory: " + run_id)
    with tempfile.TemporaryDirectory() as tmp:
        plan = build_files(run_dir, run_id, _previous_run(store, run_id, Path(tmp) / "prev"), env, now)
    have = {e.path: e.sha256 for e in store.list("runs/" + run_id + "/")}
    same = all(have.get(p) == loop_store.sha(d) for p, d in plan.items() if p != "runs/LATEST.json")
    if same and have:
        return "unchanged"
    for path, data in plan.items():
        if path != "runs/LATEST.json":
            store.put(path, data, "railway")
    names = sorted({e.path.split("/")[1] for e in store.list("runs/fleet_") if e.path.count("/") >= 2})
    newest = max(names + [run_id])
    manifest = store.get("runs/" + newest + "/MANIFEST.json")
    if manifest is not None:
        store.put("runs/LATEST.json", bundle.latest_json(manifest, now), "railway")
    loop_store.prune_runs(store, RETAIN_FLEET_RUNS)
    return "published"


def publish(run_dir: Path, env, now=None, catch_up=True, store=None) -> dict:
    """Publish ``run_dir``; with ``catch_up`` also any sibling fleet directory the store lacks."""
    result = {"run_id": run_dir.name, "logs": 0, "bundles": []}
    if store is None:
        if not configured(env):
            raise PublishError("LOOP_STORE is not set to db")
        store = open_store(env)
    result["mirror"] = publish_mirror(store, run_dir.name, env)
    result["logs"] = publish_logs(logs_root(env), store, env)
    targets = [run_dir]
    if catch_up:
        siblings = sorted((p for p in run_dir.parent.glob("fleet_*") if p.is_dir() and p != run_dir and RUN_DIR_RE.match(p.name)),
                          key=lambda p: p.name)
        targets = [p for p in siblings if _shard_dirs(p)][-RETAIN_FLEET_RUNS:] + targets
    for directory in targets:
        try:
            state = publish_bundle(directory, store, env, now)
        except (PublishError, ValueError, OSError, loop_store.StoreError) as exc:
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


# --- recipe mirror (card L12) -----------------------------------------------------------------------------------


def read_store(conn):
    """``(sets, hints)`` from ``dealer_recipes`` in one read-only transaction.

    ``sets`` maps dealer id to its rows as stored (a list), ``hints`` maps dealer id to its parsed ``scan_hints``.
    A row whose JSON cannot be read is skipped rather than guessed at.
    """
    sets, hints = {}, {}
    with conn.cursor() as cur:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute("SELECT dealer_id, recipes_json, scan_hints FROM dealer_recipes ORDER BY dealer_id")
        for dealer_id, recipes_json, scan_hints in cur.fetchall():
            try:
                rows = json.loads(recipes_json)
            except (TypeError, ValueError):
                continue
            if isinstance(rows, list) and rows:
                sets[str(dealer_id)] = rows
            try:
                parsed = json.loads(scan_hints) if scan_hints else None
            except ValueError:
                parsed = None
            if isinstance(parsed, dict):
                hints[str(dealer_id)] = parsed
    return sets, hints


def inside_repository(out: Path) -> bool:
    """True when ``out`` is inside this repository's work tree (the mirror can hold third-party API keys)."""
    try:
        out.resolve().relative_to(ROOT.resolve())
        return True
    except ValueError:
        return False


def write_mirror(out: Path, sets: dict, hints: dict, full: bool = False) -> dict:
    """Write ``mirror/recipes/<id>.json``, ``mirror/index.tsv`` and ``mirror/scan_hints.json`` under ``out``.

    Only files whose bytes changed are rewritten, and files of dealers that lost their recipes are deleted.
    Returns the counts. ``full`` also writes ``recipes_full.json.gz`` (rows as stored, with their stamps).
    """
    import gzip

    recipes_dir = out / "mirror" / "recipes"
    recipes_dir.mkdir(parents=True, exist_ok=True)
    counts = {"written": 0, "unchanged": 0, "deleted": 0}

    def put(path: Path, data: bytes) -> None:
        if path.is_file() and path.read_bytes() == data:
            counts["unchanged"] += 1
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        counts["written"] += 1

    live = {d: rows for d, rows in sets.items() if rows}
    for dealer_id, rows in live.items():
        put(recipes_dir / (dealer_id + ".json"), bundle.canonical_recipe_bytes(rows))
    for stale in recipes_dir.glob("*.json"):
        if stale.stem not in live:
            stale.unlink()
            counts["deleted"] += 1
    put(out / "mirror" / "index.tsv", bundle.mirror_index(live))
    put(out / "mirror" / "scan_hints.json", bundle.mirror_scan_hints(hints))
    if full:
        raw = json.dumps({d: live[d] for d in sorted(live)}, sort_keys=True, separators=(",", ":")).encode("utf-8")
        put(out / "recipes_full.json.gz", gzip.compress(raw, mtime=0))
    return counts
