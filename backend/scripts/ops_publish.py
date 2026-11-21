#!/usr/bin/env python3
"""Scan-loop publisher wrapper (BUNDLE.md section 13). Card L7 adds ``build``: an offline run-bundle builder.

  python backend/scripts/ops_publish.py build --shards DIR [DIR ...] --run-id fleet_YYYYMMDDTHHMMSSZ
      [--summary FILE] [--prev DIR] [--now ISO] [--out DIR] [--dry-run]

``build`` reads the shard directories (each with ``triage.json`` or ``triage.json.gz`` plus the two optional
text lists), the optional ``fleet_summary.json`` and the optional previous run directory (``MANIFEST.json`` and
``dealers.tsv``), and writes ``<out>/runs/<run_id>/`` and ``<out>/runs/LATEST.json``. ``--dry-run`` only prints
path, bytes and sha256 per file and the total; it writes nothing. The command opens no network connection.
It reads no clock: without ``--now`` the publication time is left out ("unknown" in the digest, ``null`` in
``LATEST.json``). The target comes from ``LOOP_TARGET`` (default ``prod``) and the egress tag from
``SCANNER_EGRESS_TAG``; the scrubber takes its secret values from the environment and never prints them.

  python backend/scripts/ops_publish.py mirror --out DIR [--full]
  python backend/scripts/ops_publish.py sync
  python backend/scripts/ops_publish.py publish --run-dir DIR [--now ISO] [--no-catch-up]

``mirror`` (card L12) writes the canonical recipe mirror (``mirror/recipes/<id>.json``, ``index.tsv``,
``scan_hints.json``) from ``dealer_recipes`` in a read-only transaction; it refuses an ``--out`` inside this
repository (mirror files can hold third-party API keys) with exit 2. ``sync`` clones or updates the data repository's ``main`` clone (``LOOP_DATA_DIR``), so the dealer logs of the next
run land inside it. ``publish`` (card L18, ``ops_publisher.py``) commits the run's dealer-log blocks to ``main`` and
the bundle to the ``runs`` branch of the private data repository, and records the outcome in ``publisher.rc`` of the
run directory. Both need ``LOOP_DATA_REPO`` and ``LOOP_DATA_TOKEN``; they exit 1 on any failure (the fleet ignores it).

Exit status: 0 done, 2 a usage, input or scrub error (nothing is written then); sync and publish: 1 a failure.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

if __package__ in (None, ""):  # run as a script: make ``backend`` importable
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backend.scanner.loop import bundle, scrub  # noqa: E402
from backend.scanner.loop.config import ENV_TARGET  # noqa: E402

ENV_EGRESS_TAG = "SCANNER_EGRESS_TAG"


class CliError(Exception):
    pass


def _read_json(path: Path, what: str) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, RecursionError) as exc:
        raise CliError("cannot read " + what + " " + str(path) + ": " + type(exc).__name__) from None
    if not isinstance(data, dict):
        raise CliError(what + " " + str(path) + " is not a JSON object")
    return data


def _load_prev(directory: Path) -> dict:
    manifest = _read_json(directory / "MANIFEST.json", "previous manifest")
    try:
        tsv = (directory / "dealers.tsv").read_bytes()
    except OSError as exc:
        raise CliError("cannot read previous dealers.tsv in " + str(directory) + ": " + type(exc).__name__) from None
    return {"manifest": manifest, "dealers_tsv": tsv}


def build_command(args, env) -> int:
    shards = []
    for directory in args.shards:
        try:
            shards.append(bundle.load_shard(directory))
        except (OSError, ValueError) as exc:
            raise CliError(str(exc)) from None
    summary = _read_json(Path(args.summary), "summary") if args.summary else None
    prev = _load_prev(Path(args.prev)) if args.prev else None
    target = env.get(ENV_TARGET) or "prod"
    context = {
        "run_id": args.run_id,
        "target": target,
        "kind": "fleet",
        "request_ids": [],
        "egress_tag": env.get(ENV_EGRESS_TAG) or "",
        "publisher_rc": 0,
        "env": dict(env),
    }
    try:
        files = bundle.build_bundle(shards, summary, context, prev, None, args.now)
    except ValueError as exc:
        raise CliError("cannot build the bundle: " + str(exc)) from None
    latest = bundle.latest_json(files["MANIFEST.json"], args.now)

    prefix = "runs/" + args.run_id + "/"
    plan = {prefix + path: data for path, data in files.items()}
    plan["runs/LATEST.json"] = latest
    total = 0
    for path in sorted(plan):
        data = plan[path]
        total += len(data)
        print(path + "\t" + str(len(data)) + "\t" + bundle._sha256(data))
    print("total\t" + str(len(plan)) + " files\t" + str(total) + " bytes")
    if args.dry_run:
        return 0

    out = Path(args.out)
    for path, data in plan.items():
        target_path = out / path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(data)
    return 0


def mirror_command(args, env) -> int:
    from backend.scripts import ops_publisher as pub

    out = Path(args.out)
    if pub.inside_repository(out):
        print("ops_publish: --out is inside this repository; the mirror belongs in the private data repository",
              file=sys.stderr)
        return 2
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        sets, hints = pub.read_store(conn)
    finally:
        conn.rollback()
        conn.close()
    counts = pub.write_mirror(out, sets, hints, full=args.full)
    print("ops_publish: mirror " + json.dumps(counts) + " dealers=" + str(len(sets)))
    return 0


def sync_command(args, env) -> int:
    from backend.scripts import ops_publisher as pub

    if not pub.configured(env):
        print("ops_publish: LOOP_DATA_REPO and LOOP_DATA_TOKEN are not both set; nothing to sync", file=sys.stderr)
        return 1
    try:
        print("ops_publish: data clone at " + str(pub.sync_main(env)))
    except pub.PublishError as exc:
        print("ops_publish: " + scrub.clean_line(str(exc), 300), file=sys.stderr)
        return 1
    return 0


def publish_command(args, env) -> int:
    from backend.scripts import ops_publisher as pub

    run_dir = Path(args.run_dir)
    try:
        result = pub.publish(run_dir, env, args.now, catch_up=not args.no_catch_up)
    except (pub.PublishError, ValueError, OSError) as exc:
        detail = scrub.clean_line(str(exc), 300)
        pub.record_rc(run_dir, 1, detail)
        print("ops_publish: publish failed: " + detail, file=sys.stderr)
        return 1
    pub.record_rc(run_dir, 0, json.dumps(result))
    print("ops_publish: published " + json.dumps(result))
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ops_publish.py", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("build", help="build a run bundle from shard directories (offline)")
    build.add_argument("--shards", nargs="+", required=True, metavar="DIR", help="shard directories")
    build.add_argument("--run-id", required=True, help="fleet_<UTC stamp>")
    build.add_argument("--summary", help="fleet_summary.json of the run")
    build.add_argument("--prev", metavar="DIR", help="previous run directory (MANIFEST.json, dealers.tsv)")
    build.add_argument("--now", help="publication time, ISO 8601 UTC (no clock is read without it)")
    build.add_argument("--out", metavar="DIR", help="write <DIR>/runs/<run_id>/ and <DIR>/runs/LATEST.json")
    build.add_argument("--dry-run", action="store_true", help="print path, bytes and sha256; write nothing")
    mirror = sub.add_parser("mirror", help="export the canonical recipe mirror (read-only SELECT on dealer_recipes)")
    mirror.add_argument("--out", required=True, metavar="DIR", help="a directory outside this repository")
    mirror.add_argument("--full", action="store_true", help="also write recipes_full.json.gz with the stamps")
    sub.add_parser("sync", help="clone or update the data repository's main clone")
    publish = sub.add_parser("publish", help="publish a finished fleet run to the private data repository")
    publish.add_argument("--run-dir", required=True, metavar="DIR", help="the fleet run directory (fleet_<UTC stamp>)")
    publish.add_argument("--now", help="publication time, ISO 8601 UTC")
    publish.add_argument("--no-catch-up", action="store_true", help="publish only this run")
    return parser


def main(argv=None, env=None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    env = os.environ if env is None else env
    if args.command == "mirror":
        return mirror_command(args, env)
    if args.command == "sync":
        return sync_command(args, env)
    if args.command == "publish":
        if args.now is not None:
            args.now = bundle._iso(args.now)
        return publish_command(args, env)
    try:
        if not args.dry_run and not args.out:
            raise CliError("--out is required unless --dry-run is given")
        if args.now is not None and bundle._iso(args.now) is None:
            raise CliError("--now is not an ISO 8601 time")
        if args.now is not None:
            args.now = bundle._iso(args.now)
        return build_command(args, env)
    except CliError as exc:
        print("ops_publish: " + scrub.clean_line(str(exc), 300), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
