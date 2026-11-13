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

Exit status: 0 done, 2 a usage, input or scrub error (nothing is written then).
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
    return parser


def main(argv=None, env=None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    env = os.environ if env is None else env
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
