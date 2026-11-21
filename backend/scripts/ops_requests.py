#!/usr/bin/env python3
"""Scan-loop request files (card L11, docs/scan_loop/BUNDLE.md sections 5 and 13). Offline: no network, no database.

  python -m backend.scripts.ops_requests validate FILE [FILE ...]
  python -m backend.scripts.ops_requests new --kind K --dealers a,b [--proposal PATH] [--fix-cycle N] [--note TEXT]
                                           [--repo DIR]

``validate`` checks each file against the closed schemas (a request, or a proposal when the file sits under
``proposals/``), refuses symlinks and anything that is not a regular file, and for a ``validate`` or ``apply``
request also checks that the proposal it names exists in the same repository and parses for the same dealer. It
prints ``accept FILE`` or ``reject FILE: <reason>`` per file and exits 1 if any is rejected. ``new`` writes a
correctly named request under ``<repo>/requests/`` (never overwriting) after validating it.

Exit status: 0 done, 1 a file was rejected, 2 a usage error.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

if __package__ in (None, ""):  # run as a script: make ``backend`` importable
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backend.scanner.loop import requests as req  # noqa: E402
from backend.scanner.loop.config import MAX_PROPOSAL_BYTES, MAX_REQUEST_BYTES  # noqa: E402
from backend.scanner.loop.scrub import clean_line  # noqa: E402


def _read_limited(path: Path, limit: int) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise req.ParseError("not a regular file")
    with open(path, "rb") as handle:
        return handle.read(limit + 1)


def validate_file(path: Path) -> None:
    """Raise ``req.ParseError`` when ``path`` is not an acceptable request or proposal."""
    in_proposals = "proposals" in path.parts
    if in_proposals:
        req.parse_proposal(_read_limited(path, MAX_PROPOSAL_BYTES), None)
        return
    if path.name.endswith(".status.json") or not path.name.startswith("rq-") or not path.name.endswith(".json"):
        raise req.ParseError("not a request file name")
    if not req.request_name_ok(path.name[:-5]):
        raise req.ParseError("request file name is not rq-<UTC stamp>-<nn>.json")
    parsed = req.parse_request(_read_limited(path, MAX_REQUEST_BYTES))
    if parsed["proposal"] is not None:
        root = path.parent.parent
        target = root / parsed["proposal"]
        req.parse_proposal(_read_limited(target, MAX_PROPOSAL_BYTES), parsed["dealers"][0])


def validate_command(args) -> int:
    bad = 0
    for name in args.files:
        path = Path(name)
        try:
            validate_file(path)
            print("accept " + clean_line(str(path), 200))
        except (req.ParseError, OSError) as exc:
            bad += 1
            reason = str(exc) if isinstance(exc, req.ParseError) else type(exc).__name__
            print("reject " + clean_line(str(path), 200) + ": " + clean_line(reason, 160))
    return 1 if bad else 0


def new_command(args) -> int:
    repo = Path(args.repo)
    folder = repo / "requests"
    folder.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    name = req.new_request_id(now, os.listdir(folder))
    body = {"schema": 1, "kind": args.kind, "dealers": [d for d in args.dealers.split(",") if d],
            "proposal": args.proposal, "fix_cycle": args.fix_cycle, "executor": "railway", "note": args.note or ""}
    data = (json.dumps(body, separators=(",", ":")) + "\n").encode("utf-8")
    try:
        parsed = req.parse_request(data)
        if parsed["proposal"] is not None:
            req.parse_proposal(_read_limited(repo / parsed["proposal"], MAX_PROPOSAL_BYTES), parsed["dealers"][0])
    except (req.ParseError, OSError) as exc:
        print("ops_requests: refused: " + clean_line(str(exc), 200), file=sys.stderr)
        return 2
    target = folder / (name + ".json")
    with open(target, "xb") as handle:
        handle.write(data)
    print(str(target))
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="ops_requests.py", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    v = sub.add_parser("validate", help="closed-schema check of request and proposal files")
    v.add_argument("files", nargs="+", metavar="FILE")
    n = sub.add_parser("new", help="write a correctly named request file")
    n.add_argument("--kind", required=True, choices=req.KINDS)
    n.add_argument("--dealers", required=True, help="comma-separated dealer ids")
    n.add_argument("--proposal", default=None, help="proposals/<dealer>/pr-<stamp>.json (validate and apply)")
    n.add_argument("--fix-cycle", type=int, default=1)
    n.add_argument("--note", default="")
    n.add_argument("--repo", default=".", help="the data repository clone (default: the current directory)")
    args = parser.parse_args(argv)
    return validate_command(args) if args.command == "validate" else new_command(args)


if __name__ == "__main__":
    sys.exit(main())
