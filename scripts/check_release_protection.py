#!/usr/bin/env python3
"""Compare the live release rulesets with the expected state in docs/RELEASING.md.

Read-only. The expected state is the JSON block after the marker
``<!-- release-protection:expected -->`` in ``docs/RELEASING.md``. The live
state comes from the GitHub REST API (``GET /repos/{repo}/rulesets`` and
``GET /repos/{repo}/rulesets/{id}``) with the token in ``GH_TOKEN`` or
``GITHUB_TOKEN``; nothing is written.

Each difference is printed as a ``::warning::`` line (and to
``$GITHUB_STEP_SUMMARY`` when set). The exit status is 0 unless ``--strict`` is
given and a difference was found (1), or the docs block is missing or broken (2).
An API error is a warning, not a failure: the check never blocks a release on
GitHub being unreachable.

Bypass actors are visible only to a repository admin. A token without admin
rights (the workflow's GITHUB_TOKEN) gets no ``bypass_actors`` field; that is
reported as "not visible", not as a difference.

Usage:
    python3 scripts/check_release_protection.py --repo owner/name [--strict]
    python3 scripts/check_release_protection.py --live rulesets.json   # offline
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DOC = REPO_ROOT / "docs" / "RELEASING.md"
MARKER = "<!-- release-protection:expected -->"
API = "https://api.github.com"


class DocError(ValueError):
    pass


def load_expected(text: str) -> list[dict]:
    """The expected rulesets from the docs block. Raises DocError when missing or broken."""
    m = re.search(re.escape(MARKER) + r"\s*```json\s*\n(.*?)```", text, re.S)
    if not m:
        raise DocError(f"{DOC.name}: no ```json block after {MARKER}")
    try:
        data = json.loads(m.group(1))
    except json.JSONDecodeError as exc:
        raise DocError(f"{DOC.name}: expected-state block is not JSON: {exc}") from exc
    rulesets = data.get("rulesets") if isinstance(data, dict) else None
    if not isinstance(rulesets, list) or not rulesets:
        raise DocError(f"{DOC.name}: expected-state block has no 'rulesets' list")
    for rs in rulesets:
        for key in ("name", "target", "enforcement", "include", "rules"):
            if key not in rs:
                raise DocError(f"{DOC.name}: ruleset {rs.get('name', '?')!r} lacks {key!r}")
    return rulesets


def compare(expected: list[dict], live: list[dict]) -> tuple[list[str], list[str]]:
    """(differences, notes). ``live`` holds full ruleset objects as the API returns them."""
    diffs: list[str] = []
    notes: list[str] = []
    by_name = {rs.get("name"): rs for rs in live}
    for exp in expected:
        name = exp["name"]
        got = by_name.get(name)
        if got is None:
            diffs.append(f"ruleset {name!r} is missing (docs expect it, target {exp['target']})")
            continue
        for key in ("target", "enforcement"):
            if got.get(key) != exp[key]:
                diffs.append(f"{name}: {key} is {got.get(key)!r}, docs say {exp[key]!r}")
        ref = (got.get("conditions") or {}).get("ref_name") or {}
        inc = sorted(ref.get("include") or [])
        if inc != sorted(exp["include"]):
            diffs.append(f"{name}: includes {inc}, docs say {sorted(exp['include'])}")
        exc = sorted(ref.get("exclude") or [])
        if exc != sorted(exp.get("exclude", [])):
            diffs.append(f"{name}: excludes {exc}, docs say {sorted(exp.get('exclude', []))}")
        types = sorted({r.get("type") for r in got.get("rules") or []})
        want = sorted(exp["rules"])
        if types != want:
            missing = sorted(set(want) - set(types))
            extra = sorted(set(types) - set(want))
            parts = []
            if missing:
                parts.append(f"missing {missing}")
            if extra:
                parts.append(f"not in docs {extra}")
            diffs.append(f"{name}: rules {', '.join(parts)}")
        if "bypass_actor_types" in exp:
            if "bypass_actors" not in got:
                notes.append(f"{name}: bypass actors not visible to this token (needs admin); not compared")
            else:
                actors = sorted(a.get("actor_type") for a in got.get("bypass_actors") or [])
                if actors != sorted(exp["bypass_actor_types"]):
                    diffs.append(f"{name}: bypass actor types {actors}, docs say {sorted(exp['bypass_actor_types'])}")
    return diffs, notes


def _get(url: str, token: str | None):
    req = urllib.request.Request(url, headers={
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "release-protection-check",
    })
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=20) as resp:  # noqa: S310 (fixed https host)
        return json.load(resp)


def fetch_live(repo: str, token: str | None) -> list[dict]:
    listing = _get(f"{API}/repos/{repo}/rulesets?per_page=100", token)
    return [_get(f"{API}/repos/{repo}/rulesets/{int(rs['id'])}", token) for rs in listing]


def _emit(lines: list[str], prefix: str) -> None:
    for line in lines:
        print(f"{prefix}{line}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--repo", help="owner/name, read through the GitHub API")
    src.add_argument("--live", type=Path, help="JSON file with the full ruleset objects (offline)")
    ap.add_argument("--doc", type=Path, default=DOC)
    ap.add_argument("--strict", action="store_true", help="exit 1 when a difference is found")
    args = ap.parse_args(argv)

    try:
        expected = load_expected(args.doc.read_text(encoding="utf-8"))
    except (OSError, DocError) as exc:
        print(f"::error::{exc}")
        return 2

    try:
        if args.live:
            live = json.loads(args.live.read_text(encoding="utf-8"))
        else:
            if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", args.repo):
                print("::error::--repo must be owner/name")
                return 2
            token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
            live = fetch_live(args.repo, token)
    except (OSError, ValueError, urllib.error.URLError) as exc:
        # The reason only (HTTP status or a short error), never headers or the token.
        reason = getattr(exc, "code", None) or type(exc).__name__
        print(f"::warning::could not read the live rulesets ({reason}); not compared")
        return 0

    diffs, notes = compare(expected, live)
    _emit(notes, "note: ")
    _emit(diffs, "::warning::ruleset differs from docs/RELEASING.md: ")
    status = "match docs/RELEASING.md" if not diffs else f"{len(diffs)} difference(s) from docs/RELEASING.md"
    print(f"release rulesets: {status}")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(f"release rulesets: {status}\n")
            for line in diffs + notes:
                fh.write(f"- {line}\n")
    return 1 if (diffs and args.strict) else 0


if __name__ == "__main__":
    sys.exit(main())
