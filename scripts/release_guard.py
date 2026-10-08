#!/usr/bin/env python3
"""Release guard: is this commit releasable to main? (remediation P2A.4)

    python3 scripts/release_guard.py --base <ref|sha> [--head <ref|sha>]

Passes (exit 0) when either

* HEAD is already contained in the base (nothing new is being released), or
* VERSION at HEAD is numerically semver-greater than VERSION at the base
  (1.5.10 > 1.5.9), AND CHANGELOG.md at HEAD has a ``## [<VERSION>]`` heading
  followed by at least one line of content before the next level-1 or level-2
  heading (blank lines and ``###`` sub-headings alone do not count).

A base of all zeros (git's "no such ref", as a push that creates main reports
it) means there is no earlier main to rise above: only the CHANGELOG section
and a well-formed VERSION are required.

Everything is read from the commits (``git show <sha>:VERSION``), never from
the working tree, so the hook and CI judge exactly what is pushed.

Exit codes: 0 releasable, 1 refused, 2 usage error or a ref that does not
resolve (fetch it first).

Callers: scripts/git-hooks/pre-push (pushes to refs/heads/main, with the
remote's own main sha as the base) and the ``release-guard`` job in
.github/workflows/ci.yml (pushes and PRs to main). Tests:
backend/tests/test_release_tooling.py.

Stdlib only and Python 3.9 compatible: git hooks run whatever ``python3`` is
on PATH, and macOS's /usr/bin/python3 is 3.9.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from typing import List, Optional, Tuple

EXIT_OK = 0
EXIT_REFUSED = 1
EXIT_USAGE = 2

_SEMVER = re.compile(r"(\d+)\.(\d+)\.(\d+)")
# A level-1 or level-2 Markdown heading ends a CHANGELOG section ("## [1.5.2]",
# "# Changelog"); "###" sub-headings (### Added) belong to the section.
_SECTION_END = re.compile(r"#{1,2}(?:\s|$)")
_SUBHEADING = re.compile(r"#{3,}(?:\s|$)")


class GuardError(Exception):
    """A ref or object could not be read; exit 2."""


def _git(*args: str) -> subprocess.CompletedProcess:
    # Explicit UTF-8: a hook's python3 may run under LANG=C, where text=True
    # would decode CHANGELOG.md as ASCII.
    return subprocess.run(
        ["git", *args], capture_output=True, encoding="utf-8", errors="replace"
    )


def is_zero_sha(ref: str) -> bool:
    """git's "no such ref" (40 zeros, or 64 in a SHA-256 repository)."""
    return len(ref) in (40, 64) and set(ref) == {"0"}


def resolve(ref: str) -> str:
    """Full commit sha for ``ref``; GuardError when it does not resolve locally."""
    proc = _git("rev-parse", "--verify", "--quiet", ref + "^{commit}")
    sha = proc.stdout.strip()
    if proc.returncode != 0 or not sha:
        raise GuardError(
            f"cannot resolve {ref!r} to a commit in this repository; "
            "fetch it first (git fetch origin main)"
        )
    return sha


def read_file(sha: str, path: str) -> Optional[str]:
    """``path`` as stored in commit ``sha``, or None when the commit has no such file."""
    proc = _git("show", f"{sha}:{path}")
    if proc.returncode != 0:
        return None
    return proc.stdout


def is_contained(head: str, base: str) -> bool:
    """True when ``head`` is ``base`` or one of its ancestors.

    In a shallow clone git may not see the link and answers "no", which only
    leads to the stricter VERSION/CHANGELOG check, never to a false pass.
    """
    if head == base:
        return True
    return _git("merge-base", "--is-ancestor", head, base).returncode == 0


def parse_version(text: Optional[str]) -> Optional[Tuple[int, int, int]]:
    """``(major, minor, patch)`` for a plain ``X.Y.Z`` VERSION, else None."""
    if text is None:
        return None
    m = _SEMVER.fullmatch(text.strip())
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def changelog_section(changelog: str, version: str) -> Optional[List[str]]:
    """Lines under the first ``## [<version>]`` heading, up to the next level-1/2
    heading or the end of the file; None when there is no such heading."""
    heading = re.compile(r"##[ \t]+\[" + re.escape(version) + r"\]")
    lines = changelog.splitlines()
    for i, line in enumerate(lines):
        if heading.match(line):
            body = []  # type: List[str]
            for later in lines[i + 1:]:
                if _SECTION_END.match(later):
                    break
                body.append(later)
            return body
    return None


def section_has_content(body: List[str]) -> bool:
    """At least one line that is neither blank nor a ``###`` sub-heading."""
    return any(line.strip() and not _SUBHEADING.match(line.strip()) for line in body)


def check(base_ref: str, head_ref: str = "HEAD") -> Tuple[bool, str]:
    """``(releasable, message)``. Raises GuardError for refs that do not resolve."""
    head = resolve(head_ref)
    short_head = head[:9]

    base = None  # type: Optional[str]
    if not is_zero_sha(base_ref):
        base = resolve(base_ref)
        if is_contained(head, base):
            return True, f"{short_head} is already contained in {base[:9]}; nothing new to release."

    head_text = read_file(head, "VERSION")
    if head_text is None:
        return False, f"VERSION file missing at {short_head}."
    head_version = head_text.strip()
    head_key = parse_version(head_version)
    if head_key is None:
        return False, f"VERSION {head_version!r} at {short_head} is not MAJOR.MINOR.PATCH (e.g. 1.5.3)."

    rise = f"{head_version} (no earlier main to compare with)"
    if base is not None:
        base_text = read_file(base, "VERSION")
        if base_text is not None:
            base_version = base_text.strip()
            base_key = parse_version(base_version)
            if base_key is None:
                return False, (
                    f"VERSION {base_version!r} at the base {base[:9]} is not MAJOR.MINOR.PATCH; "
                    f"cannot tell whether {head_version} rises above it."
                )
            if head_key <= base_key:
                return False, (
                    f"VERSION {head_version} at {short_head} does not rise above {base_version} "
                    f"at {base[:9]}. Bump it first:  scripts/bump_version.sh patch   (or minor / major)"
                )
            rise = f"{base_version} -> {head_version}"
        else:
            rise = f"{head_version} (the base {base[:9]} has no VERSION file)"

    changelog = read_file(head, "CHANGELOG.md")
    if changelog is None:
        return False, f"CHANGELOG.md missing at {short_head}; a release needs a [{head_version}] section."
    body = changelog_section(changelog, head_version)
    if body is None:
        return False, (
            f"CHANGELOG.md at {short_head} has no '## [{head_version}]' section. "
            "Add the release notes (scripts/bump_version.sh turns [Unreleased] into it)."
        )
    if not section_has_content(body):
        return False, (
            f"CHANGELOG.md at {short_head} has an empty '## [{head_version}]' section; "
            "a release needs at least one line of notes."
        )
    return True, f"VERSION {rise}; CHANGELOG.md has a filled [{head_version}] section."


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="release_guard.py",
        description="Refuse unless VERSION rises above the base and CHANGELOG.md has its section.",
    )
    parser.add_argument(
        "--base",
        required=True,
        help="the main this release replaces: a ref or sha (all zeros: no earlier main)",
    )
    parser.add_argument("--head", default="HEAD", help="the commit being released (default HEAD)")
    args = parser.parse_args(argv)

    try:
        ok, message = check(args.base, args.head)
    except GuardError as exc:
        print(f"release_guard: {exc}", file=sys.stderr)
        return EXIT_USAGE
    if ok:
        print(f"release_guard: OK: {message}")
        return EXIT_OK
    print(f"release_guard: REFUSED: {message}", file=sys.stderr)
    return EXIT_REFUSED


if __name__ == "__main__":
    sys.exit(main())
