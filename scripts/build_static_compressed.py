#!/usr/bin/env python3
"""Write precompressed ``.gz`` (and ``.br`` when the brotli module is importable)
siblings next to every ``.js`` and ``.css`` file under ``frontend/static``.

The Flask static view (``backend/main.py``, ``_static_view``) serves a sibling in
place of the source when the request's ``Accept-Encoding`` allows it AND the
sibling is at least as new as its source, so a stale sibling is never served;
it is simply ignored until this script runs again. Nothing else compresses
static files: gunicorn does not, and Railway's edge does not compress on the
app's behalf (see docs/EFFICIENCY_REVIEW_FRONTEND_2026_09_28.md, 1.3).

Idempotent: a sibling whose mtime already matches its source is left alone, and
siblings whose source has disappeared are removed. Each sibling's mtime is set
to its source's mtime so the freshness check is an equality, not a race.

Run it after any static edit you want served compressed:

    .venv/bin/python scripts/build_static_compressed.py            # build
    .venv/bin/python scripts/build_static_compressed.py --check    # exit 1 if stale

Dockerfile.web runs it right after ``COPY . .`` so the image ships the siblings.
The ``.gz``/``.br`` files are git-ignored (frontend/static/**/*.gz, *.br).
"""

from __future__ import annotations

import argparse
import gzip
import os
import sys
from pathlib import Path

try:
    import brotli  # type: ignore
except ImportError:  # pragma: no cover - optional
    brotli = None

REPO_ROOT = Path(__file__).resolve().parents[1]
STATIC_ROOT = REPO_ROOT / "frontend" / "static"
SOURCE_SUFFIXES = (".js", ".css")
SIBLING_SUFFIXES = (".gz", ".br")


def _sources(root: Path):
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.suffix in SOURCE_SUFFIXES:
            yield path


def _is_fresh(source: Path, sibling: Path) -> bool:
    try:
        return sibling.stat().st_mtime >= source.stat().st_mtime and sibling.stat().st_size > 0
    except OSError:
        return False


def _write(sibling: Path, data: bytes, source_mtime: float) -> None:
    tmp = sibling.with_name(sibling.name + ".tmp")
    tmp.write_bytes(data)
    os.utime(tmp, (source_mtime, source_mtime))
    os.replace(tmp, sibling)


def build(root: Path = STATIC_ROOT, check: bool = False, quiet: bool = False) -> int:
    """Return the number of siblings that were (or, with ``check``, would be) written."""
    changed = 0
    for source in _sources(root):
        raw = None
        mtime = source.stat().st_mtime
        targets = [(source.with_name(source.name + ".gz"), "gz")]
        if brotli is not None:
            targets.append((source.with_name(source.name + ".br"), "br"))
        for sibling, kind in targets:
            if _is_fresh(source, sibling):
                continue
            changed += 1
            if check:
                if not quiet:
                    print(f"stale: {sibling.relative_to(root)}")
                continue
            if raw is None:
                raw = source.read_bytes()
            if kind == "gz":
                data = gzip.compress(raw, compresslevel=9, mtime=0)
            else:
                data = brotli.compress(raw, quality=11)
            _write(sibling, data, mtime)
            if not quiet:
                print(f"{sibling.relative_to(root)}  {len(raw):>8} -> {len(data):>7}")

    # Orphans: a source was deleted or renamed; its siblings would otherwise linger.
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix not in SIBLING_SUFFIXES:
            continue
        if path.with_suffix("").exists():
            continue
        changed += 1
        if check:
            if not quiet:
                print(f"orphan: {path.relative_to(root)}")
            continue
        path.unlink()
        if not quiet:
            print(f"removed orphan {path.relative_to(root)}")
    return changed


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--root", type=Path, default=STATIC_ROOT, help="static root (default frontend/static)")
    ap.add_argument("--check", action="store_true", help="report stale/missing siblings, write nothing, exit 1 if any")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args(argv)
    if brotli is None and not args.quiet:
        print("brotli module not importable: writing .gz only", file=sys.stderr)
    n = build(args.root, check=args.check, quiet=args.quiet)
    if args.check:
        return 1 if n else 0
    if not args.quiet:
        print(f"{n} sibling(s) written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
