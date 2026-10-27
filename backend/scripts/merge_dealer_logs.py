#!/usr/bin/env python3
"""Merge a pulled copy of the Railway ``dealer_logs`` tree into the MBP canonical tree (P6A.5).

D-IF10: the MBP holds the canonical ``workspace/dealer_logs``. Railway scans write their
logs to the service volume; ``scripts/pull_railway_logs.sh`` copies that tree into a staging
directory and this tool folds it in. Default is a dry run; ``--apply`` writes.

Merge rules, per file (the merge is idempotent: a second run changes nothing):
  * file missing in the canonical tree: copied whole.
  * ``## <date> ...`` blocks (scan_runs.md, location.md, discovery.md, summary.md entries):
    a source block is added when no canonical block has the same text; added blocks go in
    date order among the dated blocks. Two blocks that share a heading but differ in body
    are both kept (the pulled one is labelled ``[pulled]`` in its heading).
  * other ``## `` sections (e.g. "Issues faced and how they were fixed") and files without
    blocks (``_learning/*``): source lines the canonical file lacks are appended to the same
    section; existing lines are never reordered or removed.
  * ``scan_instructions.md``: canonical wins, except that a PROVISIONAL canonical file is
    replaced by a source file that is not provisional.
Nothing is ever deleted from the canonical tree.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PROVISIONAL_MARK = "PROVISIONAL"
_DATED = re.compile(r"^##\s+(\d{4}-\d{2}-\d{2})")
PULLED_TAG = " [pulled]"


def split_blocks(text: str) -> tuple[str, list[str]]:
    """(preamble, blocks); each block starts at a line beginning ``## `` and keeps its body."""
    lines = text.split("\n")
    pre: list[str] = []
    blocks: list[list[str]] = []
    for ln in lines:
        if ln.startswith("## "):
            blocks.append([ln])
        elif blocks:
            blocks[-1].append(ln)
        else:
            pre.append(ln)
    return "\n".join(pre), ["\n".join(b).rstrip("\n") for b in blocks]


def _norm(block: str) -> str:
    return "\n".join(ln.rstrip() for ln in block.strip().split("\n"))


def _heading(block: str) -> str:
    return block.split("\n", 1)[0].strip()


def _is_dated(block: str) -> bool:
    return bool(_DATED.match(block))


def _union_lines(dest: str, src: str) -> str:
    have = {ln.strip() for ln in dest.split("\n") if ln.strip()}
    add = [ln for ln in src.split("\n") if ln.strip() and ln.strip() not in have]
    if not add:
        return dest
    return dest.rstrip("\n") + "\n" + "\n".join(add) + "\n"


def merge_text(dest: str, src: str, *, name: str = "") -> str:
    """Return ``dest`` with everything ``src`` has that it lacks; ``dest`` itself if nothing."""
    if name == "scan_instructions.md":
        if PROVISIONAL_MARK in dest[:400] and PROVISIONAL_MARK not in src[:400]:
            return src
        return dest
    d_pre, d_blocks = split_blocks(dest)
    s_pre, s_blocks = split_blocks(src)
    if not d_blocks and not s_blocks:
        return _union_lines(dest, src)

    out_pre = d_pre
    if s_pre.strip() and not d_pre.strip():
        out_pre = s_pre
    seen = {_norm(b) for b in d_blocks}
    heads = {_heading(b) for b in d_blocks}
    blocks = list(d_blocks)
    added = False
    for sb in s_blocks:
        if _norm(sb) in seen:
            continue
        if _is_dated(sb):
            if _heading(sb) in heads:
                _, _, body = sb.partition("\n")
                sb = _heading(sb) + PULLED_TAG + ("\n" + body if body else "")
                if _norm(sb) in seen:
                    continue
            # date order among the dated blocks; ties and undated blocks keep file order
            at = len(blocks)
            key = _DATED.match(sb).group(1)
            for i in range(len(blocks) - 1, -1, -1):
                m = _DATED.match(blocks[i])
                if m is None or m.group(1) <= key:
                    break
                at = i
            blocks.insert(at, sb)
            seen.add(_norm(sb))
            heads.add(_heading(sb))
            added = True
        else:
            # a fixed section: merge its lines into the canonical section of that heading
            for i, db in enumerate(blocks):
                if _heading(db) == _heading(sb):
                    merged = _union_lines(db, sb.partition("\n")[2])
                    if merged != db:
                        blocks[i] = merged.rstrip("\n")
                        added = True
                    break
            else:
                blocks.append(sb)
                heads.add(_heading(sb))
                added = True
            seen.add(_norm(sb))
    if not added and out_pre == d_pre:
        return dest
    body = "\n\n".join(blocks)
    return (out_pre.rstrip("\n") + "\n\n" if out_pre.strip() else "") + body + "\n"


def merge_tree(src_root: Path, dest_root: Path, *, apply: bool = False) -> dict[str, list[str]]:
    """Fold ``src_root`` into ``dest_root``. Returns {'created': [...], 'merged': [...], 'unchanged': [...]}."""
    report: dict[str, list[str]] = {"created": [], "merged": [], "unchanged": []}
    for sf in sorted(p for p in src_root.rglob("*") if p.is_file() and p.suffix in (".md", ".json", ".txt")):
        rel = sf.relative_to(src_root)
        df = dest_root / rel
        stext = sf.read_text(encoding="utf-8", errors="replace")
        if not df.exists():
            report["created"].append(str(rel))
            if apply:
                df.parent.mkdir(parents=True, exist_ok=True)
                df.write_text(stext, encoding="utf-8")
            continue
        if sf.suffix != ".md":
            report["unchanged"].append(str(rel))  # canonical copy wins for non-log files
            continue
        dtext = df.read_text(encoding="utf-8", errors="replace")
        merged = merge_text(dtext, stext, name=sf.name)
        if merged == dtext:
            report["unchanged"].append(str(rel))
        else:
            report["merged"].append(str(rel))
            if apply:
                df.write_text(merged, encoding="utf-8")
    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", required=True, help="pulled dealer_logs tree (staging directory)")
    ap.add_argument("--dest", default="workspace/dealer_logs", help="canonical tree (default workspace/dealer_logs)")
    ap.add_argument("--apply", action="store_true", help="write; without it only report")
    args = ap.parse_args(argv)
    src, dest = Path(args.src), Path(args.dest)
    if not src.is_dir():
        print(f"source {src} is not a directory", file=sys.stderr)
        return 2
    rep = merge_tree(src, dest, apply=args.apply)
    mode = "applied" if args.apply else "dry run"
    print(f"merge_dealer_logs ({mode}): {len(rep['created'])} new file(s), {len(rep['merged'])} merged, "
          f"{len(rep['unchanged'])} unchanged")
    for k in ("created", "merged"):
        for r in rep[k]:
            print(f"  {k}: {r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
