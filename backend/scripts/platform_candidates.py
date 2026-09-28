"""Platform candidates: which dealers without a working recipe share an unknown platform.

Reads the latest ``discovery_<stamp>.json`` per dealer under
``workspace/dealer_logs/`` (plus the latest ``capture_<stamp>.json`` when one
exists), keeps the dealers that have no live recipe or whose recipe is stale /
rejected (``backend.scanner.recipes.load_recipes`` + ``scan_hints.recipe_status``),
fingerprints them (``backend.scanner.platform_fingerprint``), clusters by
signature with the Jaccard merge, and writes
``workspace/dealer_logs/_learning/platform_candidates.md``: one section per
cluster of two or more dealers with the shared features, the members and their
last verdict, the nearest known template, and a suggested next step. Prints one
line per cluster.

    python -m backend.scripts.platform_candidates                 # every dealer log
    python -m backend.scripts.platform_candidates --dealers a,b   # a subset
    python -m backend.scripts.platform_candidates --all           # ignore the recipe filter
    python -m backend.scripts.platform_candidates --root <dir>    # another log root (tests)

``dealer_pipeline`` calls :func:`report_for_run` at the end of every run with
the run's ``needs_discovery`` dealers, so a platform shared by several failing
dealers is printed in the run output instead of waiting for someone to notice.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[2]
LOG_ROOT = Path(os.environ.get("DEALER_LOGS_ROOT") or (ROOT / "workspace" / "dealer_logs"))
OUTPUT_NAME = "platform_candidates.md"
MIN_CLUSTER = 2

RecipeStateFn = Callable[[str], str]
_DEFAULT_STATE = object()  # "use recipe_state as bound at call time" (tests stub the module attribute)

_DISCOVERY_RE = re.compile(r"^discovery_(\d{8}T\d{6})")
_CAPTURE_RE = re.compile(r"^capture_(\d{8}T\d{6})")


# --------------------------------------------------------------------------
# reading the logs
# --------------------------------------------------------------------------

def _latest(dir_: Path, rx: re.Pattern) -> Path | None:
    best: tuple[str, Path] | None = None
    try:
        names = list(dir_.iterdir())
    except OSError:
        return None
    for p in names:
        m = rx.match(p.name)
        if m and p.suffix == ".json" and (best is None or m.group(1) > best[0]):
            best = (m.group(1), p)
    return best[1] if best else None


def latest_discovery_files(root: Path) -> dict[str, Path]:
    """``{dealer_id: newest discovery_<stamp>.json}`` for every dealer directory."""
    out: dict[str, Path] = {}
    try:
        dirs = sorted(p for p in root.iterdir() if p.is_dir() and not p.name.startswith(("_", ".")))
    except OSError:
        return out
    for d in dirs:
        p = _latest(d, _DISCOVERY_RE)
        if p is not None:
            out[d.name] = p
    return out


def latest_capture_file(root: Path, dealer_id: str) -> Path | None:
    return _latest(root / dealer_id, _CAPTURE_RE)


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


_VERDICT_RE = re.compile(r"^- (?:verdict|recipe validation): (.+)$")
_PROBE_HEAD_RE = re.compile(r"^## \S+ discovery probe — (.+)$")


def last_verdict(root: Path, dealer_id: str, rec: dict[str, Any] | None) -> str:
    """The dealer's last verdict: the last ``- verdict:`` / ``- recipe validation:``
    line in discovery.md, else the last ``discovery probe — <class>`` heading,
    else the record's classification."""
    fallback = str((rec or {}).get("classification") or "unknown")
    p = root / dealer_id / "discovery.md"
    try:
        lines = p.read_text(encoding="utf-8").splitlines()
    except OSError:
        return fallback
    for line in reversed(lines):
        m = _VERDICT_RE.match(line.strip())
        if m:
            return m.group(1).strip()[:120]
        m = _PROBE_HEAD_RE.match(line.strip())
        if m:
            return m.group(1).strip()[:120]
    return fallback


# --------------------------------------------------------------------------
# recipe state
# --------------------------------------------------------------------------

def recipe_state(dealer_id: str) -> str:
    """``live`` | ``none`` | ``stale`` | ``rejected:<code>`` from the recipe file /
    store and ``scan_hints.recipe_status``. Never raises: a missing store reads
    as no hints, a missing recipe as ``none``."""
    status = ""
    try:
        from backend.scanner.recipe_store import get_scan_hints

        status = str((get_scan_hints(dealer_id) or {}).get("recipe_status") or "")
    except Exception:  # noqa: BLE001 - advisory
        status = ""
    if status.startswith("rejected:"):
        return status
    recipes: list[Any] = []
    try:
        from backend.scanner.recipes import load_recipes

        recipes = list(load_recipes(dealer_id) or [])
    except Exception:  # noqa: BLE001
        recipes = []
    if not recipes:
        return "none"
    if any(not getattr(r, "stale", False) for r in recipes):
        return "live"  # an un-staled recipe answers; a lagging stale: status does not outrank it
    return "stale"


def needs_platform(state: str) -> bool:
    return state != "live"


# --------------------------------------------------------------------------
# build
# --------------------------------------------------------------------------

def collect(root: Path, *, dealers: set[str] | None = None, state_fn: Any = _DEFAULT_STATE,
            only_needing: bool = True) -> tuple[list[Any], dict[str, dict[str, str]]]:
    """Fingerprints for the dealers under ``root`` (restricted to ``dealers`` when
    given) that need a platform, plus ``{dealer_id: {state, verdict, stamp}}``.
    ``state_fn=None`` or ``only_needing=False`` keeps every dealer."""
    from backend.scanner.platform_fingerprint import fingerprint

    if state_fn is _DEFAULT_STATE:
        state_fn = recipe_state

    files = latest_discovery_files(root)
    fps = []
    meta: dict[str, dict[str, str]] = {}
    for did, path in files.items():
        if dealers is not None and did not in dealers:
            continue
        rec = _load_json(path)
        if not rec:
            continue
        state = state_fn(did) if (state_fn and only_needing) else "unfiltered"
        if only_needing and state_fn and not needs_platform(state):
            continue
        cap_path = latest_capture_file(root, did)
        cap = _load_json(cap_path) if cap_path else None
        fp = fingerprint(rec, cap)
        fp.dealer_id = fp.dealer_id or did
        fps.append(fp)
        meta[did] = {"state": state, "verdict": last_verdict(root, did, rec), "stamp": str(rec.get("stamp") or "")}
    return fps, meta


def cluster_all(fps: list[Any], *, threshold: float | None = None, min_size: int = MIN_CLUSTER) -> list[Any]:
    from backend.scanner.platform_fingerprint import JACCARD_THRESHOLD, cluster

    clusters = cluster(fps, threshold=JACCARD_THRESHOLD if threshold is None else threshold)
    return [c for c in clusters if len(c.members) >= min_size]


def summary_line(c: Any) -> str:
    ids = c.dealer_ids
    shown = ", ".join(ids[:6]) + (f", +{len(ids) - 6}" if len(ids) > 6 else "")
    plats = sorted({m.platform for m in c.members if m.platform})
    what = f"platform {'/'.join(plats)} (template detects)" if plats else "unknown platform"
    return (f"discovery: {len(ids)} dealers share {what} {c.signature} ({shown})"
            f" → workspace/dealer_logs/_learning/{OUTPUT_NAME}")


def render_markdown(clusters: list[Any], meta: dict[str, dict[str, str]], *, total_considered: int,
                    stamp: str | None = None) -> str:
    from backend.scanner.platform_fingerprint import suggest_next_step

    stamp = stamp or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    L = ["# platform_candidates", "",
         f"Generated {stamp} by `backend/scripts/platform_candidates.py` from the latest `discovery_*.json` per dealer.",
         f"Dealers considered (no live recipe, or a stale / rejected one): {total_considered}. "
         f"Clusters of {MIN_CLUSTER}+ dealers: {len(clusters)}.",
         "A cluster means several dealers present their inventory the same unknown way: fix the platform once, "
         "not each dealer. Process: docs/NETWORK_SCAN_PROCESS.md; fingerprint code: backend/scanner/platform_fingerprint.py.",
         ""]
    if not clusters:
        L.append("_No cluster of two or more dealers._")
        return "\n".join(L) + "\n"
    for i, c in enumerate(clusters, 1):
        ids = c.dealer_ids
        L.append(f"## Cluster {i} — {len(ids)} dealers")
        L.append("")
        L.append(f"- signature: `{c.signature}`")
        if len(c.signatures) > 1:
            L.append(f"- merged signatures ({len(c.signatures)}): " + "; ".join(f"`{s}`" for s in c.signatures[:6])
                     + (" …" if len(c.signatures) > 6 else ""))
        shared = c.shared_features[:12]
        L.append("- shared features: " + (", ".join(f"`{f}`" for f in shared) if shared else "_none common to every member_")
                 + (f" (+{len(c.shared_features) - 12} more)" if len(c.shared_features) > 12 else ""))
        plats = sorted({m.platform for m in c.members if m.platform})
        if plats:
            L.append(f"- template already detecting: {', '.join(plats)}")
        scored = [m.nearest_template for m in c.members if m.nearest_template]
        if scored:
            name, score = max(scored, key=lambda t: t[1])
            L.append(f"- nearest known template: `{name}` (partial detect score {score:.2f})")
        else:
            L.append("- nearest known template: none scores partially (detect map is boolean, all false)")
        L.append("- members:")
        for m in c.members:
            mm = meta.get(m.dealer_id, {})
            L.append(f"  - `{m.dealer_id}` — recipe {mm.get('state', '?')}; last verdict: {mm.get('verdict', m.classification or '?')}"
                     f" (probe {mm.get('stamp') or m.stamp or '?'}; workspace/dealer_logs/{m.dealer_id}/discovery.md)")
        L.append(f"- next step: {suggest_next_step(c)}")
        L.append("")
    return "\n".join(L) + "\n"


def write_candidates(root: Path, text: str) -> Path:
    d = root / "_learning"
    d.mkdir(parents=True, exist_ok=True)
    p = d / OUTPUT_NAME
    p.write_text(text, encoding="utf-8")
    return p


def build(root: Path, *, dealers: set[str] | None = None, state_fn: Any = _DEFAULT_STATE,
          only_needing: bool = True, threshold: float | None = None, min_size: int = MIN_CLUSTER,
          write: bool = True) -> dict[str, Any]:
    """Everything the CLI and the pipeline need: the clusters, their summary
    lines, and the markdown path (``None`` when ``write`` is off)."""
    fps, meta = collect(root, dealers=dealers, state_fn=state_fn, only_needing=only_needing)
    clusters = cluster_all(fps, threshold=threshold, min_size=min_size)
    path = None
    if write:
        path = write_candidates(root, render_markdown(clusters, meta, total_considered=len(fps)))
    return {"clusters": clusters, "meta": meta, "considered": len(fps), "lines": [summary_line(c) for c in clusters],
            "path": path}


def report_for_run(needs_discovery: list[str], *, root: Path | None = None, state_fn: Any = _DEFAULT_STATE,
                   min_run_members: int = MIN_CLUSTER) -> list[str]:
    """The pipeline's end-of-run hook: cluster every dealer under ``root`` that
    needs a platform (so a run's failing dealer is matched against dealers other
    runs already logged), rewrite the markdown, and return the summary lines of
    the clusters holding at least ``min_run_members`` of this run's
    ``needs_discovery`` dealers. Never raises."""
    root = root or LOG_ROOT
    wanted = set(needs_discovery or [])
    if not wanted:
        return []
    try:
        res = build(root, state_fn=state_fn)
    except Exception as exc:  # noqa: BLE001 - the triage must still be written
        return [f"discovery: platform clustering failed: {str(exc)[:160]}"]
    out = []
    for c in res["clusters"]:
        ids = c.dealer_ids
        hits = [d for d in ids if d in wanted]
        if len(hits) >= min_run_members:
            out.append(summary_line(c))
    return out


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--root", default=str(LOG_ROOT), help="dealer_logs root (default workspace/dealer_logs or $DEALER_LOGS_ROOT)")
    ap.add_argument("--dealers", default="", help="comma-separated dealer ids (default every dealer with a discovery record)")
    ap.add_argument("--all", action="store_true", help="cluster every dealer, not only those without a live recipe")
    ap.add_argument("--threshold", type=float, default=None, help="Jaccard merge threshold (default 0.6)")
    ap.add_argument("--min-size", type=int, default=MIN_CLUSTER)
    ap.add_argument("--no-write", action="store_true", help="print only; do not rewrite _learning/platform_candidates.md")
    args = ap.parse_args(argv)
    root = Path(args.root)
    dealers = {d.strip() for d in args.dealers.split(",") if d.strip()} or None
    res = build(root, dealers=dealers, only_needing=not args.all, threshold=args.threshold, min_size=args.min_size,
                write=not args.no_write)
    for line in res["lines"]:
        print(line, flush=True)
    print(f"platform_candidates: {res['considered']} dealers considered, {len(res['clusters'])} clusters of {args.min_size}+"
          + (f" → {res['path']}" if res["path"] else ""), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
