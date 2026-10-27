"""Re-judge every live recipe under the unified gate (P12B.7).

Recipes saved before the set gate existed (2026-09-28) were never validated by the walker
that replays them, and the lifecycle re-validates only after a failing capture. This walks
each dealer's live recipes once, as a set, through the same ``walk_candidates`` +
``gate_recipes`` the save paths use, and reports the verdicts.

    # 1. judge (writes nothing but the report files you name)
    .venv/bin/python -m backend.scripts.rejudge_live_recipes --all \\
        --report docs/monolith_audit_2026_10_01/measurements/rejudge_2026_10.md \\
        --json workspace/phase_runs/N19/rejudge.json
    # 2. the owner reads the report and writes the ids to stale, one per line
    # 3. dry run of the apply (default), then the apply itself
    .venv/bin/python -m backend.scripts.rejudge_live_recipes --json workspace/phase_runs/N19/rejudge.json \\
        --approved workspace/phase_runs/N19/approved.txt --backup-dir workspace/backups/rejudge_2026_10
    ... the same command plus --apply

The report always names the one-condition dealers (the audit counted at least 14) and
NAMED_DEALERS (northparklexus, carvision, hendricklexuscharlotte), found or not.

Apply (``--apply``, the only flag that writes) takes the approved ids only, and only those
the --all run judged ``reject`` or ``uncertain``. Before the first write it backs the
dealers' ``dealer_recipes`` rows up with psql ``\\copy`` plus a sha256 manifest, and it
stops if the backup row count is not the approved row count. Each live recipe of an
approved dealer is marked stale with reason ``rejudge:<verdict>`` and the dealer's
discovery.md gets the entry. The scanner skips a dealer with no live recipe, so this is
the step that changes scans; nothing is written to ``cars``.

Pacing: ``SCANNER_SYNTH_FETCH_DELAY`` spaces every request (this script sets 4 seconds
when the shell sets none). Run from a home IP, never with SCANNER_EGRESS_TAG set; local
first, then prod, owner-run.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import subprocess
import sys
from dataclasses import fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_log = logging.getLogger("rejudge")

# Dealers the report names whether or not they have a live recipe (the audit's examples
# of an under-collecting or one-condition recipe). Matched by dealer_id prefix.
NAMED_DEALERS = ("northparklexus", "carvision", "hendricklexuscharlotte")
# The audit counted at least this many one-condition dealers (REMEDIATION_PLAN, Phase 6A).
ONE_CONDITION_EXPECTED = 14
STALE_VERDICTS = ("reject", "uncertain")
DEFAULT_FETCH_DELAY = "4"
MANIFEST_NAME = "manifest.json"
BACKUP_NAME = "dealer_recipes.csv"

Fetch = Callable[..., Any]


class Refused(Exception):
    """The apply stopped before (or between) writes; the message says why."""


# ── reading the live set ───────────────────────────────────────────────────────

def _recipe_from_row(row: dict[str, Any]) -> Any:
    from backend.scanner.recipes import EndpointRecipe

    names = {f.name for f in fields(EndpointRecipe)}
    return EndpointRecipe(**{k: v for k, v in row.items() if k in names})


def load_live(conn: Any, only: list[str] | None = None) -> dict[str, dict[str, Any]]:
    """``{dealer_id: {"recipes": [EndpointRecipe], "base_url", "dealer_name"}}`` for every
    dealer with at least one non-stale recipe. Only the live recipes are returned: they are
    the set the scan replays. SELECT only."""
    cur = conn.cursor()
    cur.execute("SELECT dealer_id, recipes_json FROM dealer_recipes ORDER BY dealer_id")
    rows = cur.fetchall()
    cur.execute("SELECT dealer_key, dealer_name, website_url FROM dealer_scan_status")
    info = {k: (n, u) for k, n, u in cur.fetchall()}
    out: dict[str, dict[str, Any]] = {}
    for did, raw in rows:
        if only and did not in only:
            continue
        try:
            rec_rows = json.loads(raw or "[]")
        except ValueError:
            continue
        if not isinstance(rec_rows, list):
            continue
        live = [_recipe_from_row(r) for r in rec_rows if isinstance(r, dict) and not r.get("stale")]
        if not live:
            continue
        name, url = info.get(did, (None, None))
        out[did] = {"recipes": live, "base_url": (url or "").rstrip("/"), "dealer_name": name or did}
    return out


# ── judging ────────────────────────────────────────────────────────────────────

def rejudge_dealer(dealer_id: str, entry: dict[str, Any], *, fetch: Fetch | None = None,
                   pages: int | None = None) -> dict[str, Any]:
    """One dealer's live set through the save paths' own walk and gate; writes nothing.
    A dealer whose walk raises is recorded as ``error`` (and never staled by the apply)."""
    from backend.scanner.recipe_validation import gate_recipes, strip_url_queries, walk_candidates

    recipes = entry["recipes"]
    base = {"dealer_id": dealer_id, "recipes": len(recipes),
            "urls": [strip_url_queries(r.url)[:140] for r in recipes]}
    try:
        checks = walk_candidates(dealer_id, recipes, base_url=entry.get("base_url", ""),
                                 dealer_name=entry.get("dealer_name", ""), place=entry.get("place"),
                                 fetch=fetch, pages=pages)
        _kept, rep = gate_recipes(dealer_id, recipes, base_url=entry.get("base_url", ""),
                                  dealer_name=entry.get("dealer_name", ""), place=entry.get("place"), context="rejudge",
                                  record=False, pages=checks.pages, checks=checks)
    except Exception as exc:  # noqa: BLE001 - one dealer's failure must not stop the sweep
        return {**base, "verdict": "error", "reason_code": "error", "reasons": [f"{type(exc).__name__}"],
                "vins_total": 0, "site_total": None, "coverage": None, "per_condition": {}, "flags": {}}
    return {**base, "verdict": rep.verdict, "reason_code": rep.reason_code, "reasons": list(rep.reasons),
            "vins_total": rep.vins_total, "site_total": rep.site_total, "coverage": rep.coverage,
            "per_condition": dict(rep.per_condition), "flags": {k: v for k, v in rep.flags.items() if v}}


def rejudge_all(live: dict[str, dict[str, Any]], *, fetch: Fetch | None = None, pages: int | None = None,
                progress: Callable[[int, int, dict[str, Any]], None] | None = None) -> list[dict[str, Any]]:
    out = []
    for i, (did, entry) in enumerate(sorted(live.items()), 1):
        j = rejudge_dealer(did, entry, fetch=fetch, pages=pages)
        out.append(j)
        if progress:
            progress(i, len(live), j)
    return out


# ── the report ─────────────────────────────────────────────────────────────────

def _is_one_condition(j: dict[str, Any]) -> bool:
    return j.get("reason_code") == "one_condition" or bool((j.get("flags") or {}).get("one_condition"))


def _line(j: dict[str, Any]) -> str:
    cov = f"{j['coverage']:.0%}" if j.get("coverage") is not None else "-"
    return (f"| {j['dealer_id']} | {j['verdict']} | {j.get('reason_code') or '-'} | {j.get('vins_total', 0)} | "
            f"{j['site_total'] if j.get('site_total') is not None else '-'} | {cov} | "
            f"{json.dumps(j.get('per_condition') or {})} |")


_HEAD = ["| dealer | verdict | reason | VINs | site total | coverage | per condition |",
         "|---|---|---|---|---|---|---|"]


def build_report(judgments: list[dict[str, Any]], *, stamp: str = "", partial: bool = False) -> str:
    """Markdown over the judgments: counts, the dealers to stale, the one-condition dealers
    and the named dealers (found or not)."""
    stamp = stamp or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    by: dict[str, int] = {}
    for j in judgments:
        by[j["verdict"]] = by.get(j["verdict"], 0) + 1
    L = [f"# Live recipe re-judge ({stamp})", "",
         "Process: backend/scripts/rejudge_live_recipes.py --all (P12B.7). Nothing was written.", "",
         f"Dealers judged: {len(judgments)}" + (" (a subset: --dealers/--limit)" if partial else ""),
         "Verdicts: " + (", ".join(f"{k} {v}" for k, v in sorted(by.items())) or "none"), ""]

    stale = [j for j in judgments if j["verdict"] in STALE_VERDICTS]
    L += [f"## Candidates to stale ({len(stale)})", "",
          "The owner approves ids from this list; apply marks them `rejudge:<verdict>`.", ""]
    L += (_HEAD + [_line(j) for j in stale]) if stale else ["None."]

    one = [j for j in judgments if _is_one_condition(j)]
    L += ["", f"## One-condition dealers ({len(one)})", ""]
    if not partial:
        note = (f"The audit counted at least {ONE_CONDITION_EXPECTED}."
                + ("" if len(one) >= ONE_CONDITION_EXPECTED else
                   f" This run found {len(one)}: the rest are judged elsewhere in this report or have no live recipe."))
        L += [note, ""]
    L += (_HEAD + [_line(j) for j in one]) if one else ["None."]

    L += ["", "## Named dealers", ""]
    for prefix in NAMED_DEALERS:
        hits = [j for j in judgments if j["dealer_id"].startswith(prefix)]
        if not hits:
            L.append(f"- {prefix}: no live recipe in this run" + (" (subset run)" if partial else ""))
            continue
        L += ["", f"- {prefix}:", ""] + _HEAD + [_line(j) for j in hits]
    errors = [j for j in judgments if j["verdict"] == "error"]
    if errors:
        L += ["", f"## Errors ({len(errors)})", ""] + [f"- {j['dealer_id']}: {'; '.join(j['reasons'])}" for j in errors]
    return "\n".join(L).rstrip() + "\n"


# ── the apply ──────────────────────────────────────────────────────────────────

def read_ids(path: Path) -> list[str]:
    """One id per line, ``#`` comments."""
    out = []
    for ln in path.read_text(encoding="utf-8").splitlines():
        ln = ln.split("#", 1)[0].strip()
        if ln and ln not in out:
            out.append(ln)
    return out


def plan_apply(judgments: list[dict[str, Any]], approved: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    """(judgments to stale, approved ids that are refused). Only an approved id the run
    judged reject or uncertain is staled: an ``ok`` or ``error`` verdict, or an id the run
    never judged, is refused."""
    by = {j["dealer_id"]: j for j in judgments}
    todo, refused = [], []
    for did in approved:
        j = by.get(did)
        if j and j["verdict"] in STALE_VERDICTS:
            todo.append(j)
        else:
            refused.append(did)
    return todo, refused


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _psql_copy(dsn: str, ids: list[str], dest: Path) -> None:
    """``\\copy (SELECT * FROM dealer_recipes WHERE dealer_id IN (...)) TO dest CSV HEADER``.
    ids are quoted as SQL literals (psql meta-commands do not take bind parameters)."""
    quoted = ", ".join("'" + i.replace("'", "''") + "'" for i in ids)
    cmd = f"\\copy (SELECT * FROM dealer_recipes WHERE dealer_id IN ({quoted})) TO '{dest}' CSV HEADER"
    subprocess.run(["psql", dsn, "-v", "ON_ERROR_STOP=1", "-c", cmd], check=True, capture_output=True, text=True, timeout=120)


def backup_rows(ids: list[str], backup_dir: Path, *, dsn: str | None = None,
                copy: Callable[[str, list[str], Path], None] | None = None) -> dict[str, Any]:
    """Back the approved dealers' ``dealer_recipes`` rows up to *backup_dir*, write the
    sha256 manifest, and verify the file holds one row per id that has a row. Returns the
    manifest. Raises :class:`Refused` on a failed copy or a row-count mismatch."""
    from backend.db.connect import connect, inventory_dsn

    backup_dir.mkdir(parents=True, exist_ok=True)
    dest = backup_dir / BACKUP_NAME
    dsn = dsn or inventory_dsn()
    try:
        (copy or _psql_copy)(dsn, ids, dest)
    except (subprocess.SubprocessError, OSError) as exc:
        raise Refused(f"the \\copy backup failed ({type(exc).__name__}); nothing was written") from exc
    conn = connect(read_only=True)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM dealer_recipes WHERE dealer_id = ANY(%s)", (ids,))
        expected = int(cur.fetchone()[0])
    finally:
        conn.close()
    if not dest.exists():
        raise Refused("the backup file was not written; nothing was written")
    with open(dest, newline="", encoding="utf-8") as fh:
        got = sum(1 for _ in csv.DictReader(fh))
    if got != expected or not expected:
        raise Refused(f"backup holds {got} row(s), the store holds {expected} for the approved ids; nothing was written")
    manifest = {"made_at": datetime.now(timezone.utc).isoformat(), "table": "dealer_recipes", "ids": ids,
                "rows": got, "files": {BACKUP_NAME: {"sha256": _sha256(dest), "bytes": dest.stat().st_size}}}
    (backup_dir / MANIFEST_NAME).write_text(json.dumps(manifest, indent=1, sort_keys=True), encoding="utf-8")
    return manifest


def write_apply_log(j: dict[str, Any], n_marked: int, *, log_root: Path | None = None) -> Path | None:
    from backend.scanner import recipe_validation as rv

    root = log_root or rv.LOG_ROOT
    try:
        d = root / j["dealer_id"]
        d.mkdir(parents=True, exist_ok=True)
        p = d / "discovery.md"
        head = "" if p.exists() else f"# {j['dealer_id']} — discovery\n\nProcess: docs/NETWORK_SCAN_PROCESS.md\n\n"
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        body = (f"## {stamp} recipe re-judge (rejudge apply) — {j['verdict'].upper()}\n"
                f"- marked {n_marked} live recipe(s) stale: rejudge:{j['verdict']}\n"
                f"- reasons: {'; '.join(j.get('reasons') or []) or '-'}\n"
                f"- VINs {j.get('vins_total', 0)}, site total {j.get('site_total') if j.get('site_total') is not None else 'not exposed'}\n"
                f"- the pre-change rows are in the rejudge backup manifest; restore by re-inserting that dealer_recipes row\n")
        with p.open("a", encoding="utf-8") as fh:
            fh.write(head + rv.strip_url_queries(body) + "\n")
        return p
    except OSError as exc:
        _log.debug("rejudge log skipped [%s]: %s", j["dealer_id"], exc)
        return None


def apply_stale(todo: list[dict[str, Any]], *, log_root: Path | None = None) -> int:
    """Mark every live recipe of each judged dealer stale (``rejudge:<verdict>``) through the
    store's key-scoped op, then log it. Returns the recipes marked."""
    from backend.scanner.recipes import load_recipes, mark_stale

    total = 0
    for j in todo:
        live = [r for r in load_recipes(j["dealer_id"]) if not r.stale]
        for r in live:
            mark_stale(j["dealer_id"], r, f"rejudge:{j['verdict']}")
        total += len(live)
        write_apply_log(j, len(live), log_root=log_root)
        _log.info("  %-38s staled %d recipe(s): rejudge:%s", j["dealer_id"][:38], len(live), j["verdict"])
    return total


# ── CLI ────────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--all", action="store_true", help="judge every live recipe (writes only --report/--json)")
    ap.add_argument("--dealers", default="", help="comma list to restrict the judging to")
    ap.add_argument("--limit", type=int, help="judge only this many dealers")
    ap.add_argument("--pages", type=int, help="pages per recipe (default: the save paths' full walk)")
    ap.add_argument("--report", help="write the markdown report here")
    ap.add_argument("--json", dest="json_path", help="write (--all) or read (apply) the judgments here")
    ap.add_argument("--approved", help="file of owner-approved dealer ids, one per line")
    ap.add_argument("--backup-dir", help="where the \\copy backup and manifest go (apply)")
    ap.add_argument("--apply", action="store_true", help="back up, then stale the approved dealers (default: show the plan)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if (os.environ.get("SCANNER_EGRESS_TAG") or "").strip():
        print("refusing: SCANNER_EGRESS_TAG is set; run from a home IP", file=sys.stderr)
        return 2
    if args.all == bool(args.approved):
        print("give --all (judge) or --approved (apply plan), not both or neither", file=sys.stderr)
        return 2
    only = [d.strip() for d in args.dealers.split(",") if d.strip()] or None

    if args.all:
        os.environ.setdefault("SCANNER_SYNTH_FETCH_DELAY", DEFAULT_FETCH_DELAY)  # before synth.http is imported
        from backend.db.connect import connect

        conn = connect(read_only=True)
        try:
            live = load_live(conn, only)
        finally:
            conn.close()
        if args.limit:
            live = dict(sorted(live.items())[: args.limit])
        _log.info("dealers with a live recipe: %d", len(live))
        judgments = rejudge_all(live, pages=args.pages, progress=lambda i, n, j: _log.info(
            "[%d/%d] %-38s %s %s", i, n, j["dealer_id"][:38], j["verdict"], j.get("reason_code") or ""))
        report = build_report(judgments, partial=bool(only or args.limit))
        if args.report:
            Path(args.report).parent.mkdir(parents=True, exist_ok=True)
            Path(args.report).write_text(report, encoding="utf-8")
        else:
            print(report)
        if args.json_path:
            Path(args.json_path).parent.mkdir(parents=True, exist_ok=True)
            Path(args.json_path).write_text(json.dumps(judgments, indent=1, sort_keys=True), encoding="utf-8")
        return 0

    if not args.json_path:
        print("--approved needs --json (the judgments an --all run wrote)", file=sys.stderr)
        return 2
    judgments = json.loads(Path(args.json_path).read_text(encoding="utf-8"))
    approved = read_ids(Path(args.approved))
    todo, refused = plan_apply(judgments, approved)
    print(f"approved {len(approved)}; to stale {len(todo)}; refused {len(refused)} (not judged reject/uncertain)")
    for did in refused:
        print(f"  refused {did}")
    for j in todo:
        print(f"  would stale {j['dealer_id']:40s} rejudge:{j['verdict']} ({j['recipes']} recipe(s))")
    if not args.apply:
        print("dry run: nothing written (add --apply with --backup-dir)")
        return 0
    if not args.backup_dir or not todo:
        print("--apply needs --backup-dir and at least one dealer to stale", file=sys.stderr)
        return 2
    try:
        manifest = backup_rows([j["dealer_id"] for j in todo], Path(args.backup_dir))
        print(f"backup: {manifest['rows']} row(s), sha256 {manifest['files'][BACKUP_NAME]['sha256'][:12]}")
        n = apply_stale(todo)
    except Refused as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    print(f"done: {n} recipe(s) staled on {len(todo)} dealer(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
