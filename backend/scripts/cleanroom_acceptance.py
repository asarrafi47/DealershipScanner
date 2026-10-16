"""Clean-room acceptance tool (N1.3, docs/NEXT_PHASES_PLAN_2026_10.md). Read-only.

Computes the baseline every later phase proves against, on any scan DB (the
frozen ``cars_cleanroom``, a ``cars_cr_*`` scratch DB, local ``cars``):

  conflicts (rows, VINs, pairs) + the CF-7 class-(b) DB checks; the triage
  identity (captured, refused, written, active, inactive); in-run retirements by
  writer; dict-trim and token-color rows; ``vpic_fuel`` PHEV false positives;
  AWD/4WD rows where vPIC decodes 2WD; page-cap walks; VDP wanted vs fetched;
  verdicts and HTTP 403/429; coverage against a reference DB; junk prices,
  non-17 and I/O/Q VINs, placeholder images; the incomplete-listings recompute
  (with and without the new-car NULL-mileage code, and split by first seen
  <=3 d / <=7 d / older); stale incomplete-index flags.

Usage::

    python backend/scripts/cleanroom_acceptance.py --db cars_cleanroom \\
        --run-dir workspace/cleanroom_2026_10/runs/fleet_20261013T140256Z \\
        --ref-db cars --json out.json [--signals fleet_signals.json] \\
        [--expect backend/tests/fixtures/cleanroom_baseline_expected.json]

Every connection is opened with ``default_transaction_read_only=on`` and
``statement_timeout=120s`` and checked; a session that is not read-only makes
the tool exit 2 before any metric runs. The recompute runs in chunks of at most
``--chunk-timeout`` seconds each (default 140) and caches finished chunks in
``--work-dir``, so ``--max-new-chunks`` can split a long run across calls
(exit 3 = recompute still pending; run the same command again).

Exit codes: 0 ok (and matches ``--expect`` when given), 1 mismatch against
``--expect``, 2 refused (not read-only, bad arguments), 3 recompute pending,
4 a recompute chunk failed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.scripts.cleanroom import expect as expect_mod  # noqa: E402
from backend.scripts.cleanroom import metrics, recompute, run_artifacts, signals  # noqa: E402
from backend.scripts.cleanroom.db import (  # noqa: E402
    PG_HOST_DEFAULT,
    PG_PORT_DEFAULT,
    STATEMENT_TIMEOUT_MS,
    ReadOnlyRequired,
    Target,
    open_target,
)

TOOL_VERSION = 1
SECTIONS = ("conflicts", "cf7", "identity", "retirements", "columns", "drivetrain", "body_style", "run",
            "coverage", "incomplete", "stale_index")
EXIT_OK, EXIT_MISMATCH, EXIT_REFUSED, EXIT_PENDING, EXIT_CHUNK_FAILED = 0, 1, 2, 3, 4


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _db_key(name: str, dsn: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", Path(name).name) + "_" + hashlib.sha1(dsn.encode()).hexdigest()[:8]


def _strip_private(tree: Any) -> Any:
    if isinstance(tree, dict):
        return {k: _strip_private(v) for k, v in tree.items() if not str(k).startswith("_")}
    if isinstance(tree, list):
        return [_strip_private(v) for v in tree]
    return tree


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--db", required=True, help="Postgres DB name on --pg-host/--pg-port, or a SQLite file path")
    ap.add_argument("--dsn", help="full libpq conninfo for --db (overrides --pg-host/--pg-port)")
    ap.add_argument("--pg-host", default=PG_HOST_DEFAULT)
    ap.add_argument("--pg-port", type=int, default=PG_PORT_DEFAULT)
    ap.add_argument("--run-dir", help="fleet run dir with s*/triage.json, s*/scanner.log, fleet_summary.json")
    ap.add_argument("--ref-db", help="reference DB for coverage and first-seen (e.g. cars)")
    ap.add_argument("--ref-dsn", help="full libpq conninfo for --ref-db")
    ap.add_argument("--json", required=True, help="write the result here")
    ap.add_argument("--signals", nargs="?", const="", default=None,
                    help="also write per-dealer fleet signals (default: <run-dir>/../../analysis/fleet_signals.json)")
    ap.add_argument("--signal-cap", type=int, default=46, help="most dealers flagged for N11.10 (plan: 46)")
    ap.add_argument("--analysis-dir", help="dir of per-dealer analysis records (default: <run-dir>/../../analysis/dealers)")
    ap.add_argument("--groups-min-rows", type=int, default=20)
    ap.add_argument("--expect", help="fixture JSON; exit 1 unless every expected leaf matches")
    ap.add_argument("--expect-key", help="run key inside the fixture (default: --db)")
    ap.add_argument("--sections", default=",".join(SECTIONS), help="comma list; default all")
    ap.add_argument("--work-dir", help="recompute chunk cache (default: $TMPDIR/cleanroom_acceptance)")
    ap.add_argument("--chunks", type=int, default=16)
    ap.add_argument("--jobs", type=int, default=min(8, os.cpu_count() or 4))
    ap.add_argument("--chunk-timeout", type=int, default=recompute.WORKER_TIMEOUT_S)
    ap.add_argument("--max-new-chunks", type=int, help="run at most this many new chunks, then exit 3 if pending")
    ap.add_argument("--code-root", default=str(ROOT), help="checkout whose backend/ the recompute imports")
    ap.add_argument("--python", default=sys.executable, help="interpreter for recompute workers")
    return ap


class Run:
    def __init__(self, args: argparse.Namespace, connector=None):
        self.a = args
        self.connector = connector
        self.out: dict[str, Any] = {}
        self.private: dict[str, Any] = {}
        self.section_seconds: dict[str, float] = {}
        self.sections = [s.strip() for s in args.sections.split(",") if s.strip()]
        unknown = set(self.sections) - set(SECTIONS)
        if unknown:
            raise ValueError(f"unknown sections: {', '.join(sorted(unknown))}")

    def timed(self, name: str, fn, *a, **k):
        t0 = time.monotonic()
        try:
            return fn(*a, **k)
        finally:
            self.section_seconds[name] = round(self.section_seconds.get(name, 0.0) + time.monotonic() - t0, 2)

    def want(self, s: str) -> bool:
        return s in self.sections

    def execute(self) -> int:
        a = self.a
        t = open_target(a.db, dsn=a.dsn, host=a.pg_host, port=a.pg_port, connector=self.connector)
        ref: Target | None = None
        if a.ref_db:
            ref = open_target(a.ref_db, dsn=a.ref_dsn, host=a.pg_host, port=a.pg_port, connector=self.connector)
        run_dir = Path(a.run_dir).resolve() if a.run_dir else None
        o = self.out
        o.update({
            "tool": "cleanroom_acceptance",
            "tool_version": TOOL_VERSION,
            "generated_at": _now(),
            "db": a.db,
            "dialect": t.dialect,
            "read_only": True,
            "ref_db": a.ref_db,
            "run_dir": str(run_dir) if run_dir else None,
            "code": {"tool_root": str(ROOT), "tool_head": recompute.git_head(ROOT),
                     "code_root": a.code_root, "code_head": recompute.git_head(a.code_root),
                     "dictionary_index_present": (Path(a.code_root) / "backend/dictionary/index/manifest.json").is_file()},
        })
        try:
            rc = self._sections(t, ref, run_dir)
        finally:
            t.close()
            if ref:
                ref.close()
        timings = dict(t.timings)
        if ref:
            timings.update({f"ref.{k}": v for k, v in ref.timings.items()})
        o["limits"] = {
            "statement_timeout_ms": STATEMENT_TIMEOUT_MS,
            "chunk_timeout_s": a.chunk_timeout,
            "query_seconds_max": max(timings.values()) if timings else None,
            "recompute_chunk_seconds_max": max(
                [r.get("chunk_seconds_max") or 0 for r in (o.get("recompute") or {}).values() if isinstance(r, dict)],
                default=None),
            "query_seconds": timings,
            "section_seconds": self.section_seconds,
        }
        return rc

    # -----------------------------------------------------------------
    def _sections(self, t: Target, ref: Target | None, run_dir: Path | None) -> int:
        a, o, p = self.a, self.out, self.private
        counts = self.timed("db_counts", metrics.db_counts, t)
        p["active_by_dealer"] = counts["_active_by_dealer"]
        p["inactive_by_dealer"] = counts["_inactive_by_dealer"]
        o["db_rows"] = _strip_private(counts)

        triage: list[dict[str, Any]] = []
        names: dict[str, str] = {}
        hosts: dict[str, str] = {}
        if run_dir:
            triage = run_artifacts.load_triage(run_dir)
            names, hosts = run_artifacts.label_maps(run_artifacts.find_manifests(run_dir),
                                                    {d["dealer_id"] for d in triage})

        pairs: list[tuple[str, str, int]] = []
        groups: dict[str, Any] = {"_groups": []}
        if self.want("conflicts"):
            c = self.timed("conflicts", metrics.conflicts, t)
            pairs = c.pop("_pairs", [])
            groups = metrics.conflict_groups(pairs, a.groups_min_rows)
            c["groups_ge_min_rows"] = _strip_private(groups)
            o["conflicts"] = c
        if self.want("cf7"):
            o["cf7_class_b"] = self.timed("cf7", metrics.cf7_checks, t)
        if self.want("identity") and run_dir:
            o["identity"] = run_artifacts.identity(triage, counts["active"], counts["inactive"],
                                                   p["active_by_dealer"])
            o["identity"]["total_rows"] = counts["total_rows"]
        if self.want("retirements"):
            o["retirements"] = run_artifacts.retirements(triage, run_dir, p["inactive_by_dealer"])

        col: dict[str, Any] = {"_per_dealer": {}}
        if self.want("columns"):
            col = self.timed("columns", metrics.column_scan, t)
            for k in ("parser_junk", "junk_prices", "vin_quality", "placeholder_images", "headline"):
                o[k] = col[k]
            o["mileage_columns"] = col["mileage"]
        awd: dict[str, Any] = {"_by_dealer": {}}
        if self.want("drivetrain"):
            awd = self.timed("drivetrain", metrics.drivetrain_vs_vpic, t)
            o["drivetrain_vs_vpic"] = _strip_private(awd)
        if self.want("body_style"):
            o["body_style"] = self.timed("body_style", metrics.body_style_vocabulary, t)

        cap_info: dict[str, Any] = {"_walks": []}
        http: dict[str, Any] = {"_by_dealer": {}}
        acc: dict[str, Any] = {}
        if self.want("run") and run_dir:
            o["verdicts"] = run_artifacts.verdicts(triage)
            o["vdp"] = run_artifacts.vdp(triage)
            acc = run_artifacts.accuracy(triage)
            o["vpic_fuel"] = acc["vpic_fuel"]
            o["verifier_hard_codes"] = {"rows": acc["hard_rows_by_code"], "dealers": acc["hard_dealers_by_code"]}
            cap = run_artifacts.max_replay_pages(Path(a.code_root))
            cap_info = run_artifacts.page_cap_walks(run_dir, cap, names, hosts)
            o["page_cap"] = _strip_private(cap_info)
            http = run_artifacts.http_403_429(run_dir, names, hosts)
            o["http_403_429"] = _strip_private(http)
            fs = run_artifacts.load_fleet_summary(run_dir)
            o["fleet_summary"] = {k: fs.get(k) for k in ("started", "dealers", "rows", "vin_owner_conflicts",
                                                         "verdicts", "http_403_429", "scan_seconds")} if fs else {}

        ref_first: dict[str, str] = {}
        ref_active_vin: dict[str, int] = {}
        ref_max = None
        ref_counts: dict[str, Any] | None = None
        if ref is not None and (self.want("coverage") or self.want("incomplete")):
            ref_first, ref_active_vin, ref_max = self.timed("ref_vins", metrics.vin_first_seen, ref)
            ref_counts = self.timed("ref_counts", metrics.db_counts, ref)
        if self.want("coverage") and ref is not None:
            roster = {d["dealer_id"] for d in triage} or None
            o["coverage_vs_ref"] = metrics.coverage_vs_ref(
                p["active_by_dealer"], ref_counts["_active_by_dealer"], self.timed("coverage", metrics.active_vins, t),
                ref_active_vin, roster)

        recs: list[dict[str, Any]] | None = None
        rc = EXIT_OK
        if self.want("incomplete") or self.want("stale_index"):
            recs, rc = self._recompute(t, "db")
            if recs is not None:
                inc = recompute.summarize(recs)
                inc["window"] = recompute.window_split(recs, ref_first, ref_vins=set(ref_active_vin) if ref else None,
                                                       ref_max_first_seen=ref_max)
                inc["transit"] = recompute.transit_split(recs, set(ref_active_vin) if ref else None)
                if ref is not None:
                    ref_recs, _ = self._recompute(ref, "ref", run_missing=False)
                    if ref_recs is not None:
                        inc["paired_vs_ref"] = recompute.paired_vs_reference(recs, ref_recs)
                o["incomplete"] = inc
                if self.want("stale_index"):
                    o["stale_index_flags"] = recompute.stale_index_flags(
                        recs, self.timed("index_rows", metrics.index_rows, t))

        if a.signals is not None:
            self._signals(triage, ref_counts, pairs, groups, cap_info, http, col, awd, recs, run_dir)
        return rc

    # -----------------------------------------------------------------
    def _work(self, t: Target) -> Path:
        base = Path(self.a.work_dir) if self.a.work_dir else Path(tempfile.gettempdir()) / "cleanroom_acceptance"
        return base / _db_key(t.name, t.dsn) / "recompute"

    def _recompute(self, t: Target, label: str, run_missing: bool = True) -> tuple[list[dict] | None, int]:
        a, o = self.a, self.out
        if not t.is_pg:
            o.setdefault("recompute", {})[label] = {"skipped": "SQLite targets use the in-process API "
                                                               "(recompute.run_inprocess)"}
            return None, EXIT_OK
        state = metrics.db_state(t)
        fp = recompute.fingerprint_for(state, a.code_root, a.chunks)
        work = self._work(t)
        if not run_missing:
            # The reference recompute is optional context (paired view): reuse it, never start it.
            if not recompute.cached_complete(work, a.chunks, fp):
                o.setdefault("recompute", {})[label] = {"skipped": f"no complete recompute of {t.name} in {work}"}
                return None, EXIT_OK
            o.setdefault("recompute", {})[label] = {"reused": str(work), "db_state": state}
            return recompute.load_records(work, a.chunks), EXIT_OK
        res = self.timed(f"recompute_{label}", recompute.run_chunks, conninfo=t.dsn, work=work, fingerprint=fp,
                         chunks=a.chunks, jobs=a.jobs, timeout=a.chunk_timeout, code_root=a.code_root,
                         python=a.python, max_new_chunks=a.max_new_chunks)
        res["work_dir"] = str(work)
        res["db_state"] = state
        o.setdefault("recompute", {})[label] = res
        if res["failed"]:
            return None, EXIT_CHUNK_FAILED
        if not res["complete"]:
            return None, EXIT_PENDING
        return recompute.load_records(work, a.chunks), EXIT_OK

    def _signals(self, triage, ref_counts, pairs, groups, cap_info, http, col, awd, recs, run_dir) -> None:
        a = self.a
        analysis_dir = Path(a.analysis_dir) if a.analysis_dir else (
            run_dir.parent.parent / "analysis" / "dealers" if run_dir else None)
        analyzed = ({p.stem for p in analysis_dir.glob("*.json")}
                    if analysis_dir and analysis_dir.is_dir() else None)
        page_cap: dict[str, int] = {}
        for w in cap_info.get("_walks", []):
            if w.get("dealer_id"):
                page_cap[w["dealer_id"]] = page_cap.get(w["dealer_id"], 0) + 1
        res = signals.build_signals(
            triage=triage,
            db_active=self.private["active_by_dealer"],
            db_inactive=self.private["inactive_by_dealer"],
            ref_active=ref_counts["_active_by_dealer"] if ref_counts else None,
            pairs=pairs,
            groups=groups.get("_groups", []),
            page_cap_dealers=page_cap,
            http_by_dealer=http.get("_by_dealer", {}),
            column_per_dealer=col.get("_per_dealer", {}),
            awd_by_dealer=awd.get("_by_dealer", {}),
            incomplete_by_dealer=recompute.per_dealer(recs) if recs is not None else None,
            analyzed=analyzed,
            cap=a.signal_cap,
        )
        res = {"generated_at": _now(), "db": a.db, "run_dir": str(run_dir) if run_dir else None,
               "analysis_dir": str(analysis_dir) if analyzed is not None else None, **res}
        path = Path(a.signals) if a.signals else (
            run_dir.parent.parent / "analysis" / "fleet_signals.json" if run_dir else None)
        if path is None:
            raise ValueError("--signals needs a path when --run-dir is not given")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(res, indent=1, default=str) + "\n")
        self.out["signals"] = {"path": str(path), **res["summary"]}


def main(argv: list[str] | None = None, connector=None) -> int:
    a = build_parser().parse_args(argv)
    try:
        run = Run(a, connector=connector)
        rc = run.execute()
    except ReadOnlyRequired as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return EXIT_REFUSED
    except ValueError as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return EXIT_REFUSED
    out = _strip_private(run.out)
    mismatches: list[dict[str, Any]] = []
    if a.expect and rc == EXIT_OK:
        expected = expect_mod.load_expected(Path(a.expect), a.expect_key or Path(a.db).name)
        mismatches = expect_mod.compare(out, expected)
        out["expect"] = {"fixture": a.expect, "key": a.expect_key or Path(a.db).name,
                         "leaves": len(expect_mod.leaves(expected)), "mismatches": mismatches,
                         "match": not mismatches}
    Path(a.json).parent.mkdir(parents=True, exist_ok=True)
    Path(a.json).write_text(json.dumps(out, indent=1, default=str) + "\n")
    if rc == EXIT_PENDING:
        print("PENDING: recompute chunks remain; run the same command again", file=sys.stderr)
        return rc
    if rc != EXIT_OK:
        print("FAILED: a recompute chunk failed (see recompute.*.failed in the JSON)", file=sys.stderr)
        return rc
    if a.expect:
        if mismatches:
            for m in mismatches:
                print(f"MISMATCH {m['path']}: expected {m['expected']!r}, got {m['actual']!r}", file=sys.stderr)
            print(f"EXPECT FAILED: {len(mismatches)} mismatches", file=sys.stderr)
            return EXIT_MISMATCH
        print(f"EXPECT OK: {out['expect']['leaves']} values match", file=sys.stderr)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
