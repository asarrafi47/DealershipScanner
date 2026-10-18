"""Metrics read from a fleet run dir (``<run>/s*/triage.json``, ``s*/scanner.log``,
``s*.out``, ``fleet_summary.json``). Pure file parsing, no DB access.

Definitions (each one reproduces a number in the N1.3 spec on the 10-13 run):
  * captured  = sum of triage ``rows`` (feed rows before the VIN-owner guard,
                ``assess.py:104-108`` from ``persist.py:18``)
  * refused   = sum of triage ``vin_owner_conflicts`` (each dealer's latest run)
  * written   = sum of triage ``rows_stamped``
  * VDP       = sum of triage ``http_first.candidates`` (wanted) / ``fetched``
  * vpic_fuel = sum of triage ``accuracy.hard.vpic_fuel`` and the dealers with > 0
  * page-cap walks = ``Recipe fetch [<label>]: N unique VIN(s) from <cap> page(s)``
                lines in ``s*/scanner.log`` (cap = ``_MAX_REPLAY_PAGES``)
  * HTTP 403/429 = the ``fleet_scan.tally_http`` rule over ``s*/scanner.log`` and
                ``s*.out`` (the same regexes, copied below; a test pins them)
  * scanner retirements = ``Inventory reconcile <dealer>: ... marked_inactive=N``
"""

from __future__ import annotations

import ast
import collections
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

# Copied from backend/scripts/fleet_scan.py:66-70 (importing fleet_scan loads .env);
# test_cleanroom_acceptance pins them equal.
STATUS_RE = re.compile(r"(?:HTTP|status[ =:]*|returned|→)\s*(403|429)\b", re.I)
URL_HOST_RE = re.compile(r"https?://([^/\s\"')]+)")
FOR_HOST_RE = re.compile(r"exhausted for ([^\s(]+)")
LABEL_RE = re.compile(r"(\w+) \[([^\]]+)\]:")
CHALLENGE_RE = re.compile(r"cloudflare|cf-ray|cf_chl|just a moment|attention required", re.I)

RECIPE_FETCH_RE = re.compile(r"Recipe fetch \[([^\]]+)\]: (\d+) unique VIN\(s\) from (\d+) page\(s\)")
RECONCILE_RE = re.compile(
    r"Inventory reconcile (\S+): scraped_candidates=(\d+) marked_inactive=(\d+)(?: skipped_reason=(\S*))?"
)
DEFAULT_PAGE_CAP = 40
BLOCKED_FETCH_RATIO = 0.10
# The 10-15 count (140 blocked, 59,523 wanted, 365 fetched) leaves out
# oldorchardnissan-com (12 candidates, all 404); any floor in 13..23 reproduces it.
BLOCKED_MIN_CANDIDATES = 20


def max_replay_pages(code_root: Path) -> int:
    """``_MAX_REPLAY_PAGES`` read from ``backend/scanner/recipes.py`` without importing it."""
    try:
        tree = ast.parse((code_root / "backend" / "scanner" / "recipes.py").read_text())
    except (OSError, SyntaxError):
        return DEFAULT_PAGE_CAP
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", "") == "_MAX_REPLAY_PAGES" for t in node.targets):
            if isinstance(node.value, ast.Constant) and isinstance(node.value.value, int):
                return node.value.value
    return DEFAULT_PAGE_CAP


def load_triage(run_dir: Path) -> list[dict[str, Any]]:
    paths = sorted(run_dir.glob("s*/triage.json")) or sorted(run_dir.glob("triage.json"))
    out: list[dict[str, Any]] = []
    for p in paths:
        data = json.loads(p.read_text())
        for d in data.get("dealers") or []:
            d = dict(d)
            d["_shard"] = p.parent.name if p.parent != run_dir else ""
            out.append(d)
    return out


def load_fleet_summary(run_dir: Path) -> dict[str, Any]:
    p = run_dir / "fleet_summary.json"
    try:
        return json.loads(p.read_text())
    except (OSError, ValueError):
        return {}


def find_manifests(run_dir: Path) -> list[Path]:
    seen: list[Path] = []
    for base in (run_dir, run_dir.parent, run_dir.parent.parent):
        for p in sorted((base / "manifest").glob("*.json")):
            if p not in seen:
                seen.append(p)
    return seen


def label_maps(manifests: list[Path], dealer_ids: set[str]) -> tuple[dict[str, str], dict[str, str]]:
    """(name -> dealer_id, host -> dealer_id) from the run's manifests."""
    names: dict[str, str] = {}
    hosts: dict[str, str] = {}
    for p in manifests:
        try:
            rows = json.loads(p.read_text())
        except (OSError, ValueError):
            continue
        if isinstance(rows, dict):
            rows = rows.get("dealers") or list(rows.values())
        for r in rows if isinstance(rows, list) else []:
            if not isinstance(r, dict) or not r.get("dealer_id"):
                continue
            did = str(r["dealer_id"])
            if r.get("name"):
                names.setdefault(str(r["name"]).strip(), did)
            host = urlsplit(str(r.get("url") or r.get("website") or "")).hostname or ""
            if host:
                hosts.setdefault(host.lower(), did)
                hosts.setdefault(host.lower().removeprefix("www."), did)
    for did in dealer_ids:
        names.setdefault(did, did)
    return names, hosts


def resolve_label(label: str, names: dict[str, str], hosts: dict[str, str]) -> str | None:
    label = label.strip()
    if label in names:
        return names[label]
    h = label.lower()
    return hosts.get(h) or hosts.get(h.removeprefix("www."))


def _logs(run_dir: Path, outs: bool = True) -> list[Path]:
    logs = sorted(run_dir.glob("s*/scanner.log"))
    if outs:
        logs += sorted(run_dir.glob("s*.out"))
    return logs


# ---------------------------------------------------------------------------

def identity(triage: list[dict[str, Any]], db_active: int, db_inactive: int,
             db_active_by_dealer: dict[str, int]) -> dict[str, Any]:
    with_rows = [d for d in triage if isinstance(d.get("rows"), (int, float))]
    null_rows = [d for d in triage if not isinstance(d.get("rows"), (int, float))]
    captured = sum(int(d["rows"]) for d in with_rows)
    refused = sum(int(d.get("vin_owner_conflicts") or 0) for d in triage)
    written = sum(int(d.get("rows_stamped") or 0) for d in triage)
    null_active = sum(db_active_by_dealer.get(d["dealer_id"], 0) for d in null_rows)
    per_dealer_ok = sum(
        1 for d in with_rows
        if int(d["rows"]) - int(d.get("active_after") or 0) == int(d.get("vin_owner_conflicts") or 0)
    )
    return {
        "captured": captured,
        "refused": refused,
        "written": written,
        "active": db_active,
        "inactive": db_inactive,
        "dealers": len(triage),
        "dealers_with_rows": len(with_rows),
        "dealers_rows_null": len(null_rows),
        "rows_null_dealers_active": null_active,
        "rows_null_dealers_with_active": sorted(
            d["dealer_id"] for d in null_rows if db_active_by_dealer.get(d["dealer_id"])
        ),
        "triage_active_after": sum(int(d.get("active_after") or 0) for d in triage),
        "dealers_rows_minus_active_eq_refused": per_dealer_ok,
        "captured_minus_refused_eq_written": captured - refused == written,
        "written_plus_rows_null_active_eq_active": written + null_active == db_active,
    }


def verdicts(triage: list[dict[str, Any]]) -> dict[str, int]:
    c = collections.Counter(str(d.get("verdict")) for d in triage)
    return dict(sorted(c.items()))


def vdp(triage: list[dict[str, Any]]) -> dict[str, Any]:
    wanted = fetched = ran = blocked = blocked_w = blocked_f = blocked_empty = skipped_cap = blocked_any = 0
    statuses: collections.Counter[str] = collections.Counter()
    for d in triage:
        h = d.get("http_first") or {}
        c, f = int(h.get("candidates") or 0), int(h.get("fetched") or 0)
        wanted += c
        fetched += f
        skipped_cap += int(h.get("skipped_cap") or 0)
        for k, v in (h.get("statuses") or {}).items():
            statuses[str(k)] += int(v or 0)
        if c > 0:
            ran += 1
            blocked_any += f < BLOCKED_FETCH_RATIO * c
            if c >= BLOCKED_MIN_CANDIDATES and f < BLOCKED_FETCH_RATIO * c:
                blocked += 1
                blocked_w += c
                blocked_f += f
                blocked_empty += not (h.get("statuses") or {})
    return {
        "wanted": wanted,
        "fetched": fetched,
        "fetched_pct": round(100 * fetched / wanted, 1) if wanted else None,
        "dealers_ran": ran,
        "blocked_dealers": blocked,
        "blocked_wanted": blocked_w,
        "blocked_fetched": blocked_f,
        "blocked_statuses_empty": blocked_empty,
        "blocked_dealers_no_floor": blocked_any,
        "skipped_cap": skipped_cap,
        "statuses": dict(sorted(statuses.items())),
        "blocked_rule": (f"fetched < {int(BLOCKED_FETCH_RATIO * 100)}% of candidates and "
                         f">= {BLOCKED_MIN_CANDIDATES} candidates"),
    }


def accuracy(triage: list[dict[str, Any]]) -> dict[str, Any]:
    rows: collections.Counter[str] = collections.Counter()
    dealers: collections.Counter[str] = collections.Counter()
    fuel_all_phev = 0
    fuel_dealers: list[str] = []
    for d in triage:
        a = d.get("accuracy") or {}
        for k, v in (a.get("hard") or {}).items():
            rows[k] += int(v or 0)
            dealers[k] += 1 if v else 0
        if (a.get("hard") or {}).get("vpic_fuel"):
            fuel_dealers.append(d["dealer_id"])
            ex = (a.get("examples") or {}).get("vpic_fuel") or []
            if ex and all(("PHEV" in str(e.get("why", ""))) or ("Plug-In" in str(e.get("why", ""))) for e in ex):
                fuel_all_phev += 1
    inacc_fuel = [d["dealer_id"] for d in triage
                  if d.get("verdict") == "inaccurate" and "vpic_fuel" in str(d.get("reason") or "")]
    return {
        "vpic_fuel": {"rows": rows.get("vpic_fuel", 0), "dealers": dealers.get("vpic_fuel", 0),
                      "dealers_all_examples_phev": fuel_all_phev,
                      "inaccurate_reason_mentions": len(inacc_fuel)},
        "hard_rows_by_code": dict(rows.most_common()),
        "hard_dealers_by_code": dict(dealers.most_common()),
        "_vpic_fuel_dealers": sorted(fuel_dealers),
    }


def page_cap_walks(run_dir: Path, cap: int, names: dict[str, str], hosts: dict[str, str]) -> dict[str, Any]:
    walks: list[dict[str, Any]] = []
    for p in _logs(run_dir, outs=False):
        with p.open(errors="ignore") as fh:
            for line in fh:
                m = RECIPE_FETCH_RE.search(line)
                if m and int(m.group(3)) == cap:
                    label = m.group(1)
                    walks.append({"label": label, "dealer_id": resolve_label(label, names, hosts),
                                  "vins": int(m.group(2)), "shard": p.parent.name})
    return {
        "cap": cap,
        "walks": len(walks),
        "dealers": sorted({w["dealer_id"] or f"?{w['label']}" for w in walks}),
        "unresolved_labels": sorted({w["label"] for w in walks if not w["dealer_id"]}),
        "_walks": walks,
    }


def http_403_429(run_dir: Path, names: dict[str, str], hosts: dict[str, str]) -> dict[str, Any]:
    by_status: collections.Counter[str] = collections.Counter()
    by_dealer: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    unresolved: collections.Counter[str] = collections.Counter()
    challenge = 0
    for p in _logs(run_dir):
        with p.open(errors="ignore") as fh:
            for line in fh:
                m = STATUS_RE.search(line)
                if not m:
                    continue
                status = m.group(1)
                by_status[status] += 1
                challenge += bool(CHALLENGE_RE.search(line))
                lab = LABEL_RE.search(line)
                hm = URL_HOST_RE.search(line) or FOR_HOST_RE.search(line)
                did = resolve_label(lab.group(2), names, hosts) if lab else None
                if not did and hm:
                    did = resolve_label(hm.group(1), names, hosts)
                if did:
                    by_dealer[did][status] += 1
                else:
                    key = lab.group(2) if lab else (hm.group(1) if hm else "?")
                    unresolved[f"{status} {key}"] += 1
    return {
        "by_status": dict(sorted(by_status.items())),
        "dealers": len(by_dealer),
        "lines_attributed": sum(sum(c.values()) for c in by_dealer.values()),
        "lines_unattributed": sum(unresolved.values()),
        "unattributed_top": dict(unresolved.most_common(15)),
        "cloudflare_marked_lines": challenge,
        "_by_dealer": {d: dict(c) for d, c in sorted(by_dealer.items())},
    }


def scanner_retirements(run_dir: Path) -> dict[str, int]:
    out: collections.Counter[str] = collections.Counter()
    for p in _logs(run_dir, outs=False):
        with p.open(errors="ignore") as fh:
            for line in fh:
                m = RECONCILE_RE.search(line)
                if m and int(m.group(3)):
                    out[m.group(1)] += int(m.group(3))
    return dict(out)


def retirements(triage: list[dict[str, Any]], run_dir: Path | None,
                db_inactive_by_dealer: dict[str, int]) -> dict[str, Any]:
    """Attribute every inactive DB row to the writer that logged it."""
    scanner = scanner_retirements(run_dir) if run_dir else {}
    pipeline = {d["dealer_id"]: int((d.get("reconcile") or {}).get("retired") or 0)
                for d in triage if int((d.get("reconcile") or {}).get("retired") or 0)}
    by_dealer: dict[str, dict[str, int]] = {}
    totals: collections.Counter[str] = collections.Counter()
    for did in sorted(set(db_inactive_by_dealer) | set(scanner) | set(pipeline)):
        left = db_inactive_by_dealer.get(did, 0)
        row: dict[str, int] = {"inactive_rows": left}
        for writer, src in (("scanner.inventory_reconcile", scanner), ("pipeline.reconcile_dealer", pipeline)):
            n = min(src.get(did, 0), left)
            if src.get(did):
                row[writer] = src[did]
                totals[writer] += n
            left -= n
        row["unattributed"] = left
        totals["unattributed"] += left
        by_dealer[did] = row
    return {
        "inactive_rows": sum(db_inactive_by_dealer.values()),
        "by_writer": {k: totals.get(k, 0) for k in
                      ("scanner.inventory_reconcile", "pipeline.reconcile_dealer", "unattributed")},
        "logged": {"scanner.inventory_reconcile": sum(scanner.values()),
                   "pipeline.reconcile_dealer": sum(pipeline.values())},
        "by_dealer": by_dealer,
    }
