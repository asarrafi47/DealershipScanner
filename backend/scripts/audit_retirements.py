#!/usr/bin/env python3
"""Audit wrongly retired listings: classify, verify, apply, roll back (N10 P5B.5, P5B.6).

Why: the 2026-09-23..09-29 fleet runs retired ~1,400-row batches in one UPDATE per dealer
(one condition bucket missing from a run, a replay cut at the page cap, a group feed that
answered for a sibling store). This tool finds those batches in ``cars`` and, only after
a live check, restores the rows that are provably still listed.

Modes (one per call):

  classify (default)  READ ONLY. Groups the retired rows since ``--since`` by
                      ``(dealer_id, listing_removed_at, writer)`` and writes
                      ``workspace/data_quality/retirements_<stamp>/{batches.csv,
                      candidates.csv, report.md}``. The connection never commits.
  --verify replay|vdp Reads ``--from DIR/candidates.csv`` and writes ``DIR/verify.json``
                      (car_id -> live | gone | cannot_assess). Network; no DB writes.
  --apply             Needs ``--from DIR`` with a ``verify.json``. Writes the backup
                      ``workspace/backups/retire_restore_<stamp>.json`` first, then restores
                      only the verified-live ids that are still exactly as the audit saw them.
  --rollback FILE     Re-retires the ids in a restore backup (only those still restored).

Batch signatures (a batch is a candidate when it has at least one):
  one_condition      the run that retired it re-saw 0 rows of the batch's bucket and at
                     least 20 of the other bucket;
  bucket_now_empty   the dealer has no active row in that bucket today;
  large_share        the batch is more than 40% of the dealer's prior lot;
  truncation_suspect the run's ``recipe_replay`` walk was incomplete, or (``--scanner-log``)
                     the log says "from 40 page(s)" / "HTTP 4xx after N page(s)" for it.

Excluded, never candidates: disown batches (``listing_retirements.writer = 'disown'``; for
rows older than that table, a ``--scanner-log`` "belonged to sibling rooftops" line of the
same dealer whose count equals the batch), portal and admin retirements, and anything in
the exclude list. The list is seeded with the lexusofknoxville-com 2026-09-27 hand
retirement (1,417 foreign rows, user-authorized) and ``--exclude-file`` adds lines of
``dealer_id,YYYY-MM-DD[,reason]`` (``#`` starts a comment).

A batch where the log names fewer disowned rows than the batch holds (a disown and a
reconcile stamped in the same second) cannot be split by identity from a log alone; it is
listed as ``excluded:mixed_disown`` for hand review.

Usage:
  python -m backend.scripts.audit_retirements --since 2026-09-23
  python -m backend.scripts.audit_retirements --verify replay --from workspace/data_quality/retirements_<stamp>
  python -m backend.scripts.audit_retirements --apply --from workspace/data_quality/retirements_<stamp>
  python -m backend.scripts.audit_retirements --rollback workspace/backups/retire_restore_<stamp>.json
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.utils.project_env import load_project_dotenv  # noqa: E402

load_project_dotenv()

logger = logging.getLogger("audit_retirements")

QUALITY_ROOT = ROOT / "workspace" / "data_quality"
BACKUP_ROOT = ROOT / "workspace" / "backups"

# Writers whose batches this audit judges; disown/portal/admin are excluded by rule.
AUDITED_WRITERS = frozenset({"scanner_reconcile", "delta_scan", "pipeline_w2", ""})
EXCLUDED_WRITERS = {
    "disown": "disown",
    "portal_sync": "portal",
    "admin_delete": "admin",
    "restore": "restored_then_reretired",
    "restore_rollback": "rollback",
}

# (dealer_id, removed_at date prefix, reason): the 09-27 Lexus of Knoxville hand retirement.
SEEDED_EXCLUDES: tuple[tuple[str, str, str], ...] = (
    ("lexusofknoxville-com", "2026-09-27", "user-authorized hand retirement of 1,417 foreign rows"),
)

OTHER_BUCKET_MIN = 20
LARGE_SHARE = 0.40
SIGNATURES = ("one_condition", "bucket_now_empty", "large_share", "truncation_suspect")

_CHUNK = 400
_VIN_RE = re.compile(r"\b[A-HJ-NPR-Z0-9]{17}\b")
_DISOWN_LOG = re.compile(
    r"(?:Rooftop|Delta) \[(?P<name>[^\]]+)\]: (?P<n>\d+) car\(s\) belonged to sibling rooftops"
)
_TRUNC_LOG = (
    re.compile(r"from 40 page\(s\)"),
    re.compile(r"HTTP 4\d\d after \d+ page\(s\)"),
)
_LOG_NAME = re.compile(r"Recipe(?: walk)? \[(?P<name>[^\]]+)\]")
_LOG_DATE = re.compile(r"^(\d{4}-\d{2}-\d{2})")


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------

def _rows(cur: Any, sql: str, params: Iterable[Any] = ()) -> list[tuple]:
    cur.execute(sql, tuple(params))
    return [tuple(r) for r in cur.fetchall()]


def _chunks(items: list, n: int = _CHUNK):
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _bucket(value: Any) -> str:
    from backend.scanner.inventory_reconcile import condition_bucket

    return condition_bucket(value)


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _day(ts: Any) -> str:
    return str(ts or "")[:10]


def _host(url: Any) -> str:
    h = (urlparse(str(url or "").strip()).hostname or "").lower()
    return h[4:] if h.startswith("www.") else h


def _table_exists(cur: Any, name: str) -> bool:
    return bool(_rows(cur, "SELECT to_regclass(?)", (f"public.{name}",))[0][0])


def load_excludes(path: str | Path | None) -> list[tuple[str, str, str]]:
    """The seeded excludes plus the lines of *path* (``dealer_id,YYYY-MM-DD[,reason]``)."""
    out = list(SEEDED_EXCLUDES)
    if path:
        for raw in Path(path).read_text(encoding="utf-8").splitlines():
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            parts = [p.strip() for p in line.split(",", 2)]
            if len(parts) >= 2 and parts[0] and parts[1]:
                out.append((parts[0], parts[1], parts[2] if len(parts) > 2 else "exclude file"))
    return out


def _excluded(excludes: list[tuple[str, str, str]], dealer_id: str, removed_at: str) -> str | None:
    for did, day, why in excludes:
        if did == dealer_id and str(removed_at).startswith(day):
            return why
    return None


# --------------------------------------------------------------------------
# scanner-log parsing (older rows have no listing_retirements provenance)
# --------------------------------------------------------------------------

def parse_scanner_log(path: str | Path | None) -> dict[str, list[dict[str, Any]]]:
    """``{'disown': [{name, n, day}], 'trunc': [{name, day, text}]}`` from a scanner log."""
    out: dict[str, list[dict[str, Any]]] = {"disown": [], "trunc": []}
    if not path:
        return out
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            dm = _LOG_DATE.match(line)
            day = dm.group(1) if dm else ""
            m = _DISOWN_LOG.search(line)
            if m:
                out["disown"].append({"name": m.group("name").strip(), "n": int(m.group("n")), "day": day})
                continue
            if any(p.search(line) for p in _TRUNC_LOG):
                nm = _LOG_NAME.search(line)
                out["trunc"].append(
                    {"name": nm.group("name").strip() if nm else "", "day": day, "text": line.strip()[:160]}
                )
    return out


# --------------------------------------------------------------------------
# classify (P5B.5)
# --------------------------------------------------------------------------

def _retired_rows(cur: Any, since: str, dealers: set[str] | None) -> list[dict[str, Any]]:
    have_log = _table_exists(cur, "listing_retirements")
    writer_sql = (
        "(SELECT lr.writer FROM listing_retirements lr WHERE lr.car_id = c.id ORDER BY lr.id DESC LIMIT 1)"
        if have_log else "NULL"
    )
    sql = (
        f"SELECT c.id, c.vin, TRIM(COALESCE(c.dealer_id, '')), c.condition, c.listing_removed_at, "
        f"c.source_url, {writer_sql} FROM cars c "
        "WHERE COALESCE(c.listing_active, 1) = 0 AND c.listing_removed_at IS NOT NULL "
        "AND c.listing_removed_at >= ?"
    )
    out = []
    for r in _rows(cur, sql, (since,)):
        if dealers and r[2] not in dealers:
            continue
        out.append({
            "car_id": int(r[0]), "vin": r[1], "dealer_id": r[2], "bucket": _bucket(r[3]),
            "removed_at": str(r[4]), "source_url": r[5] or "", "writer": r[6] or "",
        })
    return out


def _lot_counts(cur: Any, dealer_id: str, removed_at: str) -> dict[str, dict[str, int]]:
    """Per bucket: rows active today and the prior lot (active today + retired at or after *removed_at*)."""
    out: dict[str, dict[str, int]] = defaultdict(lambda: {"active_now": 0, "prior": 0})
    for cond, active, removed in _rows(
        cur,
        "SELECT condition, COALESCE(listing_active, 1), listing_removed_at FROM cars "
        "WHERE TRIM(COALESCE(dealer_id, '')) = ?",
        (dealer_id,),
    ):
        b = _bucket(cond)
        if int(active) == 1:
            out[b]["active_now"] += 1
            out[b]["prior"] += 1
        elif removed is not None and str(removed) >= removed_at:
            out[b]["prior"] += 1
    return out


def _runs_since(cur: Any, since: str, dealers: set[str]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if not dealers or not _table_exists(cur, "scan_runs"):
        return out
    ids = sorted(dealers)
    for chunk in _chunks(ids):
        for rid, did, name, fin, dur, sj in _rows(
            cur,
            f"SELECT id, dealer_id, dealer_name, finished_at, duration_seconds, summary_json FROM scan_runs "
            f"WHERE dealer_id IN ({','.join('?' * len(chunk))}) AND finished_at >= ? ORDER BY finished_at",
            (*chunk, since),
        ):
            try:
                summary = json.loads(sj or "{}")
            except ValueError:
                summary = {}
            out[did].append({
                "id": int(rid), "name": name or "", "finished_at": str(fin),
                "duration": float(dur or 0), "summary": summary if isinstance(summary, dict) else {},
            })
    return out


def _trigger_run(runs: list[dict[str, Any]], removed_at: str) -> dict[str, Any] | None:
    """The first run that finished at or after the batch stamp (the reconcile runs inside its run)."""
    cut = removed_at[:19]
    for r in runs:
        if r["finished_at"][:19] >= cut:
            return r
    return None


def _seen_by_bucket(cur: Any, dealer_id: str, run: dict[str, Any] | None) -> tuple[dict[str, int], str]:
    """Rows of each bucket the triggering run re-saw: the reconcile plan, else ``cars.scraped_at``."""
    if run is None:
        return {}, "no_run"
    buckets = ((run["summary"].get("reconcile") or {}).get("buckets")) or {}
    if isinstance(buckets, dict) and buckets:
        return {b: int((v or {}).get("matched") or 0) for b, v in buckets.items()}, "reconcile_plan"
    fin = datetime.fromisoformat(run["finished_at"].replace("Z", "+00:00"))
    start = fin.timestamp() - run["duration"]
    lo = datetime.fromtimestamp(start, tz=timezone.utc).replace(tzinfo=None).isoformat() + "Z"
    seen: dict[str, int] = defaultdict(int)
    for (cond,) in _rows(
        cur,
        "SELECT condition FROM cars WHERE TRIM(COALESCE(dealer_id, '')) = ? AND scraped_at >= ?",
        (dealer_id, lo),
    ):
        seen[_bucket(cond)] += 1
    return dict(seen), "cars_scraped_at"


def _replay_incomplete(run: dict[str, Any] | None) -> str:
    if run is None:
        return ""
    from backend.scanner.retirement_policy import _summarize_walks

    replay = run["summary"].get("recipe_replay") or {}
    if not isinstance(replay, dict):
        return ""
    summary = _summarize_walks(replay.get("walks") or replay.get("recipes") or ())
    if summary["incomplete"]:
        return "walk_" + summary["incomplete"][0]
    return "truncated" if replay.get("truncated") else ""


def _dealer_names(dealer_id: str, runs: list[dict[str, Any]]) -> set[str]:
    return {dealer_id, *(r["name"] for r in runs if r["name"])}


def classify(
    conn: Any,
    *,
    since: str,
    excludes: list[tuple[str, str, str]] | None = None,
    scanner_log: str | Path | None = None,
    dealers: set[str] | None = None,
) -> dict[str, Any]:
    """Read-only classification. Returns ``{'batches': [...], 'candidates': [...]}``.

    Never writes and never commits; the caller owns the connection and rolls back.
    """
    excludes = SEEDED_EXCLUDES if excludes is None else excludes
    log = parse_scanner_log(scanner_log)
    cur = conn.cursor()
    rows = _retired_rows(cur, since, dealers)
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        groups[(r["dealer_id"], r["removed_at"], r["writer"])].append(r)
    runs = _runs_since(cur, since, {k[0] for k in groups})

    batches: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    for (dealer_id, removed_at, writer), members in sorted(groups.items()):
        n = len(members)
        buckets = defaultdict(int)
        for m in members:
            buckets[m["bucket"]] += 1
        bucket = max(buckets, key=lambda b: buckets[b])
        batch: dict[str, Any] = {
            "dealer_id": dealer_id, "removed_at": removed_at, "writer": writer or "unknown",
            "rows": n, "bucket": bucket, "prior_lot": "", "share": "", "seen_new": "", "seen_used": "",
            "signatures": "", "status": "audited", "run_id": "", "note": "",
        }
        batches.append(batch)

        why = _excluded(excludes, dealer_id, removed_at)
        if why:
            batch["status"], batch["note"] = "excluded:exclude_list", why
            continue
        if writer in EXCLUDED_WRITERS:
            batch["status"] = f"excluded:{EXCLUDED_WRITERS[writer]}"
            continue
        if writer not in AUDITED_WRITERS:
            batch["status"] = f"excluded:writer_{writer}"
            continue
        names = _dealer_names(dealer_id, runs.get(dealer_id, []))
        if not writer:  # older row: the scanner log is the only provenance
            logged = [
                d for d in log["disown"]
                if d["name"] in names and (not d["day"] or d["day"] == _day(removed_at))
            ]
            if any(d["n"] == n for d in logged):
                batch["status"], batch["note"] = "excluded:disown", "scanner log: belonged to sibling rooftops"
                continue
            if logged and min(d["n"] for d in logged) < n:
                batch["status"] = "excluded:mixed_disown"
                batch["note"] = f"log names {sum(d['n'] for d in logged)} disowned row(s) in a batch of {n}; review by hand"
                continue

        lot = _lot_counts(cur, dealer_id, removed_at)
        prior_total = sum(v["prior"] for v in lot.values())
        run = _trigger_run(runs.get(dealer_id, []), removed_at)
        seen, seen_src = _seen_by_bucket(cur, dealer_id, run)
        other = {"new": "used", "used": "new"}.get(bucket)
        sigs: list[str] = []
        if (
            run is not None and bucket in ("new", "used") and seen.get(bucket, 0) == 0
            and seen.get(other, 0) >= OTHER_BUCKET_MIN
        ):
            sigs.append("one_condition")
        if lot.get(bucket, {"active_now": 0})["active_now"] == 0:
            sigs.append("bucket_now_empty")
        if prior_total and n / prior_total > LARGE_SHARE:
            sigs.append("large_share")
        trunc = _replay_incomplete(run)
        if not trunc:
            trunc = next(
                (t["text"] for t in log["trunc"]
                 if t["name"] in names and (not t["day"] or t["day"] == _day(removed_at))),
                "",
            )
        if trunc:
            sigs.append("truncation_suspect")
        batch.update(
            prior_lot=prior_total, share=f"{n / prior_total:.2f}" if prior_total else "",
            seen_new=seen.get("new", ""), seen_used=seen.get("used", ""), signatures="|".join(sigs),
            run_id=run["id"] if run else "", note=(f"seen via {seen_src}" + (f"; {trunc}" if trunc else "")),
        )
        if sigs:
            for m in members:
                candidates.append({**m, "signatures": "|".join(sigs)})
    return {"batches": batches, "candidates": candidates}


BATCH_FIELDS = ["dealer_id", "removed_at", "writer", "rows", "bucket", "prior_lot", "share",
                "seen_new", "seen_used", "signatures", "status", "run_id", "note"]
CANDIDATE_FIELDS = ["car_id", "vin", "dealer_id", "removed_at", "bucket", "writer", "source_url", "signatures"]


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def write_report(out_dir: Path, result: dict[str, Any], *, since: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    batches, cands = result["batches"], result["candidates"]
    _write_csv(out_dir / "batches.csv", BATCH_FIELDS, batches)
    _write_csv(out_dir / "candidates.csv", CANDIDATE_FIELDS, cands)
    flagged = [b for b in batches if b["signatures"]]
    excluded = [b for b in batches if str(b["status"]).startswith("excluded")]
    sig_n = {s: sum(1 for b in flagged if s in b["signatures"].split("|")) for s in SIGNATURES}
    lines = [
        f"# Retirement audit since {since}",
        "",
        f"- batches: {len(batches)} ({len(flagged)} flagged, {len(excluded)} excluded)",
        f"- candidate rows: {len(cands)} across {len({c['dealer_id'] for c in cands})} dealer(s)",
        "- signatures: " + ", ".join(f"{s} {sig_n[s]}" for s in SIGNATURES),
        "",
        "Read-only. Next: `--verify replay` or `--verify vdp` with `--from` this directory, review "
        "`candidates.csv`, then `--apply`. Per-dealer P0B.2 verdicts live in "
        "`workspace/dealer_logs/<dealer_id>/summary.md`.",
        "",
        "## Flagged batches",
        "",
        "| dealer | removed_at | rows | bucket | prior lot | signatures | note |",
        "| --- | --- | ---: | --- | ---: | --- | --- |",
    ]
    for b in sorted(flagged, key=lambda b: -int(b["rows"])):
        lines.append(
            f"| {b['dealer_id']} | {b['removed_at']} | {b['rows']} | {b['bucket']} | {b['prior_lot']} "
            f"| {b['signatures']} | {b['note']} |"
        )
    if excluded:
        lines += ["", "## Excluded batches", "", "| dealer | removed_at | rows | status | note |", "| --- | --- | ---: | --- | --- |"]
        for b in excluded:
            lines.append(f"| {b['dealer_id']} | {b['removed_at']} | {b['rows']} | {b['status']} | {b['note']} |")
    (out_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


# --------------------------------------------------------------------------
# verify (P5B.6)
# --------------------------------------------------------------------------

_SOLD = re.compile(r"\b(sold|no longer available|vehicle (?:is|has been) (?:sold|unavailable)|unavailable)\b", re.I)
_CHALLENGE = re.compile(r"(captcha|just a moment|access denied|cf-chl|are you a human|verify you are)", re.I)


def judge_vdp(vin: str, source_url: str, status: int | None, final_url: str, body: str) -> tuple[str, str]:
    """The verdict for one VDP fetch: ``live`` / ``gone`` / ``cannot_assess`` and why."""
    if status is None:
        return "cannot_assess", "timeout_or_error"
    if status in (403, 429) or (status >= 500):
        return "cannot_assess", f"http_{status}"
    if status in (404, 410):
        return "gone", f"http_{status}"
    if _CHALLENGE.search(body or ""):
        return "cannot_assess", "challenge"
    fin = urlparse(final_url or source_url)
    src = urlparse(source_url)
    if final_url and final_url != source_url and vin.lower() not in final_url.lower() and fin.path.rstrip("/") != src.path.rstrip("/"):
        return "gone", "redirect_to_listing_page"
    if status == 200 and vin.upper() in (body or "").upper():
        if _SOLD.search(body):
            return "gone", "sold_marker"
        return "live", "vin_in_body"
    return "cannot_assess", f"http_{status}_vin_absent"


def _default_fetch(url: str) -> tuple[int | None, str, str]:
    import urllib.error
    import urllib.request

    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (retirement-audit)"})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:  # noqa: S310 - dealer https URLs from cars.source_url
            return resp.status, resp.geturl(), resp.read(600_000).decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, url, ""
    except Exception:  # noqa: BLE001 - any transport failure is cannot_assess
        return None, url, ""


def dealer_hosts(conn: Any, dealer_ids: Iterable[str]) -> dict[str, str]:
    """``{dealer_id: host}`` from the dealerships site, using the id rule recipes use (dots become dashes)."""
    wanted = set(dealer_ids)
    out: dict[str, str] = {}
    cur = conn.cursor()
    for (url,) in _rows(
        cur, "SELECT COALESCE(website_url, dealer_website_url) FROM dealerships "
             "WHERE COALESCE(website_url, dealer_website_url) IS NOT NULL"
    ):
        h = _host(url)
        did = h.replace(".", "-")
        if did in wanted:
            out[did] = h
    return out


def verify_vdp(
    candidates: list[dict[str, Any]],
    hosts: dict[str, str],
    *,
    fetch: Callable[[str], tuple[int | None, str, str]] = _default_fetch,
    pace: float = 1.5,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, dict[str, Any]]:
    """Paced GET of each candidate's ``source_url``. A host other than the dealer's is cannot_assess."""
    out: dict[str, dict[str, Any]] = {}
    for c in candidates:
        key = str(c["car_id"])
        url = str(c.get("source_url") or "")
        want = hosts.get(c["dealer_id"], "")
        if not url or not want or _host(url) != want:
            out[key] = {"verdict": "cannot_assess", "why": "host_mismatch" if url and want else "no_source_url_or_host",
                        "method": "vdp"}
            continue
        status, final, body = fetch(url)
        if final and _host(final) != want:  # a redirect to another site (group sites answer for siblings)
            out[key] = {"verdict": "cannot_assess", "why": "redirected_off_host", "method": "vdp"}
        else:
            v, why = judge_vdp(str(c["vin"]), url, status, final, body)
            out[key] = {"verdict": v, "why": why, "method": "vdp"}
        sleep(pace)
    return out


def _default_replay(dealer_id: str, base_url: str) -> tuple[set[str], bool]:
    import asyncio

    from backend.scanner.recipes import try_fetch_via_recipes

    walk: dict[str, Any] = {}
    hit = asyncio.run(try_fetch_via_recipes(dealer_id, "", base_url, dealer_id, union=True, replay_out=walk))
    if not hit:
        return set(), False
    vins: set[str] = set()
    for _url, body in hit[0]:
        text = body if isinstance(body, str) else json.dumps(body, default=str)
        vins.update(_VIN_RE.findall(text.upper()))
    from backend.scanner.retirement_policy import _summarize_walks

    summ = _summarize_walks(walk.get("walks") or walk.get("recipes") or ())
    return vins, bool(summ["live"]) and not summ["incomplete"] and not walk.get("truncated")


def verify_replay(
    candidates: list[dict[str, Any]],
    hosts: dict[str, str],
    *,
    replay: Callable[[str, str], tuple[set[str], bool]] = _default_replay,
) -> dict[str, dict[str, Any]]:
    """Union recipe replay over HTTP, once per dealer (nothing persisted).

    A candidate VIN in the replay is live. One missing from a COMPLETE replay is gone; from an
    incomplete or empty one, cannot_assess.
    """
    out: dict[str, dict[str, Any]] = {}
    by_dealer: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for c in candidates:
        by_dealer[c["dealer_id"]].append(c)
    for did, cs in by_dealer.items():
        host = hosts.get(did)
        try:
            vins, complete = replay(did, f"https://{host}") if host else (set(), False)
        except Exception as exc:  # noqa: BLE001 - one dealer's failure never stops the audit
            logger.warning("replay %s failed: %s", did, exc)
            vins, complete = set(), False
        for c in cs:
            vin = str(c["vin"] or "").strip().upper()
            if vin in vins:
                v, why = "live", "in_replay"
            elif complete and vins:
                v, why = "gone", "absent_from_complete_replay"
            else:
                v, why = "cannot_assess", "replay_incomplete_or_empty"
            out[str(c["car_id"])] = {"verdict": v, "why": why, "method": "replay"}
    return out


def read_candidates(audit_dir: Path) -> list[dict[str, Any]]:
    with (audit_dir / "candidates.csv").open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    for r in rows:
        r["car_id"] = int(r["car_id"])
    return rows


# --------------------------------------------------------------------------
# apply and rollback (P5B.6)
# --------------------------------------------------------------------------

def apply_restore(
    conn: Any,
    audit_dir: Path,
    *,
    backup_dir: Path | None = None,
    stamp: str | None = None,
    log_dir_writer: Callable[[list[dict[str, Any]], str, str], None] | None = None,
) -> dict[str, Any]:
    """Restore the verified-live ids; return the counts and the backup path.

    Needs ``verify.json`` in *audit_dir*. Each id is restored only when it is still retired with
    exactly the batch stamp and dealer the audit saw, so a row re-retired, restored or moved
    since is skipped. The backup is written before the first UPDATE.
    """
    from backend.db.retirement_log import record_retirements

    vfile = audit_dir / "verify.json"
    if not vfile.exists():
        raise SystemExit(f"--apply needs {vfile} (run --verify first)")
    verdicts = json.loads(vfile.read_text(encoding="utf-8"))
    live = [c for c in read_candidates(audit_dir) if verdicts.get(str(c["car_id"]), {}).get("verdict") == "live"]
    stamp = stamp or _stamp()
    cur = conn.cursor()

    # Snapshot exactly the rows the guarded UPDATE can touch, before any write.
    snapshot: list[dict[str, Any]] = []
    for c in live:
        got = _rows(
            cur,
            "SELECT id, vin, TRIM(COALESCE(dealer_id, '')), listing_removed_at FROM cars "
            "WHERE id = ? AND COALESCE(listing_active, 1) = 0 AND listing_removed_at = ? "
            "AND TRIM(COALESCE(dealer_id, '')) = ?",
            (c["car_id"], c["removed_at"], c["dealer_id"]),
        )
        if got:
            snapshot.append({"id": int(got[0][0]), "vin": got[0][1], "dealer_id": got[0][2],
                             "listing_removed_at": got[0][3], "listing_active": 0})
    backup_dir = backup_dir or BACKUP_ROOT
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup = backup_dir / f"retire_restore_{stamp}.json"
    backup.write_text(json.dumps({"stamp": stamp, "audit_dir": str(audit_dir), "rows": snapshot}, indent=1), encoding="utf-8")

    restored: list[dict[str, Any]] = []
    for row in snapshot:
        cur.execute(
            "UPDATE cars SET listing_active = 1, listing_removed_at = NULL "
            "WHERE id = ? AND listing_active = 0 AND listing_removed_at = ? "
            "AND TRIM(COALESCE(dealer_id, '')) = ? RETURNING id, vin",
            (row["id"], row["listing_removed_at"], row["dealer_id"]),
        )
        got = cur.fetchall()
        if got:
            restored.append(row)
            record_retirements(
                cur, [(int(got[0][0]), got[0][1], row["dealer_id"])],
                writer="restore", reason="audit_retirements_verified_live",
                evidence={"stamp": stamp, "method": verdicts[str(row["id"])].get("method")},
            )
    conn.commit()
    (log_dir_writer or write_dealer_logs)(restored, stamp, "restore")
    return {"live": len(live), "snapshotted": len(snapshot), "restored": len(restored),
            "skipped": len(live) - len(restored), "backup": str(backup)}


def rollback(conn: Any, backup_file: Path) -> dict[str, int]:
    """Re-retire the backup's ids that are still in the restored state (active, stamp NULL)."""
    from backend.db.retirement_log import record_retirements

    rows = json.loads(Path(backup_file).read_text(encoding="utf-8"))["rows"]
    cur = conn.cursor()
    done = 0
    for row in rows:
        cur.execute(
            "UPDATE cars SET listing_active = 0, listing_removed_at = ? "
            "WHERE id = ? AND COALESCE(listing_active, 1) = 1 AND listing_removed_at IS NULL "
            "AND TRIM(COALESCE(dealer_id, '')) = ? RETURNING id, vin",
            (row["listing_removed_at"], row["id"], row["dealer_id"]),
        )
        got = cur.fetchall()
        if got:
            done += 1
            record_retirements(
                cur, [(int(got[0][0]), got[0][1], row["dealer_id"])],
                writer="restore_rollback", reason="audit_retirements_rollback",
            )
    conn.commit()
    return {"rows": len(rows), "re_retired": done, "skipped": len(rows) - done}


def write_dealer_logs(restored: list[dict[str, Any]], stamp: str, action: str) -> None:
    """Dated blocks in each dealer's scan_runs.md and summary.md, and one ``_learning`` line."""
    from backend.scanner.pipeline.dealer_logs import _log_append
    from backend.scanner.pipeline.lifecycle import _learning_append

    by_dealer: dict[str, int] = defaultdict(int)
    for r in restored:
        by_dealer[r["dealer_id"]] += 1
    for did, n in sorted(by_dealer.items()):
        block = (
            f"## {stamp} retirement audit: {action}\n\n"
            f"- audit_retirements restored {n} row(s) verified live; backup workspace/backups/"
            f"retire_restore_{stamp}.json (rollback: `--rollback`)."
        )
        _log_append(did, "scan_runs.md", block)
        _log_append(did, "summary.md", block)
    if by_dealer:
        _learning_append(
            "errors_index.md",
            f"- {stamp} wrongly_retired_restored:{sum(by_dealer.values())} rows in {len(by_dealer)} dealer(s) "
            f"(audit_retirements --apply; rollback retire_restore_{stamp}.json)",
        )


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

class ReadOnlyConnection:
    """Proxy that refuses ``commit`` and rolls back on close: the classify pass cannot write."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def cursor(self, *a: Any, **k: Any) -> Any:
        return self._conn.cursor(*a, **k)

    def commit(self) -> None:
        raise RuntimeError("audit_retirements classify is read-only; commit refused")

    def close(self) -> None:
        try:
            self._conn.rollback()
        finally:
            self._conn.close()


def _open() -> Any:
    from backend.db.repositories.base_repo import get_conn

    return get_conn()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--since", default="2026-09-23", help="retirements stamped on/after this date (default 2026-09-23)")
    ap.add_argument("--dealers", help="comma-separated dealer ids to limit the audit to")
    ap.add_argument("--exclude-file", help="extra excludes: dealer_id,YYYY-MM-DD[,reason] per line")
    ap.add_argument("--scanner-log", help="a scanner.log for disown / truncation evidence on older rows")
    ap.add_argument("--out-dir", help="classify output directory (default workspace/data_quality/retirements_<stamp>)")
    ap.add_argument("--from", dest="from_dir", help="an audit directory (candidates.csv, verify.json)")
    ap.add_argument("--verify", choices=("replay", "vdp"), help="verify the candidates in --from")
    ap.add_argument("--pace", type=float, default=1.5, help="seconds between VDP fetches")
    ap.add_argument("--apply", action="store_true", help="restore the verified-live rows of --from (writes)")
    ap.add_argument("--rollback", metavar="BACKUP", help="re-retire the ids of a retire_restore backup (writes)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.rollback:
        conn = _open()
        try:
            print(json.dumps(rollback(conn, Path(args.rollback))))
        finally:
            conn.close()
        return 0

    if args.verify or args.apply:
        if not args.from_dir:
            ap.error("--verify and --apply need --from DIR")
        audit_dir = Path(args.from_dir)
        conn = _open()
        try:
            if args.apply:
                print(json.dumps(apply_restore(conn, audit_dir)))
                return 0
            cands = read_candidates(audit_dir)
            hosts = dealer_hosts(conn, {c["dealer_id"] for c in cands})
            verdicts = (verify_replay(cands, hosts) if args.verify == "replay"
                        else verify_vdp(cands, hosts, pace=args.pace))
            (audit_dir / "verify.json").write_text(json.dumps(verdicts, indent=1), encoding="utf-8")
            tally: dict[str, int] = defaultdict(int)
            for v in verdicts.values():
                tally[v["verdict"]] += 1
            print(json.dumps(dict(tally)))
        finally:
            conn.close()
        return 0

    conn = ReadOnlyConnection(_open())
    try:
        result = classify(
            conn, since=args.since,
            excludes=load_excludes(args.exclude_file), scanner_log=args.scanner_log,
            dealers=set(filter(None, (args.dealers or "").split(","))) or None,
        )
    finally:
        conn.close()
    out_dir = Path(args.out_dir) if args.out_dir else QUALITY_ROOT / f"retirements_{_stamp()}"
    write_report(out_dir, result, since=args.since)
    print(f"{len(result['batches'])} batch(es), {len(result['candidates'])} candidate row(s) -> {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
