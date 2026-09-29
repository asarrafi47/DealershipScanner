#!/usr/bin/env python3
"""Sharded HTTP-only fleet scan + the nightly post steps, in one process tree.

This is the Railway scanner job (docs/RAILWAY_SCANNING.md), and it runs the same
way on any machine:

  1. roster   SCAN_DEALERS (comma list, never filtered) or, with SCAN_FLEET=1,
              dealers with ACTIVE inventory minus do_not_scan
              (workspace/pipeline/do_not_scan.txt + deploy/railway/do_not_scan.txt),
              plus recipe-only dealers listed in deploy/railway/revived_dealers.txt.
              Recipe-only dealers are otherwise excluded (2026-09-29 incident:
              they replayed group feeds and stole 6,961 siblings' VINs).
  2. shards   SCAN_SHARDS (default 8) `backend.scripts.dealer_pipeline` processes
              in parallel, disjoint rosters balanced by active row count, each
              with its own SCANNER_LOCK_PATH and `--batch SCAN_BATCH` (default 6).
              vPIC heal, assess, reconcile, lifecycle and per-dealer logs run
              inside each pipeline.
  3. post     compute_market_stats, then build_listings_grid_cards with
              FLASK_ENV=production (listings_include_incomplete_cars() is part of
              the card key, so cards must be built the way the web serves them)
  4. summary  one `FLEET SUMMARY {...}` JSON line on stdout (dealers, verdict
              counts, 403/429 tallies by host, duration, peak memory, CPU) and
              <run>/fleet_summary.json; exit 0 when every shard and post step
              exited 0, else 1.

With neither SCAN_DEALERS nor SCAN_FLEET=1 it prints `idle` and exits 0, so a
deploy of the scanner service never starts a fleet scan by itself.

Env:
  SCAN_FLEET=1            scan the fleet roster (active inventory, see step 1)
  SCAN_DEALERS=a,b,c      scan only these (wins over SCAN_FLEET)
  SCAN_SHARDS=8           parallel pipeline processes
  SCAN_BATCH=6            dealers per scanner process inside a shard
  SCAN_POST_STEPS=1       0 skips compute_market_stats + build_listings_grid_cards
  SCAN_PIPELINE_ARGS=...  extra dealer_pipeline flags (e.g. "--no-lifecycle")
  SCAN_OUT_ROOT           run directory parent (default workspace/pipeline)
  SCAN_KEEP_DAYS=14       prune run dirs / scan logs older than this
  SCAN_IDLE_HOLD_SECONDS  when idle, stay up this long so the /data volume can be read

Usage:
  SCAN_DEALERS=a,b SCAN_SHARDS=2 python -m backend.scripts.fleet_scan
  SCAN_FLEET=1 python -m backend.scripts.fleet_scan
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

ROOT = Path(__file__).resolve().parents[2]

# "… HTTP 403", "(last status 403)", "→ HTTP 429", "status=403", "returned 403"
_STATUS_RE = re.compile(r"(?:HTTP|status[ =:]*|returned|→)\s*(403|429)\b", re.I)
_URL_HOST_RE = re.compile(r"https?://([^/\s\"')]+)")
_FOR_HOST_RE = re.compile(r"exhausted for ([^\s(]+)")
_LABEL_RE = re.compile(r"(\w+) \[([^\]]+)\]:")  # "scanner: ShopperExpress [Dealer Name]"
_CHALLENGE_RE = re.compile(r"cloudflare|cf-ray|cf_chl|just a moment|attention required", re.I)


def _env_int(name: str, default: int) -> int:
    try:
        return int((os.environ.get(name) or "").strip() or default)
    except ValueError:
        return default


# --------------------------------------------------------------------------
# roster + sharding
# --------------------------------------------------------------------------

ROSTER_REPORT: dict[str, Any] = {}


def load_roster() -> list[str]:
    """SCAN_DEALERS (explicit, never filtered) or, with SCAN_FLEET=1, dealers with
    active inventory minus do-not-scan, plus revived recipe-only dealers.

    The 2026-09-29 Railway run rostered active inventory ∪ stored recipes and
    reassigned 6,961 VINs to the wrong dealer: 136 recipe-only dealers (most
    emptied on purpose because their recipe replays a whole group feed) claimed
    siblings' cars. See manifest.apply_scannable_roster_rule. The rule is always
    on here; SCANNABLE_ROSTER_RULE=0 does not bypass it — name dealers in
    SCAN_DEALERS instead.
    """
    ROSTER_REPORT.clear()
    raw = (os.environ.get("SCAN_DEALERS") or "").strip()
    if raw:
        return [d.strip() for d in raw.split(",") if d.strip()]
    if (os.environ.get("SCAN_FLEET") or "").strip().lower() in ("1", "true", "yes"):
        from backend.scanner.manifest import _load_scannable_dealers, apply_scannable_roster_rule

        skip = ("carmax.com", "carmax-com")
        dealers = [d for d in _load_scannable_dealers()
                   if d.get("dealer_id") and not any(s in d["dealer_id"] for s in skip)]
        kept, report = apply_scannable_roster_rule(dealers)
        ROSTER_REPORT.update(report)
        print(f"fleet   roster: {report['kept']} dealers kept; excluded "
              f"{len(report['excluded_do_not_scan'])} do-not-scan, "
              f"{len(report['excluded_recipe_only'])} recipe-only (no active cars, not in "
              f"deploy/railway/revived_dealers.txt); {len(report['revived_recipe_only'])} revived",
              flush=True)
        if report["excluded_do_not_scan"]:
            print("fleet   do-not-scan: " + ", ".join(report["excluded_do_not_scan"]), flush=True)
        if report["revived_recipe_only"]:
            print("fleet   revived: " + ", ".join(report["revived_recipe_only"]), flush=True)
        return sorted({d["dealer_id"] for d in kept})
    return []


def active_rows(dealer_ids: list[str]) -> dict[str, int]:
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT dealer_id, COUNT(*) FROM cars WHERE COALESCE(listing_active,1)=1 AND dealer_id IN ("
            + ",".join("?" * len(dealer_ids)) + ") GROUP BY dealer_id",
            tuple(dealer_ids),
        )
        return {str(r[0]): int(r[1]) for r in cur.fetchall()}
    finally:
        conn.close()


def make_shards(dealer_ids: list[str], n: int, weights: dict[str, int]) -> list[list[str]]:
    """Longest-processing-time-first: biggest lots first, each onto the lightest shard.
    Scan time tracks row count (VDP prefetch per car), so shards finish together."""
    n = max(1, min(n, len(dealer_ids)))
    shards: list[list[str]] = [[] for _ in range(n)]
    load = [0] * n
    for did in sorted(dealer_ids, key=lambda d: (-weights.get(d, 50), d)):
        i = min(range(n), key=lambda k: load[k])
        shards[i].append(did)
        load[i] += max(20, weights.get(did, 50))
    return [s for s in shards if s]


# --------------------------------------------------------------------------
# resource sampler (cgroup v2 when present, else /proc)
# --------------------------------------------------------------------------

class ResourceSampler(threading.Thread):
    def __init__(self, interval: float = 10.0) -> None:
        super().__init__(daemon=True)
        self.interval = interval
        self.stop = threading.Event()
        self.peak_mem = 0
        self.samples: list[tuple[float, float, int]] = []  # (t, cpu_seconds, mem_bytes)

    @staticmethod
    def _cpu_seconds() -> float | None:
        try:
            for line in Path("/sys/fs/cgroup/cpu.stat").read_text().splitlines():
                if line.startswith("usage_usec"):
                    return int(line.split()[1]) / 1e6
        except OSError:
            pass
        try:  # whole-machine fallback (local runs)
            parts = Path("/proc/stat").read_text().splitlines()[0].split()[1:]
            vals = [int(x) for x in parts]
            busy = sum(vals) - vals[3] - (vals[4] if len(vals) > 4 else 0)
            return busy / os.sysconf("SC_CLK_TCK")
        except (OSError, ValueError, IndexError):
            return None

    @staticmethod
    def _mem_bytes() -> int | None:
        for p in ("/sys/fs/cgroup/memory.current",):
            try:
                return int(Path(p).read_text().strip())
            except (OSError, ValueError):
                pass
        try:
            info = dict(ln.split(":", 1) for ln in Path("/proc/meminfo").read_text().splitlines())
            return (int(info["MemTotal"].split()[0]) - int(info["MemAvailable"].split()[0])) * 1024
        except (OSError, KeyError, ValueError):
            return None

    def run(self) -> None:
        while not self.stop.is_set():
            cpu, mem = self._cpu_seconds(), self._mem_bytes()
            if cpu is not None and mem is not None:
                self.samples.append((time.time(), cpu, mem))
                self.peak_mem = max(self.peak_mem, mem)
            self.stop.wait(self.interval)

    def summary(self) -> dict[str, Any]:
        out: dict[str, Any] = {"samples": len(self.samples)}
        try:  # cgroup's own high-water mark beats our 10 s sampling
            out["peak_mem_mb"] = round(max(self.peak_mem, int(Path("/sys/fs/cgroup/memory.peak").read_text().strip())) / 2**20)
        except (OSError, ValueError):
            out["peak_mem_mb"] = round(self.peak_mem / 2**20)
        if len(self.samples) >= 2:
            (t0, c0, _), (t1, c1, _) = self.samples[0], self.samples[-1]
            out["avg_cores"] = round((c1 - c0) / max(1.0, t1 - t0), 2)
            out["cpu_core_hours"] = round((c1 - c0) / 3600, 3)
            peak = 0.0
            for (ta, ca, _), (tb, cb, _) in zip(self.samples, self.samples[1:]):
                if tb - ta >= 5:
                    peak = max(peak, (cb - ca) / (tb - ta))
            out["peak_cores"] = round(peak, 2)
            out["avg_mem_mb"] = round(sum(m for _, _, m in self.samples) / len(self.samples) / 2**20)
        return out


# --------------------------------------------------------------------------
# log tallies
# --------------------------------------------------------------------------

def tally_http(log_paths: list[Path]) -> dict[str, Any]:
    by_status: Counter[str] = Counter()
    by_host: Counter[str] = Counter()
    challenge = 0
    for p in log_paths:
        try:
            fh = p.open(errors="ignore")
        except OSError:
            continue
        with fh:
            for line in fh:
                m = _STATUS_RE.search(line)
                if not m:
                    continue
                status = m.group(1)
                host_m = _URL_HOST_RE.search(line) or _FOR_HOST_RE.search(line)
                if host_m:
                    host = host_m.group(1).lower()
                else:
                    lab = _LABEL_RE.search(line)
                    host = f"{lab.group(1)}:{lab.group(2)}" if lab else "?"
                by_status[status] += 1
                by_host[f"{status} {host}"] += 1
                if _CHALLENGE_RE.search(line):
                    challenge += 1
    carscommerce = sum(v for k, v in by_host.items() if "carscommerce" in k)
    return {"by_status": dict(by_status), "top_hosts": dict(by_host.most_common(25)),
            "carscommerce_api": carscommerce, "cloudflare_marked_lines": challenge}


def http_first_statuses(results: list[dict[str, Any]]) -> dict[str, int]:
    c: Counter[str] = Counter()
    for r in results:
        for k, v in ((r.get("http_first") or {}).get("statuses") or {}).items():
            c[str(k)] += int(v or 0)
    return dict(c)


# --------------------------------------------------------------------------
# run
# --------------------------------------------------------------------------

def _pump(proc: subprocess.Popen, tag: str, sink: Path) -> None:
    with sink.open("a", encoding="utf-8") as fh:
        for raw in iter(proc.stdout.readline, b""):  # type: ignore[union-attr]
            line = raw.decode("utf-8", "replace").rstrip()
            fh.write(line + "\n")
            print(f"[{tag}] {line}", flush=True)


def run_step(tag: str, cmd: list[str], env: dict[str, str], sink: Path) -> tuple[int, float]:
    t0 = time.time()
    proc = subprocess.Popen(cmd, cwd=str(ROOT), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    _pump(proc, tag, sink)
    rc = proc.wait()
    return rc, round(time.time() - t0, 1)


def prune(out_root: Path, keep_days: int) -> None:
    cutoff = time.time() - keep_days * 86400
    for parent, pattern in ((out_root, "fleet_*"), (ROOT / "workspace" / "scanlogs", "scan_*.jsonl")):
        if not parent.is_dir():
            continue
        for p in parent.glob(pattern):
            try:
                if p.stat().st_mtime < cutoff:
                    shutil.rmtree(p) if p.is_dir() else p.unlink()
            except OSError:
                pass


def main() -> int:
    started = datetime.now(timezone.utc).replace(microsecond=0)
    t_start = time.time()
    roster = load_roster()
    if not roster:
        print("fleet   idle: set SCAN_FLEET=1 (whole roster) or SCAN_DEALERS=a,b,c to scan", flush=True)
        hold = _env_int("SCAN_IDLE_HOLD_SECONDS", 0)
        if hold > 0:
            # Keep the container (and its /data volume) up so `railway ssh` /
            # `railway volume files download` can read the logs; a stopped
            # Railway service exposes no volume.
            print(f"fleet   holding {hold}s for log retrieval (SCAN_IDLE_HOLD_SECONDS)", flush=True)
            time.sleep(hold)
        return 0

    out_root = Path(os.environ.get("SCAN_OUT_ROOT") or (ROOT / "workspace" / "pipeline"))
    keep_days = _env_int("SCAN_KEEP_DAYS", 14)
    prune(out_root, keep_days)
    run_dir = out_root / f"fleet_{started.strftime('%Y%m%dT%H%M%SZ')}"
    run_dir.mkdir(parents=True, exist_ok=True)
    if ROSTER_REPORT:
        (run_dir / "roster_rule.json").write_text(json.dumps(ROSTER_REPORT, indent=1))

    n_shards = _env_int("SCAN_SHARDS", 8)
    batch = _env_int("SCAN_BATCH", 6)
    weights = active_rows(roster)
    shards = make_shards(roster, n_shards, weights)
    extra = (os.environ.get("SCAN_PIPELINE_ARGS") or "").split()
    py = sys.executable
    print(f"fleet   {len(roster)} dealers in {len(shards)} shard(s), batch {batch}, run {run_dir}", flush=True)

    sampler = ResourceSampler()
    sampler.start()

    threads: list[threading.Thread] = []
    rcs: dict[str, int] = {}
    shard_secs: dict[str, float] = {}
    for i, ids in enumerate(shards):
        tag = f"s{i}"
        (run_dir / f"{tag}.txt").write_text("\n".join(ids) + "\n")
        env = dict(os.environ)
        env["SCANNER_LOCK_PATH"] = str(run_dir / f"{tag}.lock")
        cmd = [py, "-m", "backend.scripts.dealer_pipeline", "--dealers", ",".join(ids),
               "--out", str(run_dir / tag), "--batch", str(batch), *extra]

        def _run(tag: str = tag, cmd: list[str] = cmd, env: dict[str, str] = env) -> None:
            rc, secs = run_step(tag, cmd, env, run_dir / f"{tag}.out")
            rcs[tag], shard_secs[tag] = rc, secs
            print(f"fleet   shard {tag} rc={rc} in {secs:.0f}s", flush=True)

        th = threading.Thread(target=_run, daemon=True)
        th.start()
        threads.append(th)
        time.sleep(2)  # stagger the first DB connections
    for th in threads:
        th.join()
    scan_secs = round(time.time() - t_start, 1)
    scan_resources = sampler.summary()

    post: dict[str, dict[str, Any]] = {}
    if (os.environ.get("SCAN_POST_STEPS") or "1").strip() != "0":
        env = dict(os.environ)
        rc, secs = run_step("market", [py, "-m", "backend.scripts.compute_market_stats"], env, run_dir / "post.out")
        post["compute_market_stats"] = {"rc": rc, "seconds": secs}
        env = dict(os.environ)
        env["FLASK_ENV"] = "production"  # card key includes listings_include_incomplete_cars()
        rc, secs = run_step("cards", [py, "-m", "backend.scripts.build_listings_grid_cards"], env, run_dir / "post.out")
        post["build_listings_grid_cards"] = {"rc": rc, "seconds": secs}
    sampler.stop.set()

    results: list[dict[str, Any]] = []
    for i in range(len(shards)):
        tri = run_dir / f"s{i}" / "triage.json"
        try:
            results.extend(json.loads(tri.read_text()).get("dealers") or [])
        except (OSError, ValueError):
            print(f"fleet   shard s{i}: no triage.json", flush=True)
    verdicts = Counter(str(r.get("verdict")) for r in results)
    logs = sorted(run_dir.glob("s*/scanner.log")) + sorted(run_dir.glob("s*.out"))
    summary = {
        "started": started.isoformat(),
        "run_dir": str(run_dir),
        "dealers": len(roster),
        "roster_rule": {k: (len(v) if isinstance(v, list) else v) for k, v in ROSTER_REPORT.items()},
        "assessed": len(results),
        "shards": len(shards),
        "batch": batch,
        "verdicts": dict(verdicts),
        "rows": sum(int(r.get("rows") or 0) for r in results),
        "vin_owner_conflicts": sum(int(r.get("vin_owner_conflicts") or 0) for r in results),
        "http_403_429": tally_http(logs),
        "http_first_statuses": http_first_statuses(results),
        "scan_seconds": scan_secs,
        "total_seconds": round(time.time() - t_start, 1),
        "shard_rc": rcs,
        "shard_seconds": shard_secs,
        "post": post,
        "resources_scan": scan_resources,
        "resources_total": sampler.summary(),
    }
    ok = all(rc == 0 for rc in rcs.values()) and len(rcs) == len(shards) and all(p["rc"] == 0 for p in post.values())
    summary["exit"] = 0 if ok else 1
    (run_dir / "fleet_summary.json").write_text(json.dumps(summary, indent=1, default=str))
    print("FLEET SUMMARY " + json.dumps(summary, default=str), flush=True)
    h = summary["http_403_429"]["by_status"]
    print(f"fleet   done: {len(roster)} dealers, verdicts {dict(verdicts)}, 403={h.get('403', 0)} 429={h.get('429', 0)}, "
          f"{summary['total_seconds'] / 60:.1f} min, peak {summary['resources_total'].get('peak_mem_mb')} MB, "
          f"avg {scan_resources.get('avg_cores')} cores, exit {summary['exit']}", flush=True)
    return summary["exit"]


if __name__ == "__main__":
    sys.exit(main())
