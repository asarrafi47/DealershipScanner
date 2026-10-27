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
  3. post     rebuild_incomplete_index, compute_market_stats, then build_listings_grid_cards with
              FLASK_ENV=production (listings_include_incomplete_cars() is part of
              the card key, so cards must be built the way the web serves them)
  4. summary  one `FLEET SUMMARY {...}` JSON line on stdout (dealers, verdict
              counts, 403/429 tallies by host, duration, peak memory, CPU) and
              <run>/fleet_summary.json; exit 0 when every shard and post step
              exited 0, else 1.

With neither SCAN_DEALERS nor SCAN_FLEET=1 it prints `idle` and exits 0, so a
deploy of the scanner service never starts a fleet scan by itself. The same holds
for a whole-fleet run outside SCAN_FLEET_WINDOW_UTC (P6A.2): a redeploy at 08:35
must not start the 09:00 run's worth of scanning, so it idles instead.

Env:
  SCAN_FLEET=1            scan the fleet roster (active inventory, see step 1)
  SCAN_DEALERS=a,b,c      scan only these (wins over SCAN_FLEET)
  SCAN_SHARDS=8           parallel pipeline processes
  SCAN_SHARD_ORDER        reverse | seed:<n>  (determinism passes; recorded in the summary)
  SCAN_BATCH=6            dealers per scanner process inside a shard
  SCAN_POST_STEPS=1       0 skips the post steps; 1 (default) = index,market,cards; or a comma
                          list of index,market,recover,harvest,cards (run in that order, cards
                          last). recover = heal_from_recipes and harvest = harvest_html_jsonld,
                          both aimed at roster dealers with open index codes (cap
                          SCAN_RECOVER_MAX_DEALERS=25); harvest needs SCAN_LANE=home. An unknown
                          step exits 2 before any scan. Default 0 when SCAN_LANE=home.
  SCAN_LANE               all | railway | home (P15A.1). Default railway when SCANNER_EGRESS_TAG
                          is set, else all. railway drops the ids in deploy/railway/
                          home_lane_dealers.txt (excluded_home_lane); home scans only them
                          (minus do-not-scan); all is today's roster (the kill switch)
  SCAN_PIPELINE_ARGS=...  extra dealer_pipeline flags (e.g. "--no-lifecycle")
  SCAN_OUT_ROOT           run directory parent (default workspace/pipeline)
  SCAN_KEEP_DAYS=14       prune run dirs / scan logs older than this
  SCAN_FLEET_WINDOW_UTC='HH:MM-HH:MM'
                          with SCAN_FLEET=1, scan only inside this UTC window (wraps past
                          midnight); outside it, or with an invalid value (WARN), idle.
                          Unset: no window. SCAN_DEALERS and --resume ignore the window
                          (SCAN_DEALERS left set during a window run is a WARN in the summary).
  SCAN_FLEET_LOCK=1       0 turns the whole-fleet lease off. A whole-fleet run takes the
                          Postgres advisory lease of SCAN_FLEET_LANE (default "fleet") and
                          exits 3, naming the holder, before spawning a shard if it is held
  SCAN_IDLE_HOLD_SECONDS  when idle, stay up this long so the /data volume can be read; with a
                          valid window the hold ends 5 minutes before the window opens
  SCAN_POST_RUN_HOLD_SECONDS
                          after a run, stay up this long (same cap) so the volume logs can be
                          pulled even if a cron-scheduled service does not start on a redeploy
  SCAN_RESUME_DIR         resume this run dir (same as `--resume <run_dir>`, N9.15): reuse its
                          shard rosters, skip dealers with a triage entry or a scan_runs row
                          finished since the run start, append to each shard's triage.json

Usage:
  SCAN_DEALERS=a,b SCAN_SHARDS=2 python -m backend.scripts.fleet_scan
  python -m backend.scripts.fleet_scan --resume workspace/pipeline/fleet_<stamp>
  SCAN_FLEET=1 python -m backend.scripts.fleet_scan
"""
from __future__ import annotations

import json
import os
import random
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

from backend.scanner import scan_leases
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
# run window + fleet lease (P6A.2)
# --------------------------------------------------------------------------

EXIT_FLEET_LOCK_HELD = 3
HOLD_MARGIN_SECONDS = 300  # a hold must end this long before the window opens
_WINDOW_RE = re.compile(r"^\s*(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})\s*$")

# What this run decided (lane, window, lock, warnings); written into fleet_summary.json.
RUN_META: dict[str, Any] = {}


def parse_window(raw: str) -> tuple[int, int]:
    """``'HH:MM-HH:MM'`` -> (start, end) in minutes of the UTC day. start > end wraps midnight.
    Raises ValueError for anything else, including an empty window (start == end)."""
    m = _WINDOW_RE.match(raw or "")
    if not m:
        raise ValueError(f"expected HH:MM-HH:MM, got {raw!r}")
    h1, m1, h2, m2 = (int(x) for x in m.groups())
    if h1 > 23 or h2 > 23 or m1 > 59 or m2 > 59:
        raise ValueError(f"time out of range in {raw!r}")
    start, end = h1 * 60 + m1, h2 * 60 + m2
    if start == end:
        raise ValueError(f"empty window {raw!r}")
    return start, end


def in_window(now: datetime, window: tuple[int, int]) -> bool:
    minute = now.hour * 60 + now.minute
    start, end = window
    return start <= minute < end if start < end else (minute >= start or minute < end)


def seconds_until_open(now: datetime, window: tuple[int, int]) -> float:
    """Seconds from *now* to the next time the window opens (a full day if it opens right now)."""
    since_midnight = now.hour * 3600 + now.minute * 60 + now.second + now.microsecond / 1e6
    delta = (window[0] * 60 - since_midnight) % 86400
    return delta if delta > 0 else 86400.0


def capped_hold(hold: int, now: datetime, window: tuple[int, int] | None) -> int:
    """Shorten an idle/post-run hold so it ends HOLD_MARGIN_SECONDS before the window opens.
    Railway skips a cron fire while the previous run is still up, so a hold that spans the
    fire would swallow the whole run. No valid window: the hold is unchanged."""
    if window is None or hold <= 0:
        return max(0, hold)
    return max(0, min(hold, int(seconds_until_open(now, window) - HOLD_MARGIN_SECONDS)))


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _hold(kind: str, env_name: str, window: tuple[int, int] | None) -> None:
    hold = _env_int(env_name, 0)
    if hold <= 0:
        return
    capped = capped_hold(hold, _utcnow(), window)
    if capped <= 0:
        print(f"fleet   not holding ({env_name}={hold}): the run window opens within "
              f"{HOLD_MARGIN_SECONDS // 60} minutes", flush=True)
        return
    # Keep the container (and its /data volume) up so `railway ssh` /
    # `railway volume files download` can read the logs; a stopped
    # Railway service exposes no volume.
    capped_note = f" (capped from {hold}s to end before the window opens)" if capped < hold else ""
    print(f"fleet   holding {capped}s for log retrieval, {kind} ({env_name}){capped_note}", flush=True)
    time.sleep(capped)


def _truthy(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in ("1", "true", "yes")


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
        from backend.scanner.manifest import _load_scannable_dealers, apply_scannable_roster_rule, scan_lane

        skip = ("carmax.com", "carmax-com")
        dealers = [d for d in _load_scannable_dealers()
                   if d.get("dealer_id") and not any(s in d["dealer_id"] for s in skip)]
        try:
            lane = scan_lane()
        except ValueError as exc:
            raise SystemExit(f"fleet   {exc}")
        kept, report = apply_scannable_roster_rule(dealers, lane=lane)
        ROSTER_REPORT.update(report)
        print(f"fleet   roster: {report['kept']} dealers kept; excluded "
              f"{len(report['excluded_do_not_scan'])} do-not-scan, "
              f"{len(report['excluded_recipe_only'])} recipe-only (no active cars, not in "
              f"deploy/railway/revived_dealers.txt); {len(report['revived_recipe_only'])} revived",
              flush=True)
        if lane != "all":
            print(f"fleet   lane {lane}: {len(report['excluded_home_lane'])} home-lane dealers dropped "
                  f"(excluded_home_lane), {report['excluded_not_home_lane']} not in the home list dropped",
                  flush=True)
        if report["excluded_do_not_scan"]:
            print("fleet   do-not-scan: " + ", ".join(report["excluded_do_not_scan"]), flush=True)
        if report["revived_recipe_only"]:
            print("fleet   revived: " + ", ".join(report["revived_recipe_only"]), flush=True)
        return sorted({d["dealer_id"] for d in kept})
    return []


# --------------------------------------------------------------------------
# egress-lane alarms (P15A.1)
# --------------------------------------------------------------------------

HOME_LANE_STALE_HOURS = 36


def blocked_railway_ids(rows: list[tuple[str, str | None]]) -> set[str]:
    """Dealers blocked from Railway: ``scan_hints.recipe_blocked['railway']`` (P7.6) or the
    legacy ``recipe_status = blocked:railway:...``. *rows* are (dealer_id, scan_hints JSON)."""
    out: set[str] = set()
    for did, raw in rows:
        try:
            hints = json.loads(raw or "{}") or {}
        except (TypeError, ValueError):
            continue
        if not isinstance(hints, dict):
            continue
        if (hints.get("recipe_blocked") or {}).get("railway") or \
                str(hints.get("recipe_status") or "").startswith("blocked:railway:"):
            out.add(str(did))
    return out


def blocked_unassigned(blocked: set[str], home: set[str], do_not_scan: set[str]) -> list[str]:
    """Blocked from Railway, in no lane's hands: not on the home list, not do-not-scan."""
    return sorted(blocked - home - do_not_scan)


def _as_utc(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def home_lane_stale(home: set[str], newest: dict[str, Any], now: datetime,
                    hours: int = HOME_LANE_STALE_HOURS) -> list[str]:
    """Home-lane dealers whose newest active ``scraped_at`` is older than *hours*, or that have
    no active row or no readable stamp (nothing fresh is on the site for them)."""
    out = []
    for did in sorted(home):
        dt = _as_utc(newest.get(did)) if newest.get(did) is not None else None
        if dt is None or (now - dt).total_seconds() > hours * 3600:
            out.append(did)
    return out


def lane_alarms(now: datetime | None = None) -> dict[str, Any]:
    """fleet_summary ``alarms``: blocked_unassigned and home_lane_stale. A failed read reports
    ``error`` instead of failing the run."""
    from backend.scanner.manifest import load_do_not_scan_ids, load_home_lane_ids

    home = load_home_lane_ids()
    out: dict[str, Any] = {"blocked_unassigned": [], "home_lane_stale": []}
    try:
        from backend.db.inventory_db import get_conn

        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT dealer_id, scan_hints FROM dealer_recipes WHERE scan_hints IS NOT NULL")
            blocked = blocked_railway_ids([(r[0], r[1]) for r in cur.fetchall()])
            out["blocked_unassigned"] = blocked_unassigned(blocked, home, load_do_not_scan_ids())
            newest: dict[str, Any] = {}
            if home:
                ids = sorted(home)
                cur.execute(
                    "SELECT dealer_id, MAX(scraped_at) FROM cars WHERE COALESCE(listing_active,1)=1 "
                    "AND dealer_id IN (" + ",".join("?" * len(ids)) + ") GROUP BY dealer_id",
                    tuple(ids),
                )
                newest = {str(r[0]): r[1] for r in cur.fetchall()}
            out["home_lane_stale"] = home_lane_stale(home, newest, now or datetime.now(timezone.utc))
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 - an alarm read must not fail a finished scan
        out["error"] = f"{type(exc).__name__}: {exc}"[:300]
    return out


def egress_lane() -> str:
    """The SCAN_LANE this run resolves to (an invalid value reads as 'all' here; load_roster
    already refused it for a fleet run)."""
    from backend.scanner.manifest import scan_lane

    try:
        return scan_lane()
    except ValueError:
        return "all"


# P15A.4: the post steps, in the order they run. Cards are last: they are keyed on each car's
# public-incomplete flag, so everything that writes data runs before them. Names come from the
# N14.1 table (docs/NIGHTLY_CAPABILITIES_2026_10.md): index/market/cards are the kept steps,
# recover is the targeted heal_from_recipes port, harvest is harvest_html_jsonld (home lane only).
POST_STEP_ORDER = ("index", "market", "recover", "harvest", "cards")
DEFAULT_POST_STEPS = ("index", "market", "cards")
HOME_ONLY_POST_STEPS = frozenset({"harvest"})
POST_STEP_KEYS = {
    "index": "rebuild_incomplete_index",
    "market": "compute_market_stats",
    "recover": "heal_from_recipes",
    "harvest": "harvest_html_jsonld",
    "cards": "build_listings_grid_cards",
}


def post_steps(raw: str | None = None, lane: str | None = None) -> list[str]:
    """Resolve SCAN_POST_STEPS to the ordered step names to run. '0' -> none, '1' or unset -> the
    default (unset on the home lane -> none), else a comma list in any order (run in POST_STEP_ORDER,
    cards last). An unknown step, or a home-only step off the home lane, raises ValueError."""
    lane = lane or egress_lane()
    raw = (os.environ.get("SCAN_POST_STEPS") if raw is None else raw) or ""
    raw = raw.strip().lower()
    if not raw:
        raw = "0" if lane == "home" else "1"
    if raw == "0":
        return []
    if raw == "1":
        return list(DEFAULT_POST_STEPS)
    wanted = [t.strip() for t in raw.split(",") if t.strip()]
    bad = [t for t in wanted if t not in POST_STEP_ORDER]
    if bad or not wanted:
        raise ValueError(f"SCAN_POST_STEPS has unknown step(s) {bad or [raw]}; "
                         f"use 0, 1 or a comma list of {','.join(POST_STEP_ORDER)}")
    off_lane = sorted(t for t in set(wanted) & HOME_ONLY_POST_STEPS if lane != "home")
    if off_lane:
        raise ValueError(f"SCAN_POST_STEPS step(s) {off_lane} run on SCAN_LANE=home only "
                         f"(the target hosts are walled off Railway)")
    return [t for t in POST_STEP_ORDER if t in wanted]


def open_code_dealers(dealer_ids: list[str]) -> list[str]:
    """Roster dealers with at least one active car carrying open (non-pending) index codes: the only
    dealers the recover and harvest steps are aimed at."""
    if not dealer_ids:
        return []
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT c.dealer_id FROM incomplete_listings il JOIN cars c ON c.id = il.car_id "
            "WHERE COALESCE(il.pending, 0) = 0 AND COALESCE(c.listing_active, 1) = 1 AND c.dealer_id IN ("
            + ",".join("?" * len(dealer_ids)) + ") ORDER BY c.dealer_id",
            tuple(dealer_ids),
        )
        return [str(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()


def _targeted_cmd(py: str, module: str, roster: list[str], cap: int) -> list[str] | None:
    """`-m module --dealer-id a --dealer-id b ...` for the capped open-code dealers; None when none."""
    ids = open_code_dealers(roster)[:cap]
    if not ids:
        return None
    cmd = [py, "-m", module]
    for d in ids:
        cmd += ["--dealer-id", d]
    return cmd


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


def shard_order() -> str:
    """SCAN_SHARD_ORDER: '' (default), 'reverse', or 'seed:<n>'. Anything else fails loudly."""
    raw = (os.environ.get("SCAN_SHARD_ORDER") or "").strip().lower()
    if raw in ("", "reverse") or (raw.startswith("seed:") and raw[5:].lstrip("-").isdigit()):
        return raw
    raise SystemExit(f"SCAN_SHARD_ORDER must be reverse or seed:<n>, got {raw!r}")


def make_shards(dealer_ids: list[str], n: int, weights: dict[str, int], order: str = "") -> list[list[str]]:
    """Longest-processing-time-first: biggest lots first, each onto the lightest shard.
    Scan time tracks row count (VDP prefetch per car), so shards finish together.
    `order` (N10.8, see shard_order) reverses or seed-shuffles both the dealer order inside
    each shard and the shard assignment, so a determinism pass sees a different first writer."""
    n = max(1, min(n, len(dealer_ids)))
    shards: list[list[str]] = [[] for _ in range(n)]
    load = [0] * n
    for did in sorted(dealer_ids, key=lambda d: (-weights.get(d, 50), d)):
        i = min(range(n), key=lambda k: load[k])
        shards[i].append(did)
        load[i] += max(20, weights.get(did, 50))
    shards = [s for s in shards if s]
    if order == "reverse":
        shards = [s[::-1] for s in shards[::-1]]
    elif order.startswith("seed:"):
        rng = random.Random(int(order[5:]))
        for s in shards:
            rng.shuffle(s)
        rng.shuffle(shards)
    return shards


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


def rooftop_candidate_report(since: datetime) -> dict[str, Any]:
    """Unrostered rooftops (N8.13): the whole open queue, and the ones this run saw."""
    try:
        from backend.attribution import candidates

        new = candidates.new_candidates()
        cutoff = since.replace(tzinfo=None).isoformat()
        fresh = [c for c in new if str(c.get("last_seen") or "") >= cutoff]
        keys = ("id", "name", "address", "store_id", "host", "car_count", "first_seen", "last_seen")
        return {"open": len(new), "seen_this_run": len(fresh),
                "new_this_run": [{k: c.get(k) for k in keys} for c in fresh[:50]]}
    except Exception as exc:  # noqa: BLE001 - reporting only
        return {"error": str(exc)[:120]}


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


def prune(out_root: Path, keep_days: int, keep: Path | None = None) -> None:
    cutoff = time.time() - keep_days * 86400
    for parent, pattern in ((out_root, "fleet_*"), (ROOT / "workspace" / "scanlogs", "scan_*.jsonl")):
        if not parent.is_dir():
            continue
        for p in parent.glob(pattern):
            try:
                if keep is not None and p.resolve() == keep.resolve():
                    continue
                if p.stat().st_mtime < cutoff:
                    shutil.rmtree(p) if p.is_dir() else p.unlink()
            except OSError:
                pass


def resume_dir_arg(argv: list[str]) -> Path | None:
    """``--resume <run_dir>`` / ``--resume=<run_dir>``, else SCAN_RESUME_DIR (N9.15)."""
    for i, a in enumerate(argv):
        if a == "--resume":
            if i + 1 >= len(argv):
                raise SystemExit("fleet   --resume needs a run dir")
            return Path(argv[i + 1])
        if a.startswith("--resume="):
            return Path(a.split("=", 1)[1])
    env = (os.environ.get("SCAN_RESUME_DIR") or "").strip()
    return Path(env) if env else None


def load_resume_state(run_dir: Path) -> tuple[datetime, list[list[str]]]:
    """The run start and the shard rosters (s0.txt, s1.txt, ...) of a run dir to resume.
    The start comes from run_state.json, else from the directory name, else its mtime."""
    shards: list[list[str]] = []
    i = 0
    while (run_dir / f"s{i}.txt").is_file():
        shards.append([d.strip() for d in (run_dir / f"s{i}.txt").read_text().splitlines() if d.strip()])
        i += 1
    if not shards:
        raise SystemExit(f"fleet   cannot resume {run_dir}: no shard roster files (s0.txt ...)")
    try:
        return datetime.fromisoformat(json.loads((run_dir / "run_state.json").read_text())["started"]), shards
    except (OSError, ValueError, KeyError):
        pass
    m = re.search(r"fleet_(\d{8}T\d{6})Z", run_dir.name)
    if m:
        return datetime.strptime(m.group(1), "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc), shards
    return datetime.fromtimestamp((run_dir / "s0.txt").stat().st_mtime, timezone.utc).replace(microsecond=0), shards


def main() -> int:
    """Gate (window), lease, run, hold. The scan itself is run_fleet()."""
    RUN_META.clear()
    explicit = bool((os.environ.get("SCAN_DEALERS") or "").strip())
    resuming = resume_dir_arg(sys.argv[1:]) is not None
    fleet_run = _truthy("SCAN_FLEET") and not explicit
    window: tuple[int, int] | None = None
    raw_window = (os.environ.get("SCAN_FLEET_WINDOW_UTC") or "").strip()
    warnings: list[str] = []
    if raw_window:
        try:
            window = parse_window(raw_window)
        except ValueError as exc:
            warnings.append(f"SCAN_FLEET_WINDOW_UTC invalid ({exc}); idling")
            print(f"fleet   WARN {warnings[-1]}", flush=True)
            if fleet_run and not resuming:
                _hold("idle", "SCAN_IDLE_HOLD_SECONDS", None)
                return 0
    lane = (os.environ.get("SCAN_FLEET_LANE") or "fleet").strip() or "fleet"
    RUN_META.update({"lane": lane, "window": raw_window or None, "warnings": warnings,
                     "reason": "SCAN_DEALERS" if explicit else ("resume" if resuming else "SCAN_FLEET")})
    if window is not None:
        if explicit and _truthy("SCAN_FLEET"):
            warnings.append("SCAN_DEALERS is set during a window run: this is a partial run, "
                            "not the fleet (a leftover value silently narrows the cron run)")
            print(f"fleet   WARN {warnings[-1]}", flush=True)
        elif fleet_run and not resuming and not in_window(_utcnow(), window):
            print(f"fleet   outside run window ({raw_window} UTC); idling", flush=True)
            RUN_META["reason"] = "outside run window"
            _hold("idle", "SCAN_IDLE_HOLD_SECONDS", window)
            return 0

    if (fleet_run or resuming) and (os.environ.get("SCAN_FLEET_LOCK") or "1").strip() != "0":
        with scan_leases.fleet_lock(lane) as lease:
            RUN_META["lock"] = {"acquired": lease.acquired, "holder": lease.holder, "degraded": lease.degraded}
            if not lease.acquired:
                print(f"fleet   lane {lane!r} is already running on {lease.holder}; "
                      f"exiting {EXIT_FLEET_LOCK_HELD} before spawning any shard", flush=True)
                return EXIT_FLEET_LOCK_HELD
            rc = run_fleet()
    else:
        rc = run_fleet()
    if RUN_META.get("idle"):
        _hold("idle", "SCAN_IDLE_HOLD_SECONDS", window)
    elif RUN_META.get("ran"):
        _hold("post-run", "SCAN_POST_RUN_HOLD_SECONDS", window)
    return rc


def run_fleet() -> int:
    started = datetime.now(timezone.utc).replace(microsecond=0)
    t_start = time.time()
    resume_dir = resume_dir_arg(sys.argv[1:])
    resumed_shards: list[list[str]] | None = None
    if resume_dir is not None:
        started, resumed_shards = load_resume_state(resume_dir)
        roster = sorted({d for ids in resumed_shards for d in ids})
        try:
            ROSTER_REPORT.update(json.loads((resume_dir / "roster_rule.json").read_text()))
        except (OSError, ValueError):
            pass
    else:
        roster = load_roster()
    if not roster:
        print("fleet   idle: set SCAN_FLEET=1 (whole roster) or SCAN_DEALERS=a,b,c to scan", flush=True)
        RUN_META["idle"] = True  # main() holds after the lease is released
        return 0

    try:
        steps = post_steps()
    except ValueError as exc:  # P15A.4: a typo must not be found after a 2 h scan
        print(f"fleet   {exc}", flush=True)
        raise SystemExit(2) from exc
    RUN_META["ran"] = True
    out_root = Path(os.environ.get("SCAN_OUT_ROOT") or (ROOT / "workspace" / "pipeline"))
    keep_days = _env_int("SCAN_KEEP_DAYS", 14)
    prune(out_root, keep_days)
    if resume_dir is not None:
        run_dir = resume_dir  # never pruned or recreated: it holds the pass being resumed
        shards = resumed_shards or []
    else:
        run_dir = out_root / f"fleet_{started.strftime('%Y%m%dT%H%M%SZ')}"
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "run_state.json").write_text(json.dumps({"started": started.isoformat()}))
        if ROSTER_REPORT:
            (run_dir / "roster_rule.json").write_text(json.dumps(ROSTER_REPORT, indent=1))
        shards = make_shards(roster, _env_int("SCAN_SHARDS", 8), active_rows(roster), shard_order())
    batch = _env_int("SCAN_BATCH", 6)
    extra = (os.environ.get("SCAN_PIPELINE_ARGS") or "").split()
    py = sys.executable
    print(f"fleet   {'RESUME ' if resume_dir is not None else ''}{len(roster)} dealers in {len(shards)} shard(s), batch {batch}, run {run_dir}", flush=True)

    sampler = ResourceSampler()
    sampler.start()

    threads: list[threading.Thread] = []
    rcs: dict[str, int] = {}
    shard_secs: dict[str, float] = {}
    for i, ids in enumerate(shards):
        tag = f"s{i}"
        if resume_dir is None:
            (run_dir / f"{tag}.txt").write_text("\n".join(ids) + "\n")
        env = dict(os.environ)
        env["SCANNER_LOCK_PATH"] = str(run_dir / f"{tag}.lock")
        env.setdefault("SCANNER_ROOFTOP_CANDIDATES", "1")  # N8.13: unrostered rooftops -> profiling queue
        cmd = [py, "-m", "backend.scripts.dealer_pipeline", "--dealers", ",".join(ids),
               "--out", str(run_dir / tag), "--batch", str(batch), *extra]
        if resume_dir is not None:
            cmd += ["--resume", "--resume-since", started.isoformat()]

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
    for step in steps:
        env = dict(os.environ)
        key = POST_STEP_KEYS[step]
        if step == "index":
            # Before market stats and the grid cards: cards are keyed on each car's
            # public-incomplete flag, so the index must be final first.
            # Once per fleet, after every shard's vPIC heal and reconcile; per-car syncs run
            # before backfill and leave complete cars flagged (N6.1). Forced: shards changed data.
            cmd = [py, "-c", "from backend.db.incomplete_listings_db import rebuild_index_chunked as r;"
                             "import json;res=r(force=True);print(json.dumps(res));"
                             "raise SystemExit(0 if res['status'] in ('rebuilt','locked') else 1)"]
        elif step == "market":
            cmd = [py, "-m", "backend.scripts.compute_market_stats"]
        elif step in ("recover", "harvest"):
            module = ("backend.scripts.heal_from_recipes" if step == "recover"
                      else "backend.scripts.harvest_html_jsonld")
            cmd = _targeted_cmd(py, module, roster, _env_int("SCAN_RECOVER_MAX_DEALERS", 25))
            if cmd is None:  # nothing carries an open code: act on no rows
                post[key] = {"rc": 0, "seconds": 0.0, "skipped": "no open index codes"}
                continue
        else:
            env["FLASK_ENV"] = "production"  # card key includes listings_include_incomplete_cars()
            cmd = [py, "-m", "backend.scripts.build_listings_grid_cards"]
        rc, secs = run_step(step, cmd, env, run_dir / "post.out")
        post[key] = {"rc": rc, "seconds": secs}
    sampler.stop.set()

    results: list[dict[str, Any]] = []
    for i in range(len(shards)):
        tri = run_dir / f"s{i}" / "triage.json"
        try:
            results.extend(json.loads(tri.read_text()).get("dealers") or [])
        except (OSError, ValueError):
            print(f"fleet   shard s{i}: no triage.json", flush=True)
    verdicts = Counter(str(r.get("verdict")) for r in results)
    from backend.scanner.pipeline.assess import fleet_accounting
    from backend.scanner.pipeline.triage import group_feed_unstamped

    acct = fleet_accounting(results)
    logs = sorted(run_dir.glob("s*/scanner.log")) + sorted(run_dir.glob("s*.out"))
    summary = {
        "started": started.isoformat(),
        "lane": RUN_META.get("lane"),
        "window": RUN_META.get("window"),
        "lock": RUN_META.get("lock"),
        "reason": RUN_META.get("reason"),
        "egress_lane": egress_lane(),
        "alarms": lane_alarms(),
        "warnings": list(RUN_META.get("warnings") or []),
        "run_dir": str(run_dir),
        "dealers": len(roster),
        "roster_rule": {k: (len(v) if isinstance(v, list) else v) for k, v in ROSTER_REPORT.items()},
        "assessed": len(results),
        "shards": len(shards),
        "shard_order": shard_order() or "default",
        "batch": batch,
        "verdicts": dict(verdicts),
        # N9.12: the old "rows" summed the feed BEFORE the VIN-owner guard and read as rows
        # written (208,233 vs 198,623 written on 10-13). Kept as rows_captured; error dealers
        # without a scan_run count as zeros.
        "rows": acct["rows_captured"],
        **acct,
        "vin_owner_conflicts": acct["rows_refused_vin_owner"],
        "group_feed_unstamped": group_feed_unstamped(results),
        "handover_pending": {r["dealer_id"]: r["handover_pending"] for r in results if r.get("handover_pending")},
        "rooftop_candidates": rooftop_candidate_report(started),
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
