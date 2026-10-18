"""Incomplete-listings recompute under the current code (port of il_scripts/recompute.py).

The worker runs the same path as ``fast_rebuild`` and the 10-15 scratchpad
script: ``merge_verified_specs(car, include_extended_specs=False)`` then
``listing_missing_field_codes(car, for_public_filter=False, detail_ctx=...,
include_non_actionable=True)`` over every active car, inside
``inventory_pg.borrow_read_connection()`` after ``prime_vpic_cache``.

Each chunk is ``listing_active=1 AND id % n = k`` and runs in its own
subprocess with a hard timeout (default 140 s), with:
  * ``PGOPTIONS`` read-only + ``statement_timeout``; the worker refuses to start
    unless the backend's own connection reports ``default_transaction_read_only=on``;
  * any non-loopback socket connect blocked (no vPIC, no LLM, no geocoder);
  * paid API keys exported blank, auto-migrate and the schema check off.

Chunk results are JSONL files in a work dir with a ``.meta.json`` beside each;
a re-run reuses a chunk only when its meta matches the same DB fingerprint,
code HEAD and chunking, so the tool can be resumed one chunk batch per call.

The worker section (``_worker_main``) imports only the standard library and the
code root's ``backend`` package, so ``--code-root`` may point at a checkout that
does not contain this file.
"""

from __future__ import annotations

import collections
import hashlib
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

NON_ACTIONABLE = frozenset({"interior_color", "on_lot", "images"})
USED_TOKENS = ("used", "certif", "pre-owned", "preowned", "pre owned", "cpo", "carbravo")
WORKER_TIMEOUT_S = 140
RECORD_FIELDS = ("id", "vin", "dealer_id", "codes", "err", "used", "mileage", "first_seen_at", "scraped_at")


# ---------------------------------------------------------------------------
# shared helpers (also used by the aggregation side)
# ---------------------------------------------------------------------------

def is_used(car: dict[str, Any]) -> bool:
    """Same rule as arrivals.py's SQL and ``listing_completeness._is_used_or_certified``."""
    if car.get("is_cpo") in (1, True, "1"):
        return True
    cond = str(car.get("condition") or "").strip().lower()
    return any(t in cond for t in USED_TOKENS)


def actionable(codes: Iterable[str] | None) -> list[str]:
    return [c for c in (codes or []) if c not in NON_ACTIONABLE]


def is_new_null_mileage(rec: dict[str, Any]) -> bool:
    """The F12 shape: a new (or blank-condition) car whose odometer is NULL."""
    return (not rec.get("used")) and rec.get("mileage") is None


def actionable_excl_new_null(rec: dict[str, Any]) -> list[str]:
    nn = is_new_null_mileage(rec)
    return [c for c in actionable(rec.get("codes")) if not (c == "mileage" and nn)]


def parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    s = str(value).strip().replace("Z", "+00:00")
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        try:
            dt = datetime.fromisoformat(s[:19])
        except ValueError:
            return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def to_record(car: dict[str, Any], codes: list[str] | None, err: str | None) -> dict[str, Any]:
    m = car.get("mileage")
    if isinstance(m, str):
        m = m.strip() or None
    return {
        "id": car.get("id"),
        "vin": car.get("vin"),
        "dealer_id": car.get("dealer_id"),
        "codes": codes,
        "err": err,
        "used": is_used(car),
        "mileage": m,
        "first_seen_at": None if car.get("first_seen_at") is None else str(car.get("first_seen_at")),
        "scraped_at": None if car.get("scraped_at") is None else str(car.get("scraped_at")),
    }


# ---------------------------------------------------------------------------
# worker (subprocess side)
# ---------------------------------------------------------------------------

def _install_network_guard() -> None:
    import socket

    orig = socket.socket.connect

    def guard(self, addr):  # noqa: ANN001
        if self.family == getattr(socket, "AF_UNIX", object()):
            return orig(self, addr)
        host = addr[0] if isinstance(addr, tuple) else str(addr)
        if host.startswith("127.") or host in ("::1", "localhost"):
            return orig(self, addr)
        raise OSError(f"network blocked by cleanroom recompute: {addr}")

    socket.socket.connect = guard


def worker_env(base: dict[str, str], dsn_url: str, code_root: str) -> dict[str, str]:
    env = dict(base)
    env["INVENTORY_DATABASE_URL"] = dsn_url
    env["DATABASE_URL"] = dsn_url
    env["PGOPTIONS"] = "-c default_transaction_read_only=on -c statement_timeout=120000"
    env["INVENTORY_AUTO_MIGRATE"] = "0"
    env["INVENTORY_SCHEMA_CHECK"] = "off"
    for k in ("ANTHROPIC_API_KEY", "OPENAI_API_KEY", "GOOGLE_MAPS_API_KEY"):
        env[k] = ""
    env["PYTHONPATH"] = code_root
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env.pop("PGAPPNAME", None)
    return env


def _worker_main(argv: list[str]) -> int:
    import argparse

    ap = argparse.ArgumentParser(prog="cleanroom-recompute-worker")
    ap.add_argument("--conninfo", required=True, help="libpq conninfo for the direct row read")
    ap.add_argument("--chunk", type=int, required=True)
    ap.add_argument("--of", type=int, required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--code-root", required=True)
    a = ap.parse_args(argv)

    # The env (PGOPTIONS, INVENTORY_DATABASE_URL, blank keys) is set by the parent.
    if "default_transaction_read_only=on" not in os.environ.get("PGOPTIONS", ""):
        print("REFUSED: PGOPTIONS lacks default_transaction_read_only=on", file=sys.stderr)
        return 3
    _install_network_guard()
    here = str(Path(__file__).resolve().parent)
    sys.path[:] = [p for p in sys.path if os.path.abspath(p or ".") != here]
    sys.path.insert(0, a.code_root)
    os.chdir(a.code_root)

    import psycopg
    from psycopg.rows import dict_row

    from backend.db import inventory_pg
    from backend.enrichment.knowledge_engine import merge_verified_specs, prime_vpic_cache
    from backend.utils.listing_completeness import listing_missing_field_codes

    probe = inventory_pg.pg_connect()
    try:
        flag = str(probe.execute("SHOW default_transaction_read_only").fetchone()[0]).lower()
    finally:
        probe.close()
    if flag != "on":
        print(f"REFUSED: backend connection default_transaction_read_only={flag}", file=sys.stderr)
        return 3

    conn = psycopg.connect(
        a.conninfo,
        row_factory=dict_row,
        options="-c default_transaction_read_only=on -c statement_timeout=120000",
    )
    try:
        ro = str(conn.execute("SHOW default_transaction_read_only").fetchone()["default_transaction_read_only"])
        if ro.lower() != "on":
            print("REFUSED: row-read connection is not read-only", file=sys.stderr)
            return 3
        rows = conn.execute(
            "SELECT * FROM cars WHERE listing_active = 1 AND (id %% %s) = %s ORDER BY id", (a.of, a.chunk)
        ).fetchall()
    finally:
        conn.close()

    t0 = time.time()
    tmp = a.out + ".tmp"
    n = errors = 0
    with open(tmp, "w") as fh, inventory_pg.borrow_read_connection():
        prime_vpic_cache([r.get("vin") for r in rows])
        for car in rows:
            car = dict(car)
            try:
                ctx = {"verified_specs": merge_verified_specs(car, include_extended_specs=False)}
                codes = listing_missing_field_codes(
                    car, for_public_filter=False, detail_ctx=ctx, include_non_actionable=True
                )
                err = None
            except Exception as e:  # recorded per car, as in the original script
                codes, err = None, repr(e)[:200]
                errors += 1
            fh.write(json.dumps(to_record(car, codes, err), default=str) + "\n")
            n += 1
    os.replace(tmp, a.out)
    print(json.dumps({"rows": n, "errors": errors, "seconds": round(time.time() - t0, 1)}))
    return 0


# ---------------------------------------------------------------------------
# driver (parent side)
# ---------------------------------------------------------------------------

def git_head(root: str | Path) -> str:
    try:
        return subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True, timeout=20
        ).stdout.strip()
    except Exception:
        return ""


def chunk_paths(work: Path, k: int, n: int) -> tuple[Path, Path]:
    base = work / f"c{k:03d}of{n:03d}.jsonl"
    return base, base.with_suffix(".meta.json")


def conninfo_to_url(conninfo: str) -> str:
    """``dbname=x host=/tmp port=5432`` -> ``postgresql:///x?host=/tmp&port=5432``."""
    if conninfo.startswith(("postgresql://", "postgres://")):
        return conninfo
    parts = dict(p.split("=", 1) for p in conninfo.split() if "=" in p)
    db = parts.pop("dbname", "")
    q = "&".join(f"{k}={v}" for k, v in parts.items())
    return f"postgresql:///{db}" + (f"?{q}" if q else "")


def run_chunks(
    *,
    conninfo: str,
    work: Path,
    fingerprint: dict[str, Any],
    chunks: int = 16,
    jobs: int = 8,
    timeout: int = WORKER_TIMEOUT_S,
    code_root: str,
    python: str | None = None,
    max_new_chunks: int | None = None,
    log: Callable[[str], None] = lambda s: print(s, file=sys.stderr),
) -> dict[str, Any]:
    """Run (or reuse) every chunk. Returns per-chunk meta; ``complete`` is False when
    ``max_new_chunks`` stopped the batch early (call again to continue)."""
    work = Path(work).resolve()  # the worker chdirs into the code root
    work.mkdir(parents=True, exist_ok=True)
    python = python or sys.executable
    script = str(Path(__file__).resolve())
    env = worker_env(dict(os.environ), conninfo_to_url(conninfo), code_root)
    todo, metas = [], {}
    for k in range(chunks):
        out, meta = chunk_paths(work, k, chunks)
        if out.is_file() and meta.is_file():
            try:
                m = json.loads(meta.read_text())
            except ValueError:
                m = {}
            if m.get("fingerprint") == fingerprint and m.get("ok"):
                metas[k] = m
                continue
        todo.append(k)
    if max_new_chunks is not None:
        todo = todo[:max_new_chunks]

    def one(k: int) -> dict[str, Any]:
        out, meta = chunk_paths(work, k, chunks)
        cmd = [python, script, "--worker", "--conninfo", conninfo, "--chunk", str(k), "--of", str(chunks),
               "--out", str(out), "--code-root", code_root]
        t0 = time.monotonic()
        try:
            p = subprocess.run(cmd, env=env, cwd=code_root, capture_output=True, text=True, timeout=timeout)
            rc, so, se = p.returncode, p.stdout, p.stderr
        except subprocess.TimeoutExpired:
            rc, so, se = -9, "", f"chunk {k} exceeded {timeout}s and was killed"
        secs = round(time.monotonic() - t0, 1)
        m: dict[str, Any] = {"chunk": k, "of": chunks, "rc": rc, "seconds": secs, "fingerprint": fingerprint,
                             "ok": rc == 0}
        if rc == 0:
            try:
                w = json.loads(so.strip().splitlines()[-1])
                m.update(rows=w.get("rows"), errors=w.get("errors"), process_seconds=w.get("seconds"))
            except (ValueError, IndexError, AttributeError):
                m["ok"] = False
        if not m["ok"]:
            m["stderr_tail"] = (se or "")[-1500:]
        meta.write_text(json.dumps(m, indent=1))
        log(f"recompute chunk {k}/{chunks}: rc={rc} {secs}s rows={m.get('rows')}")
        return m

    with ThreadPoolExecutor(max_workers=max(1, jobs)) as ex:
        for m in ex.map(one, todo):
            metas[m["chunk"]] = m
    complete = all(metas.get(k, {}).get("ok") for k in range(chunks))
    failed = [m for m in metas.values() if not m.get("ok")]
    return {
        "chunks": chunks,
        "jobs": jobs,
        "complete": complete,
        "failed": [{"chunk": m["chunk"], "rc": m.get("rc"), "stderr_tail": m.get("stderr_tail", "")[-400:]}
                   for m in failed],
        "chunk_seconds_max": max((m.get("seconds") or 0) for m in metas.values()) if metas else None,
        "chunk_seconds": {str(k): metas[k].get("seconds") for k in sorted(metas)},
        "rows": sum(int(m.get("rows") or 0) for m in metas.values()),
        "errors": sum(int(m.get("errors") or 0) for m in metas.values()),
    }


def cached_complete(work: Path, chunks: int, fingerprint: dict[str, Any]) -> bool:
    """True when every chunk file exists with an ok meta for this fingerprint."""
    for k in range(chunks):
        out, meta = chunk_paths(Path(work), k, chunks)
        if not (out.is_file() and meta.is_file()):
            return False
        try:
            m = json.loads(meta.read_text())
        except ValueError:
            return False
        if not m.get("ok") or m.get("fingerprint") != fingerprint:
            return False
    return True


def load_records(work: Path, chunks: int) -> list[dict[str, Any]]:
    recs: list[dict[str, Any]] = []
    for k in range(chunks):
        out, _ = chunk_paths(work, k, chunks)
        with out.open() as fh:
            recs.extend(json.loads(line) for line in fh if line.strip())
    return recs


def run_inprocess(cars: Iterable[dict[str, Any]], codes_fn: Callable[[dict[str, Any]], list[str]]) -> list[dict]:
    """Same record shape without subprocesses (tests, SQLite targets)."""
    out = []
    for car in cars:
        try:
            codes, err = codes_fn(dict(car)), None
        except Exception as e:
            codes, err = None, repr(e)[:200]
        out.append(to_record(dict(car), codes, err))
    return out


def real_codes(car: dict[str, Any]) -> list[str]:
    """The production path, in-process (the caller has configured the backend env)."""
    from backend.enrichment.knowledge_engine import merge_verified_specs
    from backend.utils.listing_completeness import listing_missing_field_codes

    ctx = {"verified_specs": merge_verified_specs(car, include_extended_specs=False)}
    return listing_missing_field_codes(car, for_public_filter=False, detail_ctx=ctx, include_non_actionable=True)


# ---------------------------------------------------------------------------
# aggregation (summarize.py + split.py)
# ---------------------------------------------------------------------------

def summarize(recs: list[dict[str, Any]]) -> dict[str, Any]:
    n = len(recs)
    errors = sum(1 for r in recs if r.get("err"))
    by_code: collections.Counter[str] = collections.Counter()
    only: collections.Counter[str] = collections.Counter()
    by_code_x: collections.Counter[str] = collections.Counter()
    only_x: collections.Counter[str] = collections.Counter()
    per_car: collections.Counter[int] = collections.Counter()
    mileage_split: collections.Counter[str] = collections.Counter()
    inc = inc_x = new_null_only = mileage_only = 0
    new_null_cars = 0
    for r in recs:
        codes = r.get("codes") or []
        by_code.update(codes)
        a = actionable(codes)
        nn = is_new_null_mileage(r)
        new_null_cars += nn
        if "mileage" in codes:
            mileage_split["new_or_blank_condition_null" if nn else
                          ("used_zero" if r.get("mileage") == 0 else "used_null")] += 1
        if a:
            inc += 1
            per_car[len(a)] += 1
            if len(a) == 1:
                only[a[0]] += 1
                if a[0] == "mileage":
                    mileage_only += 1
                    new_null_only += nn
        ax = [c for c in a if not (c == "mileage" and nn)]
        by_code_x.update(ax)
        if ax:
            inc_x += 1
            if len(ax) == 1:
                only_x[ax[0]] += 1
    return {
        "active": n,
        "errors": errors,
        "incomplete": inc,
        "incomplete_pct": round(100 * inc / n, 2) if n else None,
        "incomplete_excl_new_null_mileage": inc_x,
        "incomplete_excl_new_null_mileage_pct": round(100 * inc_x / n, 2) if n else None,
        "by_code": {k: {"count": v, "only_gap": only.get(k, 0)} for k, v in by_code.most_common()},
        "by_code_excl_new_null_mileage": {k: {"count": v, "only_gap": only_x.get(k, 0)}
                                          for k, v in by_code_x.most_common()},
        "mileage_split": {
            "mileage_code_cars": sum(mileage_split.values()),
            "new_or_blank_condition_null": mileage_split.get("new_or_blank_condition_null", 0),
            "used_zero": mileage_split.get("used_zero", 0),
            "used_null": mileage_split.get("used_null", 0),
            "mileage_only_gap": mileage_only,
            "new_null_only_gap": new_null_only,
            "new_or_blank_condition_null_active": new_null_cars,
        },
        "actionable_codes_per_incomplete_car": {str(k): v for k, v in sorted(per_car.items())},
    }


def per_dealer(recs: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    out: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    for r in recs:
        c = out[r.get("dealer_id") or "?"]
        c["n"] += 1
        c["incomplete"] += bool(actionable(r.get("codes")))
        c["incomplete_excl_new_null_mileage"] += bool(actionable_excl_new_null(r))
        codes = r.get("codes") or []
        for k in ("price", "trim", "exterior_color", "images"):
            c[k] += k in codes
    return {d: dict(c) for d, c in out.items()}


# ---------------------------------------------------------------------------
# idxcmp.py: index flags that no longer hold
# ---------------------------------------------------------------------------

def stale_index_flags(recs: list[dict[str, Any]], index_rows: Iterable[tuple[Any, Any]]) -> dict[str, Any]:
    codes = {r["id"]: (r.get("codes") or []) for r in recs}
    act_ids = {cid for cid, cs in codes.items() if actionable(cs)}
    idx: dict[Any, list[str]] = {}
    bad_json = 0
    for cid, mf in index_rows:
        try:
            v = json.loads(mf) if isinstance(mf, str) else (mf or [])
        except ValueError:
            bad_json += 1
            v = []
        idx[cid] = list(v) if isinstance(v, (list, tuple)) else []
    stale_by_code: collections.Counter[str] = collections.Counter()
    stale_by_code_on_complete: collections.Counter[str] = collections.Counter()
    complete_now = set(idx) - act_ids
    for cid, flagged in idx.items():
        now = set(codes.get(cid, []))
        for x in flagged:
            if x not in now:
                stale_by_code[x] += 1
                if cid in complete_now:
                    stale_by_code_on_complete[x] += 1
    return {
        "index_active_rows": len(idx),
        "recompute_incomplete": len(act_ids),
        "stale_cars": len(complete_now),
        "incomplete_not_in_index": len(act_ids - set(idx)),
        "stale_codes": dict(stale_by_code.most_common()),
        "stale_codes_on_complete_cars": dict(stale_by_code_on_complete.most_common()),
        "unparsable_rows": bad_json,
    }


# ---------------------------------------------------------------------------
# arrivals.py / arr2.py / age_old.py: first-seen window split
# ---------------------------------------------------------------------------

WINDOW_BUCKETS = ("le_3d", "le_7d", "older", "no_timestamp")


def window_split(
    recs: list[dict[str, Any]],
    ref_first_seen: dict[str, Any] | None = None,
    *,
    ref_vins: set[str] | None = None,
    ref_max_first_seen: Any = None,
) -> dict[str, Any]:
    """Split by age = scraped_at - first seen, where first seen is the earlier of the
    car's own ``first_seen_at`` and the reference DB's earliest ``first_seen_at`` for
    the same VIN (any status). Buckets: <=3 d, 3-7 d, >7 d."""
    ref_first_seen = ref_first_seen or {}
    ref_vins = set(ref_first_seen) if ref_vins is None else ref_vins
    stats: dict[str, collections.Counter] = {b: collections.Counter() for b in WINDOW_BUCKETS}
    in_ref = 0
    for r in recs:
        own = parse_ts(r.get("first_seen_at"))
        ref = parse_ts(ref_first_seen.get(r.get("vin")))
        is_in_ref = r.get("vin") in ref_vins
        in_ref += is_in_ref
        first = min([t for t in (own, ref) if t is not None], default=None)
        seen = parse_ts(r.get("scraped_at"))
        if first is None or seen is None:
            b = "no_timestamp"
        else:
            age = (seen - first).total_seconds() / 86400
            b = "le_3d" if age <= 3 else ("le_7d" if age <= 7 else "older")
        s = stats[b]
        codes = r.get("codes") or []
        s["n"] += 1
        s["incomplete"] += bool(actionable(codes))
        s["incomplete_excl_new_null_mileage"] += bool(actionable_excl_new_null(r))
        s["not_in_ref"] += not is_in_ref
        for k in ("price", "trim", "exterior_color", "images"):
            s[k] += k in codes
    seen_all = [parse_ts(r.get("scraped_at")) for r in recs]
    seen_all = [t for t in seen_all if t is not None]
    rmax = parse_ts(ref_max_first_seen)
    gap = round((min(seen_all) - rmax).total_seconds() / 86400, 1) if (seen_all and rmax) else None
    return {
        "method": "age = scraped_at - min(first_seen_at, reference first_seen_at by VIN); <=3d / 3-7d / >7d",
        "buckets": {b: dict(stats[b]) for b in WINDOW_BUCKETS},
        "in_reference": in_ref,
        "not_in_reference": len(recs) - in_ref,
        "reference_max_first_seen_at": None if ref_max_first_seen is None else str(ref_max_first_seen),
        "reference_gap_days": gap,
        # A reference older than the window cannot date cars it never saw: every car
        # absent from it lands in le_3d. Say so instead of presenting a fake split.
        "split_reliable": gap is None or gap <= 3,
    }


# ---------------------------------------------------------------------------
# transit.py and paired.py
# ---------------------------------------------------------------------------

def transit_split(recs: list[dict[str, Any]], ref_vins: set[str] | None = None) -> dict[str, Any]:
    s: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    for r in recs:
        codes = r.get("codes") or []
        k = "in_transit" if "on_lot" in codes else "on_lot"
        if ref_vins is not None:
            k += "/in_ref" if r.get("vin") in ref_vins else "/not_in_ref"
        s[k]["n"] += 1
        s[k]["incomplete_excl_new_null_mileage"] += bool(actionable_excl_new_null(r))
        for f in ("price", "exterior_color", "trim", "images"):
            s[k][f] += f in codes
    return {k: dict(v) for k, v in sorted(s.items())}


def paired_vs_reference(recs: list[dict[str, Any]], ref_recs: list[dict[str, Any]]) -> dict[str, Any]:
    new = {r["vin"]: set(r.get("codes") or []) for r in recs}
    old = {r["vin"]: set(r.get("codes") or []) for r in ref_recs}
    both = [v for v in new if v in old]
    out: dict[str, Any] = {
        "active_in_both": len(both),
        "ref_active_not_in_db": sum(1 for v in old if v not in new),
        "db_active_not_in_ref_active": sum(1 for v in new if v not in old),
        "fields": {},
    }
    for k in ("price", "trim", "exterior_color", "images", "interior_color", "mileage"):
        t = collections.Counter((k in old[v], k in new[v]) for v in both)
        out["fields"][k] = {
            "missing_in_ref": t[(True, True)] + t[(True, False)],
            "filled_now": t[(True, False)],
            "still_missing": t[(True, True)],
            "newly_missing": t[(False, True)],
        }
    return out


def fingerprint_for(db_state: dict[str, Any], code_root: str, chunks: int) -> dict[str, Any]:
    raw = json.dumps(db_state, sort_keys=True, default=str)
    try:
        dirty = subprocess.run(["git", "-C", str(code_root), "status", "--porcelain", "--untracked-files=no",
                                "--", "backend"], capture_output=True, text=True, timeout=30).stdout
    except Exception:
        dirty = "?"
    return {
        "db_state": hashlib.sha256(raw.encode()).hexdigest()[:16],
        "code_head": git_head(code_root),
        "code_root": code_root,
        "code_dirty": hashlib.sha256(dirty.encode()).hexdigest()[:12] if dirty else "",
        "worker": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12],
        "chunks": chunks,
    }


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        sys.exit(_worker_main(sys.argv[2:]))
    print("internal worker; run backend/scripts/cleanroom_acceptance.py", file=sys.stderr)
    sys.exit(2)
