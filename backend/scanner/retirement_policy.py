"""The one retirement policy: a pure function from stored rows plus a run's evidence to the ids to retire.

No database, no environment, no clock of its own: ``inventory_reconcile`` (P5A.4) loads the
active rows and builds the evidence, the delta scan (P5A.6) calls it the same way, and the
pipeline only reports (P5A.7, D-RC1 A). Failing closed is the point: a run that cannot prove
it saw the dealer's lot retires nothing, and a first miss is not a retirement (the strike
store of P5B.4 sits on top of the ids returned here).

Rules, in order (docs/REMEDIATION_PLAN_2026_10.md, P5A.1):

1. a run error, or fewer than ``min_rows`` rows: retire nothing;
2. a recovery-replaced run (``source`` = ``recovery:<strategy>``): retire nothing;
3. a live recipe walk (not a stale retry) that did not complete: retire nothing (D-RC4);
4. per condition bucket the baseline is the active rows scraped in the 30 days before the
   run started (else 90 days, else everything older than the run), counted by valid VIN;
   a bucket is retirable only when the run re-saw at least ``bucket_share`` of it and
   matched at least one;
5. ``one_condition_ok`` makes an ABSENT bucket (stray rows of the other condition) retirable,
   but only when every present bucket is itself retirable;
6. a make with ``make_guard_min`` or more prior rows of which none was matched is kept;
7. seen and protected VINs are never retired;
8. rows without a valid VIN retire only when their bucket is retirable.

Rows with ``scraped_at`` at or after ``run_started`` were refreshed by this run: they never
retire and never count toward a baseline. The caller passes each row's ``scraped_at`` as it
stood before the run wrote; a row it cannot date that way is simply left alone.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping

BUCKET_UNKNOWN = "unknown"

KEPT_REASONS = (
    "condition_missing",
    "bucket_low_coverage",
    "make_missing",
    "protected_refusal",
    "walk_incomplete",
    "recovery_replaced",
)

# Walk ends that mean the replay saw the whole lot (the ``end`` codes of recipes.py).
_COMPLETE_ENDS = frozenset({"single_page", "total_reached", "exhausted", "short_page"})


@dataclass(frozen=True)
class ActiveRow:
    id: Any
    vin_norm: str | None
    bucket: str
    make: str | None = None
    scraped_at: datetime | str | None = None


@dataclass(frozen=True)
class RunEvidence:
    seen_vins: frozenset[str] | set[str] = field(default_factory=frozenset)
    matched_makes: frozenset[str] | set[str] = field(default_factory=frozenset)
    walks: tuple[Mapping[str, Any], ...] | list[Mapping[str, Any]] = ()
    protected_vins: frozenset[str] | set[str] = field(default_factory=frozenset)
    run_started_iso: str | None = None
    source: str = "recipe"
    error: str | None = None
    rows: int = 0


@dataclass(frozen=True)
class Policy:
    min_rows: int = 20
    bucket_share: float = 0.60
    baseline_days: int = 30
    fallback_days: int = 90
    make_guard_min: int = 10
    one_condition_ok: bool = False
    block_on_incomplete_walk: bool = True
    retire_on_recovery_replaced: bool = False
    # Read by the strike store (P5B.4), not by this pure function: the misses before a
    # row retires, and the least gap between two of them.
    strikes: int = 1
    min_gap_hours: int = 20


@dataclass(frozen=True)
class RetirementPlan:
    eligible: bool
    reason: str
    retire_ids: tuple[Any, ...] = ()
    kept: dict[str, int] = field(default_factory=lambda: {k: 0 for k in KEPT_REASONS})
    buckets: dict[str, dict[str, Any]] = field(default_factory=dict)
    walks_summary: dict[str, Any] = field(default_factory=dict)


def _parse_ts(value: datetime | str | None) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00").replace(" ", "T", 1))
        except ValueError:
            return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _norm_make(value: Any) -> str:
    return " ".join(str(value or "").lower().split())


def _walk_complete(walk: Mapping[str, Any]) -> bool:
    if walk.get("complete") is not None:
        return bool(walk["complete"])
    return str(walk.get("end") or "") in _COMPLETE_ENDS


def _summarize_walks(walks: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    ws = list(walks or ())
    live = [w for w in ws if not w.get("stale_retry")]
    return {
        "walks": len(ws),
        "live": len(live),
        "stale_retries": len(ws) - len(live),
        "incomplete": [str(w.get("end") or "unknown") for w in live if not _walk_complete(w)],
        "capped": sum(1 for w in live if w.get("end") == "page_cap"),
    }


def _baseline(
    mine: list[tuple[ActiveRow, datetime | None]], cut30: datetime, cut90: datetime
) -> tuple[str, list[ActiveRow]]:
    """The bucket's prior valid-VIN rows: last 30 days, else 90, else every older row."""
    for window, floor in (("30d", cut30), ("90d", cut90)):
        base = [r for r, ts in mine if r.vin_norm and ts is not None and ts >= floor]
        if base:
            return window, base
    return "all", [r for r, _ in mine if r.vin_norm]


def plan_retirement(
    rows: Iterable[ActiveRow],
    evidence: RunEvidence,
    policy: Policy | None = None,
    now: datetime | None = None,
) -> RetirementPlan:
    """The ids of *rows* to retire after the run described by *evidence* (module rules)."""
    policy = policy or Policy()
    rows = list(rows)
    now = now or datetime.now(timezone.utc)
    run_started = _parse_ts(evidence.run_started_iso) or now
    summary = _summarize_walks(evidence.walks)
    seen = evidence.seen_vins
    protected = evidence.protected_vins

    def _nothing(reason: str, kept_key: str | None = None) -> RetirementPlan:
        kept = {k: 0 for k in KEPT_REASONS}
        if kept_key:
            kept[kept_key] = sum(1 for r in rows if r.vin_norm is None or r.vin_norm not in seen)
        return RetirementPlan(False, reason, (), kept, {}, summary)

    # 1-3: is the run trustworthy at all
    if evidence.error:
        return _nothing(f"run_error:{str(evidence.error)[:80]}")
    if evidence.rows < policy.min_rows:
        return _nothing(f"partial:min_rows:{evidence.rows}<{policy.min_rows}")
    if str(evidence.source or "").startswith("recovery") and not policy.retire_on_recovery_replaced:
        return _nothing("partial:recovery_replaced", "recovery_replaced")
    if policy.block_on_incomplete_walk and summary["incomplete"]:
        return _nothing(f"partial:walk_{summary['incomplete'][0]}", "walk_incomplete")

    # 4: per-bucket baselines by valid VIN, without the rows this run refreshed
    older = [(r, ts) for r, ts in ((r, _parse_ts(r.scraped_at)) for r in rows) if ts is None or ts < run_started]
    cut30 = run_started - timedelta(days=policy.baseline_days)
    cut90 = run_started - timedelta(days=policy.fallback_days)
    buckets: dict[str, dict[str, Any]] = {}
    for b in sorted({r.bucket or BUCKET_UNKNOWN for r, _ in older}):
        window, base = _baseline([(r, ts) for r, ts in older if (r.bucket or BUCKET_UNKNOWN) == b], cut30, cut90)
        prior = len(base)
        matched = sum(1 for r in base if r.vin_norm in seen)
        coverage = matched / prior if prior else 0.0
        buckets[b] = {
            "prior": prior,
            "matched": matched,
            "coverage": coverage,
            "retirable": prior > 0 and matched > 0 and coverage >= policy.bucket_share,
            "window": window if prior else "none",
        }

    # blank-condition rows retire only when every known bucket with prior rows qualifies
    known = [v for k, v in buckets.items() if k != BUCKET_UNKNOWN and v["prior"] > 0]
    if BUCKET_UNKNOWN in buckets:
        buckets[BUCKET_UNKNOWN]["retirable"] = (
            buckets[BUCKET_UNKNOWN]["prior"] > 0 and bool(known) and all(v["retirable"] for v in known)
        )
    # 5: a single-condition lot: an absent bucket is stray rows, retirable once the present ones are whole
    if policy.one_condition_ok:
        present = [v for k, v in buckets.items() if k != BUCKET_UNKNOWN and v["matched"] > 0]
        if present and all(v["retirable"] for v in present):
            for k, v in buckets.items():
                if k != BUCKET_UNKNOWN and v["prior"] > 0 and v["matched"] == 0:
                    v["retirable"] = True
                    v["stray"] = True

    # 6: makes the run never returned although the stored rows say it should have
    matched_makes = {_norm_make(m) for m in evidence.matched_makes}
    prior_by_make: dict[str, int] = {}
    seen_by_make: dict[str, int] = {}
    for r, _ in older:
        mk = _norm_make(r.make)
        if mk:
            prior_by_make[mk] = prior_by_make.get(mk, 0) + 1
            if r.vin_norm in seen:
                seen_by_make[mk] = seen_by_make.get(mk, 0) + 1
    guarded = {
        mk for mk, n in prior_by_make.items()
        if n >= policy.make_guard_min and mk not in matched_makes and not seen_by_make.get(mk)
    }

    kept = {k: 0 for k in KEPT_REASONS}
    retire: list[Any] = []
    for r, _ in older:
        if r.vin_norm and r.vin_norm in seen:  # 7: seen
            continue
        info = buckets[r.bucket or BUCKET_UNKNOWN]
        if not info["retirable"]:
            kept["bucket_low_coverage" if info["matched"] else "condition_missing"] += 1
        elif r.vin_norm and r.vin_norm in protected:  # 7: protected
            kept["protected_refusal"] += 1
        elif _norm_make(r.make) in guarded:
            kept["make_missing"] += 1
        else:  # 8: a row without a valid VIN gets here only through a retirable bucket
            retire.append(r.id)

    eligible = any(v["retirable"] for v in buckets.values())
    return RetirementPlan(eligible, "ok" if eligible else "no_retirable_bucket", tuple(retire), kept, buckets, summary)
