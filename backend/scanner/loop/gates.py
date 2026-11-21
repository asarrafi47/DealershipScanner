"""Dealer gates and dealer state of the scan loop (card L8; specification: docs/scan_loop/GATES.md).

Pure and standard library only: callers pass the triage row (``entry``), the per-dealer ``extras``
of GATES.md section 11, the dealer's ``history`` from the previous gate report, the accepted-gap
``registry``, a ``GateConfig`` and ``now`` (a ``datetime``). Nothing is read from the clock, the
environment or a file, no argument is mutated, and every threshold lives in ``GateConfig``.
Changing a threshold is a pull request that also changes GATES.md.

A gate returns ``pass``, ``fail`` or ``cannot_assess`` with the measured value and the threshold.
A missing input is ``cannot_assess``, never a guess. The per-dealer ``permissions`` are only the coarse dealer-level part; the write predicates and the fleet gate
follow below.

Thresholds (GATES.md section 4); S = stamped rows (``rows_stamped``, 0 when absent), A = ``active_after``,
B = baseline, g = days since B, Y = S / B, F = feed rows plus parser-refused rows, X = refused rows,
R = X / F, T = the site total, W = raw VINs walked::

    Gate  Rule                                                                              Kind
    GF1   fail if S < max(20, 0.5 B); no B: fail below 20 rows, else cannot_assess          hard
    GF2   Y < 0.50 fails; Y < 0.70 fails (recipe work, retire off); Y < max(0.70,           soft warn /
          0.95 - 0.004 g) warns (flag watch). A stand-in baseline evaluates only the        hard below 0.70
          0.50 level; the other levels are cannot_assess.
    GF3   walk complete: stopped before its cap and (T unknown or W >= 0.95 T); the cap is   hard
          the report's (min(150, ceil(T / page size) + 2) with T known, else 40); no
          pagination means one page, only the W test applies; no report: cannot_assess
    GF4   fail if R >= 0.10, or F >= 50 and S / F < 0.9; a pass with R >= 0.07 flags watch   hard
    GF5   C = min(rows_new, rows_used); fail if S >= 20, C = 0 and no unexpired accepted     hard
          gap; info note if S >= 100 and C / S < 0.02
    GF6   fail if no live recipe, or last ok run older than 30 days and Y < 0.9             hard
    GF7   fail if recipe_status starts with blocked:<tag>: or >= 3 feed requests got        hard
          403 / 429 / a challenge, no VIN came back and the host has an egress tag
    GA1   price, exterior_color (and used_mileage when present) capture >= 0.90; trim:       soft
          post-fill gap missing.trim / accuracy.rows with hysteresis 5.0 / 3.5 %, without
          accuracy capture trim >= 0.73; a failure after a VDP wall-clock cut is
          cannot_assess
    GA2   incomplete_rows / accuracy.rows, hysteresis enter above 5.0 %, leave at 3.5 %,     soft
          same wall-clock rule as GA1
    GA3   hard_rows / assessable, same band; assessable = vpic_cached - vpic_gated and       soft; an
          under 75 % of accuracy.rows is cannot_assess; invariant violations > 0.2 % of      invariant
          rows breach                                                                         breach is hard
    GA4   fail if n(vpic_make) + n(vpic_year) >= max(5, 1 % of accuracy.rows)                hard

Fleet gate (GATES.md section 7; ``evaluate_fleet``): FAIL beats HOLD beats PASS.

    FG1  FAIL if assessed / roster < 0.98, any shard rc != 0, any post step rc != 0, or error share >= 0.02
    FG2  HOLD if ok share < 0.45 or more than 0.08 below trailing; HOLD if (no_rows + error + no_recipe)
         share > max(0.03, trailing + 0.015)
    FG3  HOLD if sum S / previous sum S is outside [0.90, 1.12]
    FG4  HOLD if more than 8 % of the dealers stamped in both runs have S / previous S below the GF2 warn level
    FG5  HOLD if sum X / sum F > 0.06, or an owner-claimant pair absent last run holds more than 100 VINs;
         stops retirements and moves
    FG6  HOLD if sum retired / prior active > min(0.60, 0.10 + 0.015 g); stops retirements and moves
    FG7  HOLD if the blocked share > 0.06, FAIL above 0.15
    FG8  FAIL if a shard was killed (rc < 0 or >= 128); HOLD if the duration > 2 x trailing

Write gates (GATES.md section 6): ``can_retire`` (WR, per condition bucket), ``can_move`` (WM), ``can_promote``
(WP). Each returns ``allowed`` and every reason that denies it; a missing input denies, never allows.

Fix verification (GATES.md section 9; ``verify_fix``): a dealer is ``fixed`` when the gates that failed before
clear with margin on a scan and on a second scan at least 20 h later, the NHTSA checks hold and both conditions
are present where the site sells both; ``fixed_pending`` until the second scan, ``fixed_unverifiable`` without
NHTSA evidence, else ``not_fixed``. Its thresholds are the ``fix_*`` fields of ``GateConfig``.

State, first match wins (GATES.md section 5): ``blocked`` (GF7); ``needs_code_work`` for a hard code
condition (GF3, GF4, GA4, a GA3 invariant breach, a single-page widget); ``needs_recipe_work`` (GF1, GF5,
GF6, GF2 below 0.70, verdict no_recipe); ``needs_code_work`` for a soft gate that counts (two consecutive
failing scans, or one scan in legacy mode); else ``ok``, with the flag ``watch`` for a first soft failure,
a share inside the hysteresis band or a GF2 warn. Readings that GATES.md leaves open: a dealer with verdict
``error`` has every gate cannot_assess and the state ``unassessed`` (its history is carried over unchanged);
only GF5 and the soft gates GA1 to GA3 can be exempted by the registry; a ``cannot_assess`` result leaves a
gate's history record unchanged.
"""

from __future__ import annotations

import copy
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone

from .scrub import clean_line

GATE_NAMES = ("GF1", "GF2", "GF3", "GF4", "GF5", "GF6", "GF7", "GA1", "GA2", "GA3", "GA4")
SOFT_GATES = ("GA1", "GA2", "GA3")
STATES = ("ok", "needs_recipe_work", "needs_code_work", "blocked", "unassessed")
PASS, FAIL, CANNOT = "pass", "fail", "cannot_assess"

_BLOCKED_RE = re.compile(r"^blocked:[^:\s]+:")
_DEALER_ID_MAX = 80


@dataclass(frozen=True)
class GateConfig:
    """Every threshold of the dealer gates (see the module docstring for the table)."""

    min_rows: int = 20  # GF1 floor, MIN_ROWS_UNKNOWN
    row_floor: float = 0.5  # GF1 share of the baseline, ROW_FLOOR; GF2 fail level
    yield_low: float = 0.70  # GF2 recipe-work level and floor of the warn level
    yield_warn_start: float = 0.95  # GF2 warn level at g = 0
    yield_warn_per_day: float = 0.004  # GF2 warn level falls this much per day
    walk_total_share: float = 0.95  # GF3 W >= 0.95 T
    walk_cap_unknown: int = 40  # GF3 cap when the report states none and T is unknown
    walk_cap_max: int = 150  # GF3 cap ceiling when T is known
    walk_cap_slack_pages: int = 2  # GF3 cap = ceil(T / page size) + 2
    refused_share: float = 0.10  # GF4 R
    refused_watch: float = 0.07  # GF4 passes but R at or above this marks watch (a flag, not a gate result)
    stamped_min_feed: int = 50  # GF4 second clause: F at least this
    stamped_share_min: float = 0.9  # GF4 second clause: P below this fails
    condition_min_rows: int = 20  # GF5 S at least this
    condition_note_rows: int = 100  # GF5 note: S at least this
    condition_note_share: float = 0.02  # GF5 note: C / S below this
    recipe_max_age_days: int = 30  # GF6
    recipe_yield_min: float = 0.9  # GF6 Y below this with an old recipe
    blocked_requests: int = 3  # GF7
    key_coverage_min: float = 0.90  # GA1 price, exterior_color, used_mileage
    trim_capture_min: float = 0.73  # GA1 trim without an accuracy block
    share_enter: float = 0.05  # hysteresis: a passing share gate fails above this
    share_leave: float = 0.035  # hysteresis: a failing share gate passes at or below this
    assessable_min: float = 0.75  # GA3 assessable share of accuracy.rows
    invariant_max: float = 0.002  # GA3 invariant violations / rows
    identity_min: int = 5  # GA4 floor of max(5, 1 %)
    identity_share: float = 0.01  # GA4
    soft_fail_scans: int = 2  # persistence of soft gates
    exempt_gates: tuple = ("GF5", "GA1", "GA2", "GA3")  # gates the registry can exempt
    # Fleet gate (GATES.md section 7)
    fg_assessed_min: float = 0.98  # FG1 FAIL below assessed / roster
    fg_error_max: float = 0.02  # FG1 FAIL at or above the error share
    fg_ok_min: float = 0.45  # FG2 HOLD below this ok share
    fg_ok_drop: float = 0.08  # FG2 HOLD when the ok share is more than this below trailing
    fg_bad_floor: float = 0.03  # FG2 HOLD above max(this, trailing + fg_bad_rise) for no_rows + error + no_recipe
    fg_bad_rise: float = 0.015
    fg_volume_low: float = 0.90  # FG3 HOLD outside [low, high]
    fg_volume_high: float = 1.12
    fg_shrink_share: float = 0.08  # FG4 HOLD above this share of dealers below the GF2 warn level
    fg_attribution_share: float = 0.06  # FG5 HOLD above sum X / sum F
    fg_pair_vins: int = 100  # FG5 HOLD when an owner-claimant pair absent last run holds more VINs
    fg_blocked_hold: float = 0.06  # FG7 HOLD above this blocked share
    fg_blocked_fail: float = 0.15  # FG7 FAIL above this blocked share
    fg_duration_factor: float = 2.0  # FG8 HOLD above this multiple of the trailing duration
    fg_trailing_runs: int = 3  # trailing = mean of this many previous PASS runs
    # Write gates (GATES.md section 6)
    retire_share_cap: float = 0.60  # retired share <= min(cap, base + per_day * g)
    retire_share_base: float = 0.10
    retire_share_per_day: float = 0.015
    wr_bucket_min_rows: int = 10  # WR S_b at least this
    wr_stamped_share: float = 0.8  # WR S_b / A_b at least this
    wr_seen_share: float = 0.70  # WR seen_b / A_b at least this
    wr_strike_hours: float = 20.0  # WR a VIN must have been missing this long before
    wm_moves_floor: int = 5  # WM moves per run <= max(floor, share * S)
    wm_moves_share: float = 0.01
    # Fix verification (GATES.md section 9, ``verify_fix``)
    fix_key_coverage: float = 0.93  # key-field capture needed to call GA1 cleared
    fix_share_max: float = 0.035  # incomplete, trim-gap and hard share needed to call GA2 / GA3 cleared
    fix_yield_min: float = 0.90  # Y against the baseline, or ...
    fix_walk_share: float = 0.95  # ... W / T
    fix_refused_max: float = 0.03  # R must stay under this
    fix_decode_min: float = 0.98  # share of rows with a cached vPIC decode
    fix_identity_min: int = 2  # identity mismatches at most max(2, 0.3 % of rows)
    fix_identity_share: float = 0.003
    fix_second_scan_hours: float = 20.0  # the confirming scan comes at least this long after the first


DEFAULT_CONFIG = GateConfig()

# Every field a gate reads, as "entry.<triage field>" or "extras.<field>" paths.
INPUT_FIELDS = {
    "GF1": ("entry.rows_stamped", "entry.known_before", "extras.baseline", "extras.yield.known_lot"),
    "GF2": ("entry.rows_stamped", "entry.known_before", "extras.baseline", "extras.yield.known_lot"),
    "GF3": ("extras.walk",),
    "GF4": ("entry.rows", "entry.rows_stamped", "entry.vin_owner_conflicts", "extras.refused"),
    "GF5": ("entry.rows_stamped", "entry.rows_new", "entry.rows_used"),
    "GF6": ("extras.recipe_age", "extras.baseline", "entry.rows_stamped", "entry.known_before"),
    "GF7": ("entry.recipe.recipe_status", "extras.feed_errors", "extras.code.egress_tag"),
    "GA1": ("entry.coverage", "entry.accuracy.rows", "entry.accuracy.missing", "entry.http_first.wall_clock_hit"),
    "GA2": ("entry.accuracy.rows", "entry.accuracy.incomplete_rows", "entry.http_first.wall_clock_hit"),
    "GA3": (
        "entry.accuracy.rows",
        "entry.accuracy.hard_rows",
        "entry.accuracy.hard",
        "entry.accuracy.vpic_cached",
        "extras.vpic_gated",
        "extras.nhtsa_invariants",
    ),
    "GA4": ("entry.accuracy.rows", "entry.accuracy.hard"),
    "state": ("entry.dealer_id", "entry.verdict", "extras.walk", "extras.legacy"),
    "fleet": (
        "entry.dealer_id", "entry.verdict", "entry.rows", "entry.rows_stamped", "entry.active_after",
        "entry.vin_owner_conflicts", "entry.vin_owner_conflict_owners", "entry.reconcile.retired",
        "entry.reconcile.oldest", "extras.refused", "extras.per_condition",
    ),
}


# --- small helpers ---------------------------------------------------------------------------------


def _get(obj, *path):
    """Nested ``dict.get`` that returns ``None`` as soon as a level is not a dict."""
    for key in path:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj


def _num(value):
    """A finite number as ``float``, else ``None`` (booleans are not numbers)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def _r(value):
    return None if value is None else round(float(value), 4)


def _as_dt(value):
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _as_date(value):
    if isinstance(value, str) and len(value) >= 10:
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _res(result, value=None, threshold=None, kind="hard", **more):
    out = {"result": result, "value": value, "threshold": threshold, "kind": kind}
    out.update(more)
    return out


def _hysteresis(share, prev_failed, cfg):
    """Enter above ``share_enter``, leave at or below ``share_leave``, keep the previous result between."""
    if share > cfg.share_enter:
        return FAIL
    if share <= cfg.share_leave:
        return PASS
    return FAIL if prev_failed else PASS


def _exempt(registry, dealer_id, gate, now, cfg):
    """The unexpired registry entry for this dealer and gate, or ``None``. Expired entries exempt nothing."""
    if gate not in cfg.exempt_gates or not isinstance(registry, (list, tuple)):
        return None
    today = now.astimezone(timezone.utc).date()
    for item in registry:
        if not isinstance(item, dict) or item.get("dealer_id") != dealer_id or item.get("gate") != gate:
            continue
        expires = _as_date(item.get("expires"))
        if expires is not None and expires >= today:
            return item
    return None


# --- inputs ----------------------------------------------------------------------------------------


def _stamped(entry) -> float:
    value = _num(entry.get("rows_stamped"))
    return 0.0 if value is None else value


def _baseline(entry, extras, now):
    """``{source, B, g, run_id}`` from the extras, else the known lot, else ``None`` (no baseline)."""
    base = extras.get("baseline")
    base = base if isinstance(base, dict) else {}
    value = _num(base.get("B"))
    source = base.get("source")
    if value is not None and value > 0 and source in ("accepted_run", "known_lot", "stand_in"):
        when = _as_dt(base.get("date"))
        days = None if when is None else max(0.0, (now - when).total_seconds() / 86400.0)
        return {"source": source, "B": value, "g": days, "run_id": base.get("run_id")}
    for lot in (_num(entry.get("known_before")), _num(_get(extras, "yield", "known_lot"))):
        if lot is not None and lot > 0:
            return {"source": "known_lot", "B": lot, "g": None, "run_id": None}
    return None


def _refused(entry, extras):
    """``(parse_time, owner_guard)`` refused-row counts; the owner guard falls back to ``vin_owner_conflicts``."""
    block = extras.get("refused")
    block = block if isinstance(block, dict) else {}
    parse_time = _num(block.get("parse_time")) or 0.0
    guard = _num(block.get("owner_guard"))
    if guard is None:
        guard = _num(entry.get("vin_owner_conflicts")) or 0.0
    return parse_time, guard


def _accuracy(entry):
    acc = entry.get("accuracy")
    if not isinstance(acc, dict):
        return None
    rows = _num(acc.get("rows"))
    return acc if rows is not None and rows > 0 else None


def _wall_clock_cut(entry, extras) -> bool:
    if _get(extras, "legacy", "ignore_wall_clock") is True:
        return False
    return _get(entry, "http_first", "wall_clock_hit") is True


# --- gates -----------------------------------------------------------------------------------------


def _gf1(S, base, cfg):
    if base is None:
        if S < cfg.min_rows:
            return _res(FAIL, S, cfg.min_rows, basis="floor")
        return _res(CANNOT, S, cfg.min_rows, basis="no_baseline")
    threshold = max(cfg.min_rows, cfg.row_floor * base["B"])
    return _res(FAIL if S < threshold else PASS, S, _r(threshold), basis=base["source"])


def _yield_of(S, base):
    return None if base is None else S / base["B"]


def _gf2(S, base, cfg):
    y = _yield_of(S, base)
    if y is None:
        return _res(CANNOT, None, cfg.yield_low, basis="no_baseline")
    if base["source"] == "stand_in":
        if y < cfg.row_floor:
            return _res(FAIL, _r(y), cfg.row_floor, level="fail", basis="stand_in")
        return _res(CANNOT, _r(y), cfg.row_floor, basis="stand_in", warn=False)
    warn_at = None if base["g"] is None else max(cfg.yield_low, cfg.yield_warn_start - cfg.yield_warn_per_day * base["g"])
    if y < cfg.row_floor:
        return _res(FAIL, _r(y), cfg.row_floor, kind="soft", level="fail", warn=True, warn_below=_r(warn_at))
    if y < cfg.yield_low:
        return _res(FAIL, _r(y), cfg.yield_low, kind="soft", level="low", warn=True, warn_below=_r(warn_at))
    warn = warn_at is not None and y < warn_at
    return _res(PASS, _r(y), cfg.yield_low, kind="soft", level="warn" if warn else "ok", warn=warn, warn_below=_r(warn_at))


def _gf3(extras, cfg):
    walk = extras.get("walk")
    if not isinstance(walk, dict):
        return _res(CANNOT, None, None, basis="no_walk_report")
    total = _num(walk.get("site_total"))
    total = total if total is not None and total > 0 else None
    raw = _num(walk.get("raw_vins"))
    pages = _num(walk.get("pages"))
    if walk.get("pagination") == "none":
        hit = False
        cap = 1.0
    else:
        cap = _num(walk.get("cap"))
        page_size = _num(walk.get("page_size"))
        if cap is None:
            if total is not None and page_size:
                cap = float(min(cfg.walk_cap_max, math.ceil(total / page_size) + cfg.walk_cap_slack_pages))
            elif total is None:
                cap = float(cfg.walk_cap_unknown)
        hit = walk.get("cap_hit")
        if not isinstance(hit, bool):
            if pages is None or cap is None:
                return _res(CANNOT, None, None, basis="no_cap_information")
            hit = pages >= cap
    if hit:
        return _res(FAIL, pages, cap, basis="cap_hit")
    if total is None:
        return _res(PASS, pages, cap, basis="no_site_total")
    if raw is None:
        return _res(CANNOT, None, cfg.walk_total_share, basis="no_raw_vins")
    ratio = raw / total
    if ratio < cfg.walk_total_share:
        return _res(FAIL, _r(ratio), cfg.walk_total_share, basis="below_site_total")
    return _res(PASS, _r(ratio), cfg.walk_total_share, basis="site_total")


def _gf4(entry, extras, S, cfg):
    rows = _num(entry.get("rows"))
    parse_time, guard = _refused(entry, extras)
    if rows is None or rows + parse_time <= 0:
        return _res(CANNOT, None, cfg.refused_share, basis="no_feed_rows")
    feed = rows + parse_time
    refused = parse_time + guard
    r = refused / feed
    p = S / feed
    failed = r >= cfg.refused_share or (feed >= cfg.stamped_min_feed and p < cfg.stamped_share_min)
    return _res(FAIL if failed else PASS, _r(r), cfg.refused_share, stamped_share=_r(p), stamped_min=cfg.stamped_share_min)


def _gf5(entry, S, exempt, cfg):
    new, used = _num(entry.get("rows_new")), _num(entry.get("rows_used"))
    if new is None or used is None:
        return _res(CANNOT, None, cfg.condition_min_rows, basis="no_condition_counts")
    c = min(new, used)
    out = _res(PASS, c, 0, rows=S)
    if S >= cfg.condition_min_rows and c == 0:
        if exempt is None:
            out["result"] = FAIL
        else:
            out["accepted_gap"] = True
    if out["result"] == PASS and S >= cfg.condition_note_rows and c / S < cfg.condition_note_share:
        out["note"] = "one_condition_info"
    if exempt is not None:
        out["accepted_gap"] = True
    return out


def _gf6(extras, S, base, now, cfg):
    age = extras.get("recipe_age")
    if not isinstance(age, dict):
        return _res(CANNOT, None, cfg.recipe_max_age_days, basis="no_recipe_age")
    live = _num(age.get("live_recipes"))
    if live is not None and live <= 0:
        return _res(FAIL, 0, 1, basis="no_live_recipe")
    last = _as_dt(age.get("last_ok_at"))
    if live is None or last is None:
        return _res(CANNOT, None, cfg.recipe_max_age_days, basis="incomplete_recipe_age")
    days = max(0.0, (now - last).total_seconds() / 86400.0)
    if days <= cfg.recipe_max_age_days:
        return _res(PASS, _r(days), cfg.recipe_max_age_days)
    y = _yield_of(S, base)
    if y is None:
        return _res(CANNOT, _r(days), cfg.recipe_max_age_days, basis="no_baseline")
    return _res(FAIL if y < cfg.recipe_yield_min else PASS, _r(days), cfg.recipe_max_age_days, yield_=_r(y))


def _gf7(entry, extras, cfg):
    status = _get(entry, "recipe", "recipe_status")
    if isinstance(status, str) and _BLOCKED_RE.match(status):
        return _res(FAIL, None, None, basis="recipe_status")
    errs = extras.get("feed_errors")
    if isinstance(errs, dict):
        count = sum(_num(errs.get(k)) or 0.0 for k in ("403", "429", "challenge"))
        vins = _num(errs.get("vins_returned"))
        tag = _get(extras, "code", "egress_tag")
        if vins is not None:
            hit = count >= cfg.blocked_requests and vins == 0 and bool(tag)
            return _res(FAIL if hit else PASS, count, cfg.blocked_requests, basis="feed_errors")
    if isinstance(status, str):
        return _res(PASS, None, None, basis="recipe_status")
    return _res(CANNOT, None, None, basis="no_feed_health")


def _ga1(entry, extras, registry_hit, prev_trim_failed, cfg):
    """Returns ``(gate, trim_result, in_band)``; the trim result feeds the history record."""
    cov = entry.get("coverage")
    if not isinstance(cov, dict):
        return _res(CANNOT, None, cfg.key_coverage_min, kind="soft", basis="no_coverage"), None, False
    failed, missing, values = [], False, {}
    for name in ("price", "exterior_color"):
        v = _num(cov.get(name))
        values[name] = _r(v)
        if v is None:
            missing = True
        elif v < cfg.key_coverage_min:
            failed.append(name)
    mileage = _num(cov.get("used_mileage"))
    used_n = _num(cov.get("used_n"))
    if mileage is not None and used_n != 0:
        values["used_mileage"] = _r(mileage)
        if mileage < cfg.key_coverage_min:
            failed.append("used_mileage")
    trim_result, in_band = None, False
    acc = _accuracy(entry)
    if acc is not None and isinstance(acc.get("missing"), dict):
        gap = (_num(acc["missing"].get("trim")) or 0.0) / _num(acc["rows"])
        values["trim_gap"] = _r(gap)
        trim_result = _hysteresis(gap, prev_trim_failed, cfg)
        in_band = trim_result == PASS and gap > cfg.share_leave
        if trim_result == FAIL:
            failed.append("trim")
    else:
        trim = _num(cov.get("trim"))
        values["trim"] = _r(trim)
        if trim is None:
            missing = True
        elif trim < cfg.trim_capture_min:
            failed.append("trim")
    threshold = {"coverage_min": cfg.key_coverage_min, "trim_gap_enter": cfg.share_enter, "trim_gap_leave": cfg.share_leave}
    if failed:
        if registry_hit is not None:
            return _res(PASS, values, threshold, kind="soft", accepted_gap=True, failed=failed), trim_result, False
        if _wall_clock_cut(entry, extras):
            return _res(CANNOT, values, threshold, kind="soft", basis="wall_clock", failed=failed), trim_result, False
        return _res(FAIL, values, threshold, kind="soft", failed=failed), trim_result, False
    if missing:
        return _res(CANNOT, values, threshold, kind="soft", basis="missing_coverage"), trim_result, False
    return _res(PASS, values, threshold, kind="soft"), trim_result, in_band


def _share_gate(entry, extras, share, prev_failed, registry_hit, cfg):
    result = _hysteresis(share, prev_failed, cfg)
    in_band = result == PASS and share > cfg.share_leave
    out = _res(result, _r(share), {"enter": cfg.share_enter, "leave": cfg.share_leave}, kind="soft")
    if result == FAIL and registry_hit is not None:
        out["result"], out["accepted_gap"] = PASS, True
        return out, False
    if result == FAIL and _wall_clock_cut(entry, extras):
        out["result"], out["basis"] = CANNOT, "wall_clock"
        return out, False
    return out, in_band


def _ga2(entry, extras, prev_failed, registry_hit, cfg):
    acc = _accuracy(entry)
    incomplete = None if acc is None else _num(acc.get("incomplete_rows"))
    if incomplete is None:
        return _res(CANNOT, None, cfg.share_enter, kind="soft", basis="no_accuracy"), False
    return _share_gate(entry, extras, incomplete / _num(acc["rows"]), prev_failed, registry_hit, cfg)


def _ga3(entry, extras, prev_failed, registry_hit, cfg):
    acc = _accuracy(entry)
    hard_rows = None if acc is None else _num(acc.get("hard_rows"))
    if hard_rows is None:
        return _res(CANNOT, None, cfg.share_enter, kind="soft", basis="no_accuracy"), False
    rows = _num(acc["rows"])
    inv = extras.get("nhtsa_invariants")
    if isinstance(inv, dict):
        violations = (_num(inv.get("drivetrain")) or 0.0) + (_num(inv.get("electrification")) or 0.0)
        if violations / rows > cfg.invariant_max:
            return _res(FAIL, _r(violations / rows), cfg.invariant_max, kind="hard", invariant_breach=True), False
    discount = _get(extras, "legacy", "discount_hard")
    hard = acc.get("hard") if isinstance(acc.get("hard"), dict) else {}
    if isinstance(discount, (list, tuple)):
        hard_rows = max(0.0, hard_rows - sum(_num(hard.get(code)) or 0.0 for code in discount))
    cached = _num(acc.get("vpic_cached"))
    if cached is None:
        return _res(CANNOT, None, cfg.assessable_min, kind="soft", basis="no_vpic_count"), False
    assessable = cached - (_num(extras.get("vpic_gated")) or 0.0)
    if assessable < cfg.assessable_min * rows:
        return _res(CANNOT, _r(assessable / rows), cfg.assessable_min, kind="soft", basis="assessable_share"), False
    return _share_gate(entry, extras, hard_rows / assessable, prev_failed, registry_hit, cfg)


def _ga4(entry, cfg):
    acc = _accuracy(entry)
    if acc is None:
        return _res(CANNOT, None, cfg.identity_min, basis="no_accuracy")
    hard = acc.get("hard") if isinstance(acc.get("hard"), dict) else {}
    n = (_num(hard.get("vpic_make")) or 0.0) + (_num(hard.get("vpic_year")) or 0.0)
    threshold = max(float(cfg.identity_min), cfg.identity_share * _num(acc["rows"]))
    return _res(FAIL if n >= threshold else PASS, n, _r(threshold))


# --- state -----------------------------------------------------------------------------------------


def _widget(extras, S) -> bool:
    walk = extras.get("walk")
    if not isinstance(walk, dict) or walk.get("pagination") != "none":
        return False
    size = _num(walk.get("page_size"))
    return size is not None and size > 0 and S == size


def _permissions(verdict, gates, state):
    first4 = [gates[g]["result"] for g in ("GF1", "GF2", "GF3", "GF4")]
    clear = state != "blocked" and verdict != "error" and CANNOT not in first4
    retire = clear and all(r == PASS for r in first4)
    moves = clear and gates["GF4"]["result"] == PASS
    ident_ok = gates["GA4"]["result"] != FAIL and gates["GF4"]["result"] != FAIL
    promotion = state != "blocked" and (verdict in ("ok", "thin") or (verdict == "inaccurate" and ident_ok))
    return {"retire": bool(retire), "moves": bool(moves), "promotion": bool(promotion)}


def _clean_id(value):
    return clean_line(value, _DEALER_ID_MAX) if isinstance(value, str) else None


def evaluate_dealer(entry, extras, history, registry, cfg, now) -> dict:
    """Gate results, state, reason codes, permissions and the next history record for one dealer.

    ``entry`` is the triage row, ``extras`` the per-dealer fields of GATES.md section 11 (``None`` or ``{}``
    for none; ``legacy_inputs`` builds them for the 2026-11-03 files), ``history`` the dealer's record from
    the previous report (``None`` on the first scan), ``registry`` the parsed accepted-gap list and ``now`` a
    ``datetime``. Nothing is mutated.
    """
    entry = entry if isinstance(entry, dict) else {}
    extras = extras if isinstance(extras, dict) else {}
    history = history if isinstance(history, dict) else {}
    cfg = cfg or DEFAULT_CONFIG
    now = _as_dt(now)
    if now is None:
        raise ValueError("now must be a datetime")
    dealer_id = entry.get("dealer_id")
    verdict = entry.get("verdict") if isinstance(entry.get("verdict"), str) else None
    if verdict == "error":
        return _unassessed(entry, history, now)
    prev_gates = history.get("gates") if isinstance(history.get("gates"), dict) else {}

    def prev_failed(key):
        item = prev_gates.get(key)
        return isinstance(item, dict) and item.get("result") == FAIL

    def prev_fails(key):
        item = prev_gates.get(key)
        count = _num(item.get("fails")) if isinstance(item, dict) else None
        return int(count or 0)

    S = _stamped(entry)
    base = _baseline(entry, extras, now)
    exempt = {g: _exempt(registry, dealer_id, g, now, cfg) for g in cfg.exempt_gates}

    gates = {
        "GF1": _gf1(S, base, cfg),
        "GF2": _gf2(S, base, cfg),
        "GF3": _gf3(extras, cfg),
        "GF4": _gf4(entry, extras, S, cfg),
        "GF5": _gf5(entry, S, exempt["GF5"], cfg),
        "GF6": _gf6(extras, S, base, now, cfg),
        "GF7": _gf7(entry, extras, cfg),
    }
    ga1, trim_result, band1 = _ga1(entry, extras, exempt["GA1"], prev_failed("GA1.trim"), cfg)
    ga2, band2 = _ga2(entry, extras, prev_failed("GA2"), exempt["GA2"], cfg)
    ga3, band3 = _ga3(entry, extras, prev_failed("GA3"), exempt["GA3"], cfg)
    gates.update({"GA1": ga1, "GA2": ga2, "GA3": ga3, "GA4": _ga4(entry, cfg)})
    gates = {name: gates[name] for name in GATE_NAMES}

    at_once = _get(extras, "legacy", "soft_at_once") is True
    needed = 1 if at_once else cfg.soft_fail_scans
    next_gates = copy.deepcopy(prev_gates)
    counting, first_fail = [], []
    for name in SOFT_GATES:
        result = gates[name]["result"]
        if result == CANNOT:
            continue
        fails = prev_fails(name) + 1 if result == FAIL else 0
        next_gates[name] = {"result": result, "fails": fails}
        if result == FAIL:
            (counting if fails >= needed or gates[name].get("invariant_breach") else first_fail).append(name)
    if trim_result is not None and gates["GA1"]["result"] != CANNOT:
        next_gates["GA1.trim"] = {"result": trim_result, "fails": 0}
    gf2 = gates["GF2"]
    if gf2["result"] != CANNOT:
        warned = gf2.get("warn") is True
        next_gates["GF2"] = {"result": "warn" if warned else gf2["result"], "fails": prev_fails("GF2") + 1 if warned else 0}

    hard_code = [g for g in ("GF3", "GF4", "GA4") if gates[g]["result"] == FAIL]
    if gates["GA3"].get("invariant_breach"):
        hard_code.append("GA3")
    widget = _widget(extras, S)
    recipe_work = [g for g in ("GF1", "GF5", "GF6") if gates[g]["result"] == FAIL]
    if gates["GF2"]["result"] == FAIL:
        recipe_work.append("GF2")
    no_recipe = verdict == "no_recipe"

    reasons = []
    if gates["GF7"]["result"] == FAIL:
        state = "blocked"
        reasons.append("blocked:GF7")
    elif hard_code or widget:
        state = "needs_code_work"
        reasons += ["hard:" + g for g in hard_code] + (["hard:single_page_widget"] if widget else [])
    elif recipe_work or no_recipe:
        state = "needs_recipe_work"
        reasons += ["recipe:" + g for g in recipe_work] + (["recipe:no_recipe"] if no_recipe else [])
    elif counting:
        state = "needs_code_work"
        reasons += ["soft:" + g for g in counting]
    else:
        state = "ok"
    watch = False
    if state == "ok":
        notes = ["watch:first_fail:" + g for g in first_fail]
        notes += ["watch:band:" + g for g, b in (("GA1", band1), ("GA2", band2), ("GA3", band3)) if b]
        if gf2.get("warn") is True and gf2["result"] == PASS:
            notes.append("watch:warn:GF2")
        gf4 = gates["GF4"]
        if gf4["result"] == PASS and gf4["value"] is not None and gf4["value"] >= cfg.refused_watch:
            notes.append("watch:near:GF4")
        watch = bool(notes)
        reasons += notes

    since, streak = _since_streak(history, state, now)
    next_history = {"state": state, "since": since, "streak": streak, "gates": next_gates}
    failing = [g for g in GATE_NAMES if gates[g]["result"] == FAIL]
    unknown = [g for g in GATE_NAMES if gates[g]["result"] == CANNOT]
    return {
        "dealer_id": _clean_id(dealer_id),
        "verdict": verdict,
        "state": state,
        "watch": watch,
        "stamped": S,
        "since": since,
        "streak": streak,
        "gates": gates,
        "failing": failing,
        "cannot_assess": unknown,
        "reasons": reasons,
        "permissions": _permissions(verdict, gates, state),
        "history": next_history,
    }


def _since_streak(history, state, now):
    today = now.astimezone(timezone.utc).date().isoformat()
    if history.get("state") == state and isinstance(history.get("since"), str):
        streak = _num(history.get("streak"))
        return history["since"], int(streak or 0) + 1
    return today, 1


def _unassessed(entry, history, now):
    """A crashed or killed scan has no data: every gate is cannot_assess and the history is carried over."""
    gates = {name: _res(CANNOT, None, None, basis="verdict_error") for name in GATE_NAMES}
    return {
        "dealer_id": _clean_id(entry.get("dealer_id")),
        "verdict": "error",
        "state": "unassessed",
        "watch": False,
        "stamped": 0.0,
        "since": history.get("since") if isinstance(history.get("since"), str) else now.astimezone(timezone.utc).date().isoformat(),
        "streak": int(_num(history.get("streak")) or 0),
        "gates": gates,
        "failing": [],
        "cannot_assess": list(GATE_NAMES),
        "reasons": ["unassessed:error"],
        "permissions": {"retire": False, "moves": False, "promotion": False},
        "history": copy.deepcopy(history),
    }


def evaluate_run(entries, extras_by_dealer, histories, registry, cfg, now, outcome=None) -> dict:
    """Evaluate every dealer of a run; returns ``{dealers, counts, active, history}`` keyed by dealer id.

    ``extras_by_dealer`` and ``histories`` map dealer id to its extras and previous history record (missing
    means none). With ``outcome == "FAIL"`` (the fleet outcome) every returned history record is the input one,
    unchanged (GATES.md section 5). ``counts`` has one entry per state; ``active`` sums ``active_after`` per state.
    """
    extras_by_dealer = extras_by_dealer if isinstance(extras_by_dealer, dict) else {}
    histories = histories if isinstance(histories, dict) else {}
    dealers, history = {}, {}
    counts = {state: 0 for state in STATES}
    active = {state: 0 for state in STATES}
    for entry in entries:
        dealer_id = entry.get("dealer_id") if isinstance(entry, dict) else None
        if not isinstance(dealer_id, str) or not dealer_id:
            raise ValueError("every entry needs a dealer_id")
        if dealer_id in dealers:
            raise ValueError("duplicate dealer_id " + _clean_id(dealer_id))
        result = evaluate_dealer(entry, extras_by_dealer.get(dealer_id), histories.get(dealer_id), registry, cfg, now)
        dealers[dealer_id] = result
        counts[result["state"]] += 1
        active[result["state"]] += int(_num(entry.get("active_after")) or 0)
        previous = histories.get(dealer_id)
        history[dealer_id] = copy.deepcopy(previous) if outcome == "FAIL" and previous is not None else result["history"]
    order = sorted(dealers)
    return {
        "dealers": {k: dealers[k] for k in order},
        "counts": counts,
        "active": active,
        "history": {k: history[k] for k in order},
    }


# --- adapter for the 2026-11-03 files --------------------------------------------------------------


def legacy_inputs(entry, side) -> dict:
    """Build the ``extras`` that the 2026-11-03 triage rows lack (GATES.md section 12.2); nothing is mutated.

    Stand-ins: B = ``active_after`` (source ``stand_in``, so GF2 evaluates only its fail level); soft gates
    count at once; ``wall_clock_hit`` is ignored; ``accuracy.hard.vpic_fuel`` is subtracted from the hard rows
    before GA3. ``side`` is the parsed side-input file (``walk_capped`` and ``single_page_widget`` lists of
    dealer ids, ``refused_parse_time`` a map from dealer id to a count) or ``None``. GF6 gets no input and GF7
    and GA4 fire only on data the rows actually carry.
    """
    entry = entry if isinstance(entry, dict) else {}
    side = side if isinstance(side, dict) else {}
    dealer_id = entry.get("dealer_id")
    extras = {"legacy": {"soft_at_once": True, "ignore_wall_clock": True, "discount_hard": ["vpic_fuel"]}}
    active = _num(entry.get("active_after"))
    if active is not None and active > 0:
        extras["baseline"] = {"source": "stand_in", "B": active, "run_id": entry.get("run_id"), "date": None}
    capped = side.get("walk_capped")
    widgets = side.get("single_page_widget")
    if isinstance(capped, list) and dealer_id in capped:
        extras["walk"] = {
            "pages": 40, "cap": 40, "cap_hit": True, "site_total": None, "page_size": None,
            "pagination": "paged", "raw_vins": None, "kept_vins": None, "stop_reason": "page_cap",
        }
    elif isinstance(widgets, list) and dealer_id in widgets:
        size = _stamped(entry)
        extras["walk"] = {
            "pages": 1, "cap": None, "cap_hit": False, "site_total": None, "page_size": size,
            "pagination": "none", "raw_vins": None, "kept_vins": size, "stop_reason": "single_page",
        }
    parse_map = side.get("refused_parse_time")
    parse_time = _num(parse_map.get(dealer_id)) if isinstance(parse_map, dict) else None
    extras["refused"] = {
        "parse_time": int(parse_time or 0),
        "owner_guard": int(_num(entry.get("vin_owner_conflicts")) or 0),
    }
    return extras


# --- fleet gate (GATES.md section 7) ---------------------------------------------------------------

_EPS = 1e-9
HOLD = "hold"
OUTCOMES = ("PASS", "HOLD-PROD-WRITES", "FAIL")
FLEET_GATE_NAMES = ("FG1", "FG2", "FG3", "FG4", "FG5", "FG6", "FG7", "FG8")


def _lt(a, b) -> bool:
    return a < b - _EPS


def _gt(a, b) -> bool:
    return a > b + _EPS


def _ge(a, b) -> bool:
    return a >= b - _EPS


def _le(a, b) -> bool:
    return a <= b + _EPS


def _mean(values):
    values = [v for v in values if v is not None]
    return sum(values) / len(values) if values else None


def _median(values):
    values = sorted(values)
    if not values:
        return None
    mid = len(values) // 2
    return values[mid] if len(values) % 2 else (values[mid - 1] + values[mid]) / 2.0


def _days_between(later, earlier):
    later, earlier = _as_dt(later), _as_dt(earlier)
    if later is None or earlier is None:
        return None
    return max(0.0, (later - earlier).total_seconds() / 86400.0)


def retire_share_limit(g, cfg=DEFAULT_CONFIG):
    """``min(0.60, 0.10 + 0.015 g)`` for ``g`` days (``None`` when g is unknown)."""
    if g is None:
        return None
    return min(cfg.retire_share_cap, cfg.retire_share_base + cfg.retire_share_per_day * g)


def _prior_active(entry, extras):
    per_condition = extras.get("per_condition")
    if isinstance(per_condition, dict):
        parts = [_num(_get(per_condition, b, "active_before")) for b in ("new", "used", "unknown")]
        parts = [p for p in parts if p is not None]
        if parts:
            return sum(parts)
    return (_num(entry.get("active_after")) or 0.0) + (_num(_get(entry, "reconcile", "retired")) or 0.0)


def fleet_metrics(run, summary=None) -> dict:
    """The compact numbers of one run that FG2 to FG8 compare; stored with the gate report for later runs.

    ``run`` is ``{"entries": [triage rows], "results": evaluate_run output (optional), "extras": {id: extras}
    (optional), "started": ISO time (optional), "roster": int (optional), "retire_g_days": float (optional)}``.
    Prior active rows are the sum of ``per_condition.*.active_before`` when the extras carry it, else
    ``active_after + reconcile.retired`` (a stand-in for the 2026-11-03 files). The retirement age g is
    ``retire_g_days``, else the median over dealers that retired rows of the days from ``started`` back to
    ``reconcile.oldest``.
    """
    entries = [e for e in (run.get("entries") or []) if isinstance(e, dict)]
    extras_map = run.get("extras") if isinstance(run.get("extras"), dict) else {}
    results = _get(run, "results", "dealers")
    started = run.get("started")
    verdicts, stamped, pairs = {}, {}, {}
    sum_f = sum_x = sum_s = retired = prior_active = 0.0
    ages = []
    for entry in entries:
        dealer_id = entry.get("dealer_id")
        extras = extras_map.get(dealer_id) if isinstance(extras_map.get(dealer_id), dict) else {}
        verdict = entry.get("verdict") if isinstance(entry.get("verdict"), str) else "unknown"
        verdicts[verdict] = verdicts.get(verdict, 0) + 1
        s = _stamped(entry)
        sum_s += s
        if isinstance(dealer_id, str) and s > 0:
            stamped[dealer_id] = s
        parse_time, guard = _refused(entry, extras)
        sum_x += parse_time + guard
        sum_f += (_num(entry.get("rows")) or 0.0) + parse_time
        owners = entry.get("vin_owner_conflict_owners")
        if isinstance(owners, dict) and isinstance(dealer_id, str):
            for owner, count in owners.items():
                count = _num(count)
                if isinstance(owner, str) and count and count > 0:
                    key = owner + ">" + dealer_id
                    pairs[key] = pairs.get(key, 0) + int(count)
        gone = _num(_get(entry, "reconcile", "retired")) or 0.0
        retired += gone
        prior_active += _prior_active(entry, extras)
        if gone > 0:
            age = _days_between(started, _get(entry, "reconcile", "oldest"))
            if age is not None:
                ages.append(age)
    n = len(entries)
    blocked = None
    if isinstance(results, dict):
        blocked = sum(1 for r in results.values() if isinstance(r, dict) and r.get("state") == "blocked")
    bad = sum(verdicts.get(v, 0) for v in ("no_rows", "error", "no_recipe"))
    g = _num(run.get("retire_g_days"))
    return {
        "schema": 1,
        "started": started if isinstance(started, str) else None,
        "dealers": n,
        "verdicts": {k: verdicts[k] for k in sorted(verdicts)},
        "ok_share": verdicts.get("ok", 0) / n if n else None,
        "bad_share": bad / n if n else None,
        "error_share": verdicts.get("error", 0) / n if n else None,
        "blocked": blocked,
        "sum_S": sum_s,
        "sum_X": sum_x,
        "sum_F": sum_f,
        "retired": retired,
        "prior_active": prior_active,
        "retire_g_days": g if g is not None else _median(ages),
        "stamped": {k: stamped[k] for k in sorted(stamped)},
        "pairs": {k: pairs[k] for k in sorted(pairs)},
        "seconds": _num(_get(summary, "total_seconds")),
    }


def _fg(result, value=None, threshold=None, reasons=(), unassessed=()):
    return {"result": result, "value": value, "threshold": threshold, "reasons": list(reasons), "unassessed": list(unassessed)}


def _combine(pieces):
    """``pieces`` are ``(name, result, reason)``; FAIL beats HOLD beats PASS beats cannot_assess."""
    results = [p[1] for p in pieces]
    for level in (FAIL, HOLD, PASS):
        if level in results:
            winner = level
            break
    else:
        winner = CANNOT
    reasons = [p[2] for p in pieces if p[1] in (FAIL, HOLD) and p[2]]
    unassessed = [p[0] for p in pieces if p[1] == CANNOT]
    return winner, reasons, unassessed


def _killed(rc) -> bool:
    rc = _num(rc)
    return rc is not None and (rc < 0 or rc >= 128)


def _fg1(metrics, summary, roster, cfg):
    pieces, value = [], {}
    assessed = _num(_get(summary, "assessed"))
    if assessed is None:
        assessed = float(metrics["dealers"]) if metrics["dealers"] else None
    if roster is not None and roster > 0 and assessed is not None:
        share = assessed / roster
        value["assessed_share"] = _r(share)
        pieces.append(("assessed", FAIL if _lt(share, cfg.fg_assessed_min) else PASS, "FG1:assessed_share"))
    else:
        pieces.append(("assessed", CANNOT, ""))
    rcs = _get(summary, "shard_rc")
    shards = _num(_get(summary, "shards"))
    if isinstance(rcs, dict):
        bad = sorted(k for k, v in rcs.items() if _num(v) != 0)
        missing = shards is not None and len(rcs) != shards
        value["shard_rc_failures"] = len(bad)
        pieces.append(("shard_rc", FAIL if bad or missing else PASS, "FG1:shard_rc"))
    else:
        pieces.append(("shard_rc", CANNOT, ""))
    post = _get(summary, "post")
    if isinstance(post, dict):
        bad = [k for k, v in post.items() if _num(_get(v, "rc")) != 0]
        value["post_failures"] = len(bad)
        pieces.append(("post", FAIL if bad else PASS, "FG1:post_step"))
    else:
        pieces.append(("post", CANNOT, ""))
    errors = metrics["error_share"]
    if errors is not None:
        value["error_share"] = _r(errors)
        pieces.append(("error_share", FAIL if _ge(errors, cfg.fg_error_max) else PASS, "FG1:error_share"))
    else:
        pieces.append(("error_share", CANNOT, ""))
    result, reasons, unassessed = _combine(pieces)
    return _fg(result, value, {"assessed_min": cfg.fg_assessed_min, "error_max": cfg.fg_error_max}, reasons, unassessed)


def _fg2(metrics, trailing, cfg):
    pieces, value = [], {}
    ok, bad = metrics["ok_share"], metrics["bad_share"]
    if ok is None:
        return _fg(CANNOT, None, None, (), ("ok_share",))
    value["ok_share"], value["bad_share"] = _r(ok), _r(bad)
    pieces.append(("ok_floor", HOLD if _lt(ok, cfg.fg_ok_min) else PASS, "FG2:ok_share_below_floor"))
    if trailing["ok_share"] is not None:
        pieces.append(("ok_drop", HOLD if _lt(ok, trailing["ok_share"] - cfg.fg_ok_drop) else PASS, "FG2:ok_share_drop"))
    else:
        pieces.append(("ok_drop", CANNOT, ""))
    limit = cfg.fg_bad_floor
    if trailing["bad_share"] is not None:
        limit = max(limit, trailing["bad_share"] + cfg.fg_bad_rise)
    pieces.append(("bad_share", HOLD if _gt(bad, limit) else PASS, "FG2:bad_share"))
    result, reasons, unassessed = _combine(pieces)
    return _fg(result, value, {"ok_min": cfg.fg_ok_min, "bad_max": _r(limit)}, reasons, unassessed)


def _fg3(metrics, previous, cfg):
    before = _num(previous.get("sum_S")) if previous else None
    if not before or before <= 0:
        return _fg(CANNOT, None, [cfg.fg_volume_low, cfg.fg_volume_high], (), ("previous_run",))
    ratio = metrics["sum_S"] / before
    bad = _lt(ratio, cfg.fg_volume_low) or _gt(ratio, cfg.fg_volume_high)
    return _fg(HOLD if bad else PASS, _r(ratio), [cfg.fg_volume_low, cfg.fg_volume_high], ["FG3:volume"] if bad else ())


def _fg4(metrics, previous, cfg):
    old = previous.get("stamped") if previous else None
    g = _days_between(metrics["started"], previous.get("started")) if previous else None
    if not isinstance(old, dict) or g is None:
        return _fg(CANNOT, None, cfg.fg_shrink_share, (), ("previous_run",))
    level = max(cfg.yield_low, cfg.yield_warn_start - cfg.yield_warn_per_day * g)
    both = [k for k in metrics["stamped"] if (_num(old.get(k)) or 0) > 0]
    if not both:
        return _fg(CANNOT, None, cfg.fg_shrink_share, (), ("dealers_in_both_runs",))
    below = sum(1 for k in both if _lt(metrics["stamped"][k] / _num(old[k]), level))
    share = below / len(both)
    bad = _gt(share, cfg.fg_shrink_share)
    return _fg(HOLD if bad else PASS, _r(share), {"max_share": cfg.fg_shrink_share, "warn_level": _r(level)},
               ["FG4:shrink_breadth"] if bad else ())


def _fg5(metrics, previous, cfg):
    pieces, value = [], {}
    if metrics["sum_F"] > 0:
        share = metrics["sum_X"] / metrics["sum_F"]
        value["share"] = _r(share)
        pieces.append(("share", HOLD if _gt(share, cfg.fg_attribution_share) else PASS, "FG5:attribution_share"))
    else:
        pieces.append(("share", CANNOT, ""))
    old = previous.get("pairs") if previous else None
    if isinstance(old, dict):
        fresh = sorted(((v, k) for k, v in metrics["pairs"].items() if v > cfg.fg_pair_vins and k not in old), reverse=True)
        value["new_pairs"] = [_clean_id(k) for _, k in fresh[:10]]
        pieces.append(("new_pair", HOLD if fresh else PASS, "FG5:new_pair"))
    else:
        pieces.append(("new_pair", CANNOT, ""))
    result, reasons, unassessed = _combine(pieces)
    return _fg(result, value, {"share_max": cfg.fg_attribution_share, "pair_vins": cfg.fg_pair_vins}, reasons, unassessed)


def _fg6(metrics, cfg):
    retired, prior = metrics["retired"], metrics["prior_active"]
    if retired <= 0:
        return _fg(PASS, 0.0, None)
    limit = retire_share_limit(metrics["retire_g_days"], cfg)
    if prior <= 0 or limit is None:
        return _fg(CANNOT, None, None, (), ("retirement_age" if limit is None else "prior_active",))
    share = retired / prior
    bad = _gt(share, limit)
    return _fg(HOLD if bad else PASS, _r(share), _r(limit), ["FG6:retirement_load"] if bad else ())


def _fg7(metrics, cfg):
    n, blocked = metrics["dealers"], metrics["blocked"]
    if not n or blocked is None:
        return _fg(CANNOT, None, [cfg.fg_blocked_hold, cfg.fg_blocked_fail], (), ("states",))
    share = blocked / n
    if _gt(share, cfg.fg_blocked_fail):
        result, reasons = FAIL, ["FG7:blocked_share_fail"]
    elif _gt(share, cfg.fg_blocked_hold):
        result, reasons = HOLD, ["FG7:blocked_share"]
    else:
        result, reasons = PASS, []
    return _fg(result, _r(share), [cfg.fg_blocked_hold, cfg.fg_blocked_fail], reasons)


def _fg8(metrics, summary, trailing, cfg):
    rcs = _get(summary, "shard_rc")
    if not isinstance(rcs, dict):
        return _fg(CANNOT, None, cfg.fg_duration_factor, (), ("summary",))
    pieces = [("killed", FAIL if any(_killed(v) for v in rcs.values()) else PASS, "FG8:killed")]
    seconds = metrics["seconds"]
    if seconds is not None and trailing["seconds"]:
        pieces.append(("duration", HOLD if _gt(seconds, cfg.fg_duration_factor * trailing["seconds"]) else PASS, "FG8:duration"))
    else:
        pieces.append(("duration", CANNOT, ""))
    result, reasons, unassessed = _combine(pieces)
    return _fg(result, seconds, {"factor": cfg.fg_duration_factor, "trailing_seconds": _r(trailing["seconds"])}, reasons, unassessed)


def evaluate_fleet(run, prior_runs, summary, cfg) -> dict:
    """The fleet verdict: ``PASS``, ``HOLD-PROD-WRITES`` or ``FAIL`` with FG1 to FG8 (GATES.md section 7).

    ``run`` as in ``fleet_metrics``; ``prior_runs`` is a list, newest first, of ``{"outcome", "metrics"}``
    (the stored fleet block of earlier reports): only PASS runs count, "trailing" is the mean of up to
    ``fg_trailing_runs`` of them and "previous" is the newest. Without any, relative metrics are
    ``cannot_assess`` and only the absolute limits apply. ``summary`` is ``fleet_summary.json`` or ``None``.
    A HOLD from FG5 or FG6 stops retirements and moves; any HOLD stops retirements and moves and limits
    promotion to ``ok`` dealers without ``watch``; FAIL promotes nothing. Nothing is mutated.
    """
    cfg = cfg or DEFAULT_CONFIG
    run = run if isinstance(run, dict) else {}
    summary = summary if isinstance(summary, dict) else None
    metrics = fleet_metrics(run, summary)
    priors = [p["metrics"] for p in (prior_runs or [])
              if isinstance(p, dict) and p.get("outcome") == "PASS" and isinstance(p.get("metrics"), dict)]
    priors = priors[: max(0, cfg.fg_trailing_runs)]
    previous = priors[0] if priors else None
    trailing = {
        "runs": len(priors),
        "ok_share": _mean([_num(p.get("ok_share")) for p in priors]),
        "bad_share": _mean([_num(p.get("bad_share")) for p in priors]),
        "seconds": _mean([_num(p.get("seconds")) for p in priors]),
    }
    roster = _num(_get(summary, "dealers"))
    if roster is None:
        roster = _num(run.get("roster"))
    gates = {
        "FG1": _fg1(metrics, summary, roster, cfg),
        "FG2": _fg2(metrics, trailing, cfg),
        "FG3": _fg3(metrics, previous, cfg),
        "FG4": _fg4(metrics, previous, cfg),
        "FG5": _fg5(metrics, previous, cfg),
        "FG6": _fg6(metrics, cfg),
        "FG7": _fg7(metrics, cfg),
        "FG8": _fg8(metrics, summary, trailing, cfg),
    }
    results = [gates[g]["result"] for g in FLEET_GATE_NAMES]
    outcome = "FAIL" if FAIL in results else "HOLD-PROD-WRITES" if HOLD in results else "PASS"
    stops = [g for g in ("FG5", "FG6") if gates[g]["result"] == HOLD]
    if outcome == "PASS":
        writes = {"retire": True, "moves": True, "promote": "gated"}
    elif outcome == "HOLD-PROD-WRITES":
        writes = {"retire": False, "moves": False, "promote": "ok_without_watch"}
    else:
        writes = {"retire": False, "moves": False, "promote": "none"}
    return {
        "outcome": outcome,
        "gates": gates,
        "reasons": [r for g in FLEET_GATE_NAMES for r in gates[g]["reasons"]],
        "stops_retire_and_moves": stops,
        "writes": writes,
        "trailing": trailing,
        "metrics": metrics,
    }


# --- write gates (GATES.md section 6) --------------------------------------------------------------


def _dealer_checks(dealer, reasons, cfg):
    """Shared dealer-level conditions of WR and WM; returns the GF4 measurement when there is one."""
    if not isinstance(dealer, dict) or not isinstance(dealer.get("gates"), dict):
        reasons.append("no_dealer_result")
        return None
    if dealer.get("state") in ("blocked", "unassessed"):
        reasons.append("dealer_state:" + str(dealer.get("state")))
    for name in ("GF1", "GF2", "GF3", "GF4"):
        if _get(dealer, "gates", name, "result") == CANNOT:
            reasons.append(name + ":cannot_assess")
    r = _get(dealer, "gates", "GF4", "value")
    if r is not None and _ge(r, cfg.refused_share):
        reasons.append("GF4:refused_share")
    elif _get(dealer, "gates", "GF4", "result") == FAIL:
        reasons.append("GF4:failed")  # the stamped-share clause, with R itself under the limit
    return r


def _fleet_allows(fleet, key, reasons):
    if not isinstance(fleet, dict) or not isinstance(fleet.get("writes"), dict):
        reasons.append("no_fleet_verdict")
    elif fleet["writes"].get(key) is not True:
        reasons.append("fleet:" + str(fleet.get("outcome")))


def can_retire(dealer, bucket, candidates, prior_active, fleet, cfg, now) -> dict:
    """WR: may the rows of one condition bucket that this run did not see be retired?

    ``dealer`` is the ``evaluate_dealer`` result, ``bucket`` is ``{"name": "new"|"used", "active_before",
    "stamped", "seen_again"}``, ``candidates`` lists ``{"vin", "first_miss_at", "scraped_at"}`` for the rows
    to retire (``first_miss_at`` is the earlier eligible run that already missed the VIN, with no sighting
    since; ``None`` for a first miss), ``prior_active`` the dealer's active rows before the run, ``fleet`` the
    ``evaluate_fleet`` result. Returns ``{allowed, retire, strike, reasons, share, limit}``: ``strike``
    names the VINs whose first miss is recorded now (only when the run is eligible, that is ``allowed``).
    A missing input denies.
    """
    cfg = cfg or DEFAULT_CONFIG
    reasons = []
    _fleet_allows(fleet, "retire", reasons)
    _dealer_checks(dealer, reasons, cfg)
    if isinstance(dealer, dict):
        if dealer.get("verdict") == "no_rows":
            reasons.append("verdict:no_rows")
        if _get(dealer, "gates", "GF3", "result") == FAIL:
            reasons.append("GF3:walk_incomplete")
        if _get(dealer, "gates", "GF1", "result") == FAIL:
            reasons.append("GF1:failed")
        if _get(dealer, "gates", "GF2", "result") == FAIL:
            reasons.append("GF2:yield_low")
    bucket = bucket if isinstance(bucket, dict) else {}
    if bucket.get("name") not in ("new", "used"):
        reasons.append("bucket:not_judged")
    active, stamped, seen = (_num(bucket.get(k)) for k in ("active_before", "stamped", "seen_again"))
    if active is None or active <= 0 or stamped is None or seen is None:
        reasons.append("bucket:data_missing")
    else:
        if stamped < cfg.wr_bucket_min_rows:
            reasons.append("bucket:rows_below_minimum")
        if _lt(stamped / active, cfg.wr_stamped_share):
            reasons.append("bucket:stamped_share")
        if _lt(seen / active, cfg.wr_seen_share):
            reasons.append("bucket:seen_share")
    now = _as_dt(now)
    retire, strike, dates = [], [], []
    for item in candidates if isinstance(candidates, (list, tuple)) else []:
        if not isinstance(item, dict) or not isinstance(item.get("vin"), str):
            continue
        missed = _as_dt(item.get("first_miss_at"))
        if now is not None and missed is not None and _ge((now - missed).total_seconds() / 3600.0, cfg.wr_strike_hours):
            retire.append(item["vin"])
            dates.append(_as_dt(item.get("scraped_at")))
        else:
            strike.append(item["vin"])
    share = limit = None
    if now is None:
        reasons.append("now:missing")
    prior = _num(prior_active)
    if retire:
        if prior is None or prior <= 0:
            reasons.append("prior_active:missing")
        elif any(d is None for d in dates):
            reasons.append("scraped_at:missing")
        elif now is not None:
            g = _median([(now - d).total_seconds() / 86400.0 for d in dates])
            limit = retire_share_limit(max(0.0, g), cfg)
            share = len(retire) / prior
            if _gt(share, limit):
                reasons.append("retire_share_exceeded")
    allowed = not reasons
    return {
        "allowed": allowed,
        "retire": retire if allowed else [],
        "strike": strike if allowed else [],
        "reasons": reasons,
        "share": _r(share),
        "limit": _r(limit),
    }


def can_move(claimant, evidence, fleet, cfg) -> dict:
    """WM: may an active VIN change ``dealer_id`` to the claimant?

    ``claimant`` is the claimant's ``evaluate_dealer`` result; ``evidence`` is ``{"rooftop_evidence": bool
    (per-car rooftop evidence for the claimant), "owner_listed": bool (the current owner's run listed the VIN),
    "owner_capture_ok": bool (the owner's capture would itself pass WR), "moves_in_run": int (moves already
    allowed in this run)}``. Anything missing or not exactly the required value denies.
    """
    cfg = cfg or DEFAULT_CONFIG
    reasons = []
    evidence = evidence if isinstance(evidence, dict) else {}
    _fleet_allows(fleet, "moves", reasons)
    _dealer_checks(claimant, reasons, cfg)
    if evidence.get("rooftop_evidence") is not True:
        reasons.append("no_rooftop_evidence")
    if evidence.get("owner_listed") is not False:
        reasons.append("owner_lists_vin" if evidence.get("owner_listed") is True else "owner_listing_unknown")
    if evidence.get("owner_capture_ok") is not True:
        reasons.append("owner_capture_weak")
    stamped = _num(_get(claimant, "stamped"))
    done = _num(evidence.get("moves_in_run"))
    limit = None
    if stamped is None or done is None:
        reasons.append("moves:data_missing")
    else:
        limit = max(float(cfg.wm_moves_floor), cfg.wm_moves_share * stamped)
        if _gt(done + 1, limit):
            reasons.append("moves:per_run_limit")
    return {"allowed": not reasons, "reasons": reasons, "limit": _r(limit)}


def can_promote(dealer, fleet, cfg) -> dict:
    """WP: may the dealer's fresh rows replace its previous ones?

    PASS: verdicts ok, thin and inaccurate (inaccurate only without a GA4 or GF4 failure), never a blocked
    dealer. HOLD-PROD-WRITES: only ``ok`` dealers without ``watch``. FAIL: nothing. no_rows, error and
    no_recipe carry the previous rows forward.
    """
    reasons = []
    if not isinstance(dealer, dict) or not isinstance(dealer.get("permissions"), dict):
        return {"allowed": False, "reasons": ["no_dealer_result"]}
    if not isinstance(fleet, dict) or not isinstance(fleet.get("writes"), dict):
        reasons.append("no_fleet_verdict")
    else:
        mode = fleet["writes"].get("promote")
        if mode == "none":
            reasons.append("fleet:" + str(fleet.get("outcome")))
        elif mode == "ok_without_watch" and (dealer.get("state") != "ok" or dealer.get("watch") is not False):
            reasons.append("fleet:hold_ok_without_watch_only")
        elif mode not in ("gated", "ok_without_watch"):
            reasons.append("fleet:unknown_mode")
    if dealer["permissions"].get("promotion") is not True:
        reasons.append("dealer:carry_forward")
    return {"allowed": not reasons, "reasons": reasons}


# --- fix verification (GATES.md section 9) ---------------------------------------------------------

FIX_STATUSES = ("fixed", "fixed_pending", "fixed_unverifiable", "not_fixed")


def _scan_record(record):
    """``(dealer result, triage row, extras, scan time)`` of a scan record, tolerating missing parts."""
    record = record if isinstance(record, dict) else {}
    dealer = record.get("dealer") if isinstance(record.get("dealer"), dict) else {}
    entry = record.get("entry") if isinstance(record.get("entry"), dict) else {}
    extras = record.get("extras") if isinstance(record.get("extras"), dict) else {}
    return dealer, entry, extras, _as_dt(record.get("at"))


def _check_scan(record, targets, cfg):
    """Judge one scan: ``(status, failures, unverifiable)`` with status ``pass``, ``fail`` or ``unverifiable``.

    ``targets`` are the gates that failed before the fix. A failing gate in this scan, a target that is not
    ``pass``, or a target without its margin is a failure; missing NHTSA evidence only makes the scan
    unverifiable.
    """
    dealer, entry, extras, _ = _scan_record(record)
    gates = dealer.get("gates") if isinstance(dealer.get("gates"), dict) else {}
    failures, unverifiable = [], []

    def result(name):
        return _get(gates, name, "result")

    if not gates:
        return "fail", ["no_dealer_result"], []
    if dealer.get("state") in ("blocked", "unassessed"):
        failures.append("state:" + str(dealer.get("state")))
    failures += [g + ":failing" for g in (dealer.get("failing") or ()) if g not in targets]
    for name in targets:
        if name != "GF2" and result(name) != PASS:
            failures.append(name + (":cannot_assess" if result(name) == CANNOT else ":not_cleared"))

    if {"GF1", "GF2", "GF6"} & set(targets):
        y = _num(_get(gates, "GF2", "value"))
        total, walked = _num(_get(extras, "walk", "site_total")), _num(_get(extras, "walk", "raw_vins"))
        ratio = walked / total if total and walked is not None else None
        if (y is None or _lt(y, cfg.fix_yield_min)) and (ratio is None or _lt(ratio, cfg.fix_walk_share)):
            failures.append("yield:below_margin" if y is not None or ratio is not None else "yield:no_evidence")
    if "GF4" in targets:
        r = _num(_get(gates, "GF4", "value"))
        if r is None or _ge(r, cfg.fix_refused_max):
            failures.append("refused_share:above_margin")
    if "GA1" in targets:
        values = _get(gates, "GA1", "value")
        values = values if isinstance(values, dict) else {}
        for name in ("price", "exterior_color", "used_mileage", "trim"):
            v = _num(values.get(name))
            if name in values and (v is None or _lt(v, cfg.fix_key_coverage)):
                failures.append("coverage:" + name)
        gap = _num(values.get("trim_gap"))
        if "trim_gap" in values and (gap is None or _gt(gap, cfg.fix_share_max)):
            failures.append("coverage:trim_gap")
    if "GA2" in targets:
        share = _num(_get(gates, "GA2", "value"))
        if share is None or _gt(share, cfg.fix_share_max):
            failures.append("incomplete_share:above_margin")
    if "GF5" in targets or (_get(extras, "site_census", "new") and _get(extras, "site_census", "used")):
        if min(_num(entry.get("rows_new")) or 0.0, _num(entry.get("rows_used")) or 0.0) <= 0:
            failures.append("condition:one_side_missing")

    acc = entry.get("accuracy") if isinstance(entry.get("accuracy"), dict) else None
    rows = _num(acc.get("rows")) if acc else None
    cached = _num(acc.get("vpic_cached")) if acc else None
    if not rows or rows <= 0 or cached is None:
        unverifiable.append("nhtsa:no_accuracy")
    else:
        if _lt(cached / rows, cfg.fix_decode_min):
            failures.append("nhtsa:decode_cached_share")
        if _lt((cached - (_num(extras.get("vpic_gated")) or 0.0)) / rows, cfg.assessable_min):
            unverifiable.append("nhtsa:assessable_share")
        inv = extras.get("nhtsa_invariants")
        if isinstance(inv, dict):
            violations = (_num(inv.get("drivetrain")) or 0.0) + (_num(inv.get("electrification")) or 0.0)
            if _gt(violations / rows, cfg.invariant_max):
                failures.append("nhtsa:invariants")
        else:
            unverifiable.append("nhtsa:no_invariants")
        hard = acc.get("hard") if isinstance(acc.get("hard"), dict) else {}
        mismatches = (_num(hard.get("vpic_make")) or 0.0) + (_num(hard.get("vpic_year")) or 0.0)
        if _gt(mismatches, max(float(cfg.fix_identity_min), cfg.fix_identity_share * rows)):
            failures.append("nhtsa:identity")
        share = _num(_get(gates, "GA3", "value"))
        if result("GA3") in (PASS, FAIL) and share is not None and _gt(share, cfg.fix_share_max):
            failures.append("nhtsa:hard_share")
    failures = sorted(set(failures))
    return ("fail" if failures else "unverifiable" if unverifiable else "pass"), failures, sorted(set(unverifiable))


def verify_fix(before, after, second_after, cfg=None) -> dict:
    """Is a dealer fixed? (GATES.md section 9; the log files and the 10-VIN write audit stay with the analysis session.)

    Each argument is a scan record ``{"dealer": evaluate_dealer result, "entry": triage row, "extras": extras,
    "at": scan time}``; ``second_after`` may be ``None``. The gates that failed in ``before`` are the targets.
    A scan passes when no gate fails, every target is ``pass`` with its margin (key coverage >= 0.93, incomplete,
    trim-gap and hard share <= 3.5 %, Y >= 0.90 or W >= 0.95 T, R < 0.03), both conditions are present where
    the site sells both (``extras.site_census`` with ``new`` and ``used`` counts, or a failing GF5 before), and
    the NHTSA checks hold (decode cached for >= 98 % of rows, invariant violations <= 0.2 %, identity
    mismatches <= max(2, 0.3 % of rows)). Missing evidence for the NHTSA part (no accuracy block, assessable
    share under 75 %, no invariant counters) makes the scan ``unverifiable`` instead of failing it.

    Returns ``{status, targets, hours_between, after, second_after, reasons}`` where ``status`` is
    ``not_fixed`` (a check failed), ``fixed_pending`` (``after`` passes but the second scan is missing, less
    than 20 h later or has no time), ``fixed_unverifiable`` (both scans pass but NHTSA evidence is missing) or
    ``fixed``. Nothing is mutated.
    """
    cfg = cfg or DEFAULT_CONFIG
    before_dealer = _scan_record(before)[0]
    targets = sorted(g for g in (before_dealer.get("failing") or ()) if g in GATE_NAMES)
    status1, failures1, unverified1 = _check_scan(after, targets, cfg)
    out = {
        "status": "not_fixed",
        "targets": targets,
        "hours_between": None,
        "after": {"status": status1, "failures": failures1, "unverifiable": unverified1},
        "second_after": None,
        "reasons": [],
    }
    if status1 == "fail":
        out["reasons"] = failures1
        return out
    first, second = _scan_record(after)[3], _scan_record(second_after)[3] if second_after is not None else None
    hours = None if first is None or second is None else (second - first).total_seconds() / 3600.0
    out["hours_between"] = _r(hours)
    if second_after is None:
        out.update(status="fixed_pending", reasons=["second_scan_missing"])
        return out
    if hours is None:
        out.update(status="fixed_pending", reasons=["scan_time_missing"])
        return out
    if _lt(hours, cfg.fix_second_scan_hours):
        out.update(status="fixed_pending", reasons=["second_scan_too_soon"])
        return out
    status2, failures2, unverified2 = _check_scan(second_after, targets, cfg)
    out["second_after"] = {"status": status2, "failures": failures2, "unverifiable": unverified2}
    if status2 == "fail":
        out["reasons"] = ["second_scan:" + f for f in failures2]
    elif "unverifiable" in (status1, status2):
        out.update(status="fixed_unverifiable", reasons=unverified1 + unverified2)
    else:
        out["status"] = "fixed"
    return out
