"""Dealer gates and dealer state of the scan loop (card L8; specification: docs/scan_loop/GATES.md).

Pure and standard library only: callers pass the triage row (``entry``), the per-dealer ``extras``
of GATES.md section 11, the dealer's ``history`` from the previous gate report, the accepted-gap
``registry``, a ``GateConfig`` and ``now`` (a ``datetime``). Nothing is read from the clock, the
environment or a file, no argument is mutated, and every threshold lives in ``GateConfig``.
Changing a threshold is a pull request that also changes GATES.md.

A gate returns ``pass``, ``fail`` or ``cannot_assess`` with the measured value and the threshold.
A missing input is ``cannot_assess``, never a guess. The fleet gate and the write predicates are
later cards (L9, L10); the per-dealer ``permissions`` here are only the coarse dealer-level part.

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
