"""Improvement queue of the scan loop (card L10; specification: docs/scan_loop/GATES.md section 8).

Pure and standard library only: callers pass the triage rows, the ``evaluate_run`` result, the per-dealer
extras, the learning outcomes and the ``LoopConfig``; nothing is read from the clock, the environment or a
file, no argument is mutated, and every score constant lives in the frozen ``QueueConfig``.

Score of a dealer d, over its reasons r (A_r cars affected, s_r severity, t_r tractability)::

    Q_d = max_r(A_r s_r t_r) + 0.25 x the sum of the other terms

    reason (code)          A_r (cars)                                          s     t prior
    rot                    A - S (A when S is 0), see below                    1.0   0.60
    attribution            max(min(X, A), A - S)                               1.0   0.25
    identity               rows flagged (vpic_make + vpic_year)                1.0   0.50
    one_condition          max(A - S, 0.36 S)                                  0.8   0.50
    truncated_walk         T - W, else max(0.25 S, A - S)                      0.8   0.80
    single_page_widget     A - S                                               0.8   0.90
    price_gap              (1 - price coverage) S                              0.8   0.15
    trim_gap, colour_gap,
    incomplete,
    real_discrepancy       gap rows or hard rows                               0.6   0.45 (one value each)
    secondary_gaps         rows of the weak fields outside the key fields      0.3   0.20
    comparator_only        hard rows the comparator discount removed           0.1   0.95

A is ``active_after``, S the stamped rows, X the refused rows (parser plus owner guard), T the site total and
W the raw VINs walked. The reasons come from the gate results of ``evaluate_dealer``: rot from a no_rows or
no_recipe verdict, GF6, or GF1/GF2 failing with GF3, GF4 and the widget test clean; attribution from GF4 or
parser-refused rows; identity from GA4; one_condition from GF5; truncated_walk from GF3; the widget from the
state reason ``hard:single_page_widget``; price, colour, trim and used_mileage gaps from a failing GA1;
incomplete from GA2; real_discrepancy from GA3; comparator_only from a GA3 that passes only after the
``extras.legacy.discount_hard`` discount; secondary_gaps (only for dealers that need work) from ``weak_fields``
and a used_mileage gap. Blocked dealers, dealers named in ``site_side`` (class d) and ``unassessed`` dealers
carry no score: the first two go to the owner list, an ``unassessed`` dealer is simply rescanned.

Fix units: each dealer's Q goes to the unit of its largest term (a dealer sits in exactly one unit). A
recipe unit (rot, one_condition) is one dealer with E = 1 hour; a template unit (attribution, widget, gaps,
discrepancies) is one provider family with E = 8 + 0.05 x blast radius; a guard unit (identity, truncated
walk, comparator) is one reason with E = 6. Unit score = sum of Q over its dealers / E.

Learning: ``t = (a + successes) / (a + b + tries)`` with ``a = 4 p`` and ``b = 4 (1 - p)`` for the prior p
(``learned_tractability``); a success is a dealer that ``gates.verify_fix`` calls ``fixed``.
"""

from __future__ import annotations

from dataclasses import dataclass

from .gates import DEFAULT_CONFIG, FAIL, PASS, _get, _num, _refused
from .scrub import clean_line

REASONS = (
    "rot",
    "attribution",
    "identity",
    "one_condition",
    "truncated_walk",
    "single_page_widget",
    "price_gap",
    "trim_gap",
    "colour_gap",
    "incomplete",
    "real_discrepancy",
    "secondary_gaps",
    "comparator_only",
)

_DEALER_ID_MAX = 80
_FAMILY_MAX = 40
_KEY_FIELDS = ("price", "trim", "exterior_color", "used_mileage")


@dataclass(frozen=True)
class QueueConfig:
    """Severity, tractability prior and fix kind per reason, plus the effort of a fix unit."""

    # (reason, severity, prior tractability)
    table: tuple = (
        ("rot", 1.0, 0.60),
        ("attribution", 1.0, 0.25),
        ("identity", 1.0, 0.50),
        ("one_condition", 0.8, 0.50),
        ("truncated_walk", 0.8, 0.80),
        ("single_page_widget", 0.8, 0.90),
        ("price_gap", 0.8, 0.15),
        ("trim_gap", 0.6, 0.45),
        ("colour_gap", 0.6, 0.45),
        ("incomplete", 0.6, 0.45),
        ("real_discrepancy", 0.6, 0.45),
        ("secondary_gaps", 0.3, 0.20),
        ("comparator_only", 0.1, 0.95),
    )
    # (reason, fix kind): recipe = one dealer, template = one provider family, guard = one reason
    fix_kinds: tuple = (
        ("rot", "recipe"),
        ("one_condition", "recipe"),
        ("attribution", "template"),
        ("single_page_widget", "template"),
        ("price_gap", "template"),
        ("trim_gap", "template"),
        ("colour_gap", "template"),
        ("incomplete", "template"),
        ("real_discrepancy", "template"),
        ("secondary_gaps", "template"),
        ("identity", "guard"),
        ("truncated_walk", "guard"),
        ("comparator_only", "guard"),
    )
    other_weight: float = 0.25  # weight of the terms other than the largest
    condition_share: float = 0.36  # one_condition: 0.36 S
    walk_share: float = 0.25  # truncated_walk without T and W: 0.25 S
    prior_weight: float = 4.0  # learning: a = 4 p, b = 4 (1 - p)
    recipe_hours: float = 1.0  # E of a recipe regeneration, per dealer
    template_hours: float = 8.0  # E of a template change: 8 + 0.05 x blast radius
    template_hours_per_dealer: float = 0.05
    guard_hours: float = 6.0  # E of a guard or policy change
    key_fields: tuple = _KEY_FIELDS  # weak fields that have a reason of their own

    def severity(self, reason: str) -> float:
        return {r: s for r, s, _ in self.table}[reason]

    def prior(self, reason: str) -> float:
        return {r: p for r, _, p in self.table}[reason]

    def fix_kind(self, reason: str) -> str:
        return dict(self.fix_kinds)[reason]


DEFAULT_QUEUE_CONFIG = QueueConfig()


def learned_tractability(prior, successes, tries, cfg=DEFAULT_QUEUE_CONFIG) -> float:
    """``(a + successes) / (a + b + tries)``, ``a = w p``, ``b = w (1 - p)``: 0.60 with 1 of 1 gives 0.68."""
    a = cfg.prior_weight * prior
    b = cfg.prior_weight * (1.0 - prior)
    return (a + successes) / (a + b + tries)


def _clean_id(value):
    return clean_line(value, _DEALER_ID_MAX) if isinstance(value, str) else None


def _round(value):
    return round(float(value), 4)


def _tractability(reason, family, outcomes, cfg):
    """The learned t for ``reason`` (narrowed by ``reason:family`` when that signature has data) or the prior."""
    prior = cfg.prior(reason)
    if not isinstance(outcomes, dict):
        return prior
    for key in (reason + ":" + family, reason):
        item = outcomes.get(key)
        if isinstance(item, dict):
            tries, wins = _num(item.get("tries")), _num(item.get("successes"))
            if tries is not None and wins is not None and tries >= 0 and 0 <= wins <= tries:
                return learned_tractability(prior, wins, tries, cfg)
    return prior


def _hard_discount(entry, extras):
    """``(hard_rows after the discount, rows discounted)`` for the accuracy block, or ``(None, 0)``."""
    acc = entry.get("accuracy")
    hard_rows = _num(acc.get("hard_rows")) if isinstance(acc, dict) else None
    if hard_rows is None:
        return None, 0.0
    codes = _get(extras, "legacy", "discount_hard")
    hard = acc.get("hard") if isinstance(acc.get("hard"), dict) else {}
    removed = 0.0
    if isinstance(codes, (list, tuple)):
        removed = min(hard_rows, sum(_num(hard.get(code)) or 0.0 for code in codes))
    return hard_rows - removed, removed


def _coverage(entry, name):
    value = _num(_get(entry, "coverage", name))
    return None if value is None else min(1.0, max(0.0, value))


def _gap_rows(entry, name, stamped):
    cov = _coverage(entry, name)
    return 0.0 if cov is None else (1.0 - cov) * stamped


def _secondary_rows(entry, stamped, state, cfg):
    """Rows of the weak fields outside the key fields (largest single gap), for dealers that need work."""
    if state == "ok":
        return 0.0
    skip = set(cfg.key_fields) | {f for f in (entry.get("weak_key_fields") or []) if isinstance(f, str)}
    weak = [f for f in (entry.get("weak_fields") or []) if isinstance(f, str) and f not in skip]
    return max([_gap_rows(entry, f, stamped) for f in weak] or [0.0])


def dealer_reasons(entry, result, extras, cfg=DEFAULT_QUEUE_CONFIG, gate_cfg=DEFAULT_CONFIG) -> list:
    """The reasons of one dealer as ``[(reason, cars)]`` in ``REASONS`` order (see the module docstring)."""
    entry = entry if isinstance(entry, dict) else {}
    extras = extras if isinstance(extras, dict) else {}
    gates = result["gates"]
    failing = set(result.get("failing") or ())
    verdict = result.get("verdict")
    stamped = _num(result.get("stamped")) or 0.0
    active = _num(entry.get("active_after")) or 0.0
    shortfall = max(active - stamped, 0.0)
    widget = "hard:single_page_widget" in (result.get("reasons") or ())
    parse_time, guard = _refused(entry, extras)
    refused = parse_time + guard
    cars = {}

    if (
        verdict in ("no_rows", "no_recipe")
        or "GF6" in failing
        or (failing & {"GF1", "GF2"} and not failing & {"GF3", "GF4"} and not widget)
    ):
        cars["rot"] = shortfall
    if "GF4" in failing or parse_time > 0:
        cars["attribution"] = max(min(refused, active), shortfall)
    if "GA4" in failing:
        hard = _get(entry, "accuracy", "hard")
        hard = hard if isinstance(hard, dict) else {}
        cars["identity"] = (_num(hard.get("vpic_make")) or 0.0) + (_num(hard.get("vpic_year")) or 0.0)
    if "GF5" in failing:
        cars["one_condition"] = max(shortfall, cfg.condition_share * stamped)
    if "GF3" in failing:
        total, walked = _num(_get(extras, "walk", "site_total")), _num(_get(extras, "walk", "raw_vins"))
        if total is not None and walked is not None and total > walked >= 0:
            cars["truncated_walk"] = total - walked
        else:
            cars["truncated_walk"] = max(cfg.walk_share * stamped, shortfall)
    if widget:
        cars["single_page_widget"] = shortfall

    secondary = 0.0
    if gates["GA1"]["result"] == FAIL:
        names = gates["GA1"].get("failed") or []
        if "price" in names:
            cars["price_gap"] = _gap_rows(entry, "price", stamped)
        if "exterior_color" in names:
            cars["colour_gap"] = _gap_rows(entry, "exterior_color", stamped)
        if "trim" in names:
            missing = _num(_get(entry, "accuracy", "missing", "trim"))
            cars["trim_gap"] = missing if missing is not None else _gap_rows(entry, "trim", stamped)
        if "used_mileage" in names:
            used = _num(_get(entry, "coverage", "used_n"))
            secondary = _gap_rows(entry, "used_mileage", used if used else stamped)
    if gates["GA2"]["result"] == FAIL:
        cars["incomplete"] = _num(_get(entry, "accuracy", "incomplete_rows")) or 0.0
    hard_rows, removed = _hard_discount(entry, extras)
    if gates["GA3"]["result"] == FAIL:
        cars["real_discrepancy"] = hard_rows or 0.0
    elif removed > 0 and gates["GA3"]["result"] == PASS and _raw_share_fails(entry, extras, gate_cfg.share_enter):
        cars["comparator_only"] = removed
    secondary = max(secondary, _secondary_rows(entry, stamped, result.get("state"), cfg))
    if secondary > 0:
        cars["secondary_gaps"] = secondary
    return [(r, cars[r]) for r in REASONS if r in cars]


def _raw_share_fails(entry, extras, enter) -> bool:
    """True when the hard share would exceed ``enter`` (GateConfig.share_enter) without the comparator discount (GA3 passes only through it)."""
    acc = entry.get("accuracy")
    if not isinstance(acc, dict):
        return False
    hard_rows, cached = _num(acc.get("hard_rows")), _num(acc.get("vpic_cached"))
    if hard_rows is None or cached is None:
        return False
    assessable = cached - (_num(extras.get("vpic_gated")) or 0.0)
    return assessable > 0 and hard_rows / assessable > enter


def score_dealers(entries, results, extras_by_dealer=None, outcomes=None, site_side=(), cfg=DEFAULT_QUEUE_CONFIG, gate_cfg=DEFAULT_CONFIG) -> dict:
    """Rank the dealers that carry a reason; returns ``{"ranked": [...], "excluded": [...]}``.

    ``entries`` are the triage rows, ``results`` the ``evaluate_run`` result (or its ``dealers`` map),
    ``extras_by_dealer`` the extras used for the gates, ``outcomes`` the learning data (``{reason: {successes,
    tries}}``, optionally ``reason:family``) and ``site_side`` the dealer ids no change of ours can fix (class
    d). A ranked item is ``{dealer_id, state, family, Q, top_reason, terms}`` with ``terms`` sorted by score;
    the list is sorted by Q descending, then dealer id. ``excluded`` lists ``{dealer_id, why}`` for blocked
    and site-side dealers. Nothing is mutated.
    """
    dealers = results.get("dealers") if isinstance(results, dict) and isinstance(results.get("dealers"), dict) else results
    dealers = dealers if isinstance(dealers, dict) else {}
    extras_by_dealer = extras_by_dealer if isinstance(extras_by_dealer, dict) else {}
    site_side = set(site_side or ())
    ranked, excluded = [], []
    for entry in entries:
        dealer_id = entry.get("dealer_id") if isinstance(entry, dict) else None
        result = dealers.get(dealer_id)
        if not isinstance(result, dict) or result.get("state") == "unassessed":
            continue
        if result.get("state") == "blocked":
            excluded.append({"dealer_id": _clean_id(dealer_id), "why": "blocked"})
            continue
        if dealer_id in site_side:
            excluded.append({"dealer_id": _clean_id(dealer_id), "why": "site_side"})
            continue
        family = clean_line(entry.get("provider"), _FAMILY_MAX) if isinstance(entry.get("provider"), str) else ""
        family = family or "unknown"
        extras = extras_by_dealer.get(dealer_id)
        terms = []
        for reason, count in dealer_reasons(entry, result, extras, cfg, gate_cfg):
            sev, t = cfg.severity(reason), _tractability(reason, family, outcomes, cfg)
            terms.append({"reason": reason, "cars": _round(count), "severity": sev, "tractability": _round(t),
                          "score": _round(count * sev * t)})
        if not terms:
            continue
        terms.sort(key=lambda x: (-x["score"], REASONS.index(x["reason"])))
        total = terms[0]["score"] + cfg.other_weight * sum(x["score"] for x in terms[1:])
        ranked.append({
            "dealer_id": _clean_id(dealer_id),
            "state": result.get("state"),
            "family": family,
            "Q": _round(total),
            "top_reason": terms[0]["reason"],
            "terms": terms,
        })
    ranked.sort(key=lambda x: (-x["Q"], x["dealer_id"]))
    excluded.sort(key=lambda x: (x["why"], x["dealer_id"]))
    return {"ranked": ranked, "excluded": excluded}


def fix_units(ranked, blast_radius=None, cfg=DEFAULT_QUEUE_CONFIG) -> list:
    """Group ranked dealers by fix unit and rank the units by sum of Q / E (see the module docstring).

    ``blast_radius`` maps a provider family to the number of dealers a change to it touches fleet-wide
    (the unit's own dealer count is the floor). Each item: ``{unit, kind, family, reason, effort_hours,
    blast_radius, q_sum, score, dealers}``, dealers sorted by Q descending; sorted by score, then unit id.
    """
    blast_radius = blast_radius if isinstance(blast_radius, dict) else {}
    units = {}
    for item in ranked:
        reason = item["top_reason"]
        kind = cfg.fix_kind(reason)
        if kind == "recipe":
            key, family = "recipe:" + item["dealer_id"], None
        elif kind == "template":
            key, family = "template:" + item["family"], item["family"]
        else:
            key, family = "guard:" + reason, None
        unit = units.setdefault(key, {"unit": key, "kind": kind, "family": family,
                                      "reason": reason if kind != "template" else None, "members": []})
        unit["members"].append(item)
    out = []
    for key, unit in units.items():
        members = sorted(unit["members"], key=lambda x: (-x["Q"], x["dealer_id"]))
        if unit["kind"] == "recipe":
            blast, hours = len(members), cfg.recipe_hours
        elif unit["kind"] == "template":
            blast = max(len(members), int(_num(blast_radius.get(unit["family"])) or 0))
            hours = cfg.template_hours + cfg.template_hours_per_dealer * blast
        else:
            blast, hours = len(members), cfg.guard_hours
        q_sum = sum(m["Q"] for m in members)
        out.append({
            "unit": key,
            "kind": unit["kind"],
            "family": unit["family"],
            "reason": unit["reason"],
            "effort_hours": _round(hours),
            "blast_radius": blast,
            "q_sum": _round(q_sum),
            "score": _round(q_sum / hours),
            "dealers": [m["dealer_id"] for m in members],
        })
    out.sort(key=lambda x: (-x["score"], x["unit"]))
    return out


def _attempt_count(value) -> int:
    if isinstance(value, dict):
        value = value.get("attempts")
    count = _num(value)
    return int(count) if count is not None and count > 0 else 0


def select_cycle(ranked, attempts, pending, loop_cfg) -> dict:
    """Take the top ``dealers_per_cycle`` dealers by Q for one loop cycle.

    ``attempts`` maps dealer id to the attempts used (an integer, or a ``fix_cycles`` record with an
    ``attempts`` key), ``pending`` is the collection of dealer ids with a request in flight and ``loop_cfg``
    the ``LoopConfig``. Skipped: dealers at ``attempts_per_dealer`` attempts (state ``escalated``, listed for
    the owner), dealers with a pending request, blocked dealers, and dealers whose state is ``ok`` (only a
    comparator or secondary note: nothing to rescan). A disabled loop selects nobody. Returns ``{cycle,
    escalated, pending, owner_list}``; ``owner_list`` holds one ``{dealer_id, why: "escalated", attempts}``
    per escalated dealer. Nothing is mutated.
    """
    attempts = attempts if isinstance(attempts, dict) else {}
    pending = set(pending or ())
    limit = max(0, int(loop_cfg.dealers_per_cycle)) if loop_cfg.enabled else 0
    cycle, escalated, waiting, owner = [], [], [], []
    for item in sorted(ranked, key=lambda x: (-x["Q"], x["dealer_id"])):
        dealer_id = item["dealer_id"]
        if item.get("state") in ("blocked", "ok", "unassessed"):
            continue
        used = _attempt_count(attempts.get(dealer_id))
        if used >= loop_cfg.attempts_per_dealer:
            escalated.append(dealer_id)
            owner.append({"dealer_id": dealer_id, "why": "escalated", "attempts": used})
        elif dealer_id in pending:
            waiting.append(dealer_id)
        elif len(cycle) < limit:
            cycle.append(dealer_id)
    return {"cycle": cycle, "escalated": escalated, "pending": waiting, "owner_list": owner}
