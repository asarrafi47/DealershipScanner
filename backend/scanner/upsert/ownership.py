"""Evidence-aware VIN ownership policy (N8.6, D-N3, D-IF5).

``cars`` is one row per VIN, so two stores of one group that both list a VIN contest
it. The guard in :mod:`backend.scanner.upsert.guard` and the atomic backstop in
``UPSERT_CARS_SQL`` decide the contest with the same five rules, whatever the guard
window (a lapsed window must never fall back to scan order):

1. a claimant with strong stamped evidence takes a VIN from an owner whose stored
   ``rooftop_tier`` is none or weak;
2. a claimant without strong evidence never takes from an owner with strong evidence
   (two strong claims: the stored owner stays);
3. an evidence-less contest between stores of one pool in ``group_pools.json`` goes to
   the pool's designated primary;
4. an evidence-less contest between stores not covered by the file follows the 48 h
   window as before; a refusal is reported as ``group_feed_unstamped``;
5. alias ids of one store never conflict.

A non-reverted manual ``car_move_log`` row that put the VIN at its current owner is
honoured against every claimant.
"""
from __future__ import annotations

import json
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, NamedTuple

logger = logging.getLogger("backend.scanner.database")

STRONG, WEAK, NONE = "strong", "weak", "none"
# Evidence text of a move the scanner itself made; anything else in car_move_log is a human's.
SCANNER_MOVE_PREFIX = "scanner_guard:"

_DEFAULT_POOLS_PATH = Path(__file__).resolve().parents[2] / "config" / "group_pools.json"


class Pools(NamedTuple):
    pools: tuple[tuple[str, tuple[str, ...]], ...]  # (primary, members incl. primary)
    alias_groups: tuple[frozenset[str], ...]


def norm_tier(value: Any) -> str:
    t = str(value or "").strip().lower()
    return t if t in (STRONG, WEAK) else NONE


def tier_rank(value: Any) -> int:
    return {STRONG: 2, WEAK: 1}.get(norm_tier(value), 0)


def _load(path: str) -> Pools:
    try:
        with open(path, encoding="utf-8") as fh:
            doc = json.load(fh)
    except FileNotFoundError:
        return Pools((), ())
    except (OSError, ValueError):
        logger.warning("group_pools: could not read %s; treating as empty", path, exc_info=True)
        return Pools((), ())
    pools = []
    for p in doc.get("pools") or []:
        primary = str(p.get("primary") or "").strip()
        members = tuple(dict.fromkeys([primary] + [str(m).strip() for m in p.get("members") or []]))
        if primary and len(members) > 1:
            pools.append((primary, members))
    aliases = tuple(frozenset(str(a).strip() for a in g if str(a).strip()) for g in doc.get("aliases") or [])
    return Pools(tuple(pools), tuple(g for g in aliases if len(g) > 1))


@lru_cache(maxsize=4)
def _load_cached(path: str, mtime: float) -> Pools:
    return _load(path)


def load_pools() -> Pools:
    """``backend/config/group_pools.json`` (override: ``GROUP_POOLS_PATH``); empty when absent."""
    path = (os.environ.get("GROUP_POOLS_PATH") or "").strip() or str(_DEFAULT_POOLS_PATH)
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        mtime = 0.0
    return _load_cached(path, mtime)


def are_aliases(pools: Pools, a: str, b: str) -> bool:
    return any(a in g and b in g for g in pools.alias_groups)


def _covering(pools: Pools, a: str, b: str) -> tuple[str, ...] | None:
    for primary, members in pools.pools:
        if a in members and b in members:
            return (primary,) + members
    return None


class Verdict(NamedTuple):
    allow: bool
    reason: str


def decide(
    owner: str,
    owner_tier: Any,
    owner_active: bool,
    owner_fresh: bool,
    claimant: str,
    claimant_tier: Any,
    pools: Pools,
) -> Verdict:
    """Rules 1-5 for one VIN whose stored row belongs to a different, non-empty ``owner``.

    ``owner_fresh``: the stored row was scraped inside the guard window (only rule 4 reads it).
    """
    if not owner or owner == claimant:
        return Verdict(True, "same_owner")
    if not owner_active:
        return Verdict(True, "owner_inactive")
    if are_aliases(pools, owner, claimant):
        return Verdict(True, "alias")
    ct, ot = norm_tier(claimant_tier), norm_tier(owner_tier)
    if ct == STRONG and ot != STRONG:
        return Verdict(True, "strong_claimant")
    if ot == STRONG:
        return Verdict(False, "owner_strong")
    pool = _covering(pools, owner, claimant)
    if pool is not None:
        return Verdict(claimant == pool[0], "pool_primary" if claimant == pool[0] else "pool_non_primary")
    if owner_fresh:
        return Verdict(False, "group_feed_unstamped")
    return Verdict(True, "window_lapsed")


def backstop_lists(pools: Pools, claimant: str) -> tuple[str, str, str]:
    """``(alias_csv, takes_csv, covered_csv)`` bound into the SQL backstop for one claimant.

    Each is ``,a,b,`` (or ``''``) so SQL can test membership with ``replace``:
    aliases (rule 5), owners the claimant may take as its pool's primary (rule 3), and every
    store sharing a pool with the claimant (rule 4 applies to owners outside it).
    """
    aliases: set[str] = set()
    for g in pools.alias_groups:
        if claimant in g:
            aliases |= g - {claimant}
    takes: set[str] = set()
    covered: set[str] = set()
    for primary, members in pools.pools:
        if claimant in members:
            covered |= set(members) - {claimant}
            if claimant == primary:
                takes |= set(members) - {claimant}

    def csv(ids: set[str]) -> str:
        return ("," + ",".join(sorted(ids)) + ",") if ids else ""

    return csv(aliases), csv(takes), csv(covered)


def manual_holds(cursor, vins: list[str]) -> set[str]:
    """VINs whose current owner was set by a non-reverted manual ``car_move_log`` row."""
    held: set[str] = set()
    if not vins:
        return held
    try:
        for i in range(0, len(vins), 500):
            chunk = vins[i : i + 500]
            cursor.execute(
                "SELECT c.vin FROM car_move_log m JOIN cars c ON c.id = m.car_id "
                "WHERE c.vin IN (" + ",".join("?" * len(chunk)) + ") "
                "AND m.reverted_at IS NULL AND m.to_dealer_id = c.dealer_id "
                "AND SUBSTR(COALESCE(m.evidence, ''), 1, " + str(len(SCANNER_MOVE_PREFIX)) + ") <> ?",
                (*chunk, SCANNER_MOVE_PREFIX),
            )
            held.update(str(r[0]) for r in cursor.fetchall())
    except Exception:
        logger.debug("manual_holds: car_move_log unreadable", exc_info=True)
        try:
            cursor.connection.rollback()  # a failed statement aborts a PG transaction; this one is read-only
        except Exception:
            pass
        return set()
    return held


class Move(NamedTuple):
    vin: str
    owner: str
    owner_name: str
    owner_registry_id: Any
    claimant: str
    reason: str
    evidence: str


def log_moves(cursor, moves: list[Move]) -> int:
    """One ``car_move_log`` row per move that actually landed; returns the number logged."""
    n = 0
    for m in moves:
        cursor.execute(
            "INSERT INTO car_move_log (car_id, from_dealer_id, from_dealer_name, from_registry_id, "
            "to_dealer_id, to_dealer_name, to_registry_id, evidence) "
            "SELECT id, ?, ?, ?, dealer_id, dealer_name, dealership_registry_id, ? "
            "FROM cars WHERE vin = ? AND dealer_id = ?",
            (m.owner, m.owner_name, m.owner_registry_id, SCANNER_MOVE_PREFIX + m.reason + " " + m.evidence, m.vin, m.claimant),
        )
        n += max(0, int(getattr(cursor, "rowcount", 0) or 0))
    return n
