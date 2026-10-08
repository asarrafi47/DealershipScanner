#!/usr/bin/env python3
"""
Reconcile a recipe cache (or a second store) with a ``dealer_recipes`` store, per recipe.

Two copies of a dealer's recipe set drift apart when a write-through fails, when code
older than P1B.3 saved a set without a stamp (``saved_at=0``: synthesize, cascade) or kept
the old stamp on a stale flag or a replay update, or when a host scans against another
store. ``load_recipes`` settles a drift wholesale by max ``saved_at``: the newer copy
replaces the other, so a July cache file over a September ``saved_at=0`` row pushes the July
set back up. This tool settles it per recipe instead, and never reads the cache through
``load_recipes`` (which writes): cache files are read with ``json.load``.

Modes
  --cache-dir DIR                  file -> DB. The cache files in DIR (``*.json`` without a
                                   leading underscore) against this process's store (the one
                                   ``recipe_store`` writes: INVENTORY_DATABASE_URL /
                                   DATABASE_URL). After the DB writes, every cache file that
                                   differs from the merged set is rewritten from the DB
                                   atomically, so the cache mirrors the store.
  --source-dsn S --target-dsn T    DB -> DB (P7.9): store S plays the cache's part below and
                                   is never written; only T is.

Classes (per dealer; "cache" is the source store in DB -> DB mode)
  identical   both copies hold the same rows (JSON-equal, in order)
  differs     both hold a copy and they differ. The report adds the raw max saved_at
              relation (equal / db_newer / cache_newer; the DB side is the max_saved_at
              column), which is how the 2026-10-07 baseline was counted
  cache_only  a cache copy and no DB row: inserted
  db_only     a DB row and no cache copy: left as it is

Merge, per recipe. A recipe is ``EndpointRecipe.key()``; a key that repeats on either side
(CarsCommerce new and used sections share one) is matched by key, url and post_template.
  - On both sides: the recipe comes from the side with the newer last_ok_at, so last_ok_at,
    field_coverage and total_count travel with the request they vouch for. stale therefore
    follows the DB unless the cache shows a strictly newer success. Equal last_ok_at keeps
    the DB's recipe, except that a DB stale flag the cache does not share resolves to live
    (stale_retry re-marks a dead recipe at the cost of one request).
  - Only in the cache: added only when its write stamp is newer than the DB set's last
    write; otherwise the DB set was written after it without it, and it is dropped.
  - Only in the DB: kept.

Write stamps. A row's stamp is its ``saved_at``; a row saved with 0 (pre-P1B.3 synthesize /
cascade output) counts as written at its set's newest ``last_ok_at``, the value P1C.3
backfills. ``updated_at`` is never used: every scan-hint write bumps it. A DB row the merge
keeps is written back unchanged (its 0 stays for P1C.3). A row the merge takes or changes
carries the newer of the two stamps, so the merged set's ``max_saved_at`` is never older
than either copy and older caches elsewhere adopt it instead of pushing their own set up.

Holds. A dealer the rules cannot order is reported and left alone (no DB write, no cache
rewrite): a copy that is not a JSON list of recipe rows; a cache-only recipe while the DB
set carries no stamp at all (run the P1C.3 backfill first); a cache-only recipe with no
stamp at all.

Dry run (the default) reads only. It writes report.tsv (one line per dealer), keys.tsv (one
line per recipe of every dealer that is not identical) and summary.json into
``<backup parent>/recipes_reconcile_<UTC stamp>/``; the parent is ``--backup-dir``, else
``<repo>/workspace/backups``.

--apply needs ``--backup-dir``. Before its first write it puts a tar of the cache dir
(``recipes_cache.tgz``) and a JSON export of every row it will write
(``dealer_recipes_export.json``) into the run dir. Each write is a guarded
``UPDATE ... WHERE dealer_id=? AND updated_at=?`` (an insert fails on a row that appeared);
a row changed since it was read is skipped and reported, and its cache file is left alone.
Every write is logged to ``applied.jsonl``. A second --apply finds nothing to do. In
--cache-dir mode it refuses a cache tagged for another store (``_store.json``, see
``backend.scanner.recipes.check_cache_store``), and after a run with no hold, skip or
failure it writes ``<cache>/_reconciled`` with the store's fingerprint (P7.2 reads it).
The writes switch to ``db_mutate`` once P7.1 lands.

--restore RUN_DIR undoes an --apply from its run dir: each DB row it wrote goes back to the
exported copy (an inserted row is deleted) and each cache file it rewrote comes back from
the tar, all guarded: a row or file changed since the apply is skipped and reported.

auth_headers are never printed: the report names a recipe by method, host and path only,
and errors by class. The backups hold whole rows (auth headers included) and stay under
workspace/backups, which is not committed.

  PYTHONPATH=. python backend/scripts/reconcile_recipe_store.py --cache-dir workspace/recipes
  PYTHONPATH=. python backend/scripts/reconcile_recipe_store.py --cache-dir workspace/recipes \\
      --apply --backup-dir workspace/backups
  PYTHONPATH=. python backend/scripts/reconcile_recipe_store.py --source-dsn <S> --target-dsn <T>
  PYTHONPATH=. python backend/scripts/reconcile_recipe_store.py --cache-dir workspace/recipes \\
      --restore workspace/backups/recipes_reconcile_<stamp>
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sqlite3
import sys
import tarfile
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv  # noqa: E402

# Before backend.scanner.recipes resolves RECIPES_DIR (a RECIPES_CACHE_DIR set only in
# .env must count, as for the scanner). A no-op under pytest (PROJECT_DOTENV_DISABLE).
load_project_dotenv()

from backend.scanner import recipe_store  # noqa: E402
from backend.scanner import recipes as rec  # noqa: E402

RUN_PREFIX = "recipes_reconcile_"
REPORT_NAME = "report.tsv"
KEYS_NAME = "keys.tsv"
SUMMARY_NAME = "summary.json"
CACHE_TAR_NAME = "recipes_cache.tgz"
EXPORT_NAME = "dealer_recipes_export.json"
APPLIED_NAME = "applied.jsonl"
RECONCILED_MARKER = "_reconciled"
MARKER_VERSION = 1
DEFAULT_BACKUP_PARENT = _REPO_ROOT / "workspace" / "backups"

CLASSES = ("identical", "differs", "cache_only", "db_only")
_COLUMNS = ("dealer_id", "recipes_json", "recipe_count", "provider_hint", "max_saved_at",
            "last_ok_at", "stale_count", "updated_at", "scan_hints")
_RECIPE_FIELDS = frozenset(rec.EndpointRecipe.__dataclass_fields__)

REPORT_COLUMNS = (
    "dealer_id", "class", "saved_relation", "action", "outcome", "hold_reason",
    "db_n", "cache_n", "merged_n", "db_max_saved_at", "cache_max_saved_at",
    "db_set_stamp", "cache_set_stamp", "db_last_ok", "cache_last_ok",
    "db_kept", "cache_won", "tie_live", "db_only_kept", "added", "dropped",
)
KEY_COLUMNS = ("dealer_id", "recipe", "decision", "db_saved_at", "cache_saved_at",
               "db_last_ok", "cache_last_ok", "db_stale", "cache_stale", "out_stale")


class Hold(Exception):
    """The rules cannot order this dealer's copies; it is reported and left alone."""


class Refused(Exception):
    """The run cannot go ahead (bad arguments, foreign cache, no store)."""


# ── reading ───────────────────────────────────────────────────────────────────


@dataclass
class StoreRow:
    dealer_id: str
    columns: dict[str, Any]            # every column as read (the export)
    rows: list[dict] | None            # parsed recipes_json; None when unreadable

    @property
    def updated_at(self) -> Any:
        return self.columns.get("updated_at")

    @property
    def max_saved_at(self) -> float:
        return _num(self.columns.get("max_saved_at"))


def _parse_rows(raw: Any) -> list[dict] | None:
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            return None
    if not isinstance(raw, list):
        return None
    return raw


def read_store(conn: Any, dealer_id: str | None = None) -> dict[str, StoreRow]:
    """Every ``dealer_recipes`` row (or one), read only: no DDL, no write."""
    cur = conn.cursor()
    sql = f"SELECT {', '.join(_COLUMNS)} FROM dealer_recipes"
    if dealer_id is None:
        cur.execute(sql)
    else:
        cur.execute(sql + " WHERE dealer_id = ?", (dealer_id,))
    out: dict[str, StoreRow] = {}
    for raw in cur.fetchall():
        cols = dict(zip(_COLUMNS, tuple(raw)))
        did = str(cols["dealer_id"])
        out[did] = StoreRow(did, cols, _parse_rows(cols.get("recipes_json")))
    return out


@dataclass
class CacheRead:
    rows: dict[str, list[dict] | None] = field(default_factory=dict)  # None: unreadable
    hold: dict[str, str] = field(default_factory=dict)


def read_cache_dir(cache_dir: Path) -> CacheRead:
    """The dealer recipe files of a cache dir, parsed with ``json.load``.

    ``*.json`` without a leading underscore, as every recipe script lists them: the alias
    map, the store tag and the atomic-write temp files (``<name>.json.tmp.<pid>.<thread>``)
    never match.
    """
    out = CacheRead()
    if not cache_dir.is_dir():
        return out
    for path in sorted(p for p in cache_dir.glob("*.json") if not p.name.startswith("_")):
        slug = path.stem
        try:
            with open(path, encoding="utf-8") as fh:
                rows = _parse_rows(json.load(fh))
        except (OSError, ValueError):
            rows = None
        out.rows[slug] = rows
        if rows is None:
            out.hold[slug] = "the cache file is not a JSON list"
        elif rec._recipe_slug(slug) != slug:
            out.hold[slug] = "the cache file name is not a recipe slug (load_recipes never reads it)"
    return out


# ── stamps and keys ───────────────────────────────────────────────────────────


def _num(v: Any) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _saved(row: Any) -> float:
    return _num(row.get("saved_at")) if isinstance(row, dict) else 0.0


def _last_ok(row: Any) -> float:
    return _num(row.get("last_ok_at")) if isinstance(row, dict) else 0.0


def _newest_ok(rows: list[dict]) -> float:
    return max((_last_ok(r) for r in rows), default=0.0)


def row_stamp(row: dict, rows: list[dict]) -> float:
    """When ``row`` was written: its saved_at, else its set's newest last_ok_at (P1C.3's
    backfill value). 0 when neither is known."""
    return _saved(row) or _newest_ok(rows)


def set_stamp(rows: list[dict]) -> float:
    """The set's last write (its newest row stamp); 0 for an empty or unstamped set."""
    newest_ok = _newest_ok(rows)
    return max((_saved(r) or newest_ok for r in rows), default=0.0)


def _max_saved(rows: list[dict] | None) -> float:
    return max((_saved(r) for r in rows or []), default=0.0)


def recipe_key(row: Any) -> tuple[str, str]:
    """``EndpointRecipe.key()`` of a stored row (the row itself is not modified)."""
    if not isinstance(row, dict):
        raise Hold("a recipe row is not a JSON object")
    try:
        return rec.EndpointRecipe(**{k: v for k, v in row.items() if k in _RECIPE_FIELDS}).key()
    except Exception as exc:  # noqa: BLE001 — a row load_recipes would also drop
        raise Hold(f"a recipe row has no usable key ({type(exc).__name__})") from None


def _key_label(key: tuple[str, str]) -> str:
    """Method, host and path (plus the section tag): never the query, body or headers."""
    return f"{key[0]} {key[1]}"


def _identities(rows: list[dict], keys: list[tuple[str, str]], repeated: set) -> list[tuple]:
    seen: Counter = Counter()
    out = []
    for row, key in zip(rows, keys):
        if key in repeated:
            base: tuple = ("f", key, str(row.get("url") or ""),
                           json.dumps(row.get("post_template"), sort_keys=True))
        else:
            base = ("k", key)
        out.append(base + (seen[base],))
        seen[base] += 1
    return out


# ── the merge ─────────────────────────────────────────────────────────────────


@dataclass
class KeyDecision:
    recipe: str
    decision: str  # db_newer_success | cache_newer_success | tie_db | tie_live | db_only_kept | added | dropped
    db: dict | None = None
    cache: dict | None = None
    out: dict | None = None


def _merge_shared(d: dict, c: dict, db_rows: list[dict], cache_rows: list[dict]) -> tuple[dict, str]:
    d_ok, c_ok = _last_ok(d), _last_ok(c)
    if c_ok > d_ok:
        # The cache's recipe answered later: its request, stamp, coverage, totals and
        # stale state (the state after that success) replace the DB's.
        out, decision = dict(c), "cache_newer_success"
    elif d_ok > c_ok:
        return d, "db_newer_success"
    elif d.get("stale") and not c.get("stale"):
        out, decision = dict(d), "tie_live"
        out["stale"] = False
        out["stale_reason"] = ""
    else:
        return d, "tie_db"
    stamp = max(row_stamp(d, db_rows), row_stamp(c, cache_rows))
    if stamp > 0:
        out["saved_at"] = stamp
    return out, decision


def merge_sets(cache_rows: list[dict], db_rows: list[dict], *, db_exists: bool = True
               ) -> tuple[list[dict], list[KeyDecision]]:
    """The merged set (DB order, then added cache recipes in cache order) and one
    decision per recipe. Raises ``Hold`` when the copies cannot be ordered."""
    keys_c = [recipe_key(r) for r in cache_rows]
    keys_d = [recipe_key(r) for r in db_rows]
    count_c, count_d = Counter(keys_c), Counter(keys_d)
    repeated = {k for k in set(keys_c) | set(keys_d) if count_c[k] > 1 or count_d[k] > 1}
    ids_c = _identities(cache_rows, keys_c, repeated)
    ids_d = _identities(db_rows, keys_d, repeated)
    cache_by_id = dict(zip(ids_c, cache_rows))
    db_ids = set(ids_d)
    db_stamp = set_stamp(db_rows)

    merged: list[dict] = []
    decisions: list[KeyDecision] = []
    for ident, key, d in zip(ids_d, keys_d, db_rows):
        c = cache_by_id.get(ident)
        if c is None:
            merged.append(d)
            decisions.append(KeyDecision(_key_label(key), "db_only_kept", db=d, out=d))
            continue
        out, decision = _merge_shared(d, c, db_rows, cache_rows)
        merged.append(out)
        decisions.append(KeyDecision(_key_label(key), decision, db=d, cache=c, out=out))
    for ident, key, c in zip(ids_c, keys_c, cache_rows):
        if ident in db_ids:
            continue
        stamp = row_stamp(c, cache_rows)
        if db_exists:
            if db_rows and db_stamp <= 0:
                raise Hold("the DB set carries no saved_at or last_ok_at, so a cache-only recipe "
                           "cannot be ordered against it (run the P1C.3 backfill first)")
            if stamp <= 0:
                raise Hold("a cache-only recipe carries no saved_at or last_ok_at, so it cannot be "
                           "ordered against the DB set")
            if stamp <= db_stamp:
                decisions.append(KeyDecision(_key_label(key), "dropped", cache=c))
                continue
        out = dict(c)
        if stamp > 0:
            out["saved_at"] = stamp
        merged.append(out)
        decisions.append(KeyDecision(_key_label(key), "added", cache=c, out=out))
    return merged, decisions


# ── per-dealer plan ───────────────────────────────────────────────────────────


@dataclass
class DealerPlan:
    dealer_id: str
    cls: str
    relation: str = ""
    db: StoreRow | None = None
    cache_rows: list[dict] | None = None
    has_cache: bool = False
    merged: list[dict] | None = None
    decisions: list[KeyDecision] = field(default_factory=list)
    hold: str = ""
    db_write: str = ""          # "" | "update" | "insert"
    cache_rewrite: bool = False
    outcome: str = ""           # set by apply

    @property
    def action(self) -> str:
        if self.hold:
            return "hold"
        parts = [f"db_{self.db_write}"] if self.db_write else []
        if self.cache_rewrite:
            parts.append("cache_rewrite")
        return "+".join(parts) or "none"


def plan_dealer(dealer_id: str, cache: CacheRead, db: StoreRow | None, *, rewrite_cache: bool) -> DealerPlan:
    has_cache = dealer_id in cache.rows
    cache_rows = cache.rows.get(dealer_id)
    if has_cache and db is not None:
        cls = "identical" if (cache_rows is not None and db.rows is not None
                              and cache_rows == db.rows) else "differs"
    else:
        cls = "cache_only" if has_cache else "db_only"
    plan = DealerPlan(dealer_id, cls, db=db, cache_rows=cache_rows, has_cache=has_cache)
    if cls == "differs":
        c_max = _max_saved(cache_rows)
        d_max = db.max_saved_at
        plan.relation = "equal" if c_max == d_max else ("db_newer" if d_max > c_max else "cache_newer")
    if cls in ("identical", "db_only"):
        return plan
    if dealer_id in cache.hold:
        plan.hold = cache.hold[dealer_id]
        return plan
    if db is not None and db.rows is None:
        plan.hold = "the DB row's recipes_json is not a JSON list"
        return plan
    try:
        merged, decisions = merge_sets(cache_rows or [], db.rows if db is not None else [],
                                       db_exists=db is not None)
    except Hold as exc:
        plan.hold = str(exc)
        return plan
    plan.merged, plan.decisions = merged, decisions
    if db is None:
        plan.db_write = "insert" if merged else ""
    elif merged != db.rows:
        plan.db_write = "update"
    plan.cache_rewrite = rewrite_cache and merged != cache_rows
    return plan


def build_plans(cache: CacheRead, store: dict[str, StoreRow], *, rewrite_cache: bool) -> list[DealerPlan]:
    return [plan_dealer(did, cache, store.get(did), rewrite_cache=rewrite_cache)
            for did in sorted(set(cache.rows) | set(store))]


# ── report ────────────────────────────────────────────────────────────────────


def _iso(ts: float) -> str:
    if not ts:
        return "0"
    try:
        return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (OverflowError, OSError, ValueError):
        return repr(ts)


def _tsv(v: Any) -> str:
    return str(v).replace("\t", " ").replace("\n", " ")


def _report_row(p: DealerPlan) -> dict[str, Any]:
    db_rows = p.db.rows if p.db is not None and p.db.rows is not None else []
    cache_rows = p.cache_rows or []
    count = Counter(d.decision for d in p.decisions)
    return {
        "dealer_id": p.dealer_id, "class": p.cls, "saved_relation": p.relation,
        "action": p.action, "outcome": p.outcome, "hold_reason": p.hold,
        "db_n": len(db_rows) if p.db is not None else "", "cache_n": len(cache_rows) if p.has_cache else "",
        "merged_n": len(p.merged) if p.merged is not None else "",
        "db_max_saved_at": _iso(p.db.max_saved_at) if p.db is not None else "",
        "cache_max_saved_at": _iso(_max_saved(cache_rows)) if p.has_cache else "",
        "db_set_stamp": _iso(set_stamp(db_rows)) if p.db is not None else "",
        "cache_set_stamp": _iso(set_stamp(cache_rows)) if p.has_cache else "",
        "db_last_ok": _iso(_newest_ok(db_rows)) if p.db is not None else "",
        "cache_last_ok": _iso(_newest_ok(cache_rows)) if p.has_cache else "",
        "db_kept": count["db_newer_success"] + count["tie_db"], "cache_won": count["cache_newer_success"],
        "tie_live": count["tie_live"], "db_only_kept": count["db_only_kept"],
        "added": count["added"], "dropped": count["dropped"],
    }


def _key_rows(p: DealerPlan) -> list[dict[str, Any]]:
    def side(row: dict | None, fn: Callable[[dict], Any]) -> Any:
        return "" if row is None else fn(row)

    out = []
    for d in p.decisions:
        out.append({
            "dealer_id": p.dealer_id, "recipe": d.recipe, "decision": d.decision,
            "db_saved_at": side(d.db, lambda r: _iso(_saved(r))),
            "cache_saved_at": side(d.cache, lambda r: _iso(_saved(r))),
            "db_last_ok": side(d.db, lambda r: _iso(_last_ok(r))),
            "cache_last_ok": side(d.cache, lambda r: _iso(_last_ok(r))),
            "db_stale": side(d.db, lambda r: int(bool(r.get("stale")))),
            "cache_stale": side(d.cache, lambda r: int(bool(r.get("stale")))),
            "out_stale": side(d.out, lambda r: int(bool(r.get("stale")))),
        })
    return out


def summarize(plans: list[DealerPlan]) -> dict[str, Any]:
    classes = Counter(p.cls for p in plans)
    relations = Counter(p.relation for p in plans if p.cls == "differs")
    over_zero = sum(1 for p in plans if p.relation == "cache_newer" and p.db is not None
                    and p.db.max_saved_at == 0)
    actions = Counter()
    for p in plans:
        if p.hold:
            actions["hold"] += 1
            continue
        if p.db_write:
            actions[f"db_{p.db_write}"] += 1
        if p.cache_rewrite:
            actions["cache_rewrite"] += 1
        if not p.db_write and not p.cache_rewrite:
            actions["unchanged"] += 1
    outcomes = Counter(p.outcome for p in plans if p.outcome)
    return {
        "dealers": len(plans),
        "classes": {c: classes[c] for c in CLASSES},
        "differs_saved_relation": {r: relations[r] for r in ("equal", "db_newer", "cache_newer")},
        "differs_cache_newer_over_db_saved_at_0": over_zero,
        "actions": {a: actions[a] for a in ("db_update", "db_insert", "cache_rewrite", "hold", "unchanged")},
        "outcomes": dict(sorted(outcomes.items())),
        "holds": {p.dealer_id: p.hold for p in plans if p.hold},
        "ids": {
            "differs_equal": [p.dealer_id for p in plans if p.relation == "equal"],
            "differs_db_newer": [p.dealer_id for p in plans if p.relation == "db_newer"],
            "differs_cache_newer": [p.dealer_id for p in plans if p.relation == "cache_newer"],
            "cache_only": [p.dealer_id for p in plans if p.cls == "cache_only"],
            "db_update": [p.dealer_id for p in plans if p.db_write == "update" and not p.hold],
            "db_insert": [p.dealer_id for p in plans if p.db_write == "insert" and not p.hold],
            "cache_rewrite": [p.dealer_id for p in plans if p.cache_rewrite and not p.hold],
        },
    }


def write_report(run_dir: Path, plans: list[DealerPlan], header: dict[str, Any]) -> dict[str, Any]:
    with open(run_dir / REPORT_NAME, "w", encoding="utf-8") as fh:
        fh.write("\t".join(REPORT_COLUMNS) + "\n")
        for p in plans:
            row = _report_row(p)
            fh.write("\t".join(_tsv(row[c]) for c in REPORT_COLUMNS) + "\n")
    with open(run_dir / KEYS_NAME, "w", encoding="utf-8") as fh:
        fh.write("\t".join(KEY_COLUMNS) + "\n")
        for p in plans:
            if p.cls == "identical":
                continue
            for row in _key_rows(p):
                fh.write("\t".join(_tsv(row[c]) for c in KEY_COLUMNS) + "\n")
    summary = dict(header)
    summary.update(summarize(plans))
    with open(run_dir / SUMMARY_NAME, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=1, sort_keys=True)
    return summary


def _ids(ids: list[str], limit: int = 20) -> str:
    more = f" (+{len(ids) - limit} more)" if len(ids) > limit else ""
    return (", ".join(ids[:limit]) + more) if ids else "-"


def print_summary(summary: dict[str, Any], plans: list[DealerPlan], run_dir: Path, *, show: int) -> None:
    c, r, a = summary["classes"], summary["differs_saved_relation"], summary["actions"]
    print(f"classes: identical {c['identical']} | differs {c['differs']} (saved_at equal {r['equal']}, "
          f"db newer {r['db_newer']}, cache newer {r['cache_newer']} "
          f"[{summary['differs_cache_newer_over_db_saved_at_0']} over a db max_saved_at of 0]) | "
          f"cache_only {c['cache_only']} | db_only {c['db_only']}")
    print(f"plan:    db update {a['db_update']} | db insert {a['db_insert']} | cache rewrite "
          f"{a['cache_rewrite']} | hold {a['hold']} | unchanged {a['unchanged']}")
    if summary.get("outcomes"):
        print("outcome: " + " | ".join(f"{k} {v}" for k, v in summary["outcomes"].items()))
    ids = summary["ids"]
    for label, key in (("differs, saved_at equal", "differs_equal"), ("differs, db newer", "differs_db_newer"),
                       ("differs, cache newer", "differs_cache_newer"), ("cache_only", "cache_only")):
        print(f"  {label}: {_ids(ids[key])}")
    for did, why in list(summary["holds"].items())[:20]:
        print(f"  hold {did}: {why}")
    shown = [p for p in plans if p.cls in ("differs", "cache_only")][:show]
    for p in shown:
        row = _report_row(p)
        print(f"  {p.dealer_id}: {p.cls}{'/' + p.relation if p.relation else ''} -> {p.action}"
              f"{' [' + p.outcome + ']' if p.outcome else ''} (db {row['db_n']} / cache {row['cache_n']} "
              f"-> {row['merged_n']}; kept {row['db_kept']}, cache won {row['cache_won']}, tie live "
              f"{row['tie_live']}, db-only {row['db_only_kept']}, added {row['added']}, dropped {row['dropped']})")
    print(f"report: {run_dir / REPORT_NAME}")


# ── connections and identities ────────────────────────────────────────────────


def _sqlite_dsn_path(dsn: str) -> str | None:
    return dsn[len("sqlite:///"):] if dsn.startswith("sqlite:///") else None


def connect_dsn(dsn: str) -> Any:
    """A connection to a store named by URL: Postgres, or ``sqlite:///<path>`` (tests)."""
    path = _sqlite_dsn_path(dsn)
    if path is not None:
        if not Path(path).is_file():
            raise Refused(f"no SQLite store at {path}")
        return sqlite3.connect(path)
    if dsn.startswith(("postgresql://", "postgres://")):
        import psycopg

        from backend.db.inventory_compat import InventoryConnection

        return InventoryConnection(psycopg.connect(dsn, autocommit=False), backend="postgres")
    raise Refused("a DSN must be a postgresql:// URL (or sqlite:///<path> in tests)")


def dsn_identity(dsn: str) -> str:
    """Credential-free name of the store a DSN reaches (host, port, database)."""
    path = _sqlite_dsn_path(dsn)
    if path is not None:
        return "sqlite:" + os.path.realpath(path)
    return rec._pg_store_identity(dsn)


def _fingerprint(identity: str | None) -> str:
    return (rec.store_fingerprint(identity) or "") if identity else ""


def _open(connect: Callable[[], Any], identity: str) -> Any:
    """Open a store connection; a failure is a refusal naming the error class only (the
    error text can quote the URL)."""
    try:
        return connect()
    except Refused:
        raise
    except Exception as exc:  # noqa: BLE001
        raise Refused(f"cannot connect to {identity} ({type(exc).__name__})") from None


# ── run dir and backups ───────────────────────────────────────────────────────


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def make_run_dir(parent: Path) -> Path:
    base = parent / f"{RUN_PREFIX}{_stamp()}"
    run_dir, n = base, 1
    while True:  # never mix two runs' files
        try:
            run_dir.mkdir(parents=True, exist_ok=False)
            return run_dir
        except FileExistsError:
            n += 1
            run_dir = base.with_name(f"{base.name}-{n}")


def _fsync_file(path: Path) -> None:
    with open(path, "rb") as fh:
        os.fsync(fh.fileno())


def backup_cache_dir(cache_dir: Path, run_dir: Path) -> tuple[Path, int]:
    """Tar the whole cache dir (members under ``<cache dir name>/``); returns the tar and
    its member count. Refuses an empty or unreadable tar."""
    tar_path = run_dir / CACHE_TAR_NAME
    with tarfile.open(tar_path, "w:gz") as tf:
        tf.add(str(cache_dir), arcname=cache_dir.name)
    _fsync_file(tar_path)
    with tarfile.open(tar_path, "r:gz") as tf:
        members = tf.getnames()
    files = [m for m in members if m != cache_dir.name]
    if tar_path.stat().st_size == 0 or not files:
        raise Refused(f"the cache backup {tar_path} came out empty; nothing was written")
    return tar_path, len(files)


def export_rows(run_dir: Path, plans: list[DealerPlan], header: dict[str, Any]) -> Path:
    """A JSON export of every DB row the apply will write, as read (whole rows)."""
    rows = []
    for p in plans:
        if p.hold or not p.db_write:
            continue
        rows.append({"dealer_id": p.dealer_id, "existed": p.db is not None,
                     "columns": dict(p.db.columns) if p.db is not None else None})
    path = run_dir / EXPORT_NAME
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(dict(header, rows=rows), fh, indent=1, sort_keys=True, default=str)
        fh.flush()
        os.fsync(fh.fileno())
    return path


class AppliedLog:
    """``applied.jsonl``: one line per write, flushed as it happens (``--restore`` input)."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fh = open(path, "a", encoding="utf-8")

    def add(self, entry: dict[str, Any]) -> None:
        self._fh.write(json.dumps(entry, sort_keys=True) + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def close(self) -> None:
        self._fh.close()


# ── apply ─────────────────────────────────────────────────────────────────────


def _is_conflict(exc: BaseException) -> bool:
    return any(c.__name__ in ("IntegrityError", "UniqueViolation") for c in type(exc).__mro__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _guard(updated_at: Any) -> tuple[str, tuple]:
    if updated_at is None:
        return "updated_at IS NULL", ()
    return "updated_at = ?", (updated_at,)


def _write_row(conn: Any, plan: DealerPlan, now: str) -> bool:
    """One guarded write of the merged set; False when the row changed since the read."""
    meta = recipe_store._derive_meta(plan.merged or [])
    payload = json.dumps(plan.merged, ensure_ascii=False)
    values = (payload, meta["recipe_count"], meta["provider_hint"], meta["max_saved_at"],
              meta["last_ok_at"], meta["stale_count"], now)
    cur = conn.cursor()
    try:
        if plan.db_write == "update":
            guard, guard_params = _guard(plan.db.updated_at)
            cur.execute(
                "UPDATE dealer_recipes SET recipes_json=?, recipe_count=?, provider_hint=?, max_saved_at=?, "
                f"last_ok_at=?, stale_count=?, updated_at=? WHERE dealer_id=? AND {guard}",
                values + (plan.dealer_id,) + guard_params,
            )
            done = cur.rowcount == 1
        else:
            cur.execute(
                "INSERT INTO dealer_recipes (recipes_json, recipe_count, provider_hint, max_saved_at, "
                "last_ok_at, stale_count, updated_at, dealer_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                values + (plan.dealer_id,),
            )
            done = True
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        if _is_conflict(exc):
            return False
        raise
    if done:
        conn.commit()
    else:
        conn.rollback()
    return done


def _read_file_rows(path: Path) -> Any:
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def apply_plans(plans: list[DealerPlan], conn: Any, *, cache_dir: Path | None, log: AppliedLog) -> None:
    """DB writes first (each guarded, each its own transaction), then the cache rewrites.
    Sets ``plan.outcome``; backups must already be on disk."""
    for p in plans:
        if p.hold or not p.db_write:
            continue
        now = _now_iso()
        try:
            done = _write_row(conn, p, now)
        except Exception as exc:  # noqa: BLE001 — the class only (the text can quote the URL)
            p.outcome = f"failed:{type(exc).__name__}"
            continue
        if not done:
            p.outcome = "skipped:db_row_changed_since_read"
            continue
        p.outcome = {"update": "db_updated", "insert": "db_inserted"}[p.db_write]
        log.add({"kind": "db", "dealer_id": p.dealer_id, "op": p.db_write, "updated_at": now})
    if cache_dir is None:
        return
    for p in plans:
        if p.hold or not p.cache_rewrite or p.outcome.startswith(("skipped", "failed")):
            continue
        path = cache_dir / f"{p.dealer_id}.json"
        if _read_file_rows(path) != p.cache_rows:
            p.outcome = _join(p.outcome, "skipped:cache_file_changed_since_read")
            continue
        try:
            current = read_store(conn, p.dealer_id).get(p.dealer_id)
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            p.outcome = _join(p.outcome, f"failed:cache_reread_{type(exc).__name__}")
            continue
        conn.rollback()  # end the read transaction (Postgres)
        if current is None or current.rows is None:
            p.outcome = _join(p.outcome, "skipped:db_row_gone_or_unreadable")
            continue
        if current.rows != p.cache_rows:
            rec._atomic_write_json(path, current.rows)
            log.add({"kind": "cache", "dealer_id": p.dealer_id, "file": path.name, "sha256": _sha256(path)})
        p.outcome = _join(p.outcome, "cache_rewritten")


def _join(a: str, b: str) -> str:
    return f"{a}+{b}" if a else b


def write_marker(cache_dir: Path, fingerprint: str, identity: str, run_dir: Path,
                 summary: dict[str, Any]) -> Path:
    path = cache_dir / RECONCILED_MARKER
    rec._atomic_write_json(path, {
        "version": MARKER_VERSION,
        "fingerprint": fingerprint,
        "kind": identity.split(":", 1)[0],
        "reconciled_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_dir": str(run_dir),
        "classes": summary.get("classes"),
        "actions": summary.get("actions"),
    })
    return path


# ── restore ───────────────────────────────────────────────────────────────────


def restore_run(run_dir: Path, conn: Any, *, fingerprint: str, cache_dir: Path | None) -> Counter:
    """Undo an --apply: guarded, per row and per file. Returns outcome counts."""
    export = json.loads((run_dir / EXPORT_NAME).read_text(encoding="utf-8"))
    if export.get("fingerprint") != fingerprint:
        raise Refused("the run was applied to another store (fingerprint differs); nothing restored")
    before = {r["dealer_id"]: r for r in export.get("rows", [])}
    applied_path = run_dir / APPLIED_NAME
    entries = [json.loads(line) for line in applied_path.read_text(encoding="utf-8").splitlines()
               if line.strip()] if applied_path.is_file() else []
    out: Counter = Counter()
    for e in (e for e in entries if e.get("kind") == "db"):
        did = e["dealer_id"]
        prior = before.get(did)
        cur = conn.cursor()
        if e["op"] == "insert":
            cur.execute("DELETE FROM dealer_recipes WHERE dealer_id=? AND updated_at=?", (did, e["updated_at"]))
        elif prior is not None and prior.get("columns"):
            cols = prior["columns"]
            cur.execute(
                "UPDATE dealer_recipes SET recipes_json=?, recipe_count=?, provider_hint=?, max_saved_at=?, "
                "last_ok_at=?, stale_count=?, updated_at=? WHERE dealer_id=? AND updated_at=?",
                (cols["recipes_json"], cols["recipe_count"], cols["provider_hint"], cols["max_saved_at"],
                 cols["last_ok_at"], cols["stale_count"], _now_iso(), did, e["updated_at"]),
            )
        else:
            out["db_skipped_no_export"] += 1
            continue
        if cur.rowcount == 1:
            conn.commit()
            out["db_restored"] += 1
        else:
            conn.rollback()
            out["db_skipped_changed_since_apply"] += 1
            print(f"  restore skipped {did}: the row changed since the apply")
    cache_entries = [e for e in entries if e.get("kind") == "cache"]
    if cache_entries and cache_dir is not None:
        prefix = Path(export.get("cache_dir") or cache_dir).name  # the tar's arcname
        with tarfile.open(run_dir / CACHE_TAR_NAME, "r:gz") as tf:
            for e in cache_entries:
                path = cache_dir / e["file"]
                if not path.is_file() or _sha256(path) != e["sha256"]:
                    out["cache_skipped_changed_since_apply"] += 1
                    print(f"  restore skipped {e['file']}: the file changed since the apply")
                    continue
                try:
                    member = tf.extractfile(f"{prefix}/{e['file']}")
                except KeyError:
                    member = None
                if member is None:
                    out["cache_skipped_not_in_tar"] += 1
                    continue
                rec._atomic_write_json(path, json.load(io.TextIOWrapper(member, encoding="utf-8")))
                out["cache_restored"] += 1
    marker = cache_dir / RECONCILED_MARKER if cache_dir is not None else None
    if marker is not None and marker.is_file():
        try:
            if json.loads(marker.read_text(encoding="utf-8")).get("run_dir") == str(run_dir):
                marker.unlink()
                out["marker_removed"] += 1
        except (OSError, ValueError):
            pass
    return out


# ── CLI ───────────────────────────────────────────────────────────────────────


@dataclass
class Target:
    identity: str
    fingerprint: str
    connect: Callable[[], Any]
    cache_dir: Path | None = None
    source_dsn: str | None = None
    tag_verdict: str = ""


def _resolve_target(args: argparse.Namespace) -> Target:
    if args.cache_dir:
        cache_dir = Path(args.cache_dir).expanduser().absolute()
        if not cache_dir.is_dir():
            raise Refused(f"no cache dir at {cache_dir}")
        if not recipe_store._enabled():
            raise Refused("the recipe store is switched off (RECIPES_DB_DISABLED); nothing to reconcile with")
        check = rec.check_cache_store(cache_dir)
        if not check.identity:
            raise Refused("this process uses no recipe store (INVENTORY_DATABASE_URL / DATABASE_URL), "
                          "or its identity could not be worked out")
        return Target(check.identity, check.fingerprint or "", lambda: recipe_store._conn(),
                      cache_dir=cache_dir, tag_verdict=check.verdict)
    identity = dsn_identity(args.target_dsn)
    if dsn_identity(args.source_dsn) == identity:
        raise Refused("--source-dsn and --target-dsn name the same store")
    return Target(identity, _fingerprint(identity), lambda: connect_dsn(args.target_dsn),
                  source_dsn=args.source_dsn)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog="reconcile_recipe_store",
        description="Reconcile a recipe cache dir (or a second store) with dealer_recipes, per recipe. "
                    "Dry run by default.",
    )
    ap.add_argument("--cache-dir", help="file -> DB: this cache dir against this process's store")
    ap.add_argument("--source-dsn", help="DB -> DB: the store merged in (never written)")
    ap.add_argument("--target-dsn", help="DB -> DB: the store written")
    ap.add_argument("--backup-dir",
                    help=f"parent of the run dir (default {DEFAULT_BACKUP_PARENT}); required with --apply")
    ap.add_argument("--apply", action="store_true", help="write: backups first, then guarded writes")
    ap.add_argument("--restore", metavar="RUN_DIR", help="undo an --apply from its run dir")
    ap.add_argument("--show", type=int, default=60, help="dealer lines to print (default 60)")
    args = ap.parse_args(argv)
    if bool(args.cache_dir) == bool(args.source_dsn or args.target_dsn):
        ap.error("give --cache-dir, or --source-dsn with --target-dsn")
    if not args.cache_dir and not (args.source_dsn and args.target_dsn):
        ap.error("--source-dsn and --target-dsn go together")
    if args.apply and args.restore:
        ap.error("--apply and --restore do not go together")
    if args.apply and not args.backup_dir:
        ap.error("--apply requires --backup-dir: the cache tar and the row export go there first")
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        target = _resolve_target(args)
        if args.restore:
            return _main_restore(args, target)
        return _main_reconcile(args, target)
    except Refused as exc:
        print(f"reconcile_recipe_store: refused: {exc}")
        return 2


def _main_restore(args: argparse.Namespace, target: Target) -> int:
    run_dir = Path(args.restore).expanduser().absolute()
    if not (run_dir / EXPORT_NAME).is_file():
        raise Refused(f"{run_dir} holds no {EXPORT_NAME}: not an --apply run dir")
    conn = _open(target.connect, target.identity)
    try:
        counts = restore_run(run_dir, conn, fingerprint=target.fingerprint, cache_dir=target.cache_dir)
    finally:
        conn.close()
    print("restore: " + (" | ".join(f"{k} {v}" for k, v in sorted(counts.items())) or "nothing to restore"))
    return 1 if any(k.startswith(("db_skipped", "cache_skipped")) for k in counts) else 0


def _main_reconcile(args: argparse.Namespace, target: Target) -> int:
    if args.apply and target.cache_dir is not None and target.tag_verdict not in ("match", "untagged"):
        raise Refused(f"the cache dir's store tag verdict is '{target.tag_verdict}': it mirrors another "
                      "store, so its files must not be merged into this one (see "
                      "`python -m backend.scanner.recipes --status`)")
    if target.cache_dir is not None:
        cache = read_cache_dir(target.cache_dir)
        source_label = f"cache dir {target.cache_dir} ({len(cache.rows)} dealer files; store tag {target.tag_verdict})"
    else:
        sconn = _open(lambda: connect_dsn(args.source_dsn), dsn_identity(args.source_dsn))
        try:
            source = read_store(sconn)
        finally:
            sconn.close()
        cache = CacheRead({did: r.rows for did, r in source.items()},
                          {did: "the source row's recipes_json is not a JSON list"
                           for did, r in source.items() if r.rows is None})
        source_label = f"source store {dsn_identity(args.source_dsn)} ({len(source)} rows)"
    conn = _open(target.connect, target.identity)
    try:
        store = read_store(conn)
        conn.rollback()  # end the read transaction (Postgres); nothing was written
        plans = build_plans(cache, store, rewrite_cache=target.cache_dir is not None)
        parent = Path(args.backup_dir).expanduser().absolute() if args.backup_dir else DEFAULT_BACKUP_PARENT
        run_dir = make_run_dir(parent)
        header = {
            "mode": "cache-dir" if target.cache_dir is not None else "db-to-db",
            "apply": bool(args.apply),
            "store": target.identity, "fingerprint": target.fingerprint,
            "cache_dir": str(target.cache_dir) if target.cache_dir is not None else None,
            "source": dsn_identity(args.source_dsn) if args.source_dsn else None,
            "started_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        print(f"reconcile_recipe_store: {'APPLY' if args.apply else 'DRY RUN (reads only; writes the report)'}")
        print(f"from {source_label}")
        print(f"into store {target.identity} (fingerprint {target.fingerprint[:12]}; {len(store)} rows)")
        if not args.apply:
            summary = write_report(run_dir, plans, header)
            print_summary(summary, plans, run_dir, show=args.show)
            return 0
        write_report(run_dir, plans, header)  # the plan, before any write (rewritten with outcomes below)
        if target.cache_dir is not None:
            tar_path, n = backup_cache_dir(target.cache_dir, run_dir)
            print(f"backup: {tar_path} ({n} entries)")
        export = export_rows(run_dir, plans, header)
        print(f"backup: {export} ({sum(1 for p in plans if p.db_write and not p.hold)} rows to write)")
        log = AppliedLog(run_dir / APPLIED_NAME)
        try:
            apply_plans(plans, conn, cache_dir=target.cache_dir, log=log)
        finally:
            log.close()
        summary = write_report(run_dir, plans, header)
        print_summary(summary, plans, run_dir, show=args.show)
        problems = [p for p in plans if p.hold or "skipped" in p.outcome or "failed" in p.outcome]
        if target.cache_dir is not None:
            if problems:
                print(f"marker: not written ({len(problems)} dealer(s) held, skipped or failed; "
                      f"re-run after resolving them)")
            else:
                marker = write_marker(target.cache_dir, target.fingerprint, target.identity, run_dir, summary)
                print(f"marker: {marker}")
        return 1 if any("skipped" in p.outcome or "failed" in p.outcome for p in plans) else 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
