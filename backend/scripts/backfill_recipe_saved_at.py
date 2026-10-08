#!/usr/bin/env python3
"""
Backfill ``saved_at`` on ``dealer_recipes`` entries that carry 0 or null (P1C.3).

Why: until P1B.3, a set built by ``ensure_recipe(force)``, the cascade or
``synthesize_recipes`` was saved with ``saved_at=0`` on every entry. ``load_recipes``
compares max ``saved_at`` between a host's cache file and the DB row, so a DB set
stamped 0 loses to ANY older cache file with a non-zero stamp: that host reverts the
synthesized set and pushes its old set back up (normreeves-com: a July file of 827
chars over the September DB set of 1,141 chars). P1B.3 stamps every new save; this
tool stamps the rows written before it.

What it does, for each ``dealer_recipes`` row whose entries have ``saved_at`` 0 or null:

- those entries get one stamp per row: the row's max ``last_ok_at`` (``ensure_recipe``
  sets ``last_ok_at`` when it validates a recipe, just before the save), falling back
  to the row's ``updated_at`` when no entry has a success; ``max_saved_at`` is then
  recomputed with ``recipe_store._derive_meta``. Entries that already carry a stamp,
  every other field, every other column and ``scan_hints`` are left as they are;
- a row is skipped and listed when the cache dir (default: the scanner's,
  ``RECIPES_CACHE_DIR`` else ``<repo>/workspace/recipes``) holds evidence the stamp
  would make the host discard: a key whose cache ``last_ok_at`` is newer than the
  DB's for that key (or, for a key the DB set lacks, newer than the DB set's latest
  success). ``load_recipes`` would adopt the stamped DB copy over that file, so those
  dealers are left to the reconcile tool (P1C.4). A cache file that cannot be read,
  or whose ``last_ok_at`` is not a number, skips its dealer too (``cache_unreadable``);
- the cache check fails closed. ``--apply`` is refused (exit 2, nothing written, no
  export) when the default cache dir does not exist or holds no dealer files (a run
  from a git worktree, which has no ``workspace/recipes``), when the cache dir is tagged for
  another store (verdict ``mismatch``: its successes say nothing about this store,
  and this store's own cache is elsewhere), or when this process's store identity
  cannot be worked out (the export could not be tied to a store for ``--restore``).
  A dry run prints each of these as a WARNING and says ``--apply`` would be refused.
  ``--cache-dir`` names the cache explicitly; ``--no-cache-check`` turns the check
  off on purpose (a run against a store whose cache lives elsewhere, e.g. prod in
  P6B.2);
- only this one cache dir is checked. Another host that writes the same store (the
  mini, over the 15432 tunnel) must not scan between an ``--apply`` here and its own
  P1C.4 reconcile: its cache files older than the new stamps would adopt the DB sets,
  and a newer mini-side success (a scottclarkhonda-com shape there) would be lost
  before P1C.4 could merge it;
- a row is also skipped when its ``recipes_json`` is unreadable, when no stamp can be
  worked out, or (``--apply``) when it changed after it was read.

Modes (a dry run is the default for both; nothing is written without ``--apply``):

  python -m backend.scripts.backfill_recipe_saved_at
      Dry run: counts plus up to 20 ids per class. Reads only (SELECT, JSON files).
  python -m backend.scripts.backfill_recipe_saved_at --apply --backup-dir DIR
      Writes ``DIR/dealer_recipes_saved_at_backfill_<UTC stamp>.json`` FIRST (every
      column of every row it will touch, plus the values it will write), then one
      guarded ``UPDATE ... WHERE dealer_id=? AND updated_at=?`` per row. A row whose
      ``updated_at`` moved since it was read (a scan saved it, or a hint write) is
      skipped and listed. The UPDATE sets ``recipes_json``, ``max_saved_at`` and
      ``updated_at`` (bumped, as every ``dealer_recipes`` write does, so other guarded
      writers see the change). It never sets ``scan_hints``.
  python -m backend.scripts.backfill_recipe_saved_at --restore EXPORT [--apply]
      Puts ``recipes_json``, ``max_saved_at`` and ``updated_at`` back from an export,
      only for rows still exactly as this tool left them (same ``updated_at`` and
      ``recipes_json``); a row written since is skipped and listed. No new backup is
      needed: what a restore overwrites is the export's own "after" copy. Refused when
      the export was made against another store.

Run it only on a host whose scanners carry P1B.3's stamping code, or the next
synthesized save brings the 0 stamps back. Output never contains ``recipes_json`` or
``auth_headers``; the export file holds both (it is a backup) and is created mode 0600.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import backend.scanner.recipes as rec  # noqa: E402
from backend.scanner import recipe_store  # noqa: E402

TOOL = "backfill_recipe_saved_at"
EXPORT_VERSION = 1
MAX_IDS = 20
EXPORT_PREFIX = "dealer_recipes_saved_at_backfill_"

ROW_COLUMNS = (
    "dealer_id", "recipes_json", "recipe_count", "provider_hint", "max_saved_at",
    "last_ok_at", "stale_count", "updated_at", "scan_hints",
)

# Skip classes (dry run and apply).
SKIP_UNREADABLE = "unreadable"            # recipes_json is not a JSON list of objects with numeric stamps
SKIP_NO_STAMP = "no_stamp"                # no last_ok_at success and no parseable updated_at
SKIP_CACHE_NEWER = "cache_newer_ok"       # the cache file has a newer success for a key (P1C.4)
SKIP_CACHE_UNREADABLE = "cache_unreadable"  # the cache file exists but cannot be read (P1C.4)
SKIP_CHANGED = "changed"                  # --apply: updated_at moved after the read
SKIP_REASONS = {
    SKIP_UNREADABLE: "recipes_json unreadable",
    SKIP_NO_STAMP: "no last_ok_at and no parseable updated_at",
    SKIP_CACHE_NEWER: "cache has a newer success for a key; left to P1C.4",
    SKIP_CACHE_UNREADABLE: "cache file or its last_ok_at unreadable; left to P1C.4",
    SKIP_CHANGED: "row changed after it was read; not written",
}


class CacheUnreadable(Exception):
    """A cache file exists for the dealer but could not be read as a JSON list."""


@dataclass
class Candidate:
    dealer_id: str
    before: dict[str, Any]          # the row as read, every column
    recipes_json_after: str
    max_saved_at_after: float
    stamp: float
    stamp_source: str               # "last_ok_at" | "updated_at"
    entries_stamped: int
    cache_saved_at: float | None    # the cache file's max saved_at, when one was read
    # This host's cache file holds another set (saved_at aside) and is older than the
    # stamp, so after the backfill load_recipes adopts the DB set here: the host's
    # effective set changes (a discovery.md line in the MAIN run).
    effective_change: bool = False


@dataclass
class Plan:
    rows_read: int = 0
    rows_with_zero: int = 0         # readable rows with a saved_at 0/null entry
    candidates: list[Candidate] = field(default_factory=list)
    skipped: dict[str, list[str]] = field(default_factory=dict)
    cache_dir: Path | None = None
    cache_note: str = ""
    # Why --apply would be refused (a dry run prints these as WARNING lines).
    warnings: list[str] = field(default_factory=list)

    def skip(self, reason: str, dealer_id: str) -> None:
        self.skipped.setdefault(reason, []).append(dealer_id)


@dataclass
class ApplyResult:
    export_path: Path
    applied: list[str] = field(default_factory=list)
    changed: list[str] = field(default_factory=list)
    verify_failed: list[str] = field(default_factory=list)


@dataclass
class RestoreResult:
    restorable: list[str] = field(default_factory=list)
    changed: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    restored: list[str] = field(default_factory=list)


# ── reading ────────────────────────────────────────────────────────────────────


def _num(value: Any) -> float:
    """A stamp as a float; 0 for 0 / null / missing. Raises on anything else."""
    if value is None or value == "":
        return 0.0
    if isinstance(value, bool):
        raise ValueError("boolean stamp")
    return float(value)


def _epoch_from_updated_at(text: Any) -> float | None:
    """``updated_at`` (ISO text, as ``recipe_store`` writes it) as epoch seconds."""
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(str(text).strip())
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    epoch = dt.timestamp()
    return epoch if epoch > 0 else None


def _entry_key(row: Any) -> tuple[str, str] | None:
    """The recipe key ``load_recipes`` / ``promote`` use (``EndpointRecipe.key``)."""
    if not isinstance(row, dict):
        return None
    try:
        recs = rec._rows_to_recipes([row])
        return recs[0].key() if recs else None
    except Exception:  # noqa: BLE001 — a row the scanner could not key either
        return None


def _read_rows(conn) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(f"SELECT {', '.join(ROW_COLUMNS)} FROM dealer_recipes ORDER BY dealer_id")
    return [dict(zip(ROW_COLUMNS, r)) for r in cur.fetchall()]


def _cache_aliases(cache_dir: Path) -> dict[str, str]:
    """``{old_slug: current_dealer_id}`` from ``<cache dir>/_aliases.json`` (as
    ``recipes._load_recipe_aliases`` reads it for the scanner's cache)."""
    try:
        raw = json.loads((cache_dir / rec.ALIASES_FILENAME).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {
        rec._recipe_slug(k): v.strip()
        for k, v in raw.items()
        if isinstance(k, str) and isinstance(v, str) and k.strip() and v.strip()
    }


def _cache_read_path(cache_dir: Path, dealer_id: str) -> Path:
    """The file ``load_recipes`` would read for the dealer in this cache dir: the
    slug's own file, else an alias-mapped retired slug's file (``_recipe_read_path``)."""
    slug = rec._recipe_slug(dealer_id)
    path = cache_dir / f"{slug}.json"
    if path.exists():
        return path
    for old_slug, new_id in _cache_aliases(cache_dir).items():
        if old_slug != slug and rec._recipe_slug(new_id) == slug:
            aliased = cache_dir / f"{old_slug}.json"
            if aliased.exists():
                return aliased
    return path


def _cache_rows(cache_dir: Path, dealer_id: str) -> list[dict[str, Any]] | None:
    """The cache file's rows (``json.load``, never ``load_recipes``, which writes);
    None when the dealer has no file. Raises ``CacheUnreadable``."""
    path = _cache_read_path(cache_dir, dealer_id)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise CacheUnreadable(type(exc).__name__) from exc
    if not isinstance(raw, list):
        raise CacheUnreadable("not a list")
    return [r for r in raw if isinstance(r, dict)]


def _cache_has_newer_success(cache_rows: list[dict[str, Any]], db_rows: list[dict[str, Any]]) -> bool:
    """True when a cache key's ``last_ok_at`` is newer than the DB's for that key, or,
    for a key the DB set lacks, newer than the DB set's latest success."""
    db_ok: dict[tuple[str, str], float] = {}
    for r in db_rows:
        k = _entry_key(r)
        if k is not None:
            db_ok[k] = max(db_ok.get(k, 0.0), _num(r.get("last_ok_at")))
    db_max_ok = max((_num(r.get("last_ok_at")) for r in db_rows), default=0.0)
    for r in cache_rows:
        try:
            ok = _num(r.get("last_ok_at"))
        except (TypeError, ValueError) as exc:
            # Fail closed, as for an unreadable file: a success that cannot be read
            # cannot be shown to be older than the DB's.
            raise CacheUnreadable("non-numeric last_ok_at") from exc
        k = _entry_key(r)
        if k is None:
            continue
        if ok > db_ok.get(k, db_max_ok):
            return True
    return False


def _without_saved_at(rows: list[dict[str, Any]]) -> str:
    return json.dumps([{k: v for k, v in r.items() if k != "saved_at"} for r in rows],
                      sort_keys=True, ensure_ascii=False)


def _max_saved(rows: list[dict[str, Any]]) -> float:
    vals = []
    for r in rows:
        try:
            vals.append(_num(r.get("saved_at")))
        except (TypeError, ValueError):
            continue
    return max(vals) if vals else 0.0


# ── planning ───────────────────────────────────────────────────────────────────


def plan_backfill(conn, *, cache_dir: Path | None) -> Plan:
    """Read every ``dealer_recipes`` row and decide what the backfill would write.
    Reads only."""
    plan = Plan(cache_dir=cache_dir)
    rows = _read_rows(conn)
    plan.rows_read = len(rows)
    for row in rows:
        did = row["dealer_id"]
        try:
            entries = json.loads(row["recipes_json"] or "")
            if not isinstance(entries, list) or not all(isinstance(e, dict) for e in entries):
                raise ValueError("not a list of objects")
            zero = [i for i, e in enumerate(entries) if _num(e.get("saved_at")) == 0]
            max_ok = max((_num(e.get("last_ok_at")) for e in entries), default=0.0)
        except (TypeError, ValueError):
            plan.skip(SKIP_UNREADABLE, did)  # cannot show it has no 0 stamps: listed, never written
            continue
        if not zero:
            continue
        plan.rows_with_zero += 1
        if max_ok > 0:
            stamp, source = max_ok, "last_ok_at"
        else:
            stamp, source = _epoch_from_updated_at(row["updated_at"]), "updated_at"
            if stamp is None:
                plan.skip(SKIP_NO_STAMP, did)
                continue
        cache_saved: float | None = None
        cache_rows: list[dict[str, Any]] | None = None
        if cache_dir is not None:
            try:
                cache_rows = _cache_rows(cache_dir, did)
            except CacheUnreadable:
                plan.skip(SKIP_CACHE_UNREADABLE, did)
                continue
            if cache_rows is not None:
                try:
                    newer = _cache_has_newer_success(cache_rows, entries)
                except CacheUnreadable:
                    plan.skip(SKIP_CACHE_UNREADABLE, did)
                    continue
                if newer:
                    plan.skip(SKIP_CACHE_NEWER, did)
                    continue
                cache_saved = _max_saved(cache_rows)
        after = [dict(e) for e in entries]
        for i in zero:
            after[i]["saved_at"] = stamp
        max_after = float(recipe_store._derive_meta(after)["max_saved_at"])
        plan.candidates.append(Candidate(
            dealer_id=did,
            before=dict(row),
            recipes_json_after=json.dumps(after, ensure_ascii=False),
            max_saved_at_after=max_after,
            stamp=stamp,
            stamp_source=source,
            entries_stamped=len(zero),
            cache_saved_at=cache_saved,
            effective_change=(
                cache_rows is not None and cache_saved is not None and cache_saved < max_after
                and _without_saved_at(cache_rows) != _without_saved_at(entries)
            ),
        ))
    return plan


def count_unstamped(conn) -> int:
    """Rows the backfill exists for: ``max_saved_at=0`` with recipes."""
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM dealer_recipes WHERE max_saved_at = 0 AND recipe_count > 0")
    return int(cur.fetchone()[0])


# ── apply ──────────────────────────────────────────────────────────────────────


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_export(path: Path, payload: dict[str, Any]) -> None:
    """Create ``path`` (mode 0600, never over an existing file), write, fsync."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=1)
            fh.flush()
            os.fsync(fh.fileno())
    except BaseException:
        try:
            os.unlink(path)
        except OSError:
            pass
        raise


def _export_path(backup_dir: Path) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = backup_dir / f"{EXPORT_PREFIX}{stamp}.json"
    n = 1
    while path.exists():
        n += 1
        path = backup_dir / f"{EXPORT_PREFIX}{stamp}-{n}.json"
    return path


def _guarded(sql_head: str, updated_at: Any) -> tuple[str, bool]:
    """``sql_head`` plus the ``updated_at`` guard (``IS NULL`` for a null stamp, so
    Postgres never sees an untyped null parameter)."""
    if updated_at is None:
        return sql_head + " AND updated_at IS NULL", False
    return sql_head + " AND updated_at = ?", True


def apply_plan(conn, plan: Plan, *, backup_dir: Path, store: str | None) -> ApplyResult:
    """Export every candidate row, then write each through a guarded UPDATE.

    The export is complete and fsynced before the first UPDATE. Each UPDATE matches
    the row only while its ``updated_at`` is the one read by ``plan_backfill``; a row
    written since is skipped (``changed``). One transaction, committed at the end."""
    updated_after = _now_iso()
    export_path = _export_path(backup_dir)
    _write_export(export_path, {
        "tool": TOOL,
        "version": EXPORT_VERSION,
        "created_at": updated_after,
        "store": store,
        "columns_written": ["recipes_json", "max_saved_at", "updated_at"],
        "rows": [
            {
                "dealer_id": c.dealer_id,
                "stamp": c.stamp,
                "stamp_source": c.stamp_source,
                "cache_saved_at": c.cache_saved_at,
                "effective_change": c.effective_change,
                "before": c.before,
                "after": {
                    "recipes_json": c.recipes_json_after,
                    "max_saved_at": c.max_saved_at_after,
                    "updated_at": updated_after,
                },
            }
            for c in plan.candidates
        ],
    })
    result = ApplyResult(export_path=export_path)
    cur = conn.cursor()
    try:
        for c in plan.candidates:
            sql, bind = _guarded(
                "UPDATE dealer_recipes SET recipes_json = ?, max_saved_at = ?, updated_at = ? "
                "WHERE dealer_id = ?",
                c.before["updated_at"],
            )
            params: tuple[Any, ...] = (c.recipes_json_after, c.max_saved_at_after, updated_after, c.dealer_id)
            if bind:
                params += (c.before["updated_at"],)
            cur.execute(sql, params)
            if cur.rowcount == 1:
                result.applied.append(c.dealer_id)
            else:
                result.changed.append(c.dealer_id)
        conn.commit()
    except BaseException:
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        raise
    by_id = {c.dealer_id: c for c in plan.candidates}
    for did in result.applied:
        cur.execute("SELECT recipes_json, max_saved_at, updated_at FROM dealer_recipes WHERE dealer_id = ?", (did,))
        got = cur.fetchone()
        c = by_id[did]
        if (not got or got[0] != c.recipes_json_after or float(got[1] or 0) != c.max_saved_at_after
                or got[2] != updated_after):
            result.verify_failed.append(did)
    return result


# ── restore ────────────────────────────────────────────────────────────────────


def load_export(path: Path) -> dict[str, Any]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.get("tool") != TOOL or data.get("version") != EXPORT_VERSION:
        raise ValueError(f"{path} is not a {TOOL} export (version {EXPORT_VERSION})")
    if not isinstance(data.get("rows"), list):
        raise ValueError(f"{path} has no rows list")
    return data


def restore(conn, export: dict[str, Any], *, apply: bool) -> RestoreResult:
    """Put the exported "before" values back on rows still exactly as the backfill
    left them (``updated_at`` and ``recipes_json`` equal to the export's "after").
    Dry run unless ``apply``."""
    res = RestoreResult()
    cur = conn.cursor()
    todo: list[dict[str, Any]] = []
    for item in export["rows"]:
        did = item["dealer_id"]
        after = item["after"]
        cur.execute("SELECT updated_at, recipes_json FROM dealer_recipes WHERE dealer_id = ?", (did,))
        got = cur.fetchone()
        if not got:
            res.missing.append(did)
        elif got[0] == after["updated_at"] and got[1] == after["recipes_json"]:
            res.restorable.append(did)
            todo.append(item)
        else:
            res.changed.append(did)
    if not apply:
        return res
    try:
        for item in todo:
            before = item["before"]
            cur.execute(
                "UPDATE dealer_recipes SET recipes_json = ?, max_saved_at = ?, updated_at = ? "
                "WHERE dealer_id = ? AND updated_at = ? AND recipes_json = ?",
                (before["recipes_json"], before["max_saved_at"], before["updated_at"],
                 item["dealer_id"], item["after"]["updated_at"], item["after"]["recipes_json"]),
            )
            if cur.rowcount == 1:
                res.restored.append(item["dealer_id"])
            else:
                res.changed.append(item["dealer_id"])
        conn.commit()
    except BaseException:
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        raise
    return res


# ── report ─────────────────────────────────────────────────────────────────────


def _ids(ids: list[str], limit: int | None = MAX_IDS) -> str:
    """Up to ``limit`` ids (all of them when ``limit`` is None)."""
    if limit is None:
        return ", ".join(ids)
    shown = ", ".join(ids[:limit])
    more = len(ids) - limit
    return shown + (f" ... (+{more} more)" if more > 0 else "")


def _day(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d")


def print_plan(plan: Plan, *, header: str) -> None:
    print(header)
    print(f"cache check: {plan.cache_note}")
    for w in plan.warnings:
        print(f"WARNING: {w}")
    print(f"dealer_recipes rows read: {plan.rows_read}")
    print(f"rows with saved_at 0/null entries: {plan.rows_with_zero} "
          f"(plus {len(plan.skipped.get(SKIP_UNREADABLE) or [])} unreadable row(s), listed below)")
    cands = plan.candidates
    sources = Counter(c.stamp_source for c in cands)
    print(f"  to backfill: {len(cands)} row(s), {sum(c.entries_stamped for c in cands)} entries; "
          f"stamp from last_ok_at: {sources.get('last_ok_at', 0)}, "
          f"from updated_at: {sources.get('updated_at', 0)}")
    if cands:
        by_upd = Counter(str(c.before.get("updated_at") or "")[:10] or "none" for c in cands)
        print("    by updated_at date: " + ", ".join(f"{d} {n}" for d, n in sorted(by_upd.items())))
        by_stamp = Counter(_day(c.stamp) for c in cands)
        print("    by stamp date:      " + ", ".join(f"{d} {n}" for d, n in sorted(by_stamp.items())))
        print(f"    ids: {_ids([c.dealer_id for c in cands])}")
    skipped = sum(len(v) for k, v in plan.skipped.items() if k != SKIP_UNREADABLE)
    print(f"  skipped: {skipped}")
    for reason in (SKIP_CACHE_NEWER, SKIP_CACHE_UNREADABLE, SKIP_NO_STAMP):
        ids = plan.skipped.get(reason) or []
        if ids:
            print(f"    {reason} ({SKIP_REASONS[reason]}): {len(ids)}: {_ids(ids)}")
    unreadable = plan.skipped.get(SKIP_UNREADABLE) or []
    if unreadable:
        print(f"  {SKIP_UNREADABLE} ({SKIP_REASONS[SKIP_UNREADABLE]}; left as they are): "
              f"{len(unreadable)}: {_ids(unreadable)}")
    changes = [c.dealer_id for c in cands if c.effective_change]
    print(f"note: effective set changes on this host (its cache file holds another set and will "
          f"adopt the DB set): {len(changes)}" + (f": {_ids(changes)}" if changes else ""))
    still_newer = [c.dealer_id for c in cands
                   if c.cache_saved_at is not None and c.cache_saved_at > c.max_saved_at_after]
    print(f"note: cache files still newer than the backfilled stamp (load_recipes keeps pushing "
          f"these up; P1C.4): {len(still_newer)}" + (f": {_ids(still_newer)}" if still_newer else ""))
    if plan.cache_dir is not None:
        print("note: only this cache dir is checked; another host writing this store (the mini over "
              "the tunnel) must not scan between an --apply here and its own P1C.4 reconcile")
    if plan.warnings:
        print(f"WARNING: --apply would be refused ({len(plan.warnings)} reason(s) above); "
              f"this dry run is not the plan --apply would carry out")


# ── CLI ────────────────────────────────────────────────────────────────────────


@dataclass
class CacheCheck:
    cache_dir: Path | None          # the dir plan_backfill checks (None: no check)
    note: str
    refusals: list[str] = field(default_factory=list)  # why --apply is refused


def _cache_check(args: argparse.Namespace, ap: argparse.ArgumentParser) -> CacheCheck:
    """Which cache dir to check. Fails closed: when the check cannot do its job,
    ``refusals`` says why and ``--apply`` is refused (a dry run warns)."""
    if args.no_cache_check:
        return CacheCheck(None, "off (--no-cache-check)")
    explicit = args.cache_dir is not None
    if explicit:
        d = Path(args.cache_dir).expanduser().absolute()
        if not d.is_dir():
            ap.error(f"--cache-dir {d} is not a directory")
    else:
        d = rec.resolve_recipes_dir()
        if not d.is_dir():
            return CacheCheck(None, f"NOT RUN: no cache dir at {d}", [
                f"the default cache dir {d} does not exist (a git worktree has no workspace/recipes), so the "
                f"cache check that skips dealers like scottclarkhonda-com did not run; pass --cache-dir "
                f"<the scanner's cache dir>, or --no-cache-check on purpose",
            ])
    check = rec.check_cache_store(d)
    files = sum(1 for p in d.glob("*.json") if not p.name.startswith("_"))
    out = CacheCheck(d, f"{d} ({files} dealer file(s); store tag verdict: {check.verdict})")
    if files == 0 and not explicit:
        out.refusals.append(
            f"the default cache dir {d} holds no dealer files, so the cache check skips nothing; "
            f"pass --cache-dir <the scanner's cache dir>, or --no-cache-check on purpose")
    if check.verdict == "mismatch":
        out.refusals.append(
            f"the cache dir {d} is tagged for another store (verdict mismatch): its successes are no "
            f"evidence about this store, and this store's own cache is not the one checked; pass the "
            f"cache dir that mirrors this store, or --no-cache-check on purpose")
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m backend.scripts.backfill_recipe_saved_at",
        description="Stamp dealer_recipes entries whose saved_at is 0/null (dry run by default).",
    )
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="report only (the default)")
    mode.add_argument("--apply", action="store_true", help="write (needs --backup-dir, except with --restore)")
    ap.add_argument("--backup-dir", type=Path, help="where --apply writes the JSON export first")
    ap.add_argument("--restore", type=Path, metavar="EXPORT",
                    help="put rows back from an export (dry run unless --apply)")
    ap.add_argument("--cache-dir", help="recipe cache dir to check (default: RECIPES_CACHE_DIR, "
                                        "else <repo>/workspace/recipes; --apply is refused when the "
                                        "default is missing or empty)")
    ap.add_argument("--no-cache-check", action="store_true",
                    help="do not check any cache dir (on purpose: rows with a newer cache success "
                         "are then stamped too)")
    args = ap.parse_args(argv)
    if args.restore is not None and (args.cache_dir or args.no_cache_check or args.backup_dir):
        ap.error("--restore takes no --cache-dir, --no-cache-check or --backup-dir")
    if args.apply and args.restore is None and args.backup_dir is None:
        ap.error("--apply needs --backup-dir (the JSON export of the touched rows is written first)")
    if args.cache_dir and args.no_cache_check:
        ap.error("--cache-dir and --no-cache-check exclude each other")

    from backend.utils.project_env import load_project_dotenv

    # The scanner's .env (shell values win), so the store and the cache are the scanner's.
    load_project_dotenv()
    if not recipe_store._enabled():
        print("refused: RECIPES_DB_DISABLED is on; this tool works on the dealer_recipes store")
        return 2
    store = rec.store_identity()

    if args.restore is not None:
        try:
            export = load_export(args.restore)
        except (OSError, ValueError) as exc:
            print(f"restore refused: {type(exc).__name__}: {exc}")
            return 2
        if store is None:
            print("restore refused: this process's store identity could not be worked out, so the "
                  "export cannot be matched to it")
            return 2
        if export.get("store") != store:
            print(f"restore refused: the export was made against store {export.get('store')!r}, "
                  f"this process uses {store!r}")
            return 2
        conn = recipe_store._conn()
        try:
            res = restore(conn, export, apply=args.apply)
        finally:
            conn.close()
        print(f"{TOOL} --restore {args.restore}: {'APPLY' if args.apply else 'DRY RUN (nothing written)'}")
        print(f"store: {store}")
        print(f"export rows: {len(export['rows'])}")
        print(f"  restorable (unchanged since the backfill): {len(res.restorable)}")
        if args.apply:
            print(f"  restored: {len(res.restored)}" + (f": {_ids(res.restored)}" if res.restored else ""))
        limit = None if args.apply else MAX_IDS  # an apply lists every id it did not restore
        print(f"  changed since the backfill (left as they are): {len(res.changed)}"
              + (f": {_ids(res.changed, limit)}" if res.changed else ""))
        print(f"  missing: {len(res.missing)}" + (f": {_ids(res.missing, limit)}" if res.missing else ""))
        return 0

    cc = _cache_check(args, ap)
    refusals = list(cc.refusals)
    if store is None:
        refusals.append("this process's store identity could not be worked out, so the export "
                        "could not be tied to a store for --restore")
    if args.apply and refusals:
        print(f"{TOOL}: APPLY refused (nothing written, no export made)")
        print(f"store: {store}")
        print(f"cache check: {cc.note}")
        for r in refusals:
            print(f"refused: {r}")
        return 2
    conn = recipe_store._conn()
    try:
        plan = plan_backfill(conn, cache_dir=cc.cache_dir)
        plan.cache_note = cc.note
        plan.warnings = refusals
        if not args.apply:
            print_plan(plan, header=f"{TOOL}: DRY RUN (nothing written)\nstore: {store}")
            return 0
        print_plan(plan, header=f"{TOOL}: APPLY\nstore: {store}")
        if not plan.candidates:
            remaining = count_unstamped(conn)
            print("nothing to write; no export made")
            print(f"rows with max_saved_at=0 and recipes: {remaining}")
            return 0
        result = apply_plan(conn, plan, backup_dir=args.backup_dir, store=store)
        remaining = count_unstamped(conn)
    finally:
        conn.close()
    print(f"export written first: {result.export_path} ({len(plan.candidates)} row(s))")
    print(f"  applied: {len(result.applied)}")
    # Every id (no cap): the export lists every candidate, so these two lists are
    # what separates the applied rows from the rest.
    print(f"  {SKIP_CHANGED} ({SKIP_REASONS[SKIP_CHANGED]}): {len(result.changed)}"
          + (f": {_ids(result.changed, None)}" if result.changed else ""))
    if result.verify_failed:
        print(f"  VERIFY FAILED (row read back differs from what was written; written again since?): "
              f"{len(result.verify_failed)}: "
              f"{_ids(result.verify_failed, None)}")
    print(f"rows with max_saved_at=0 and recipes after the run: {remaining}")
    print(f"rollback: python -m backend.scripts.{TOOL} --restore {result.export_path} --apply")
    return 3 if result.verify_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
