#!/usr/bin/env python3
"""
Apply the N8.9 repair set (``workspace/recipes/repairs_2026_10/*.json``): dry run, backup,
apply, verify. Idempotent: a second apply finds nothing to do.

One JSON file per change. Four shapes, told apart by name and content:

  recipe_<dealer_id>.json   a JSON list of recipe rows, or ``{"schema": "n8.9.recipe_set.v1",
                            "dealer_id", "recipes": [...], "archive": [...]}``. The dealer's
                            ``dealer_recipes`` set is replaced by ``recipes``; ``archive``
                            (the set the repair retires, LE-14) stays in the repair file.
  roster_add_<dealer_id>.json  ``n1.11.roster_add.v1`` with action ``add`` (insert the
                            ``dealerships_row`` and its ``dealer_geopoints`` point),
                            ``repair`` or ``complete_registry_row`` (update the existing
                            registry row with the differing columns of ``dealerships_row``).
  registry_fix_<label>.json ``n8.9.registry_fix.v1``: ``{"match": {"host", optional "id" /
                            "street_address" / "name"}, "set": {column: value}, optional
                            "expect": {column: value}}``. Exactly one row must match; with
                            ``expect`` a row whose column holds neither the expected nor the
                            new value is a conflict (the row moved since the repair was cut).

Registry rows are matched by host first (``normalize_dealer_host`` over ``website_url`` and
``dealer_website_url``), then by street (normalized) or the explicit id; a name alone never
identifies a store, because group sites and campuses share hosts and streets.

Per repair the dry run reports one of: ``change`` (what would be written), ``noop`` (already
applied), ``gated`` (a roster_add with ``apply_gate`` or ``shared_host``, and the recipe file
of the same dealer: skipped unless ``--allow-gated <dealer_id>`` names it), ``conflict`` (the
store disagrees with the repair: reported, never forced) or ``invalid``. Auth headers are
never printed; a recipe is named by method, host and path.

The default is a dry run. ``--apply`` needs ``--backup-dir``: before its first write the run
dir ``<backup-dir>/recipe_repairs_<UTC stamp>/`` gets ``backup.json`` (every ``dealer_recipes``
and ``dealerships`` row the run will change, whole, plus the inserts it makes) and the plan;
every write is logged to ``applied.jsonl``; afterwards each applied repair is re-planned and
must be a ``noop`` (verify). Exit status: 0 done / nothing to do, 1 a conflict, invalid file,
failed write or failed verify, 2 refused.

  PYTHONPATH=. python backend/scripts/apply_recipe_repairs.py [--dir DIR] [--only ID ...]
  PYTHONPATH=. python backend/scripts/apply_recipe_repairs.py --apply --backup-dir workspace/backups
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.db.dealer_geo import normalize_dealer_host  # noqa: E402
from backend.scanner import recipe_store  # noqa: E402

DEFAULT_DIR = _REPO_ROOT / "workspace" / "recipes" / "repairs_2026_10"
DEFAULT_BACKUP_PARENT = _REPO_ROOT / "workspace" / "backups"

SCHEMA_ROSTER_ADD = "n1.11.roster_add.v1"
SCHEMA_RECIPE_SET = "n8.9.recipe_set.v1"
SCHEMA_REGISTRY_FIX = "n8.9.registry_fix.v1"

# Columns a repair may write on a registry row (insert, repair or fix).
_REGISTRY_COLUMNS = (
    "name", "website_url", "dealer_website_url", "city", "state", "zip_code", "street_address",
    "latitude", "longitude", "phone", "platform", "strategy", "oem_brand", "is_active",
    "street_address_source", "street_address_verified_at",
)
_INSERT_REQUIRED = ("name", "website_url", "city", "state")
_FLOAT_COLUMNS = ("latitude", "longitude")
_INT_COLUMNS = ("is_active",)

_STREET_WORDS = {
    "parkway": "pkwy", "road": "rd", "drive": "dr", "street": "st", "avenue": "ave", "boulevard": "blvd",
    "highway": "hwy", "lane": "ln", "court": "ct", "north": "n", "south": "s", "east": "e", "west": "w",
}


# ── plan model ───────────────────────────────────────────────────────────────

@dataclass
class Repair:
    path: Path
    kind: str            # recipe_set | roster_add | registry_fix | invalid
    dealer_id: str
    data: Any = None
    error: str = ""


@dataclass
class Plan:
    repair: Repair
    status: str          # change | noop | gated | conflict | invalid
    detail: list[str] = field(default_factory=list)
    ops: list[dict[str, Any]] = field(default_factory=list)


# ── helpers ──────────────────────────────────────────────────────────────────

def connect():
    """The store connection every read and write goes through (``recipe_store``'s)."""
    return recipe_store._conn()


def norm_street(s: Any) -> str:
    """Street key: lowercase words, common suffixes abbreviated, suite parts dropped."""
    text = re.sub(r"\b(suite|ste|unit|bldg|building|#)\b.*$", "", str(s or "").lower())
    words = re.findall(r"[a-z0-9]+", text)
    return " ".join(_STREET_WORDS.get(w, w) for w in words)


def _same(col: str, old: Any, new: Any) -> bool:
    if col in _FLOAT_COLUMNS:
        try:
            return abs(float(old) - float(new)) < 1e-6
        except (TypeError, ValueError):
            return False
    if col in _INT_COLUMNS:
        try:
            return int(old) == int(new)
        except (TypeError, ValueError):
            return False
    return (old if old is not None else "") == (new if new is not None else "")


def _recipe_label(r: dict) -> str:
    u = urlparse(str(r.get("url") or ""))
    return f"{r.get('method', 'GET')} {u.netloc}{u.path} [{r.get('provider_hint') or '-'}]"


def _registry_rows(cur) -> list[dict[str, Any]]:
    cur.execute("SELECT * FROM dealerships")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _hosts(row: dict[str, Any]) -> set[str]:
    return {h for h in (normalize_dealer_host(str(row.get(k) or "")) for k in ("website_url", "dealer_website_url")) if h}


def _by_host(rows: list[dict[str, Any]], host: str) -> list[dict[str, Any]]:
    return [r for r in rows if host in _hosts(r)]


def _url_key(url: Any) -> str:
    """A store-page URL with scheme, ``www.`` and trailing slash dropped: the path is what
    tells stores on a shared host apart (``autosavvy.com/dealers/lindon-utah``)."""
    return re.sub(r"^https?://(www\.)?", "", str(url or "").strip().lower()).rstrip("/")


def _same_store(row: dict[str, Any], street: str, page: str) -> bool:
    """A registry row is this store when it has the same street, or (no street known)
    the same store-page URL with a path. Never by host alone (N8.14)."""
    if street and norm_street(row.get("street_address")) == street:
        return True
    return bool(page and "/" in page and page in {_url_key(row.get(k)) for k in ("website_url", "dealer_website_url")})


def _has_table(conn, table: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT 1 FROM {table} LIMIT 1")
        cur.fetchall()
        return True
    except Exception:  # noqa: BLE001 - absent table on either backend
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        return False


# ── loading ──────────────────────────────────────────────────────────────────

def load_repairs(directory: Path, only: list[str] | None = None) -> list[Repair]:
    out: list[Repair] = []
    for path in sorted(Path(directory).glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            out.append(Repair(path, "invalid", path.stem, error=f"unreadable ({type(exc).__name__})"))
            continue
        rep = _classify(path, data)
        if only and rep.dealer_id not in only:
            continue
        out.append(rep)
    return out


def _classify(path: Path, data: Any) -> Repair:
    stem = path.stem
    if isinstance(data, list):
        ids = {str(r.get("dealer_id") or "") for r in data if isinstance(r, dict)}
        if len(ids) != 1 or "" in ids or not all(isinstance(r, dict) for r in data):
            return Repair(path, "invalid", stem, error="recipe list must be dicts of one dealer_id")
        return Repair(path, "recipe_set", ids.pop(), {"recipes": data, "archive": []})
    if not isinstance(data, dict):
        return Repair(path, "invalid", stem, error="not a JSON list or object")
    schema = data.get("schema")
    if schema == SCHEMA_RECIPE_SET:
        did = str(data.get("dealer_id") or "")
        recs = data.get("recipes")
        if not did or not isinstance(recs, list) or any(not isinstance(r, dict) or r.get("dealer_id") != did for r in recs):
            return Repair(path, "invalid", stem, error="recipe_set needs dealer_id and recipes of that dealer")
        return Repair(path, "recipe_set", did, {"recipes": recs, "archive": data.get("archive") or []})
    if schema == SCHEMA_ROSTER_ADD:
        row = data.get("dealerships_row")
        action = data.get("action")
        if not data.get("dealer_id") or not isinstance(row, dict) or action not in ("add", "repair", "complete_registry_row"):
            return Repair(path, "invalid", stem, error="roster_add needs dealer_id, dealerships_row and a known action")
        if not normalize_dealer_host(str(row.get("website_url") or "")):
            return Repair(path, "invalid", str(data["dealer_id"]), error="dealerships_row.website_url has no host")
        return Repair(path, "roster_add", str(data["dealer_id"]), data)
    if schema == SCHEMA_REGISTRY_FIX:
        m, s = data.get("match"), data.get("set")
        bad_cols = [c for c in (s or {}) if c not in _REGISTRY_COLUMNS]
        if not isinstance(m, dict) or not m.get("host") or not isinstance(s, dict) or not s or bad_cols:
            return Repair(path, "invalid", stem, error=f"registry_fix needs match.host and set of known columns {bad_cols}")
        return Repair(path, "registry_fix", str(data.get("dealer_id") or stem), data)
    return Repair(path, "invalid", stem, error=f"unknown schema {schema!r}")


# ── planning ─────────────────────────────────────────────────────────────────

def _gated_ids(repairs: list[Repair]) -> set[str]:
    return {
        r.dealer_id for r in repairs
        if r.kind == "roster_add" and (r.data.get("apply_gate") or r.data.get("shared_host"))
    }


def build_plans(conn, repairs: list[Repair], allow_gated: set[str] | None = None) -> list[Plan]:
    allow = allow_gated or set()
    gated = _gated_ids(repairs) - allow
    cur = conn.cursor()
    registry = _registry_rows(cur)
    has_geo = _has_table(conn, "dealer_geopoints")
    plans: list[Plan] = []
    for rep in repairs:
        if rep.kind == "invalid":
            plans.append(Plan(rep, "invalid", [rep.error]))
        elif rep.dealer_id in gated and rep.kind in ("roster_add", "recipe_set"):
            why = "apply_gate / shared_host: see the file (N8.14 owns shared-host identity); pass --allow-gated to override"
            plans.append(Plan(rep, "gated", [why]))
        elif rep.kind == "recipe_set":
            plans.append(_plan_recipe_set(rep))
        elif rep.kind == "roster_add":
            plans.append(_plan_roster_add(rep, registry, has_geo))
        else:
            plans.append(_plan_registry_fix(rep, registry))
    return plans


def _plan_recipe_set(rep: Repair) -> Plan:
    new = rep.data["recipes"]
    try:
        found = recipe_store._db_load_recipes_exact(rep.dealer_id, strict=True)
    except recipe_store.RecipeStoreReadError as exc:
        return Plan(rep, "conflict", [str(exc)])
    old = found[0] if found else None
    if old is not None and json.dumps(old, sort_keys=True) == json.dumps(new, sort_keys=True):
        return Plan(rep, "noop", [f"dealer_recipes already holds these {len(new)} recipe(s)"])
    detail = [f"dealer_recipes {rep.dealer_id}: {len(old) if old is not None else 'no row'} -> {len(new)} recipe(s)"]
    detail += [f"  - drop {_recipe_label(r)}" for r in (old or [])]
    detail += [f"  + keep {_recipe_label(r)}" for r in new]
    if rep.data["archive"]:
        detail.append(f"  ({len(rep.data['archive'])} recipe(s) archived in the repair file)")
    return Plan(rep, "change", detail, [{"op": "recipe_set", "dealer_id": rep.dealer_id, "rows": new, "before": old}])


def _column_changes(old: dict[str, Any], new: dict[str, Any]) -> dict[str, tuple[Any, Any]]:
    return {
        c: (old.get(c), v) for c, v in new.items()
        if c in _REGISTRY_COLUMNS and v is not None and not _same(c, old.get(c), v)
    }


def _plan_roster_add(rep: Repair, registry: list[dict[str, Any]], has_geo: bool) -> Plan:
    data = rep.data
    row = {k: v for k, v in data["dealerships_row"].items() if k in _REGISTRY_COLUMNS}
    host = normalize_dealer_host(row["website_url"])
    street = norm_street(row.get("street_address"))
    hits = _by_host(registry, host)
    shared = bool(data.get("shared_host"))
    street_hits = [h for h in hits if street and norm_street(h.get("street_address")) == street]
    if shared:
        # stores on one host (AutoSavvy, Ted Russell Parkside) are told apart by street or
        # store page, and a registry row on that host is a sister, not this store
        street_hits = [h for h in hits if _same_store(h, street, _url_key(row["website_url"]))]
    action = data["action"]
    if action == "add":
        if street_hits:
            return Plan(rep, "noop", [f"registry already has {host} at this street (id {street_hits[0]['id']})"])
        if shared and not street and "/" not in _url_key(row["website_url"]):
            return Plan(rep, "conflict", [f"shared host {host}: the row needs a street_address or a store-page path in website_url to be told from its sister stores"])
        if hits and not shared:
            seen = ", ".join(f"id {h['id']} '{h.get('street_address') or '-'}'" for h in hits)
            return Plan(rep, "conflict", [f"host {host} is in the registry at another or unknown street ({seen}); "
                                          "a repair/complete_registry_row action, or a registry_fix, must settle it"])
        missing = [c for c in _INSERT_REQUIRED if not row.get(c)]
        if missing:
            return Plan(rep, "invalid", [f"dealerships_row lacks {missing}"])
        ops: list[dict[str, Any]] = [{"op": "registry_insert", "row": row}]
        if has_geo and row.get("latitude") is not None and row.get("longitude") is not None:
            ops.append({"op": "geopoint_insert", "row": row})
        return Plan(rep, "change", [f"dealerships + {row['name']} ({row['city']}, {row['state']}) {host}, {row.get('street_address') or 'no street'}"], ops)
    # repair / complete_registry_row: the registry row exists
    rid = data.get("existing_dealerships_id") or data.get("existing_registry_id")
    if rid is not None:
        target = [h for h in registry if str(h["id"]) == str(rid)]
        if not target or host not in _hosts(target[0]):
            return Plan(rep, "conflict", [f"registry id {rid} is missing or is not on host {host}"])
    elif len(street_hits) == 1:
        target = street_hits
    elif len(hits) == 1 and not norm_street(hits[0].get("street_address")) and not shared:
        target = hits      # the streetless row of this host: the one complete_registry_row fills
    else:
        return Plan(rep, "conflict", [f"{len(hits)} registry row(s) on {host}; name the row with existing_dealerships_id"])
    changes = _column_changes(target[0], row)
    if not changes:
        return Plan(rep, "noop", [f"registry id {target[0]['id']} already carries this row"])
    detail = [f"dealerships id {target[0]['id']}:"] + [f"  {c}: {o!r} -> {n!r}" for c, (o, n) in changes.items()]
    return Plan(rep, "change", detail, [{"op": "registry_update", "id": target[0]["id"], "changes": changes}])


def _plan_registry_fix(rep: Repair, registry: list[dict[str, Any]]) -> Plan:
    m, want = rep.data["match"], rep.data["set"]
    hits = _by_host(registry, normalize_dealer_host(m["host"]) or str(m["host"]).lower().removeprefix("www."))
    if m.get("id") is not None:
        hits = [h for h in hits if str(h["id"]) == str(m["id"])]
    if m.get("street_address"):
        hits = [h for h in hits if norm_street(h.get("street_address")) == norm_street(m["street_address"])]
    if m.get("name"):
        hits = [h for h in hits if str(h.get("name") or "").strip().lower() == str(m["name"]).strip().lower()]
    if len(hits) != 1:
        return Plan(rep, "conflict", [f"match {m} selects {len(hits)} registry row(s); need exactly 1"])
    if (rep.data.get("shared_host") or len(_by_host(registry, normalize_dealer_host(m["host"]) or str(m["host"]).lower().removeprefix("www."))) > 1) \
            and m.get("id") is None and not m.get("street_address"):
        return Plan(rep, "conflict", [f"host {m['host']} is shared by several stores: match.id or match.street_address must name the store"])
    row = hits[0]
    for col, exp in (rep.data.get("expect") or {}).items():
        if not _same(col, row.get(col), exp) and not _same(col, row.get(col), want.get(col)):
            return Plan(rep, "conflict", [f"id {row['id']} {col} is {row.get(col)!r}, expected {exp!r} or {want.get(col)!r}"])
    changes = _column_changes(row, want)
    clears = {c: (row.get(c), None) for c, v in want.items() if v is None and row.get(c) is not None}
    changes.update(clears)
    if not changes:
        return Plan(rep, "noop", [f"registry id {row['id']} already carries the fix"])
    detail = [f"dealerships id {row['id']} ({row.get('name')}):"] + [f"  {c}: {o!r} -> {n!r}" for c, (o, n) in changes.items()]
    return Plan(rep, "change", detail, [{"op": "registry_update", "id": row["id"], "changes": changes}])


# ── apply ────────────────────────────────────────────────────────────────────

def _export_rows(conn, plans: list[Plan]) -> dict[str, Any]:
    cur = conn.cursor()
    out: dict[str, Any] = {"dealer_recipes": [], "dealerships": [], "inserts": []}
    for plan in plans:
        for op in plan.ops:
            if op["op"] == "recipe_set":
                cur.execute("SELECT * FROM dealer_recipes WHERE dealer_id = ?", (op["dealer_id"],))
                cols = [d[0] for d in cur.description]
                held = [dict(zip(cols, r)) for r in cur.fetchall()]
                out["dealer_recipes"] += held
                if not held:
                    out["inserts"].append({"table": "dealer_recipes", "dealer_id": op["dealer_id"]})
            elif op["op"] == "registry_update":
                cur.execute("SELECT * FROM dealerships WHERE id = ?", (op["id"],))
                cols = [d[0] for d in cur.description]
                out["dealerships"] += [dict(zip(cols, r)) for r in cur.fetchall()]
            elif op["op"] == "registry_insert":
                out["inserts"].append({"table": "dealerships", "website_url": op["row"]["website_url"],
                                       "street_address": op["row"].get("street_address")})
    return out


def _run_op(conn, op: dict[str, Any]) -> str:
    cur = conn.cursor()
    kind = op["op"]
    if kind == "recipe_set":
        if not recipe_store.db_save_recipes(op["dealer_id"], op["rows"]):
            raise RuntimeError("db_save_recipes reported a failed write")
        return f"dealer_recipes {op['dealer_id']} = {len(op['rows'])} recipe(s)"
    if kind == "registry_insert":
        row = op["row"]
        cols = list(row)
        cur.execute(
            f"INSERT INTO dealerships ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)}) RETURNING id",
            [row[c] for c in cols],
        )
        new_id = cur.fetchone()[0]
        conn.commit()
        return f"dealerships id {new_id} {row['name']}"
    if kind == "geopoint_insert":
        row = op["row"]
        cur.execute("SELECT 1 FROM dealer_geopoints WHERE dealer_url = ?", (row["website_url"],))
        if cur.fetchone():
            return "geopoint already present"
        cur.execute(
            "INSERT INTO dealer_geopoints (dealer_url, dealer_name, lat, lon, zip_code, city, state, geocode_source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (row["website_url"], row["name"], row["latitude"], row["longitude"], row.get("zip_code"),
             row.get("city"), row.get("state"), "n8.9_repair"),
        )
        conn.commit()
        return "geopoint added"
    if kind == "registry_update":
        sets = list(op["changes"])
        cur.execute(
            f"UPDATE dealerships SET {', '.join(f'{c} = ?' for c in sets)} WHERE id = ?",
            [op["changes"][c][1] for c in sets] + [op["id"]],
        )
        conn.commit()
        return f"dealerships id {op['id']} {', '.join(sets)}"
    raise ValueError(f"unknown op {kind}")


def run(directory: Path, *, apply: bool, backup_dir: Path | None, only: list[str] | None = None,
        allow_gated: set[str] | None = None, out=print) -> int:
    if apply and backup_dir is None:
        out("refused: --apply needs --backup-dir")
        return 2
    repairs = load_repairs(directory, only)
    conn = connect()
    try:
        plans = build_plans(conn, repairs, allow_gated)
        tally: dict[str, int] = {}
        for p in plans:
            tally[p.status] = tally.get(p.status, 0) + 1
            out(f"[{p.status}] {p.repair.kind} {p.repair.dealer_id} ({p.repair.path.name})")
            for line in p.detail:
                out(f"    {line}")
        out("summary: " + ", ".join(f"{k}={v}" for k, v in sorted(tally.items())) + (" (dry run)" if not apply else ""))
        bad = tally.get("conflict", 0) + tally.get("invalid", 0)
        todo = [p for p in plans if p.status == "change"]
        if not apply:
            return 1 if bad else 0
        failed = 0
        if todo:
            run_dir = Path(backup_dir) / f"recipe_repairs_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
            run_dir.mkdir(parents=True, exist_ok=True)
            (run_dir / "backup.json").write_text(json.dumps(_export_rows(conn, todo), indent=1, default=str), encoding="utf-8")
            (run_dir / "plan.txt").write_text("\n".join(f"{p.status} {p.repair.path.name}: " + " | ".join(p.detail) for p in plans), encoding="utf-8")
            out(f"backup: {run_dir}")
            with open(run_dir / "applied.jsonl", "a", encoding="utf-8") as log:
                for p in todo:
                    try:
                        done = [_run_op(conn, op) for op in p.ops]
                        log.write(json.dumps({"file": p.repair.path.name, "done": done}) + "\n")
                    except Exception as exc:  # noqa: BLE001 - one repair failing must not strand the rest
                        failed += 1
                        try:
                            conn.rollback()
                        except Exception:  # noqa: BLE001
                            pass
                        log.write(json.dumps({"file": p.repair.path.name, "error": type(exc).__name__}) + "\n")
                        out(f"    FAILED {p.repair.path.name}: {type(exc).__name__}")
            # verify: every applied repair must now plan as a noop
            again = {p.repair.path.name: p for p in build_plans(conn, [q.repair for q in todo], allow_gated)}
            for name, p in again.items():
                if p.status != "noop":
                    failed += 1
                    out(f"    VERIFY FAILED {name}: {p.status} {' '.join(p.detail)[:200]}")
            out(f"verify: {sum(1 for p in again.values() if p.status == 'noop')}/{len(todo)} applied repairs read back")
        return 1 if (bad or failed) else 0
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Apply the N8.9 repair set (dry run by default)")
    ap.add_argument("--dir", default=str(DEFAULT_DIR), help=f"repair files (default {DEFAULT_DIR})")
    ap.add_argument("--only", nargs="*", default=None, help="only these dealer ids")
    ap.add_argument("--allow-gated", nargs="*", default=[], help="dealer ids whose apply_gate / shared_host gate the owner lifted")
    ap.add_argument("--apply", action="store_true", help="write (default is a dry run)")
    ap.add_argument("--backup-dir", default=None, help="parent of the run dir; required with --apply")
    args = ap.parse_args(argv)
    return run(Path(args.dir), apply=args.apply, backup_dir=Path(args.backup_dir) if args.backup_dir else None,
               only=args.only, allow_gated=set(args.allow_gated))


if __name__ == "__main__":
    raise SystemExit(main())
