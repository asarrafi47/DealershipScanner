#!/usr/bin/env python3
"""P8C.5: relabel legacy mixed-source provenance to ``unknown``.

``inventory_repair`` and ``listing_gap_fill`` were written as the *source* of a
fact by writers that mixed several real origins (dealer VDP, web search, the
catalog), so the label says nothing about how far to trust the value.
``precedence`` already ranks both as ``unknown`` (``_ALIASES``); this tool makes
the stored label say so, which closes P8C.5 ("0 unlabeled legacy facts").

Per entry of ``spec_source_json``: ``source`` becomes ``unknown`` and the old
label is kept in ``legacy_source``. The value, ``detail`` and ``url`` are left
alone, so nothing a later pass could re-derive is lost. An entry whose *detail*
says ``listing_gap_fill`` but whose source is a real one (new writes) is not
touched. Re-running changes nothing.

Dry run by default: counts per label and field, nothing written. ``--apply``
needs ``--owner-ok`` and first creates ``cars_provenance_backup_<tag>`` (``id``,
``spec_source_json`` of the rows that carry a legacy label), verifies it holds
every row to be changed, and only then updates, each row guarded by the text it
was read with, so a concurrent rescan write is never clobbered.

  PYTHONPATH=. .venv/bin/python backend/scripts/relabel_spec_provenance.py
  PYTHONPATH=. .venv/bin/python backend/scripts/relabel_spec_provenance.py --apply --owner-ok --date 20261027
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv  # noqa: E402

load_project_dotenv()

LEGACY_LABELS: tuple[str, ...] = ("inventory_repair", "listing_gap_fill")
UNKNOWN = "unknown"
_BACKUP_RE = re.compile(r"^cars_provenance_backup_[a-z0-9_]+$")
_CHUNK = 2000
_LIKE_ANY = " OR ".join(f"spec_source_json LIKE '%{label}%'" for label in LEGACY_LABELS)


def backup_table_name(date: str | None = None) -> str:
    name = "cars_provenance_backup_" + (date or time.strftime("%Y%m%d"))
    return _check_table(name)


def _check_table(name: str) -> str:
    if not _BACKUP_RE.match(name or ""):
        raise ValueError(f"backup table must match cars_provenance_backup_<tag>: {name!r}")
    return name


def relabel_spec(spec_json: Any) -> tuple[str | None, list[tuple[str, str]]]:
    """(new JSON text or None when nothing changes, [(legacy label, field), ...])."""
    if not spec_json:
        return None, []
    try:
        d = json.loads(spec_json) if isinstance(spec_json, str) else spec_json
    except (json.JSONDecodeError, TypeError):
        return None, []
    if not isinstance(d, dict):
        return None, []
    changed: list[tuple[str, str]] = []
    out: dict[str, Any] = {}
    for field, entry in d.items():
        if isinstance(entry, dict) and entry.get("source") in LEGACY_LABELS:
            label = entry["source"]
            out[field] = {**entry, "source": UNKNOWN, "legacy_source": label}
            changed.append((label, field))
        elif isinstance(entry, str) and entry in LEGACY_LABELS:  # bare-string form
            out[field] = {"source": UNKNOWN, "legacy_source": entry}
            changed.append((entry, field))
        else:
            out[field] = entry
    if not changed:
        return None, []
    return json.dumps(out, ensure_ascii=False), changed


def plan(conn) -> dict[str, Any]:
    """Read-only scan: rows to change (id, old text, new text) and counts per label and field."""
    cur = conn.cursor()
    cur.execute(f"SELECT id, spec_source_json FROM cars WHERE spec_source_json LIKE '{{%' AND ({_LIKE_ANY}) ORDER BY id")
    todo: list[tuple[Any, str, str]] = []
    counts: Counter[tuple[str, str]] = Counter()
    for car_id, old in cur.fetchall():
        new, changed = relabel_spec(old)
        if new is None:
            continue
        todo.append((car_id, old, new))
        counts.update(changed)
    return {
        "rows": len(todo),
        "entries": sum(counts.values()),
        "by_label": {k: sum(v for (lab, _), v in counts.items() if lab == k) for k in LEGACY_LABELS},
        "by_label_field": {f"{lab}.{field}": n for (lab, field), n in sorted(counts.items())},
        "todo": todo,
    }


def create_backup(conn, table: str, ids: list[Any]) -> dict[str, Any]:
    """Snapshot id and spec_source_json of every row with a legacy label; it must hold all of *ids*."""
    _check_table(table)
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {table} AS SELECT id, spec_source_json FROM cars WHERE {_LIKE_ANY}")
    conn.commit()
    cur.execute(f"SELECT id FROM {table}")
    have = {r[0] for r in cur.fetchall()}
    missing = [i for i in ids if i not in have]
    if missing:
        cur.execute(f"DROP TABLE {table}")
        conn.commit()
        raise RuntimeError(f"backup {table} misses {len(missing)} rows to change")
    return {"table": table, "rows": len(have)}


def apply_plan(conn, todo: list[tuple[Any, str, str]]) -> int:
    """Write the new text where the row still holds the text it was read with."""
    cur = conn.cursor()
    done = 0
    for i, (car_id, old, new) in enumerate(todo, 1):
        cur.execute("UPDATE cars SET spec_source_json = ? WHERE id = ? AND spec_source_json = ?", (new, car_id, old))
        done += cur.rowcount or 0
        if i % _CHUNK == 0:
            conn.commit()
    conn.commit()
    return done


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write (default is a dry run)")
    ap.add_argument("--owner-ok", action="store_true", help="the owner's apply OK (--apply needs it)")
    ap.add_argument("--date", help="backup table suffix (default today, YYYYMMDD)")
    args = ap.parse_args(argv)
    if args.apply and not args.owner_ok:
        print("refused: --apply needs --owner-ok", file=sys.stderr)
        return 2
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        p = plan(conn)
        todo = p.pop("todo")
        print(("APPLY " if args.apply else "DRY RUN ") + json.dumps(p, sort_keys=True))
        if not args.apply:
            return 0
        print("backup:", json.dumps(create_backup(conn, backup_table_name(args.date), [t[0] for t in todo])))
        print("updated:", apply_plan(conn, todo), "of", len(todo))
        again = plan(conn)
        print("remaining legacy rows:", again["rows"])
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
