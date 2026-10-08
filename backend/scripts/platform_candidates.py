"""Platform candidates: which dealers without a working recipe share an unknown platform.

Reads the latest ``discovery_<stamp>.json`` per dealer under
``workspace/dealer_logs/`` (plus the latest ``capture_<stamp>.json`` when one
exists), keeps the dealers that have no live recipe or whose recipe is stale /
rejected (:func:`recipe_state`: the recipe file in the anchored cache dir plus a
read-only read of ``dealer_recipes``), fingerprints them
(``backend.scanner.platform_fingerprint``), clusters by signature with the
Jaccard merge, and writes ``workspace/dealer_logs/_learning/platform_candidates.md``:
a recipe census, then one section per cluster of two or more dealers with the
shared features, the members and their dated last verdict, the nearest known
template, and a suggested next step. Prints one line per cluster.

Fail closed: a dealer whose recipe lookup failed, or whose recipe file exists
but yields no recipe, is ``unknown``. It is counted in the census and listed,
never clustered, so a read failure can no longer pass for "recipe none". A
dealer whose newest ``scan_runs.md`` verdict (ok / thin / inaccurate) is newer
than its probe is dropped with a note: the probe no longer describes the site.
The report never writes recipe state (it does not call ``load_recipes``, which
re-materializes files and pushes rows up).

    python -m backend.scripts.platform_candidates                 # every dealer log
    python -m backend.scripts.platform_candidates --dealers a,b   # a subset
    python -m backend.scripts.platform_candidates --all           # ignore the recipe filter
    python -m backend.scripts.platform_candidates --root <dir>    # another log root (tests)

``dealer_pipeline`` calls :func:`report_for_run` at the end of every run with
the run's ``needs_discovery`` dealers, so a platform shared by several failing
dealers is printed in the run output instead of waiting for someone to notice.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[2]
LOG_ROOT = Path(os.environ.get("DEALER_LOGS_ROOT") or (ROOT / "workspace" / "dealer_logs"))
OUTPUT_NAME = "platform_candidates.md"
MIN_CLUSTER = 2

logger = logging.getLogger("scanner.platform_candidates")

RecipeStateFn = Callable[[str], str]
_DEFAULT_STATE = object()  # "use recipe_state as bound at call time" (tests stub the module attribute)

UNKNOWN = "unknown"
CENSUS_KEYS = ("live", "none", "stale", "rejected", "unknown")
# scan_runs.md verdicts that mean the scanner got rows: newer than the probe,
# the probe's picture of the site is out of date.
GOOD_SCAN_VERDICTS = frozenset({"ok", "thin", "inaccurate"})

_DISCOVERY_RE = re.compile(r"^discovery_(\d{8}T\d{6})")
_CAPTURE_RE = re.compile(r"^capture_(\d{8}T\d{6})")


# --------------------------------------------------------------------------
# reading the logs
# --------------------------------------------------------------------------

def _latest(dir_: Path, rx: re.Pattern) -> Path | None:
    best: tuple[str, Path] | None = None
    try:
        names = list(dir_.iterdir())
    except OSError:
        return None
    for p in names:
        m = rx.match(p.name)
        if m and p.suffix == ".json" and (best is None or m.group(1) > best[0]):
            best = (m.group(1), p)
    return best[1] if best else None


def latest_discovery_files(root: Path) -> dict[str, Path]:
    """``{dealer_id: newest discovery_<stamp>.json}`` for every dealer directory."""
    out: dict[str, Path] = {}
    try:
        dirs = sorted(p for p in root.iterdir() if p.is_dir() and not p.name.startswith(("_", ".")))
    except OSError:
        return out
    for d in dirs:
        p = _latest(d, _DISCOVERY_RE)
        if p is not None:
            out[d.name] = p
    return out


def latest_capture_file(root: Path, dealer_id: str) -> Path | None:
    return _latest(root / dealer_id, _CAPTURE_RE)


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


_VERDICT_RE = re.compile(r"^- (?:verdict|recipe validation): (.+)$")
_PROBE_HEAD_RE = re.compile(r"^## \S+ discovery probe — (.+)$")
# The timestamp every dealer-log heading starts with: "2026-09-28 13:19 UTC"
# (pipeline blocks) or "2026-09-26T12:23:19+00:00" (probe blocks).
_HEAD_STAMP_RE = re.compile(
    r"^## (\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?)?(?:Z|[+-]\d{2}:?\d{2}| UTC)?)(?=\s|$)")
_SCAN_HEAD_RE = re.compile(r"^## (.+?) scan — verdict \*\*([^*]+)\*\*")
_ROWS_WRITTEN_RE = re.compile(r"^- rows written: (\d+)")


def parse_stamp(text: Any) -> datetime | None:
    """An aware UTC datetime from a log or record stamp, or ``None``.

    Takes ``2026-09-28 13:19 UTC``, ISO 8601 with an offset or ``Z``, a bare
    date, and the ``20260926T122319`` form of discovery file names. A naive
    stamp is read as UTC (every writer stamps UTC)."""
    s = str(text or "").strip()
    if not s:
        return None
    if s.endswith(" UTC"):
        s = s[:-4].strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt: datetime | None = None
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        for fmt in ("%Y%m%dT%H%M%S%z", "%Y%m%dT%H%M%S"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except ValueError:
                continue
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def format_stamp(dt: datetime | None) -> str:
    """``2026-09-28 13:19 UTC`` (the dealer-log heading form), or ``""``."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if dt else ""


def _heading_stamp(line: str) -> datetime | None:
    m = _HEAD_STAMP_RE.match(line)
    return parse_stamp(m.group(1)) if m else None


def last_verdict_dated(root: Path, dealer_id: str, rec: dict[str, Any] | None) -> tuple[str, str]:
    """``(verdict, date)``: the dealer's last verdict (see :func:`last_verdict`)
    and the ``YYYY-MM-DD HH:MM UTC`` stamp of the discovery.md block it sits
    in. The record-classification fallback is dated by the record's own
    ``stamp``. The date is ``""`` when no stamp parses."""
    rec = rec or {}
    fallback = (str(rec.get("classification") or "unknown"), format_stamp(parse_stamp(rec.get("stamp"))))
    p = root / dealer_id / "discovery.md"
    try:
        lines = p.read_text(encoding="utf-8").splitlines()
    except OSError:
        return fallback
    found: tuple[str, str] | None = None
    block_at: datetime | None = None
    for raw in lines:
        line = raw.strip()
        if line.startswith("## "):
            block_at = _heading_stamp(line)
        m = _VERDICT_RE.match(line) or _PROBE_HEAD_RE.match(line)
        if m:
            found = (m.group(1).strip()[:120], format_stamp(block_at))
    return found or fallback


def last_verdict(root: Path, dealer_id: str, rec: dict[str, Any] | None) -> str:
    """The dealer's last verdict: the last ``- verdict:`` / ``- recipe validation:``
    line in discovery.md, else the last ``discovery probe — <class>`` heading,
    else the record's classification. :func:`last_verdict_dated` adds its date."""
    return last_verdict_dated(root, dealer_id, rec)[0]


def newest_scan(root: Path, dealer_id: str) -> dict[str, Any] | None:
    """The newest ``## <stamp> scan — verdict **<v>**`` block of the dealer's
    scan_runs.md: ``{verdict, at, stamp, rows}`` (``rows`` from its
    ``- rows written:`` line, ``None`` when absent). ``None`` when the file is
    missing or holds no dated scan block. Reconcile blocks are not scans."""
    p = root / dealer_id / "scan_runs.md"
    try:
        lines = p.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    best: dict[str, Any] | None = None
    cur: dict[str, Any] | None = None
    for raw in lines:
        line = raw.strip()
        if line.startswith("## "):
            cur = None
            m = _SCAN_HEAD_RE.match(line)
            at = parse_stamp(m.group(1)) if m else None
            if m and at is not None:
                cur = {"verdict": m.group(2).strip(), "at": at, "stamp": format_stamp(at), "rows": None}
                if best is None or at >= best["at"]:
                    best = cur
            continue
        if cur is not None and cur["rows"] is None:
            mr = _ROWS_WRITTEN_RE.match(line)
            if mr:
                cur["rows"] = int(mr.group(1))
    return best


def probe_time(rec: dict[str, Any], path: Path | None = None) -> datetime | None:
    """When the discovery record was probed: its ``stamp``, else the stamp in
    its ``discovery_<stamp>.json`` file name."""
    at = parse_stamp(rec.get("stamp"))
    if at is None and path is not None:
        m = _DISCOVERY_RE.match(path.name)
        at = parse_stamp(m.group(1)) if m else None
    return at


def superseding_scan(root: Path, dealer_id: str, rec: dict[str, Any], path: Path | None = None) -> dict[str, Any] | None:
    """The dealer's newest scan when it got rows (ok / thin / inaccurate) after
    the probe, else ``None``. A newest scan that failed keeps the member: the
    site may well still need a platform. An undatable probe is never dropped."""
    probed = probe_time(rec, path)
    scan = newest_scan(root, dealer_id)
    if probed is None or scan is None:
        return None
    if scan["verdict"] in GOOD_SCAN_VERDICTS and scan["at"] > probed:
        return scan
    return None


# --------------------------------------------------------------------------
# recipe state
# --------------------------------------------------------------------------

_warned: set[str] = set()


def _warn_once(key: str, msg: str, *args: Any) -> None:
    """WARNING the first time *key* is seen in this process, DEBUG after."""
    if key in _warned:
        logger.debug(msg, *args)
        return
    _warned.add(key)
    logger.warning(msg, *args)


def _store_table_exists(cur: Any) -> bool:
    from backend.db.inventory_pg import is_inventory_postgres

    if is_inventory_postgres():
        cur.execute("SELECT to_regclass('dealer_recipes') IS NOT NULL")
    else:
        cur.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'dealer_recipes'")
    row = cur.fetchone()
    return bool(row and row[0])


def _store_read(dealer_id: str) -> dict[str, Any] | None:
    """A read-only look at the dealer's ``dealer_recipes`` row, mirroring what
    ``load_recipes`` / ``get_scan_hints`` would see: ``{rows, saved, hints}``
    (``rows`` is ``None`` when no row carries a recipe payload). ``None`` when
    the store is switched off (``RECIPES_DB_DISABLED``). Only SELECTs: no
    ``_ensure_table`` DDL, no push-up, no hint write. Raises when the store is
    on but cannot be read, or a stored payload does not parse: the caller
    turns that into ``unknown``."""
    from backend.scanner import recipe_store
    from backend.scanner.recipes import _recipe_slug, resolve_alias_slug

    if not recipe_store._enabled():
        return None
    slug = _recipe_slug(dealer_id)
    old_slug = resolve_alias_slug(slug)  # what db_load_recipes falls back to
    old_hint_key = resolve_alias_slug(dealer_id)  # what get_scan_hints falls back to
    rows_by_key: dict[str, Any] = {}
    conn = recipe_store._conn()
    try:
        cur = conn.cursor()
        if not _store_table_exists(cur):
            return {"rows": None, "saved": -1.0, "hints": {}}
        for key in dict.fromkeys(k for k in (slug, dealer_id, old_slug, old_hint_key) if k):
            cur.execute("SELECT recipes_json, max_saved_at, scan_hints FROM dealer_recipes WHERE dealer_id = ?", (key,))
            rows_by_key[key] = cur.fetchone()
    finally:
        conn.close()

    def _recipes_of(key: str | None) -> tuple[list[Any], float] | None:
        row = rows_by_key.get(key) if key else None
        if not row or not row[0]:
            return None
        payload = json.loads(row[0])
        if not isinstance(payload, list):
            raise ValueError(f"dealer_recipes.recipes_json for {key} is not a JSON list")
        try:
            saved = float(row[1] or 0.0)
        except (TypeError, ValueError):
            saved = 0.0
        return payload, saved

    def _hints_of(key: str | None) -> dict[str, Any] | None:
        row = rows_by_key.get(key) if key else None
        if not row or not row[2]:
            return None
        payload = json.loads(row[2])
        if not isinstance(payload, dict):
            raise ValueError(f"dealer_recipes.scan_hints for {key} is not a JSON object")
        return payload

    found = _recipes_of(slug) or _recipes_of(old_slug)
    hints = _hints_of(dealer_id)
    if hints is None:  # an explicitly stored {} is "cleared" and does not fall through
        hints = _hints_of(old_hint_key) or {}
    rows, saved = found if found is not None else (None, -1.0)
    return {"rows": rows, "saved": saved, "hints": hints}


def recipe_state_detail(dealer_id: str) -> tuple[str, str]:
    """``(state, reason)``; see :func:`recipe_state`. ``reason`` says why the
    state is ``unknown`` (``""`` otherwise)."""
    try:
        from backend.scanner import recipes as rcp

        store = _store_read(dealer_id)
        status = str(((store or {}).get("hints") or {}).get("recipe_status") or "")
        if status.startswith("rejected:"):
            return status, ""
        path = rcp._recipe_read_path(dealer_id)  # anchored RECIPES_DIR, alias-aware, read-only
        file_exists = path.exists()
        file_rows: list[Any] = []
        if file_exists:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(raw, list):
                return UNKNOWN, f"recipe file {path} is not a JSON list"
            file_rows = raw
        db_rows = list((store or {}).get("rows") or [])
        db_saved = float((store or {}).get("saved", -1.0))
        # The same pick load_recipes makes, without its writes.
        file_saved = max((float(r.get("saved_at") or 0) for r in file_rows if isinstance(r, dict)),
                         default=0.0) if file_rows else -1.0
        rows = db_rows if (db_rows and db_saved > file_saved) else (file_rows or db_rows)
        recipes = rcp._rows_to_recipes(rows)
    except Exception as exc:  # noqa: BLE001 - fail closed: a failed lookup is not "none"
        return UNKNOWN, f"recipe lookup raised {type(exc).__name__}: {str(exc)[:160]}"
    if not recipes:
        if file_exists:
            return UNKNOWN, f"recipe file {path} exists but no recipe came back from it"
        return "none", ""
    if any(not getattr(r, "stale", False) for r in recipes):
        return "live", ""  # an un-staled recipe answers; a lagging stale: status does not outrank it
    return "stale", ""


def recipe_state(dealer_id: str) -> str:
    """``live`` | ``none`` | ``stale`` | ``rejected:<code>`` | ``unknown``.

    Read-only: the recipe file under the anchored cache dir
    (``backend.scanner.recipes.RECIPES_DIR``, alias-aware) and a SELECT-only
    read of the dealer's ``dealer_recipes`` row; never ``load_recipes``, which
    re-materializes files and pushes rows up. File and DB copies are picked the
    way ``load_recipes`` picks them (newer ``saved_at`` wins), and a
    ``rejected:`` ``scan_hints.recipe_status`` outranks the recipes.

    Fails closed: ``unknown`` when the lookup raised (store unreachable, a file
    or payload that does not read or parse) or when a recipe file exists for
    the dealer but no recipe came back from it (an empty ``[]`` file included).
    ``none`` only when no recipe is stored for the dealer anywhere.
    ``RECIPES_DB_DISABLED=1`` reads the file alone. Never raises; an
    ``unknown`` logs its reason (WARNING once per error class or dealer)."""
    return _recipe_state_logged(dealer_id)[0]


def _recipe_state_logged(dealer_id: str) -> tuple[str, str]:
    state, reason = recipe_state_detail(dealer_id)
    if state == UNKNOWN:
        cls = reason.split(":", 1)[0]
        key = cls if cls.startswith("recipe lookup raised") else f"{dealer_id}:{cls}"
        _warn_once(key, "platform_candidates: recipe state unknown for %s (%s); counted, not clustered",
                   dealer_id, reason)
    return state, reason


# The lookup this module ships. collect() asks it for the reason behind an
# ``unknown``; a state_fn stubbed in its place (tests) gives states only.
_BUILTIN_RECIPE_STATE = recipe_state


def is_unknown(state: str) -> bool:
    return state == UNKNOWN or state.startswith(UNKNOWN + ":")


def needs_platform(state: str) -> bool:
    """A dealer the clustering should consider: no live recipe, and a known state."""
    return state != "live" and not is_unknown(state)


def census_key(state: str) -> str:
    """The census bucket of a recipe state (``other`` for a state no bucket names)."""
    if is_unknown(state):
        return UNKNOWN
    for key in ("live", "none", "stale", "rejected"):
        if state == key or state.startswith(key + ":"):
            return key
    return "other"


# --------------------------------------------------------------------------
# build
# --------------------------------------------------------------------------

def collect(root: Path, *, dealers: set[str] | None = None, state_fn: Any = _DEFAULT_STATE,
            only_needing: bool = True) -> tuple[list[Any], dict[str, dict[str, Any]]]:
    """Fingerprints for the dealers under ``root`` (restricted to ``dealers`` when
    given) that need a platform, plus ``{dealer_id: {...}}`` for every dealer
    with a readable discovery record: ``state``, ``verdict`` / ``verdict_at``
    (:func:`last_verdict_dated`), ``stamp`` (the probe's), ``classification``
    and ``outcome``:

    - ``candidate``: fingerprinted, goes to the clustering;
    - ``live``: has a live recipe, left out;
    - ``unknown``: the recipe state could not be read; counted, not clustered;
    - ``newer_scan``: its newest scan_runs.md verdict (ok / thin / inaccurate)
      is newer than the probe, so the probe no longer describes the site;
      dropped, with ``scan`` = :func:`newest_scan`.

    ``state_fn=None`` or ``only_needing=False`` keeps every dealer (state
    ``unfiltered``, no census, no newer-scan drop)."""
    from backend.scanner.platform_fingerprint import fingerprint

    if state_fn is _DEFAULT_STATE:
        state_fn = recipe_state
    filtering = bool(only_needing and state_fn)

    files = latest_discovery_files(root)
    fps = []
    meta: dict[str, dict[str, Any]] = {}
    for did, path in files.items():
        if dealers is not None and did not in dealers:
            continue
        rec = _load_json(path)
        if not rec:
            continue
        reason = ""
        if not filtering:
            state = "unfiltered"
        elif state_fn is _BUILTIN_RECIPE_STATE:
            state, reason = _recipe_state_logged(did)
        else:
            state = state_fn(did)
        verdict, verdict_at = last_verdict_dated(root, did, rec)
        info: dict[str, Any] = {"state": state, "verdict": verdict, "verdict_at": verdict_at,
                                "stamp": str(rec.get("stamp") or ""), "classification": str(rec.get("classification") or ""),
                                "outcome": "candidate", "reason": reason}
        meta[did] = info
        if filtering:
            if is_unknown(state):
                info["outcome"] = "unknown"
                continue
            if not needs_platform(state):
                info["outcome"] = "live"
                continue
            scan = superseding_scan(root, did, rec, path)
            if scan is not None:
                info["outcome"] = "newer_scan"
                info["scan"] = scan
                continue
        cap_path = latest_capture_file(root, did)
        cap = _load_json(cap_path) if cap_path else None
        fp = fingerprint(rec, cap)
        fp.dealer_id = fp.dealer_id or did
        fps.append(fp)
    return fps, meta


def census(meta: dict[str, dict[str, Any]]) -> dict[str, int] | None:
    """``{live, none, stale, rejected, unknown[, other]: count}`` over every
    dealer in ``meta``; ``None`` when the recipe filter was off."""
    if any(m.get("state") == "unfiltered" for m in meta.values()):
        return None
    counts = {k: 0 for k in CENSUS_KEYS}
    for m in meta.values():
        key = census_key(str(m.get("state") or ""))
        counts[key] = counts.get(key, 0) + 1
    return counts


def census_text(counts: dict[str, int] | None) -> str:
    if counts is None:
        return "not taken (recipe filter off: every dealer is clustered whatever its recipe state)"
    keys = list(CENSUS_KEYS) + [k for k in counts if k not in CENSUS_KEYS]
    return " / ".join(f"{k} {counts.get(k, 0)}" for k in keys)


def _recipes_dir() -> str:
    try:
        from backend.scanner import recipes as rcp

        return str(rcp.RECIPES_DIR)
    except Exception as exc:  # noqa: BLE001 - only a header line
        return f"? ({type(exc).__name__})"


def cluster_all(fps: list[Any], *, threshold: float | None = None, min_size: int = MIN_CLUSTER) -> list[Any]:
    from backend.scanner.platform_fingerprint import JACCARD_THRESHOLD, cluster

    clusters = cluster(fps, threshold=JACCARD_THRESHOLD if threshold is None else threshold)
    return [c for c in clusters if len(c.members) >= min_size]


def summary_line(c: Any) -> str:
    ids = c.dealer_ids
    shown = ", ".join(ids[:6]) + (f", +{len(ids) - 6}" if len(ids) > 6 else "")
    plats = sorted({m.platform for m in c.members if m.platform})
    what = f"platform {'/'.join(plats)} (template detects)" if plats else "unknown platform"
    return (f"discovery: {len(ids)} dealers share {what} {c.signature} ({shown})"
            f" → workspace/dealer_logs/_learning/{OUTPUT_NAME}")


def _dated(mm: dict[str, Any]) -> str:
    return f"dated {mm['verdict_at']}" if mm.get("verdict_at") else "undated"


def render_markdown(clusters: list[Any], meta: dict[str, dict[str, Any]], *, total_considered: int,
                    stamp: str | None = None, root: Path | str | None = None, recipes_dir: str | None = None,
                    cwd: str | None = None) -> str:
    from backend.scanner.platform_fingerprint import suggest_next_step

    stamp = stamp or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    counts = census(meta)
    L = ["# platform_candidates", "",
         f"Generated {stamp} by `backend/scripts/platform_candidates.py` from the latest `discovery_*.json` per dealer.",
         f"Recipe census ({len(meta)} dealers with a discovery record): {census_text(counts)}.",
         f"Read from: dealer logs `{root if root is not None else '?'}`; recipe cache `{recipes_dir or _recipes_dir()}`; "
         f"cwd `{cwd or os.getcwd()}`.",
         f"Dealers considered (no live recipe, or a stale / rejected one; unknown and newer-scan dealers left out): "
         f"{total_considered}. Clusters of {MIN_CLUSTER}+ dealers: {len(clusters)}.",
         "A cluster means several dealers present their inventory the same unknown way: fix the platform once, "
         "not each dealer. Process: docs/NETWORK_SCAN_PROCESS.md; fingerprint code: backend/scanner/platform_fingerprint.py.",
         ""]
    if not clusters:
        L.append("_No cluster of two or more dealers._")
        L.append("")
    for i, c in enumerate(clusters, 1):
        ids = c.dealer_ids
        L.append(f"## Cluster {i} — {len(ids)} dealers")
        L.append("")
        L.append(f"- signature: `{c.signature}`")
        if len(c.signatures) > 1:
            L.append(f"- merged signatures ({len(c.signatures)}): " + "; ".join(f"`{s}`" for s in c.signatures[:6])
                     + (" …" if len(c.signatures) > 6 else ""))
        shared = c.shared_features[:12]
        L.append("- shared features: " + (", ".join(f"`{f}`" for f in shared) if shared else "_none common to every member_")
                 + (f" (+{len(c.shared_features) - 12} more)" if len(c.shared_features) > 12 else ""))
        plats = sorted({m.platform for m in c.members if m.platform})
        if plats:
            detecting = sum(1 for m in c.members if m.platform)
            L.append(f"- template already detecting: {', '.join(plats)} ({detecting} of {len(c.members)} members)")
        scored = [m.nearest_template for m in c.members if m.nearest_template]
        if scored:
            name, score = max(scored, key=lambda t: t[1])
            L.append(f"- nearest known template: `{name}` (partial detect score {score:.2f})")
        elif not plats:
            # Only true when no member's detect map names a template.
            L.append("- nearest known template: none scores partially (detect map is boolean, all false)")
        L.append("- members:")
        for m in c.members:
            mm = meta.get(m.dealer_id, {})
            L.append(f"  - `{m.dealer_id}` — recipe {mm.get('state', '?')}; last verdict: {mm.get('verdict', m.classification or '?')}"
                     f" — {_dated(mm)} (probe {mm.get('stamp') or m.stamp or '?'}; workspace/dealer_logs/{m.dealer_id}/discovery.md)")
        L.append(f"- next step: {suggest_next_step(c)}")
        L.append("")
    dropped = sorted(d for d, m in meta.items() if m.get("outcome") == "newer_scan")
    unknown = sorted(d for d, m in meta.items() if m.get("outcome") == "unknown")
    if dropped:
        L.append(f"## Dropped: scanned after the probe ({len(dropped)})")
        L.append("")
        L.append("The newest scan_runs.md verdict (ok / thin / inaccurate) is newer than the probe, so the probe no longer "
                 "describes the site. Re-probe before clustering if the dealer fails again.")
        L.append("")
        for d in dropped:
            mm = meta[d]
            scan = mm.get("scan") or {}
            rows = scan.get("rows")
            rows_txt = f", {rows:,} rows" if isinstance(rows, int) else ""
            probe_at = format_stamp(parse_stamp(mm.get("stamp"))) or mm.get("stamp") or "?"
            L.append(f"- `{d}` — probe {probe_at} ({mm.get('classification') or '?'}); newest scan {scan.get('stamp', '?')} "
                     f"verdict {scan.get('verdict', '?')}{rows_txt} (workspace/dealer_logs/{d}/scan_runs.md)")
        L.append("")
    if unknown:
        L.append(f"## Recipe state unknown: counted, not clustered ({len(unknown)})")
        L.append("")
        L.append("The recipe lookup failed, or a recipe file exists but no recipe came back from it. "
                 "Fix the read, then rerun: an unreadable recipe is not the same as no recipe.")
        L.append("")
        for d in unknown:
            mm = meta[d]
            why = f"; {mm['reason']}" if mm.get("reason") else ""
            L.append(f"- `{d}` — last verdict: {mm.get('verdict', '?')} — {_dated(mm)}"
                     f" (probe {mm.get('stamp') or '?'}; workspace/dealer_logs/{d}/discovery.md){why}")
        L.append("")
    return "\n".join(L).rstrip("\n") + "\n"


def write_candidates(root: Path, text: str) -> Path:
    d = root / "_learning"
    d.mkdir(parents=True, exist_ok=True)
    p = d / OUTPUT_NAME
    p.write_text(text, encoding="utf-8")
    return p


def build(root: Path, *, dealers: set[str] | None = None, state_fn: Any = _DEFAULT_STATE,
          only_needing: bool = True, threshold: float | None = None, min_size: int = MIN_CLUSTER,
          write: bool = True) -> dict[str, Any]:
    """Everything the CLI and the pipeline need: the clusters, their summary
    lines, the recipe census, the unknown and dropped dealers, and the markdown
    path (``None`` when ``write`` is off)."""
    fps, meta = collect(root, dealers=dealers, state_fn=state_fn, only_needing=only_needing)
    clusters = cluster_all(fps, threshold=threshold, min_size=min_size)
    path = None
    if write:
        path = write_candidates(root, render_markdown(clusters, meta, total_considered=len(fps), root=root,
                                                      recipes_dir=_recipes_dir(), cwd=os.getcwd()))
    return {"clusters": clusters, "meta": meta, "considered": len(fps), "lines": [summary_line(c) for c in clusters],
            "census": census(meta), "unknown": sorted(d for d, m in meta.items() if m.get("outcome") == "unknown"),
            "dropped": sorted(d for d, m in meta.items() if m.get("outcome") == "newer_scan"), "path": path}


def report_for_run(needs_discovery: list[str], *, root: Path | None = None, state_fn: Any = _DEFAULT_STATE,
                   min_run_members: int = MIN_CLUSTER) -> list[str]:
    """The pipeline's end-of-run hook: cluster every dealer under ``root`` that
    needs a platform (so a run's failing dealer is matched against dealers other
    runs already logged), rewrite the markdown, and return the summary lines of
    the clusters holding at least ``min_run_members`` of this run's
    ``needs_discovery`` dealers. Never raises."""
    root = root or LOG_ROOT
    wanted = set(needs_discovery or [])
    if not wanted:
        return []
    try:
        res = build(root, state_fn=state_fn)
    except Exception as exc:  # noqa: BLE001 - the triage must still be written
        return [f"discovery: platform clustering failed: {str(exc)[:160]}"]
    out = []
    for c in res["clusters"]:
        ids = c.dealer_ids
        hits = [d for d in ids if d in wanted]
        if len(hits) >= min_run_members:
            out.append(summary_line(c))
    return out


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--root", default=str(LOG_ROOT), help="dealer_logs root (default workspace/dealer_logs or $DEALER_LOGS_ROOT)")
    ap.add_argument("--dealers", default="", help="comma-separated dealer ids (default every dealer with a discovery record)")
    ap.add_argument("--all", action="store_true", help="cluster every dealer, not only those without a live recipe")
    ap.add_argument("--threshold", type=float, default=None, help="Jaccard merge threshold (default 0.6)")
    ap.add_argument("--min-size", type=int, default=MIN_CLUSTER)
    ap.add_argument("--no-write", action="store_true", help="print only; do not rewrite _learning/platform_candidates.md")
    args = ap.parse_args(argv)
    # The recipe lookup reads the dealer_recipes store and fails closed when it
    # cannot: load the project .env (as dealer_pipeline does) so a shell run
    # reaches the same store. Set values win; a no-op under pytest.
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
    root = Path(args.root)
    dealers = {d.strip() for d in args.dealers.split(",") if d.strip()} or None
    res = build(root, dealers=dealers, only_needing=not args.all, threshold=args.threshold, min_size=args.min_size,
                write=not args.no_write)
    for line in res["lines"]:
        print(line, flush=True)
    print(f"platform_candidates: {res['considered']} dealers considered, {len(res['clusters'])} clusters of {args.min_size}+"
          + (f" → {res['path']}" if res["path"] else ""), flush=True)
    print(f"platform_candidates: recipe census {census_text(res['census'])}"
          + (f"; unknown: {', '.join(res['unknown'])}" if res["unknown"] else "")
          + (f"; dropped (scanned after the probe): {', '.join(res['dropped'])}" if res["dropped"] else "")
          + f" (dealer logs {root}, recipe cache {_recipes_dir()}, cwd {os.getcwd()})", flush=True)
    for d in res["unknown"]:
        why = res["meta"][d].get("reason")
        if why:
            print(f"platform_candidates: unknown {d}: {why}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
