#!/usr/bin/env python3
"""Railway tick of the scan loop (cards L19 and L20, docs/scan_loop/BUNDLE.md sections 5, 6 and 13).

  python -m backend.scripts.ops_tick [--now ISO]

One tick: load requests, proposals and config from the store, read ``config/loop.json``, find the requests that have no status yet, reject or accept each
(``backend.scanner.loop.requests``), run the accepted ones within the budgets, write one ``*.status.json`` per request,
append every event to ``ledger.jsonl`` on the volume, and store the statuses and ``fix_cycles/`` in the scan loop store.

The orchestration (``process``) and the apply step (``apply_proposal``) take their effects as arguments, so the tests
run them with fakes and no network or database. ``apply`` is off unless ``config/loop.json`` lists it in ``kinds``;
the template leaves it out. The apply step never writes unless every check before it has passed, and it restores the
previous recipe set when the canary rescan fails.

Exit status: 0 done, 1 the tick failed (nothing partial is pushed), 2 not configured.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

if __package__ in (None, ""):  # run as a script: make ``backend`` importable
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backend.scanner.loop import bundle  # noqa: E402
from backend.scanner.loop import requests as req  # noqa: E402
from backend.scanner.loop.config import MAX_PROPOSAL_BYTES, MAX_REQUEST_BYTES, load_loop_config  # noqa: E402

CANARY_MIN_STAMPED = 0.9
HISTORY_KEEP = 20
STATUS_SUFFIX = ".status.json"


# --- files ------------------------------------------------------------------------------------------


def _read_regular(path: Path, limit: int) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise req.ParseError("not a regular file")
    with open(path, "rb") as handle:
        return handle.read(limit + 1)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_ledger(path: Path) -> list:
    events = []
    if not path.is_file():
        return events
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue  # a torn last line never blocks the tick
        if isinstance(event, dict):
            events.append(event)
    return events


def append_ledger(path: Path, event: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _stamp(now: datetime) -> str:
    return now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def unprocessed(clone: Path) -> list:
    """Request names (without ``.json``) that have no status file, oldest first."""
    folder = clone / "requests"
    if not folder.is_dir():
        return []
    names = []
    for entry in folder.iterdir():
        name = entry.name
        if not name.endswith(".json") or name.endswith(STATUS_SUFFIX):
            continue
        stem = name[: -len(".json")]
        if not req.request_name_ok(stem) or (folder / (stem + STATUS_SUFFIX)).exists():
            continue
        names.append(stem)
    return sorted(names)


# --- the apply step ----------------------------------------------------------------------------------


class ApplyIO:
    """The effects ``apply_proposal`` needs. The tick wires real ones; tests pass fakes."""

    def current_rows(self, dealer_id):  # pragma: no cover - interface
        raise NotImplementedError

    def gate(self, dealer_id, rows):  # pragma: no cover - interface
        """Dry-run validation of ``rows``: ``'ok'``, ``'uncertain'`` or ``'reject'``. Writes nothing."""
        raise NotImplementedError

    def save(self, dealer_id, rows):  # pragma: no cover - interface
        raise NotImplementedError

    def canary(self, dealer_id):  # pragma: no cover - interface
        """Rescan the dealer once: ``(verdict, stamped_ratio)``."""
        raise NotImplementedError


def apply_proposal(io, request_id: str, dealer_id: str, proposal: dict, rollback_dir: Path):
    """Apply one proposal. Returns ``(state, reason_code, detail)``.

    Order: base hash, dry-run validation, rollback file, write, canary rescan, rollback on a failed canary. Nothing is
    written before the rollback file is on disk; a failure of the write itself restores the old set too.
    """
    try:
        before = io.current_rows(dealer_id)
    except Exception as exc:  # noqa: BLE001
        return "failed", "executor_error", "cannot read the current recipes: " + type(exc).__name__
    if bundle.canonical_sha256(before) != proposal["base_canonical_sha256"]:
        return "rejected", "base_changed", "the recipe set changed since the proposal was written"
    rows = proposal["recipes"]
    try:
        verdict = io.gate(dealer_id, rows)
    except Exception as exc:  # noqa: BLE001
        return "failed", "executor_error", "validation did not run: " + type(exc).__name__
    if verdict == "reject":
        return "rejected", "validation_reject", "the validation gate rejected the proposed set"
    rollback_dir.mkdir(parents=True, exist_ok=True)
    rollback = rollback_dir / (request_id + "." + dealer_id + ".json")
    rollback.write_bytes(bundle.canonical_recipe_bytes(before) if before else b"[]\n")
    raw_before = json.loads(rollback.read_text(encoding="utf-8"))
    try:
        io.save(dealer_id, rows)
    except Exception as exc:  # noqa: BLE001
        _restore(io, dealer_id, raw_before)
        return "rolled_back", "executor_error", "the write failed and the old set was restored: " + type(exc).__name__
    try:
        canary_verdict, stamped = io.canary(dealer_id)
    except Exception as exc:  # noqa: BLE001
        _restore(io, dealer_id, raw_before)
        return "rolled_back", "canary_fail", "the canary rescan did not run: " + type(exc).__name__
    if canary_verdict not in ("ok", "uncertain") or stamped < CANARY_MIN_STAMPED:
        _restore(io, dealer_id, raw_before)
        return "rolled_back", "canary_fail", "canary verdict " + str(canary_verdict) + ", stamped " + format(stamped, ".2f")
    return "done", "", "applied; canary " + str(canary_verdict) + ", stamped " + format(stamped, ".2f")


def _restore(io, dealer_id, rows) -> None:
    try:
        io.save(dealer_id, rows)
    except Exception:  # noqa: BLE001 - the rollback file stays on the volume for the owner
        pass


# --- one tick ---------------------------------------------------------------------------------------


def _write_status(clone: Path, name: str, kind: str, state: str, code: str, detail: str, now, per_dealer=None,
                  run_id=None) -> str:
    data = req.status_json(kind, state, code, detail, finished=_stamp(now), run_id=run_id, per_dealer=per_dealer)
    (clone / "requests").mkdir(parents=True, exist_ok=True)
    (clone / "requests" / (name + STATUS_SUFFIX)).write_bytes(data)
    return _sha(data)


def _event(now, ev, name, sha, kind, dealers, state="", code="", tick="", status_sha=""):
    return {"t": _stamp(now), "ev": ev, "rq": name, "sha256": sha, "kind": kind, "dealers": list(dealers),
            "state": state, "reason_code": code, "tick": tick, "status_sha256": status_sha}


def process(clone: Path, ledger_path: Path, config, ctx: dict, executors: dict, now, rollback_dir: Path,
            tick_id: str = "") -> dict:
    """Run one tick over ``clone``. ``executors[kind](name, request, proposal_or_None)`` returns
    ``(state, reason_code, detail)``; ``apply`` goes through ``apply_proposal`` inside its executor.

    Returns ``{"rejected": n, "ran": n, "deferred": n}``. Every decision is written as a status file and a ledger line
    before the next request starts, so a crash loses at most the request that was running.
    """
    tick_id = tick_id or _stamp(now)
    events = read_ledger(ledger_path)
    finished = {e.get("rq") for e in events if e.get("ev") in ("finish", "reject")}
    started = {e.get("rq") for e in events if e.get("ev") == "start"}
    first_sha = {e.get("rq"): e.get("sha256") for e in events if e.get("ev") in ("start", "reject") and e.get("sha256")}
    counts = {"rejected": 0, "ran": 0, "deferred": 0}

    def log(event):
        events.append(event)
        append_ledger(ledger_path, event)

    names = [n for n in unprocessed(clone) if n not in finished]
    considered, overflow = req.limit_unprocessed(names)
    for name in overflow:
        _reject(clone, name, "probe", (), "rate_limited", "too many unprocessed requests", now, log, tick_id, "")
        counts["rejected"] += 1

    accepted = []
    for name in considered:
        path = clone / "requests" / (name + ".json")
        try:
            raw = _read_regular(path, MAX_REQUEST_BYTES)
        except (req.ParseError, OSError):
            raw = b""
        sha = _sha(raw)
        if name in first_sha and first_sha[name] != sha:
            continue  # the file changed after it was first recorded: ignored for good
        if name in started:  # started but never finished: the process died while it ran
            parsed_kind = "probe"
            try:
                parsed_kind = req.parse_request(raw)["kind"]
            except req.ParseError:
                pass
            _fail(clone, name, parsed_kind, "interrupted", "a tick stopped while this request ran", now, log, tick_id, sha)
            counts["rejected"] += 1
            continue
        try:
            request = req.parse_request(raw)
        except req.ParseError as exc:
            _reject(clone, name, "probe", (), "schema", str(exc), now, log, tick_id, sha)
            counts["rejected"] += 1
            continue
        verdict = req.check_request(request, ctx, config, events, now)
        if isinstance(verdict, req.Reject):
            _reject(clone, name, request["kind"], request["dealers"], verdict.code, verdict.detail, now, log, tick_id,
                    sha, verdict.per_dealer)
            counts["rejected"] += 1
        else:
            accepted.append((name, request, sha))

    by_name = {n: (r, s) for n, r, s in accepted}
    to_run, deferred = req.plan_tick([(n, r) for n, r, _ in accepted], config, events, now)
    counts["deferred"] = len(deferred)
    for name, request in to_run:
        _sha_of = by_name[name][1]
        kind, dealers = request["kind"], request["dealers"]
        log(_event(now, "start", name, _sha_of, kind, dealers, tick=tick_id))
        proposal = None
        try:
            if request["proposal"]:
                proposal = req.parse_proposal(_read_regular(clone / request["proposal"], MAX_PROPOSAL_BYTES), dealers[0])
            state, code, detail = executors[kind](name, request, proposal)
        except (req.ParseError, OSError):
            state, code, detail = "rejected", "schema", "the proposal is missing or does not parse"
        except Exception as exc:  # noqa: BLE001
            state, code, detail = "failed", "executor_error", type(exc).__name__
        status_sha = _write_status(clone, name, kind, state, code, detail, now)
        log(_event(now, "finish", name, _sha_of, kind, dealers, state, code, tick_id, status_sha))
        counts["ran"] += 1
    write_fix_cycles(clone, events, config, now)
    return counts


def _reject(clone, name, kind, dealers, code, detail, now, log, tick_id, sha, per_dealer=None):
    status_sha = _write_status(clone, name, kind, "rejected", code, detail, now, per_dealer)
    log(_event(now, "reject", name, sha, kind, dealers, "rejected", code, tick_id, status_sha))


def _fail(clone, name, kind, code, detail, now, log, tick_id, sha):
    status_sha = _write_status(clone, name, kind, "failed", code, detail, now)
    log(_event(now, "finish", name, sha, kind, (), "failed", code, tick_id, status_sha))


def write_fix_cycles(clone: Path, events: list, config, now) -> None:
    """Rewrite ``fix_cycles/<dealer>.json`` from the ledger: attempts, state and the last 20 events of each dealer."""
    view = req.ledger_view(events, config, now)
    history: dict = {}
    for event in events:
        if event.get("ev") in ("finish", "reject") and event.get("kind"):
            for dealer in event.get("dealers") or []:
                history.setdefault(dealer, []).append({
                    "request": event.get("rq"), "kind": event.get("kind"), "state": event.get("state"),
                    "reason_code": event.get("reason_code"), "at": event.get("t")})
    folder = clone / "fix_cycles"
    for dealer, rows in history.items():
        attempts = view["attempts"].get(dealer, 0)
        state = "escalated" if dealer in view["escalated"] else ("open" if attempts else "open")
        body = {"schema": 1, "dealer_id": dealer, "attempts": attempts, "state": state,
                "history": rows[-HISTORY_KEEP:]}
        folder.mkdir(parents=True, exist_ok=True)
        (folder / (dealer + ".json")).write_text(json.dumps(body, indent=1, sort_keys=True) + "\n", encoding="utf-8")


# --- wiring -----------------------------------------------------------------------------------------


def build_executors(config, io, probe_rescan, rollback_dir: Path) -> dict:
    """The four executors. ``probe_rescan(kind, dealers)`` returns ``(ok, detail)``. ``apply`` exists only when the
    config lists it; the tick never builds an executor for a kind the owner has not switched on."""

    def run_scan(name, request, proposal):
        ok, detail = probe_rescan(request["kind"], request["dealers"])
        return ("done", "", detail) if ok else ("failed", "executor_error", detail)

    def validate(name, request, proposal):
        dealer = request["dealers"][0]
        verdict = io.gate(dealer, proposal["recipes"])
        if verdict == "reject":
            return "rejected", "validation_reject", "the validation gate rejected the proposed set"
        return "done", "", "validation verdict " + str(verdict)

    def apply(name, request, proposal):
        return apply_proposal(io, name, request["dealers"][0], proposal, rollback_dir)

    out = {"probe": run_scan, "rescan": run_scan, "validate": validate}
    if "apply" in config.kinds:
        out["apply"] = apply
    return out


TEMPLATE_CONFIG = Path(__file__).resolve().parents[2] / "docs" / "scan_loop" / "data_template" / "config" / "loop.json"


def work_dir(env) -> Path:
    return Path(env.get("LOOP_WORK_DIR") or (Path(env.get("SCAN_DATA_DIR") or "/data") / "loop"))


def seed_config(store) -> None:
    """Put the template ``config/loop.json`` (``apply`` off) into the store when it has none."""
    if store.get("config/loop.json") is None and TEMPLATE_CONFIG.is_file():
        store.put("config/loop.json", TEMPLATE_CONFIG.read_bytes(), "railway")


def sync_down(store, clone: Path) -> None:
    """Write the requests, proposals and config of the store under ``clone`` (replacing what was there)."""
    import shutil

    for folder in ("requests", "proposals", "config"):
        shutil.rmtree(clone / folder, ignore_errors=True)
    for prefix in ("requests/", "proposals/", "config/"):
        for entry in store.list(prefix):
            data = store.get(entry.path)
            if data is not None:
                target = clone / entry.path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)


def sync_up(store, clone: Path) -> int:
    """Store the tick's statuses and ``fix_cycles/`` files; returns how many changed."""
    changed = 0
    paths = sorted((clone / "requests").glob("*" + STATUS_SUFFIX)) + sorted((clone / "fix_cycles").glob("*.json"))
    for file in paths:
        if file.is_file() and not file.is_symlink() and store.put(file.relative_to(clone).as_posix(), file.read_bytes(), "railway"):
            changed += 1
    return changed


def main(argv=None, env=None) -> int:
    env = os.environ if env is None else env
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--now", help="ISO time (tests)")
    args = parser.parse_args(argv)
    from backend.scripts import ops_publisher as pub

    if not pub.configured(env):
        print("ops_tick: LOOP_STORE is not set to db", file=sys.stderr)
        return 2
    now = datetime.fromisoformat(args.now) if args.now else datetime.now(timezone.utc)
    store = pub.open_store(env)
    seed_config(store)
    base = work_dir(env)
    clone = base / "tree"
    sync_down(store, clone)
    config_path = clone / "config" / "loop.json"
    config = load_loop_config(config_path.read_bytes() if config_path.is_file() else None)
    if config.idle:
        print("ops_tick: loop is idle (config/loop.json missing, invalid or disabled)")
        return 0
    from backend.scripts import ops_tick_wiring as wiring  # real effects: store, gate, pipeline

    io, probe_rescan, ctx = wiring.build(env, config, clone)
    executors = build_executors(config, io, probe_rescan, base / "rollback")
    counts = process(clone, base / "ledger.jsonl", config, ctx, executors, now, base / "rollback")
    counts["stored"] = sync_up(store, clone)
    print("ops_tick: " + json.dumps(counts, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
