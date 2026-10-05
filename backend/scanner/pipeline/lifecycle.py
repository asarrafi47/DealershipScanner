"""Step 5: the recipe lifecycle (route_verdict -> re-synth -> discovery capture -> retry). Moved verbatim from backend/scripts/dealer_pipeline.py (audit F11)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.scanner.pipeline.assess import assess
from backend.scanner.pipeline.constants import LOG_ROOT
from backend.scanner.pipeline.db import _assess_conn, _conn_alive
from backend.scanner.pipeline.dealer_logs import _log_append, log_scan_run, write_instructions_if_first_success
from backend.scanner.pipeline.reconcile import reconcile_dealer
from backend.scanner.pipeline.recipes import ensure_recipe
from backend.scanner.pipeline.runner import run_discovery_capture, scan_retry_batch
from backend.scanner.pipeline.vpic import vpic_for_dealers


# --------------------------------------------------------------------------
# 5. recipe lifecycle (docs/HTTP_ONLY_SCANS_PLAN.md Phase 3)
#
#   verdict / recipe_status  ->  route_verdict  ->  step 1 force re-synth + validation
#                                               ->  step 2 discovery capture (own process,
#                                                   the one sanctioned browser) + validation
#                                               ->  step 3 retry batch (HTTP-only) + assess
#
# Runs AFTER the main batches so it never delays the fleet; one attempt per
# dealer per UTC day (scan_hints.lifecycle_last_attempt); --no-lifecycle turns
# it off. Every step writes its block to discovery.md and its failure class to
# _learning/errors_index.md; a dealer still failing at the end of the run goes
# to <out>/needs_discovery.txt for the dealer-discovery workflow.
# --------------------------------------------------------------------------

LIFECYCLE_VERDICTS = ("no_recipe", "no_rows")
# Verdicts under which a scan produced rows the assess could judge: the recipe on
# file replays. A ``rejected:`` status beside one of these describes a re-synth
# that lost to the working recipe, not a dealer without one.
SCANNED_VERDICTS = ("ok", "thin", "inaccurate")
FAILING_VERDICTS = ("no_recipe", "no_rows", "error")
LIFECYCLE_OK = ("resynth_ok", "capture_ok")
_AUTH_ERROR_MARKERS = ("401", "403", "auth", "forbidden", "unauthori")
_NO_URL_SYNTHS = ("not_in_manifest_or_db",)


def _scan_hints(dealer_id: str) -> dict[str, Any]:
    try:
        from backend.scanner.recipe_store import get_scan_hints

        return dict(get_scan_hints(dealer_id) or {})
    except Exception:  # noqa: BLE001 - the hint store is advisory
        return {}


def _set_scan_hints(dealer_id: str, hints: dict[str, Any]) -> bool:
    try:
        from backend.scanner.recipe_store import set_scan_hints

        return bool(set_scan_hints(dealer_id, hints))
    except Exception:  # noqa: BLE001
        return False


def _learning_append(name: str, line: str) -> None:
    try:
        d = LOG_ROOT / "_learning"
        d.mkdir(parents=True, exist_ok=True)
        with (d / name).open("a", encoding="utf-8") as fh:
            fh.write(line.rstrip() + "\n")
    except OSError as exc:
        print(f"log     _learning/{name}: {str(exc)[:80]}", flush=True)


def route_verdict(dealer: dict[str, Any], result: dict[str, Any], recipe_status: str | None = None) -> dict[str, Any]:
    """Decide whether an assessed dealer enters the recipe lifecycle.

    ``{"action": "lifecycle" | "none", "trigger": <why>}``. Enters on the verdicts
    ``no_recipe`` / ``no_rows``, a ``validated_zero`` synthesis, an ``error``
    whose reason smells of auth (401/403/forbidden), any dealer whose
    ``scan_hints.recipe_status`` starts with ``stale:`` (a replay answered
    401/403), and a ``rejected:`` status (the validator refused the last
    synthesis) only when the scan itself failed (``no_recipe`` / ``no_rows`` /
    ``error``). ``thin`` / ``inaccurate`` / ``ok`` stay out, ``rejected:`` or
    not: their recipe replays; those are parser and attribution problems, not
    recipe problems, and a daily browser capture would learn nothing new.
    """
    did = str(dealer.get("dealer_id") or result.get("dealer_id") or "")
    verdict = str(result.get("verdict") or "")
    reason = str(result.get("reason") or "")
    info = result.get("recipe") or {}
    synth = str(info.get("synth") or "")
    if not dealer.get("url") or synth in _NO_URL_SYNTHS:
        return {"action": "none", "trigger": "no_url", "verdict": verdict}
    if recipe_status is None:
        recipe_status = str(_scan_hints(did).get("recipe_status") or "")
    recipe_status = str(recipe_status or "")
    from backend.scanner.recipes import blocked_on_this_host

    if blocked_on_this_host(recipe_status):
        # This host's IP is refused (SCANNER_EGRESS_TAG); re-synthesis from here
        # would only 403 again and a browser capture is not available. A home
        # scanner still owns the dealer.
        return {"action": "none", "trigger": f"blocked_on_this_host ({recipe_status})", "verdict": verdict}
    if recipe_status.startswith("stale:"):
        return {"action": "lifecycle", "trigger": f"recipe_status {recipe_status}", "verdict": verdict}
    if recipe_status.startswith("rejected:"):
        if verdict in FAILING_VERDICTS:
            return {"action": "lifecycle", "trigger": f"recipe_status {recipe_status}", "verdict": verdict}
        if verdict in SCANNED_VERDICTS:
            return {"action": "none", "trigger": f"{verdict} (recipe_status {recipe_status} ignored: the recipe on file replays)",
                    "verdict": verdict}
    if verdict in LIFECYCLE_VERDICTS:
        return {"action": "lifecycle", "trigger": verdict, "verdict": verdict}
    if synth.startswith("validated_zero"):
        return {"action": "lifecycle", "trigger": "validated_zero", "verdict": verdict}
    if verdict == "error" and any(m in reason.lower() for m in _AUTH_ERROR_MARKERS):
        return {"action": "lifecycle", "trigger": "error_auth", "verdict": verdict}
    return {"action": "none", "trigger": verdict or "unassessed", "verdict": verdict}


def lifecycle_attempted_today(dealer_id: str, hints: dict[str, Any] | None = None, now: datetime | None = None) -> bool:
    """The once-per-day guard: ``scan_hints.lifecycle_last_attempt`` on today's UTC date."""
    hints = hints if hints is not None else _scan_hints(dealer_id)
    last = str(hints.get("lifecycle_last_attempt") or "")
    if not last:
        return False
    try:
        ts = datetime.fromisoformat(last.replace("Z", "+00:00"))
    except ValueError:
        return False
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    return ts.astimezone(timezone.utc).date() == now.astimezone(timezone.utc).date()


def validate_live_recipes(dealer: dict[str, Any], context: str = "lifecycle capture"):
    """Re-validate the recipes now on file for *dealer* over HTTP (page 1 + 2
    against the site's own count); the report lands in discovery.md and
    scan_hints.recipe_status. ``None`` when nothing live is on file."""
    from urllib.parse import urlparse

    from backend.scanner.recipe_validation import record_recipe_status, validate_recipe_set, write_discovery_log
    from backend.scanner.recipes import load_recipes

    did = dealer["dealer_id"]
    live = [r for r in load_recipes(did) if not r.stale]
    if not live:
        return None
    u = urlparse(str(dealer.get("url") or ""))
    origin = f"{u.scheme}://{u.netloc}" if u.netloc else ""
    place = None
    try:
        from backend.scanner.dealer_place import roster_place_with_hints

        place = roster_place_with_hints(dealer.get("url") or "", did) or None
    except Exception:  # noqa: BLE001
        place = None
    rep = validate_recipe_set(did, live, base_url=origin, dealer_name=dealer.get("name") or did, place=place)
    write_discovery_log(did, rep, context, LOG_ROOT)
    record_recipe_status(did, rep, context)
    return rep


def _lifecycle_block(stamp: str, step: str, trigger: str, lines: list[str], verdict: str) -> str:
    return "\n".join([f"## {stamp} lifecycle {step} (trigger: {trigger})"] + [f"- {ln}" for ln in lines] + [f"- verdict: {verdict}"])


def run_lifecycle(dealer: dict[str, Any], result: dict[str, Any], *, trigger: str = "", no_discover: bool = False,
                  stamp: str = "", now: datetime | None = None) -> dict[str, Any]:
    """Steps 1 and 2 for one dealer. Returns ``{"lifecycle": resynth_ok | capture_ok |
    failed:<reason>, "steps": [...], "recipe": <ensure_recipe info when saved>}``.
    Step 3 (the retry batch) is :func:`run_lifecycle_pass`'s job, since it batches."""
    did = dealer["dealer_id"]
    now = now or datetime.now(timezone.utc)
    stamp = stamp or now.strftime("%Y-%m-%d %H:%M UTC")
    trigger = trigger or str(result.get("verdict") or "")
    out: dict[str, Any] = {"lifecycle": "none", "trigger": trigger, "steps": []}
    hints = _scan_hints(did)
    if lifecycle_attempted_today(did, hints, now):
        out["lifecycle"] = "failed:attempted_today"
        out["last_attempt"] = hints.get("lifecycle_last_attempt")
        _log_append(did, "discovery.md", _lifecycle_block(
            stamp, "skipped", trigger,
            [f"last attempt: {hints.get('lifecycle_last_attempt')} (one lifecycle attempt per dealer per UTC day)",
             f"last verdict: {result.get('verdict')} ({str(result.get('reason') or '')[:120]})"],
            "not retried today; still needs discovery"))
        return out
    _set_scan_hints(did, {"lifecycle_last_attempt": now.replace(microsecond=0).isoformat()})

    # step 1 — force re-synth through the platform templates + the validator gate
    try:
        info = ensure_recipe(dealer, force=True)
    except Exception as exc:  # noqa: BLE001
        info = {"had_recipes": 0, "synth": f"error:{str(exc)[:100]}"}
    synth = str(info.get("synth") or "")
    step1_ok = synth.startswith("saved")
    out["steps"].append({"step": "resynth", "ok": step1_ok, "synth": synth, "platform": info.get("platform"),
                         "recipe_status": info.get("recipe_status"), "validation": info.get("validation")})
    lines = [f"url: {dealer.get('url')}", f"last verdict: {result.get('verdict')} ({str(result.get('reason') or '')[:120]})",
             f"fingerprint: {info.get('platform') or 'unknown'}", f"synthesis: {synth}"
             + (f", {info.get('synth_vins')} VINs validated" if info.get("synth_vins") else "")]
    if info.get("validation"):
        lines.append(f"validation: {json.dumps(info['validation'], default=str)[:400]}")
    if info.get("traceback"):
        lines.append("traceback:\n```\n" + info["traceback"] + "\n```")
    if step1_ok:
        out["lifecycle"], out["recipe"] = "resynth_ok", info
        _log_append(did, "discovery.md", _lifecycle_block(stamp, "step 1 — force re-synth", trigger, lines,
                                                          "recipe validated; queued for this run's retry batch (HTTP-only scan)"))
        return out
    fail1 = synth or "no_template"
    verdict = str(result.get("verdict") or "")
    if verdict in SCANNED_VERDICTS:
        # The scan produced rows from the recipe on file (a stale: status the
        # replay could not clear, or a rejected: re-synth beside a working
        # recipe): a browser capture would only re-learn the endpoint that
        # already replays. Not a needs_discovery outcome.
        out["lifecycle"] = f"resynth_failed:{fail1[:60]}"
        _log_append(did, "discovery.md", _lifecycle_block(stamp, "step 1 — force re-synth", trigger, lines,
                                                          f"failed ({fail1}); scan verdict {verdict}: the recipe on file replays, no discovery capture"))
        return out
    _log_append(did, "discovery.md", _lifecycle_block(stamp, "step 1 — force re-synth", trigger, lines,
                                                      f"failed ({fail1}); next: discovery capture" if not no_discover else f"failed ({fail1}); discovery capture disabled (--no-discover)"))
    _learning_append("errors_index.md", f"- {stamp} lifecycle_resynth_failed:{fail1[:60]} -> {did} (trigger {trigger}; workspace/dealer_logs/{did}/discovery.md)")
    if no_discover:
        out["lifecycle"] = f"failed:{fail1[:60]}"
        return out

    # step 2 — discovery capture in its own process (SCANNER_ALLOW_BROWSER lives there only)
    try:
        cap = run_discovery_capture(did)
    except Exception as exc:  # noqa: BLE001
        cap = {"error": f"capture failed: {str(exc)[:100]}", "recipes_after": 0}
    n_after = int(cap.get("recipes_after") or 0)
    step2: dict[str, Any] = {"step": "capture", "ok": False, "recipes_after": n_after,
                             **{k: cap.get(k) for k in ("skipped", "error", "records", "endpoints", "profile", "errors", "validation", "seconds") if cap.get(k) is not None}}
    lines = [f"capture: {json.dumps({k: v for k, v in cap.items() if k != 'stdout_tail'}, default=str)[:500]}"]
    if cap.get("skipped"):
        fail2 = "capture_skipped_today"
    elif cap.get("error"):
        fail2 = "capture_error"
    elif n_after <= 0:
        fail2 = "capture_no_recipes"
    else:
        try:
            rep = validate_live_recipes(dealer)
        except Exception as exc:  # noqa: BLE001
            rep = None
            lines.append(f"validation error: {str(exc)[:120]}")
        if rep is None:
            fail2 = "capture_no_live_recipes"
        else:
            step2["validation"] = {**rep.summary(), "status": rep.status}
            lines.append(f"validation: {rep.verdict} {'; '.join(rep.reasons)[:200]} (VINs {rep.vins_total}, site total {rep.site_total})")
            fail2 = "" if rep.verdict != "reject" else f"capture_validation_{rep.status}"
    step2["ok"] = not fail2
    out["steps"].append(step2)
    if not fail2:
        out["lifecycle"] = "capture_ok"
        _log_append(did, "discovery.md", _lifecycle_block(stamp, "step 2 — discovery capture", trigger, lines,
                                                          "captured recipe validated; queued for this run's retry batch (HTTP-only scan)"))
        return out
    out["lifecycle"] = f"failed:{fail2[:60]}"
    _log_append(did, "discovery.md", _lifecycle_block(stamp, "step 2 — discovery capture", trigger, lines,
                                                      f"failed ({fail2}); needs discovery (see <out>/needs_discovery.txt)"))
    _learning_append("errors_index.md", f"- {stamp} lifecycle_capture_failed:{fail2[:60]} -> {did} (trigger {trigger}; workspace/dealer_logs/{did}/discovery.md)")
    return out


def run_lifecycle_pass(results: list[dict[str, Any]], dealers: dict[str, dict[str, Any]], known: dict[str, int], *,
                       out_dir: Path, stamp: str, no_discover: bool = False, batch: int = 4, scan_timeout: int = 3600,
                       lock_wait: int = 5400, no_vpic: bool = False, no_reconcile: bool = False,
                       now: datetime | None = None) -> dict[str, Any]:
    """Route every assessed dealer, run the lifecycle for the ones that need it,
    rescan the dealers whose recipe now validates in one final retry batch and
    assess them again. Mutates *results* in place (each row gains ``lifecycle``,
    ``lifecycle_detail`` and, when rescanned, ``retried``). Returns the summary."""
    now = now or datetime.now(timezone.utc)
    retry: list[str] = []
    summary: dict[str, Any] = {"routed": 0, "resynth_ok": 0, "capture_ok": 0, "failed": 0, "resynth_failed": 0, "retried": {}, "chromium_leaks": []}
    for r in results:
        did = r["dealer_id"]
        r.setdefault("lifecycle", "none")
        d = dealers.get(did) or {"dealer_id": did, "url": ""}
        route = route_verdict(d, r)
        r["lifecycle_route"] = route
        if route["action"] != "lifecycle":
            continue
        summary["routed"] += 1
        lc = run_lifecycle(d, r, trigger=route["trigger"], no_discover=no_discover, stamp=stamp, now=now)
        r["lifecycle"], r["lifecycle_detail"] = lc["lifecycle"], lc
        if lc.get("recipe"):
            r["recipe"] = {**(r.get("recipe") or {}), **lc["recipe"], "lifecycle": lc["lifecycle"]}
        print(f"lifecyc {did:36s} {route['trigger'][:40]:40s} -> {lc['lifecycle']}", flush=True)
        if lc["lifecycle"] in LIFECYCLE_OK:
            summary[lc["lifecycle"]] += 1
            retry.append(did)
        elif lc["lifecycle"].startswith("resynth_failed:"):
            summary["resynth_failed"] += 1  # the scan passed; the recipe on file stays, no capture
        else:
            summary["failed"] += 1
    if not retry:
        return summary
    # The retry's scan_runs rows are the ones finished from here on.
    retry_since = datetime.now(timezone.utc).replace(microsecond=0)
    print(f"retry   {len(retry)} dealer(s) with a validated recipe: {', '.join(retry)[:200]}", flush=True)
    scanned = scan_retry_batch(retry, out_dir=out_dir, batch=batch, scan_timeout=scan_timeout, lock_wait=lock_wait)
    summary["chromium_leaks"] = scanned["chromium_leaks"]
    if not no_vpic:
        try:
            vp = vpic_for_dealers(retry)
            print(f"vpic    retry {json.dumps(vp)[:200]}", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"vpic    retry failed: {str(exc)[:120]}", flush=True)
    conn = _assess_conn()
    try:
        for idx, r in enumerate(results):
            did = r["dealer_id"]
            if did not in retry:
                continue
            if not _conn_alive(conn):
                conn = _assess_conn()
            old = str(r.get("verdict"))
            info = dict(r.get("recipe") or {})
            info["had_recipes"] = info.get("had_recipes") or 1
            r2 = assess(conn, did, retry_since.isoformat(), known.get(did, 0), info)
            try:
                # stamped rows, never the raw feed count (same rule as the main pass)
                rows_kept2 = int(r2.get("rows_stamped") if r2.get("rows_stamped") is not None else (r2.get("rows") or 0))
                r2["reconcile"] = reconcile_dealer(conn, did, retry_since.isoformat(), known.get(did, 0), rows_kept2,
                                                   str(r2.get("verdict")), dry_run=no_reconcile)
            except Exception as exc:  # noqa: BLE001
                r2["reconcile"] = {"error": str(exc)[:120]}
            for k in ("lifecycle", "lifecycle_detail", "lifecycle_route"):
                if k in r:
                    r2[k] = r[k]
            r2["retried"] = f"retried: {old} → {r2.get('verdict')}"
            r2["previous_verdict"] = old
            summary["retried"][did] = r2["retried"]
            results[idx] = r2
            try:
                log_scan_run(r2, stamp + " (lifecycle retry)")
                write_instructions_if_first_success(r2, stamp)
                _log_append(did, "discovery.md", _lifecycle_block(stamp, "step 3 — retry scan", str(r.get("lifecycle_route", {}).get("trigger") or old),
                                                                  [f"rows: {r2.get('rows', '-')} (new {r2.get('rows_new', '-')}, used {r2.get('rows_used', '-')}); before: {r2.get('known_before')}",
                                                                   f"reason: {str(r2.get('reason') or '')[:160]}"],
                                                                  r2["retried"]))
            except Exception as exc:  # noqa: BLE001
                print(f"log     retry {did}: {str(exc)[:80]}", flush=True)
            print(f"retry   {did:36s} {r2['retried']}", flush=True)
    finally:
        conn.close()
    return summary
