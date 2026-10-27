"""Step 1: make sure a dealer has a non-stale recipe. Moved verbatim from backend/scripts/dealer_pipeline.py (audit F11)."""
from __future__ import annotations

import time
from typing import Any


# --------------------------------------------------------------------------
# 1. recipe
# --------------------------------------------------------------------------

def ensure_recipe(dealer: dict[str, Any], *, force: bool = False) -> dict[str, Any]:
    from backend.scanner.recipes import load_recipes, replace_recipes
    from backend.scanner.recipe_synth import fetch_dealer_html, fingerprint_platform, synthesize_recipes, validate_recipe

    did = dealer["dealer_id"]
    existing = load_recipes(did)
    live = [r for r in existing if not r.stale]
    info: dict[str, Any] = {
        "had_recipes": len(live),
        "providers": sorted({r.provider_hint for r in live if r.provider_hint}),
        "synth": None,
    }
    # A cosmos store with a single recipe predates the per-section synth (used and new
    # have separate pageIds): re-synthesize so both conditions are collected.
    from backend.scanner.recipes import recipe_is_section_scoped

    # A captured carscommerce recipe pinned to one SRP section (type_slug / make /
    # model_slug facets) replays a fraction of the lot: 16 no_rows verdicts on
    # 2026-09-24. The synth builds the whole-lot body (+ store filter), so rebuild.
    section_scoped = bool(live) and all(recipe_is_section_scoped(r) for r in live)
    cosmos_single = len(live) == 1 and live[0].provider_hint == "dealer_on_cosmos"
    if live and not force and not cosmos_single and not section_scoped:
        return info
    if live and not force:
        info["resynth"] = "cc_section_scoped" if section_scoped else "cosmos_single_section"
    html = fetch_dealer_html(dealer["url"])
    if not html:
        status = None
        try:
            from backend.scanner.net import client as net_client

            # STATUS_PROBE: one 'chrome' GET through the scanner proxy; only the status is read.
            status = net_client.send(
                net_client.import_curl_cffi(), "GET", dealer["url"], via_get=True,
                impersonate="chrome", timeout=net_client.STATUS_PROBE.timeout,
                proxies=net_client.scanner_proxies(),
            ).status_code
        except Exception:  # noqa: BLE001
            pass
        info["synth"] = f"homepage_unreachable_{status or 'exc'}"
        return info
    platform = fingerprint_platform(html, dealer["url"])
    info["platform"] = platform
    try:
        from backend.scanner.dealer_place import learn_place, place_kwargs

        place_full = learn_place(did, dealer["url"], html)
        place = place_kwargs(place_full)
        if place:
            info["place"] = f"{place.get('dealer_city')}, {place.get('dealer_state')} ({place_full.get('place_source')})"
    except Exception:  # noqa: BLE001
        place = {}
    cands = synthesize_recipes(did, dealer["url"], html, platform)
    if not cands:
        info["synth"] = f"no_template_for_{platform or 'unknown'}"
        return info
    # One full walk per candidate (recipe_validation.walk_candidates); the gate below
    # judges those cached reads, and vehicle_rows is that walk's count.
    from backend.scanner.recipe_validation import walk_candidates

    base = dealer["url"].rstrip("/")
    try:
        walked = walk_candidates(did, cands, base_url=base, dealer_name=dealer.get("name") or did, place=place or None)
    except Exception as exc:  # noqa: BLE001
        info["synth"] = f"validate_error:{str(exc)[:80]}"
        return info
    kept = []
    for c, chk, *_ in walked.walks:
        n = chk.vins
        if n and n >= 5:
            c.vehicle_rows = int(n)
            c.last_ok_at = time.time()
            kept.append(c)
    if not kept:
        info["synth"] = info.get("synth") or "validated_zero"
        return info
    # The SET is judged against the site's own count before the save
    # (recipe_validation, Phase 3): one condition while the site sells both,
    # section-scoped bodies, a short page under half the lot, dead auth. A
    # reject is logged to discovery.md and nothing is saved; uncertain saves
    # with scan_hints.recipe_status = "uncertain:<reason>".
    from backend.scanner.recipe_validation import gate_recipes

    kept, report = gate_recipes(did, kept, base_url=base, dealer_name=dealer.get("name") or did,
                                place=place or None, context="synth", checks=walked.keep(kept))
    info["recipe_status"] = report.status
    info["validation"] = report.summary()
    if not kept:
        info["synth"] = report.status
        return info
    # Replace with archive (D-RS2): what the new set replaces moves to superseded_json.
    # A forced run replaces the whole set; a non-force run supersedes only the class
    # that triggered it and leaves the other recipes live.
    if force or not live:
        reason, final = ("synth:force" if force else "synth:no_live"), kept
    else:
        reason = "synth:cc_section_scoped" if section_scoped else "synth:cosmos_single_section"
        trigger = recipe_is_section_scoped if section_scoped else (lambda r: r.provider_hint == "dealer_on_cosmos")
        won = {c.key() for c in kept}
        final = kept + [r for r in existing if not trigger(r) and r.key() not in won]
    replace_recipes(did, final, reason=reason)
    info["synth"] = f"saved_{len(kept)}"
    info["providers"] = sorted({r.provider_hint for r in kept if r.provider_hint})
    info["synth_vins"] = sum(r.vehicle_rows for r in kept)
    return info
